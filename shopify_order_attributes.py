#!/usr/bin/env python3
"""
Terra Health Essentials — Shopify Order Attributes → BigQuery
=============================================================
Pulls customAttributes from all orders via Bulk API and flattens
terra_ctx + all terra keys into a dedicated table.

Table produced:
  terra-analytics-prod.sources.shopify_order_attributes

Run:
  python shopify_order_attributes.py
  BQ_PROJECT=terra-analytics-dev python shopify_order_attributes.py

Requirements:
  pip install requests google-cloud-bigquery
"""

import os, sys, json, time, traceback
import requests
from requests.adapters import HTTPAdapter, Retry
from google.cloud import bigquery
import configparser

_cfg = configparser.ConfigParser()
_cfg.read(os.path.join(os.path.dirname(__file__), "config.ini"))
_shopify = _cfg["shopify"] if _cfg.has_section("shopify") else {}

SHOPIFY_STORE = os.environ.get("SHOPIFY_STORE") or _shopify.get("store", "")
ACCESS_TOKEN  = os.environ.get("SHOPIFY_ACCESS_TOKEN") or _shopify.get("access_token", "")
API_VERSION   = os.environ.get("SHOPIFY_API_VERSION") or _shopify.get("api_version", "2026-01")
BQ_PROJECT    = os.environ.get("BQ_PROJECT", "terra-analytics-prod")
BQ_DATASET    = "sources"
GRAPHQL_URL   = f"https://{SHOPIFY_STORE}/admin/api/{API_VERSION}/graphql.json"

session = requests.Session()
session.headers.update({"X-Shopify-Access-Token": ACCESS_TOKEN, "Content-Type": "application/json"})
session.mount("https://", HTTPAdapter(max_retries=Retry(total=5, backoff_factor=2, status_forcelist=[429,500,502,503,504])))

bq = bigquery.Client(project=BQ_PROJECT)

# ── Schema ────────────────────────────────────────────────────────────────────
SF = bigquery.SchemaField

SCHEMA = [
    SF("order_id",                    "STRING"),
    SF("order_name",                  "STRING"),
    SF("created_at",                  "TIMESTAMP"),
    SF("terra_ctx",                   "STRING"),
    SF("ctx_id",                      "STRING"),
    SF("ctx_version",                 "STRING"),
    SF("th_vid",                      "STRING"),
    SF("session_key",                 "STRING"),
    SF("session_start",               "STRING"),
    SF("terra_ft_source",             "STRING"),
    SF("terra_ft_medium",             "STRING"),
    SF("terra_ft_campaign",           "STRING"),
    SF("terra_ft_content",            "STRING"),
    SF("terra_ft_term",               "STRING"),
    SF("terra_ft_id",                 "STRING"),
    SF("terra_ft_ad_id",              "STRING"),
    SF("terra_lt_source",             "STRING"),
    SF("terra_lt_medium",             "STRING"),
    SF("terra_lt_campaign",           "STRING"),
    SF("terra_lt_content",            "STRING"),
    SF("terra_lt_term",               "STRING"),
    SF("terra_lt_id",                 "STRING"),
    SF("terra_lt_ad_id",              "STRING"),
    SF("terra_fbclid",                "STRING"),
    SF("terra_ga_cid",                "STRING"),
    SF("terra_ga_sid",                "STRING"),
    SF("terra_ga_sn",                 "STRING"),
    SF("terra_session_landing_page",  "STRING"),
    SF("terra_session_referrer_host", "STRING"),
    SF("edge_delivery_visitor_id",    "STRING"),
    SF("edge_delivery_session_id",    "STRING"),
    SF("edge_delivery_experiment_id", "STRING"),
    SF("metorik_utm_source",          "STRING"),
    SF("metorik_utm_medium",          "STRING"),
    SF("metorik_utm_campaign",        "STRING"),
    SF("metorik_utm_content",         "STRING"),
    SF("metorik_utm_term",            "STRING"),
    SF("metorik_utm_id",              "STRING"),
    SF("raw_attributes",              "STRING"),
]

# ── Bulk Query ────────────────────────────────────────────────────────────────
BULK_QUERY = """{
  orders {
    edges {
      node {
        id
        name
        createdAt
        customAttributes {
          key
          value
        }
      }
    }
  }
}"""

# ── Bulk Op Runner ────────────────────────────────────────────────────────────
def cancel_existing():
    poll = "{ currentBulkOperation { id status } }"
    result = session.post(GRAPHQL_URL, json={"query": poll}, timeout=30).json()
    op = result.get("data", {}).get("currentBulkOperation")
    if op and op["status"] in ("CREATED", "RUNNING"):
        print(f"  Canceling existing operation {op['id']}...")
        cancel = f'mutation {{ bulkOperationCancel(id: "{op["id"]}") {{ bulkOperation {{ id status }} }} }}'
        session.post(GRAPHQL_URL, json={"query": cancel}, timeout=30)
        time.sleep(5)

def run_bulk():
    cancel_existing()
    mutation = 'mutation { bulkOperationRunQuery(query: """' + BULK_QUERY + '""") { bulkOperation { id status } userErrors { field message } } }'
    resp = session.post(GRAPHQL_URL, json={"query": mutation}, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    errors = (data.get("data") or {}).get("bulkOperationRunQuery", {}).get("userErrors", [])
    if errors:
        raise Exception(f"Bulk operation errors: {errors}")

    poll = "{ currentBulkOperation { id status errorCode objectCount fileSize url } }"
    start = time.time()
    while True:
        time.sleep(10)
        op = session.post(GRAPHQL_URL, json={"query": poll}, timeout=30).json()["data"]["currentBulkOperation"]
        elapsed = int(time.time() - start)
        count = int(op.get("objectCount") or 0)
        mb = int(op.get("fileSize") or 0) / 1024 / 1024
        print(f"\r  {op['status']} — {count:,} objects — {mb:.1f} MB — {elapsed}s", end="", flush=True)
        if op["status"] == "COMPLETED":
            print()
            return op["url"]
        elif op["status"] in ("FAILED", "CANCELED"):
            raise Exception(f"Bulk operation {op['status']}: {op.get('errorCode')}")

def download_jsonl(url):
    print("  Downloading...")
    resp = requests.get(url, stream=True, timeout=600)
    resp.raise_for_status()
    rows = [json.loads(line) for line in resp.iter_lines() if line]
    print(f"  {len(rows):,} objects")
    return rows

# ── Parser ────────────────────────────────────────────────────────────────────
def s(v):
    return str(v) if v is not None else None

def parse_objects(objects):
    def g(attrs, key):
        return attrs.get(key)

    def parse_ctx(attrs):
        raw = attrs.get("terra_ctx")
        if not raw:
            return {}
        try:
            return json.loads(raw)
        except Exception:
            return {}

    rows = []
    for obj in objects:
        if "__parentId" in obj or "name" not in obj:
            continue

        order    = obj
        order_id = obj["id"]

        attrs = {}
        for attr in (obj.get("customAttributes") or []):
            k = attr.get("key")
            v = attr.get("value")
            if k:
                attrs[k] = v

        ctx   = parse_ctx(attrs)
        ft    = ctx.get("ft")    or {}
        lt    = ctx.get("lt")    or {}
        ga    = ctx.get("ga")    or {}
        click = ctx.get("click") or {}

        rows.append({
            "order_id":    s(order_id).split("/")[-1],
            "order_name":  order.get("name"),
            "created_at":  order.get("createdAt"),
            "terra_ctx":   g(attrs, "terra_ctx"),
            "ctx_id":      g(attrs, "ctx_id")      or ctx.get("ctx_id"),
            "ctx_version": g(attrs, "ctx_version") or ctx.get("ctx_version"),
            "th_vid":        g(attrs, "th_vid")        or ctx.get("th_vid"),
            "session_key":   g(attrs, "session_key")   or ctx.get("session_key"),
            "session_start": g(attrs, "session_start") or ctx.get("session_start"),
            "terra_ft_source":   g(attrs, "terra_ft_source")   or ft.get("source"),
            "terra_ft_medium":   g(attrs, "terra_ft_medium")   or ft.get("medium"),
            "terra_ft_campaign": g(attrs, "terra_ft_campaign") or ft.get("campaign"),
            "terra_ft_content":  g(attrs, "terra_ft_content")  or ft.get("content"),
            "terra_ft_term":     g(attrs, "terra_ft_term")     or ft.get("term"),
            "terra_ft_id":       g(attrs, "terra_ft_id")       or ft.get("id"),
            "terra_ft_ad_id":    g(attrs, "terra_ft_ad_id")    or ft.get("ad_id"),
            "terra_lt_source":   g(attrs, "terra_lt_source")   or lt.get("source"),
            "terra_lt_medium":   g(attrs, "terra_lt_medium")   or lt.get("medium"),
            "terra_lt_campaign": g(attrs, "terra_lt_campaign") or lt.get("campaign"),
            "terra_lt_content":  g(attrs, "terra_lt_content")  or lt.get("content"),
            "terra_lt_term":     g(attrs, "terra_lt_term")     or lt.get("term"),
            "terra_lt_id":       g(attrs, "terra_lt_id")       or lt.get("id"),
            "terra_lt_ad_id":    g(attrs, "terra_lt_ad_id")    or lt.get("ad_id"),
            "terra_fbclid": g(attrs, "terra_fbclid") or click.get("fbclid"),
            "terra_ga_cid": g(attrs, "terra_ga_cid") or s(ga.get("cid")),
            "terra_ga_sid": g(attrs, "terra_ga_sid") or s(ga.get("sid")),
            "terra_ga_sn":  g(attrs, "terra_ga_sn")  or s(ga.get("sn")),
            "terra_session_landing_page":  g(attrs, "terra_session_landing_page")  or ctx.get("terra_session_landing_page"),
            "terra_session_referrer_host": g(attrs, "terra_session_referrer_host") or ctx.get("terra_session_referrer_host"),
            "edge_delivery_visitor_id":    g(attrs, "edge_delivery_visitor_id"),
            "edge_delivery_session_id":    g(attrs, "edge_delivery_session_id"),
            "edge_delivery_experiment_id": g(attrs, "edge_delivery_experiment_id"),
            "metorik_utm_source":   g(attrs, "_metorik_utm_source"),
            "metorik_utm_medium":   g(attrs, "_metorik_utm_medium"),
            "metorik_utm_campaign": g(attrs, "_metorik_utm_campaign"),
            "metorik_utm_content":  g(attrs, "_metorik_utm_content"),
            "metorik_utm_term":     g(attrs, "_metorik_utm_term"),
            "metorik_utm_id":       g(attrs, "_metorik_utm_id"),
            "raw_attributes": json.dumps(attrs) if attrs else None,
        })

    return rows

# ── Load ──────────────────────────────────────────────────────────────────────
def load_to_bq(rows):
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.shopify_order_attributes"
    job = bq.load_table_from_json(rows, table_id, job_config=bigquery.LoadJobConfig(
        schema=SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ignore_unknown_values=True,
    ))
    job.result()
    print(f"  ✅ {table_id} — {bq.get_table(table_id).num_rows:,} rows")

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print(f"🚀 Shopify Order Attributes → {BQ_PROJECT}.{BQ_DATASET}.shopify_order_attributes")
    print("📦 Running bulk operation...")
    url = run_bulk()
    objects = download_jsonl(url)
    print("🔧 Parsing attributes...")
    rows = parse_objects(objects)
    print(f"  {len(rows):,} orders parsed")
    print("💾 Loading to BigQuery...")
    load_to_bq(rows)
    print("\n✅ Done")

if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("❌ Fatal error:")
        traceback.print_exc()
        sys.exit(1)
