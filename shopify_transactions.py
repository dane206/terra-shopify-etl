#!/usr/bin/env python3
"""
Terra Health Essentials — Shopify Transactions → BigQuery
=========================================================
Pulls payment transactions for all orders via REST API.
One row per transaction (an order can have multiple: sale, capture, refund, void).

Table produced:
  terra-analytics-prod.sources.shopify_transactions

Run modes:
  python shopify_transactions.py --mode backfill     # full history, TRUNCATE
  python shopify_transactions.py --mode incremental  # orders updated last 2 days, APPEND
"""

import os, sys, json, time, argparse, traceback
from datetime import datetime, timedelta, timezone

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
REST_URL      = f"https://{SHOPIFY_STORE}/admin/api/{API_VERSION}"

session = requests.Session()
session.headers.update({"X-Shopify-Access-Token": ACCESS_TOKEN})
session.mount("https://", HTTPAdapter(max_retries=Retry(
    total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])))

bq = bigquery.Client(project=BQ_PROJECT)

# ── Schema ────────────────────────────────────────────────────────────────────
SF = bigquery.SchemaField

TRANSACTIONS_SCHEMA = [
    SF("transaction_id",  "STRING"),
    SF("order_id",        "STRING"),
    SF("order_name",      "STRING"),
    SF("kind",            "STRING"),
    SF("gateway",         "STRING"),
    SF("status",          "STRING"),
    SF("amount",          "FLOAT"),
    SF("currency",        "STRING"),
    SF("card_brand",      "STRING"),
    SF("card_last4",      "STRING"),
    SF("payment_method",  "STRING"),
    SF("created_at",      "TIMESTAMP"),
    SF("processed_at",    "TIMESTAMP"),
    SF("error_code",      "STRING"),
    SF("parent_id",       "STRING"),
]

# ── Helpers ───────────────────────────────────────────────────────────────────
def s(v): return str(v) if v is not None else None
def f(v):
    try: return float(v) if v is not None else None
    except: return None

def rest_paginate(endpoint, key, params=None):
    url = f"{REST_URL}/{endpoint}"
    p = {"limit": 250, **(params or {})}
    total = 0
    while url:
        resp = session.get(url, params=p, timeout=30)
        if resp.status_code == 429:
            time.sleep(float(resp.headers.get("Retry-After", 4)))
            continue
        resp.raise_for_status()
        batch = resp.json().get(key, [])
        if not batch:
            break
        total += len(batch)
        print(f"\r  {total:,} {key}   ", end="", flush=True)
        yield from batch
        url = resp.links.get("next", {}).get("url")
        p = None
    print()

def fetch_transactions(order_id):
    resp = session.get(f"{REST_URL}/orders/{order_id}/transactions.json", timeout=30)
    if resp.status_code == 429:
        time.sleep(float(resp.headers.get("Retry-After", 4)))
        resp = session.get(f"{REST_URL}/orders/{order_id}/transactions.json", timeout=30)
    resp.raise_for_status()
    return resp.json().get("transactions", [])

def xform_transaction(t, order_name):
    pd = t.get("payment_details") or {}
    return {
        "transaction_id": s(t.get("id")),
        "order_id":       s(t.get("order_id")),
        "order_name":     order_name,
        "kind":           t.get("kind"),
        "gateway":        t.get("gateway"),
        "status":         t.get("status"),
        "amount":         f(t.get("amount")),
        "currency":       t.get("currency"),
        "card_brand":     pd.get("credit_card_company"),
        "card_last4":     pd.get("credit_card_number"),
        "payment_method": pd.get("payment_method_name"),
        "created_at":     t.get("created_at"),
        "processed_at":   t.get("processed_at"),
        "error_code":     t.get("error_code"),
        "parent_id":      s(t.get("parent_id")),
    }

def load_to_bq(rows, mode):
    if not rows:
        print(f"  ⚠️  No rows to load")
        return
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.shopify_transactions"
    job = bq.load_table_from_json(rows, table_id, job_config=bigquery.LoadJobConfig(
        schema=TRANSACTIONS_SCHEMA, write_disposition=mode, ignore_unknown_values=True))
    job.result()
    print(f"  ✅ {table_id} — {bq.get_table(table_id).num_rows:,} rows")

# ── Runners ───────────────────────────────────────────────────────────────────
def get_last_transaction_time():
    """Query BQ for the latest transaction timestamp to use as incremental start."""
    try:
        query = f"SELECT FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', MAX(created_at)) AS latest FROM `{BQ_PROJECT}.{BQ_DATASET}.shopify_transactions`"
        result = list(bq.query(query).result())
        latest = result[0].latest if result else None
        if latest:
            print(f"  Resuming from last transaction: {latest}")
        return latest
    except Exception:
        return None

def run(since=None):
    params = {"status": "any"}
    if since:
        params["updated_at_min"] = since

    print("  Fetching orders...")
    orders = list(rest_paginate("orders.json", "orders", params))
    print(f"  {len(orders):,} orders — fetching transactions...")

    rows = []
    for i, o in enumerate(orders):
        order_id   = s(o.get("id"))
        order_name = o.get("name")
        txns = fetch_transactions(order_id)
        for t in txns:
            rows.append(xform_transaction(t, order_name))
        if (i + 1) % 100 == 0:
            print(f"\r  {i+1:,}/{len(orders):,} orders processed — {len(rows):,} transactions", end="", flush=True)

    print(f"\n  {len(rows):,} transactions total")
    return rows

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["backfill", "incremental"], default="incremental")
    args = parser.parse_args()

    print(f"🚀 Shopify Transactions — mode: {args.mode} → {BQ_PROJECT}.{BQ_DATASET}.shopify_transactions")

    if args.mode == "incremental":
        since = get_last_transaction_time() or (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
        mode  = bigquery.WriteDisposition.WRITE_APPEND
    else:
        since = None
        mode  = bigquery.WriteDisposition.WRITE_TRUNCATE
        print("  Backfill: all orders")

    rows = run(since)
    print("\n💾 Loading to BigQuery...")
    load_to_bq(rows, mode)
    print("\n✅ Done")

if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("❌ Fatal error:")
        traceback.print_exc()
        sys.exit(1)
