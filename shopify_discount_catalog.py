#!/usr/bin/env python3
"""
Terra Health Essentials — Shopify Discount Catalog → BigQuery
=============================================================
Pulls all discount codes and price rules via GraphQL Bulk API.
Captures the full discount configuration — not usage on orders
(that's in shopify_discounts), but what discounts exist, their
rules, eligibility, stackability, and status.

Table produced:
  terra-analytics-prod.sources.shopify_discount_catalog

Always does a full TRUNCATE + reload (discount configs change frequently).

Run:
  python shopify_discount_catalog.py
"""

import os, sys, json, time, traceback
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter, Retry
from google.cloud import bigquery

import configparser
_cfg = configparser.ConfigParser()
_cfg.read(os.path.join(os.path.dirname(__file__), "config.ini"))
_shopify = _cfg["shopify"] if _cfg.has_section("shopify") else {}

SHOPIFY_STORE = os.environ.get("SHOPIFY_STORE") or _shopify.get("store", "")
ACCESS_TOKEN  = os.environ.get("SHOPIFY_ACCESS_TOKEN") or _shopify.get("access_token", "")
API_VERSION   = os.environ.get("SHOPIFY_API_VERSION") or _shopify.get("api_version", "2025-04")
BQ_PROJECT    = os.environ.get("BQ_PROJECT", "terra-analytics-prod")
BQ_DATASET    = "sources"
GRAPHQL_URL   = f"https://{SHOPIFY_STORE}/admin/api/{API_VERSION}/graphql.json"

session = requests.Session()
session.headers.update({"X-Shopify-Access-Token": ACCESS_TOKEN, "Content-Type": "application/json"})
session.mount("https://", HTTPAdapter(max_retries=Retry(
    total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])))

bq = bigquery.Client(project=BQ_PROJECT)

# ── Schema ────────────────────────────────────────────────────────────────────
SF = bigquery.SchemaField

DISCOUNT_CATALOG_SCHEMA = [
    SF("discount_id",                    "STRING"),
    SF("title",                          "STRING"),
    SF("code",                           "STRING"),
    SF("status",                         "STRING"),
    SF("discount_type",                  "STRING"),
    SF("value_type",                     "STRING"),
    SF("value",                          "FLOAT"),
    SF("target_type",                    "STRING"),
    SF("allocation_method",              "STRING"),
    SF("minimum_purchase_amount",        "FLOAT"),
    SF("minimum_quantity",               "INTEGER"),
    SF("customer_selection",             "STRING"),
    SF("applies_once_per_customer",      "BOOLEAN"),
    SF("usage_limit",                    "INTEGER"),
    SF("times_used",                     "INTEGER"),
    SF("combines_with_order_discounts",  "BOOLEAN"),
    SF("combines_with_product_discounts","BOOLEAN"),
    SF("combines_with_shipping_discounts","BOOLEAN"),
    SF("starts_at",                      "TIMESTAMP"),
    SF("ends_at",                        "TIMESTAMP"),
    SF("created_at",                     "TIMESTAMP"),
    SF("updated_at",                     "TIMESTAMP"),
]

# ── GraphQL Bulk Query ────────────────────────────────────────────────────────
# Field names verified against Shopify GraphQL Admin API 2025-04:
#   - DiscountCodeBxgy (not BxGy)
#   - DiscountAutomaticBxgy (not BxGy)
#   - asyncUsageCount (not usageCount) on DiscountRedeemCode
DISCOUNTS_BULK_QUERY = """{
  discountNodes {
    edges {
      node {
        id
        discount {
          __typename
          ... on DiscountCodeBasic {
            title status startsAt endsAt createdAt updatedAt
            usageLimit appliesOncePerCustomer
            codes(first: 1) { edges { node { code asyncUsageCount } } }
            customerSelection { __typename ... on DiscountCustomerAll { allCustomers } }
            minimumRequirement {
              ... on DiscountMinimumSubtotal { greaterThanOrEqualToSubtotal { amount } }
              ... on DiscountMinimumQuantity { greaterThanOrEqualToQuantity }
            }
            customerGets {
              value {
                ... on DiscountPercentage { percentage }
                ... on DiscountAmount { amount { amount } }
              }
              items { __typename }
            }
            combinesWith { orderDiscounts productDiscounts shippingDiscounts }
          }
          ... on DiscountCodeFreeShipping {
            title status startsAt endsAt createdAt updatedAt
            usageLimit appliesOncePerCustomer
            codes(first: 1) { edges { node { code asyncUsageCount } } }
            customerSelection { __typename ... on DiscountCustomerAll { allCustomers } }
            minimumRequirement {
              ... on DiscountMinimumSubtotal { greaterThanOrEqualToSubtotal { amount } }
              ... on DiscountMinimumQuantity { greaterThanOrEqualToQuantity }
            }
            combinesWith { orderDiscounts productDiscounts shippingDiscounts }
          }
          ... on DiscountCodeBxgy {
            title status startsAt endsAt createdAt updatedAt
            usageLimit appliesOncePerCustomer
            codes(first: 1) { edges { node { code asyncUsageCount } } }
            customerSelection { __typename ... on DiscountCustomerAll { allCustomers } }
            combinesWith { orderDiscounts productDiscounts shippingDiscounts }
          }
          ... on DiscountAutomaticBasic {
            title status startsAt endsAt createdAt updatedAt
            minimumRequirement {
              ... on DiscountMinimumSubtotal { greaterThanOrEqualToSubtotal { amount } }
              ... on DiscountMinimumQuantity { greaterThanOrEqualToQuantity }
            }
            customerGets {
              value {
                ... on DiscountPercentage { percentage }
                ... on DiscountAmount { amount { amount } }
              }
              items { __typename }
            }
            combinesWith { orderDiscounts productDiscounts shippingDiscounts }
          }
          ... on DiscountAutomaticFreeShipping {
            title status startsAt endsAt createdAt updatedAt
            minimumRequirement {
              ... on DiscountMinimumSubtotal { greaterThanOrEqualToSubtotal { amount } }
              ... on DiscountMinimumQuantity { greaterThanOrEqualToQuantity }
            }
            combinesWith { orderDiscounts productDiscounts shippingDiscounts }
          }
          ... on DiscountAutomaticBxgy {
            title status startsAt endsAt createdAt updatedAt
            combinesWith { orderDiscounts productDiscounts shippingDiscounts }
          }
        }
      }
    }
  }
}"""

# ── Bulk op runner ────────────────────────────────────────────────────────────
def cancel_existing_bulk_op():
    poll = "{ currentBulkOperation { id status } }"
    result = session.post(GRAPHQL_URL, json={"query": poll}, timeout=30).json()
    op = result.get("data", {}).get("currentBulkOperation")
    if op and op["status"] in ("CREATED", "RUNNING"):
        print(f"  Canceling existing operation {op['id']}...")
        cancel = f'mutation {{ bulkOperationCancel(id: "{op["id"]}") {{ bulkOperation {{ id status }} }} }}'
        session.post(GRAPHQL_URL, json={"query": cancel}, timeout=30)
        time.sleep(5)

def run_bulk_operation():
    cancel_existing_bulk_op()
    mutation = 'mutation { bulkOperationRunQuery(query: """' + DISCOUNTS_BULK_QUERY + '""") { bulkOperation { id status } userErrors { field message } } }'
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
        count   = int(op.get("objectCount") or 0)
        mb      = int(op.get("fileSize") or 0) / 1024 / 1024
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
def s(v): return str(v) if v is not None else None
def f(v):
    try: return float(v) if v is not None else None
    except: return None

def parse_objects(objects):
    rows = []
    for obj in objects:
        if "__parentId" in obj:
            continue

        node_id  = obj.get("id")
        discount = obj.get("discount") or {}
        typename = discount.get("__typename", "")

        codes      = discount.get("codes") or {}
        code_edges = codes.get("edges") or []
        first_code = code_edges[0]["node"] if code_edges else {}
        code       = first_code.get("code")
        times_used = first_code.get("asyncUsageCount")

        cg    = discount.get("customerGets") or {}
        val   = cg.get("value") or {}
        items = cg.get("items") or {}

        if "percentage" in val:
            value_type = "percentage"
            value      = f(val["percentage"])
        elif "amount" in val:
            value_type = "fixed_amount"
            value      = f((val["amount"] or {}).get("amount"))
        elif "FreeShipping" in typename:
            value_type = "free_shipping"
            value      = None
        else:
            value_type = None
            value      = None

        target_type = "shipping_line" if "FreeShipping" in typename else "line_item"

        min_req = discount.get("minimumRequirement") or {}
        min_amt = f((min_req.get("greaterThanOrEqualToSubtotal") or {}).get("amount"))
        min_qty = min_req.get("greaterThanOrEqualToQuantity")

        cs = discount.get("customerSelection") or {}
        customer_selection = "all" if cs.get("allCustomers") else cs.get("__typename", "specific")

        cw = discount.get("combinesWith") or {}

        rows.append({
            "discount_id":                     node_id,
            "title":                           discount.get("title"),
            "code":                            code,
            "status":                          discount.get("status"),
            "discount_type":                   typename,
            "value_type":                      value_type,
            "value":                           value,
            "target_type":                     target_type,
            "allocation_method":               "across" if "Bxgy" in typename else "each",
            "minimum_purchase_amount":         min_amt,
            "minimum_quantity":                int(min_qty) if min_qty is not None else None,
            "customer_selection":              customer_selection,
            "applies_once_per_customer":       discount.get("appliesOncePerCustomer"),
            "usage_limit":                     discount.get("usageLimit"),
            "times_used":                      int(times_used) if times_used is not None else None,
            "combines_with_order_discounts":   cw.get("orderDiscounts"),
            "combines_with_product_discounts": cw.get("productDiscounts"),
            "combines_with_shipping_discounts":cw.get("shippingDiscounts"),
            "starts_at":                       discount.get("startsAt"),
            "ends_at":                         discount.get("endsAt"),
            "created_at":                      discount.get("createdAt"),
            "updated_at":                      discount.get("updatedAt"),
        })

    return rows

# ── Load ──────────────────────────────────────────────────────────────────────
def load_to_bq(rows):
    if not rows:
        print("  ⚠️  No rows to load")
        return
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.shopify_discount_catalog"
    job = bq.load_table_from_json(rows, table_id, job_config=bigquery.LoadJobConfig(
        schema=DISCOUNT_CATALOG_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ignore_unknown_values=True,
    ))
    job.result()
    print(f"  ✅ {table_id} — {bq.get_table(table_id).num_rows:,} rows")

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print(f"🚀 Shopify Discount Catalog → {BQ_PROJECT}.{BQ_DATASET}.shopify_discount_catalog")
    print("📦 Running bulk operation...")
    url = run_bulk_operation()
    objects = download_jsonl(url)
    print("🔧 Parsing discounts...")
    rows = parse_objects(objects)
    print(f"  {len(rows):,} discounts parsed")
    print("\n💾 Loading to BigQuery...")
    load_to_bq(rows)
    print("\n✅ Done")

if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("❌ Fatal error:")
        traceback.print_exc()
        sys.exit(1)
