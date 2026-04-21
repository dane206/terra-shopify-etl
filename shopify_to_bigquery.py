#!/usr/bin/env python3
"""
Terra Health Essentials — Shopify Bulk Operations → BigQuery ETL
================================================================
All field names verified against Shopify GraphQL Admin API docs (2025-04).

Bulk API restrictions:
  - Max 2 levels of nested connections
  - No connections inside list fields
  - Solution: orders + refunds as separate bulk operations

Tables produced:
  terra-analytics-prod.sources.shopify_orders
  terra-analytics-prod.sources.shopify_line_items
  terra-analytics-prod.sources.shopify_discounts
  terra-analytics-prod.sources.shopify_refunds
  terra-analytics-prod.sources.shopify_customers
  terra-analytics-prod.sources.shopify_products
  terra-analytics-prod.sources.shopify_variants

Run modes:
  python shopify_to_bigquery.py --mode backfill     # full history, TRUNCATE
  python shopify_to_bigquery.py --mode incremental  # last 2 days, APPEND
"""

import os, sys, json, time, argparse, traceback
from datetime import datetime, timedelta, timezone

import requests
from requests.adapters import HTTPAdapter, Retry
from google.cloud import bigquery

import configparser
config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), "config.ini"))

SHOPIFY_STORE = config["shopify"]["store"]
ACCESS_TOKEN  = config["shopify"]["access_token"]
API_VERSION   = config["shopify"].get("api_version", "2025-04")
BQ_PROJECT    = "terra-analytics-prod"
BQ_DATASET    = "sources"
GRAPHQL_URL   = f"https://{SHOPIFY_STORE}/admin/api/{API_VERSION}/graphql.json"
REST_URL      = f"https://{SHOPIFY_STORE}/admin/api/{API_VERSION}"

session = requests.Session()
session.headers.update({"X-Shopify-Access-Token": ACCESS_TOKEN, "Content-Type": "application/json"})
session.mount("https://", HTTPAdapter(max_retries=Retry(total=5, backoff_factor=2, status_forcelist=[429,500,502,503,504])))

bq = bigquery.Client(project=BQ_PROJECT)

# ── Helpers ───────────────────────────────────────────────────────────────────
def s(v): return str(v) if v is not None else None
def f(v):
    try: return float(v) if v is not None else None
    except: return None
def gid(v): return str(v).split("/")[-1] if v else None
def money(ps):
    try: return f(ps["shopMoney"]["amount"])
    except: return None

def load_to_bq(table, rows, schema, mode=bigquery.WriteDisposition.WRITE_TRUNCATE):
    if not rows:
        print(f"  ⚠️  {BQ_PROJECT}.{BQ_DATASET}.{table} — no rows, skipping")
        return
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{table}"
    job = bq.load_table_from_json(rows, table_id, job_config=bigquery.LoadJobConfig(
        schema=schema, write_disposition=mode, ignore_unknown_values=True))
    job.result()
    print(f"  ✅ {table_id} — {bq.get_table(table_id).num_rows:,} rows")

# ── Bulk queries (all fields verified against Shopify GraphQL docs 2025-04) ───

# Order fields used:
#   number (Int!), name (String!), confirmationNumber (String),
#   createdAt, updatedAt, processedAt, cancelledAt, closedAt,
#   displayFinancialStatus, displayFulfillmentStatus, cancelReason,
#   currencyCode, presentmentCurrencyCode,
#   currentTotalPriceSet, currentSubtotalPriceSet, currentTotalDiscountsSet, currentTotalTaxSet,
#   totalPriceSet, subtotalPriceSet, totalDiscountsSet, totalTaxSet, totalShippingPriceSet,
#   totalWeight (UnsignedInt64),
#   customer { id email }, email (String),
#   customerLocale, customerAcceptsMarketing (Boolean!),
#   sourceName, tags ([String!]!), note, test (Boolean!),
#   registeredSourceUrl (URL), clientIp (String),
#   paymentGatewayNames ([String!]!),
#   billingAddress (MailingAddress), shippingAddress (MailingAddress)
#
# LineItem fields used:
#   id, product { id }, variant { id sku }, title, variantTitle, name, vendor,
#   quantity, originalUnitPriceSet, totalDiscountSet,
#   isGiftCard, taxable, requiresShipping
#
# DiscountApplication fields used:
#   __typename, allocationMethod, targetSelection, targetType, value,
#   code (DiscountCodeApplication), title (Manual/ScriptDiscountApplication)

ORDERS_BULK_QUERY = """{
  orders {
    edges {
      node {
        id
        name
        number
        confirmationNumber
        createdAt
        updatedAt
        processedAt
        cancelledAt
        closedAt
        displayFinancialStatus
        displayFulfillmentStatus
        cancelReason
        currencyCode
        presentmentCurrencyCode
        currentTotalPriceSet { shopMoney { amount } }
        currentSubtotalPriceSet { shopMoney { amount } }
        currentTotalDiscountsSet { shopMoney { amount } }
        currentTotalTaxSet { shopMoney { amount } }
        totalPriceSet { shopMoney { amount } }
        subtotalPriceSet { shopMoney { amount } }
        totalDiscountsSet { shopMoney { amount } }
        totalTaxSet { shopMoney { amount } }
        totalShippingPriceSet { shopMoney { amount } }
        totalWeight
        customer { id email }
        email
        customerLocale
        customerAcceptsMarketing
        sourceName
        tags
        note
        test
        registeredSourceUrl
        clientIp
        paymentGatewayNames
        billingAddress {
          firstName lastName address1 city province provinceCode
          country countryCode zip phone
        }
        shippingAddress {
          firstName lastName address1 city province provinceCode
          country countryCode zip phone latitude longitude
        }
        lineItems {
          edges {
            node {
              id
              product { id }
              variant { id sku }
              title
              variantTitle
              name
              vendor
              quantity
              originalUnitPriceSet { shopMoney { amount } }
              totalDiscountSet { shopMoney { amount } }
              isGiftCard
              taxable
              requiresShipping
            }
          }
        }
        discountApplications {
          edges {
            node {
              __typename
              allocationMethod
              targetSelection
              targetType
              value { ... on PricingPercentageValue { percentage } ... on MoneyV2 { amount } }
              ... on DiscountCodeApplication { code }
              ... on ManualDiscountApplication { title }
              ... on ScriptDiscountApplication { title }
            }
          }
        }
      }
    }
  }
}"""

# Refund fields (from Refund object docs):
#   id (ID!), createdAt (DateTime), note (String)
# RefundLineItem fields:
#   id (ID!), lineItem { id }, quantity (Int!), restockType, subtotalSet, totalTaxSet
REFUNDS_BULK_QUERY = """{
  orders {
    edges {
      node {
        id
        name
        refunds {
          id
          createdAt
          note
          refundLineItems {
            edges {
              node {
                id
                lineItem { id }
                quantity
                restockType
                subtotalSet { shopMoney { amount } }
                totalTaxSet { shopMoney { amount } }
              }
            }
          }
        }
      }
    }
  }
}"""

# ── Bulk operation runner ─────────────────────────────────────────────────────
def cancel_existing_bulk_op():
    poll = "{ currentBulkOperation { id status } }"
    result = session.post(GRAPHQL_URL, json={"query": poll}, timeout=30).json()
    op = result.get("data", {}).get("currentBulkOperation")
    if op and op["status"] in ("CREATED", "RUNNING"):
        print(f"  Canceling existing operation {op['id']}...")
        cancel = f'mutation {{ bulkOperationCancel(id: "{op["id"]}") {{ bulkOperation {{ id status }} }} }}'
        session.post(GRAPHQL_URL, json={"query": cancel}, timeout=30)
        time.sleep(5)

def run_bulk_operation(query, label=""):
    cancel_existing_bulk_op()
    mutation = 'mutation { bulkOperationRunQuery(query: """' + query + '""") { bulkOperation { id status } userErrors { field message } } }'
    resp = session.post(GRAPHQL_URL, json={"query": mutation}, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    errors = (data.get("data") or {}).get("bulkOperationRunQuery", {}).get("userErrors", [])
    if errors:
        raise Exception(f"Bulk operation errors: {errors}")
    op_id = data["data"]["bulkOperationRunQuery"]["bulkOperation"]["id"]
    print(f"  {label} operation ID: {op_id}")

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

# ── Parsers ───────────────────────────────────────────────────────────────────
def parse_orders_jsonl(objects):
    orders_out, line_items_out, discounts_out = [], [], []

    # Separate top-level order nodes from child nodes
    order_nodes = [o for o in objects if "__parentId" not in o and "number" in o]
    children_by_parent = {}
    for obj in objects:
        if "__parentId" in obj:
            children_by_parent.setdefault(obj["__parentId"], []).append(obj)

    for o in order_nodes:
        order_gid  = o.get("id")
        order_id   = gid(order_gid)
        order_name = o.get("name")
        created_at = o.get("createdAt")
        ba = o.get("billingAddress") or {}
        sa = o.get("shippingAddress") or {}
        c  = o.get("customer") or {}

        orders_out.append({
            "order_id":                order_id,
            "order_name":              order_name,
            "order_number":            o.get("number"),
            "confirmation_number":     o.get("confirmationNumber"),
            "created_at":              created_at,
            "updated_at":              o.get("updatedAt"),
            "processed_at":            o.get("processedAt"),
            "cancelled_at":            o.get("cancelledAt"),
            "closed_at":               o.get("closedAt"),
            "financial_status":        o.get("displayFinancialStatus"),
            "fulfillment_status":      o.get("displayFulfillmentStatus"),
            "cancel_reason":           o.get("cancelReason"),
            "currency":                o.get("currencyCode"),
            "presentment_currency":    o.get("presentmentCurrencyCode"),
            "current_total_price":     money(o.get("currentTotalPriceSet")),
            "current_subtotal_price":  money(o.get("currentSubtotalPriceSet")),
            "current_total_discounts": money(o.get("currentTotalDiscountsSet")),
            "current_total_tax":       money(o.get("currentTotalTaxSet")),
            "total_price":             money(o.get("totalPriceSet")),
            "subtotal_price":          money(o.get("subtotalPriceSet")),
            "total_discounts":         money(o.get("totalDiscountsSet")),
            "total_tax":               money(o.get("totalTaxSet")),
            "total_shipping_price":    money(o.get("totalShippingPriceSet")),
            "total_weight":            o.get("totalWeight"),
            "customer_id":             gid(c.get("id")),
            "customer_email":          c.get("email"),
            "contact_email":           o.get("email"),
            "customer_locale":         o.get("customerLocale"),
            "customer_accepts_marketing": o.get("customerAcceptsMarketing"),
            "source_name":             o.get("sourceName"),
            "tags":                    json.dumps(o.get("tags") or []),
            "note":                    o.get("note"),
            "test":                    o.get("test"),
            "landing_site":            s(o.get("registeredSourceUrl")),
            "client_ip":               o.get("clientIp"),
            "payment_gateway_names":   json.dumps(o.get("paymentGatewayNames") or []),
            "billing_first_name":      ba.get("firstName"),
            "billing_last_name":       ba.get("lastName"),
            "billing_address1":        ba.get("address1"),
            "billing_city":            ba.get("city"),
            "billing_province":        ba.get("province"),
            "billing_province_code":   ba.get("provinceCode"),
            "billing_country":         ba.get("country"),
            "billing_country_code":    ba.get("countryCode"),
            "billing_zip":             ba.get("zip"),
            "billing_phone":           ba.get("phone"),
            "shipping_first_name":     sa.get("firstName"),
            "shipping_last_name":      sa.get("lastName"),
            "shipping_address1":       sa.get("address1"),
            "shipping_city":           sa.get("city"),
            "shipping_province":       sa.get("province"),
            "shipping_province_code":  sa.get("provinceCode"),
            "shipping_country":        sa.get("country"),
            "shipping_country_code":   sa.get("countryCode"),
            "shipping_zip":            sa.get("zip"),
            "shipping_latitude":       f(sa.get("latitude")),
            "shipping_longitude":      f(sa.get("longitude")),
        })

        for child in children_by_parent.get(order_gid, []):
            if "quantity" in child and "title" in child:
                # LineItem
                line_items_out.append({
                    "line_item_id":      gid(child.get("id")),
                    "order_id":          order_id,
                    "order_name":        order_name,
                    "order_created_at":  created_at,
                    "product_id":        gid((child.get("product") or {}).get("id")),
                    "variant_id":        gid((child.get("variant") or {}).get("id")),
                    "sku":               (child.get("variant") or {}).get("sku"),
                    "title":             child.get("title"),
                    "variant_title":     child.get("variantTitle"),
                    "name":              child.get("name"),
                    "vendor":            child.get("vendor"),
                    "quantity":          child.get("quantity"),
                    "price":             money(child.get("originalUnitPriceSet")),
                    "total_discount":    money(child.get("totalDiscountSet")),
                    "gift_card":         child.get("isGiftCard"),
                    "taxable":           child.get("taxable"),
                    "requires_shipping": child.get("requiresShipping"),
                })
            elif "allocationMethod" in child:
                # DiscountApplication
                val = child.get("value") or {}
                discounts_out.append({
                    "order_id":          order_id,
                    "order_name":        order_name,
                    "order_created_at":  created_at,
                    "discount_type":     child.get("__typename"),
                    "code":              child.get("code"),
                    "title":             child.get("title"),
                    "value":             f(val.get("amount") or val.get("percentage")),
                    "value_type":        "percentage" if "percentage" in val else "fixed_amount",
                    "allocation_method": child.get("allocationMethod"),
                    "target_type":       child.get("targetType"),
                    "target_selection":  child.get("targetSelection"),
                })

    return orders_out, line_items_out, discounts_out

def parse_refunds_jsonl(objects):
    """
    Bulk JSONL structure for refunds query:
      order node (no __parentId, has 'number')
      refund node (__parentId = order GID)
      refundLineItem node (__parentId = refund GID)
    """
    refunds_out = []

    order_map  = {}
    refund_map = {}

    for obj in objects:
        pid = obj.get("__parentId")
        if pid is None and "number" in obj:
            # Order node
            order_map[obj["id"]] = {"id": gid(obj["id"]), "name": obj.get("name")}
        elif pid in order_map:
            # Refund node
            refund_map[obj["id"]] = {
                "refund_id":  gid(obj["id"]),
                "order_id":   order_map[pid]["id"],
                "order_name": order_map[pid]["name"],
                "created_at": obj.get("createdAt"),
                "note":       obj.get("note"),
            }
        elif pid in refund_map:
            # RefundLineItem node
            r = refund_map[pid]
            refunds_out.append({
                "refund_id":           r["refund_id"],
                "order_id":            r["order_id"],
                "order_name":          r["order_name"],
                "created_at":          r["created_at"],
                "note":                r["note"],
                "refund_line_item_id": gid(obj.get("id")),
                "line_item_id":        gid((obj.get("lineItem") or {}).get("id")),
                "quantity":            obj.get("quantity"),
                "restock_type":        obj.get("restockType"),
                "subtotal":            money(obj.get("subtotalSet")),
                "total_tax":           money(obj.get("totalTaxSet")),
            })

    return refunds_out

# ── Schemas (matching parse output exactly) ───────────────────────────────────
SF = bigquery.SchemaField

ORDERS_SCHEMA = [
    SF("order_id","STRING"), SF("order_name","STRING"), SF("order_number","INTEGER"),
    SF("confirmation_number","STRING"), SF("created_at","TIMESTAMP"), SF("updated_at","TIMESTAMP"),
    SF("processed_at","TIMESTAMP"), SF("cancelled_at","TIMESTAMP"), SF("closed_at","TIMESTAMP"),
    SF("financial_status","STRING"), SF("fulfillment_status","STRING"), SF("cancel_reason","STRING"),
    SF("currency","STRING"), SF("presentment_currency","STRING"),
    SF("current_total_price","FLOAT"), SF("current_subtotal_price","FLOAT"),
    SF("current_total_discounts","FLOAT"), SF("current_total_tax","FLOAT"),
    SF("total_price","FLOAT"), SF("subtotal_price","FLOAT"),
    SF("total_discounts","FLOAT"), SF("total_tax","FLOAT"),
    SF("total_shipping_price","FLOAT"), SF("total_weight","INTEGER"),
    SF("customer_id","STRING"), SF("customer_email","STRING"), SF("contact_email","STRING"),
    SF("customer_locale","STRING"), SF("customer_accepts_marketing","BOOLEAN"),
    SF("source_name","STRING"), SF("tags","STRING"), SF("note","STRING"), SF("test","BOOLEAN"),
    SF("landing_site","STRING"), SF("client_ip","STRING"), SF("payment_gateway_names","STRING"),
    SF("billing_first_name","STRING"), SF("billing_last_name","STRING"),
    SF("billing_address1","STRING"), SF("billing_city","STRING"),
    SF("billing_province","STRING"), SF("billing_province_code","STRING"),
    SF("billing_country","STRING"), SF("billing_country_code","STRING"),
    SF("billing_zip","STRING"), SF("billing_phone","STRING"),
    SF("shipping_first_name","STRING"), SF("shipping_last_name","STRING"),
    SF("shipping_address1","STRING"), SF("shipping_city","STRING"),
    SF("shipping_province","STRING"), SF("shipping_province_code","STRING"),
    SF("shipping_country","STRING"), SF("shipping_country_code","STRING"),
    SF("shipping_zip","STRING"), SF("shipping_latitude","FLOAT"), SF("shipping_longitude","FLOAT"),
]

LINE_ITEMS_SCHEMA = [
    SF("line_item_id","STRING"), SF("order_id","STRING"), SF("order_name","STRING"),
    SF("order_created_at","TIMESTAMP"), SF("product_id","STRING"), SF("variant_id","STRING"),
    SF("sku","STRING"), SF("title","STRING"), SF("variant_title","STRING"),
    SF("name","STRING"), SF("vendor","STRING"), SF("quantity","INTEGER"),
    SF("price","FLOAT"), SF("total_discount","FLOAT"),
    SF("gift_card","BOOLEAN"), SF("taxable","BOOLEAN"), SF("requires_shipping","BOOLEAN"),
]

DISCOUNTS_SCHEMA = [
    SF("order_id","STRING"), SF("order_name","STRING"), SF("order_created_at","TIMESTAMP"),
    SF("discount_type","STRING"), SF("code","STRING"), SF("title","STRING"),
    SF("value","FLOAT"), SF("value_type","STRING"), SF("allocation_method","STRING"),
    SF("target_type","STRING"), SF("target_selection","STRING"),
]

REFUNDS_SCHEMA = [
    SF("refund_id","STRING"), SF("order_id","STRING"), SF("order_name","STRING"),
    SF("created_at","TIMESTAMP"), SF("note","STRING"),
    SF("refund_line_item_id","STRING"), SF("line_item_id","STRING"),
    SF("quantity","INTEGER"), SF("restock_type","STRING"),
    SF("subtotal","FLOAT"), SF("total_tax","FLOAT"),
]

CUSTOMERS_SCHEMA = [
    SF("customer_id","STRING"), SF("email","STRING"), SF("phone","STRING"),
    SF("first_name","STRING"), SF("last_name","STRING"),
    SF("created_at","TIMESTAMP"), SF("updated_at","TIMESTAMP"),
    SF("state","STRING"), SF("verified_email","BOOLEAN"), SF("tax_exempt","BOOLEAN"),
    SF("buyer_accepts_marketing","BOOLEAN"), SF("email_marketing_state","STRING"),
    SF("sms_marketing_state","STRING"), SF("tags","STRING"), SF("currency","STRING"),
    SF("note","STRING"), SF("default_address1","STRING"), SF("default_city","STRING"),
    SF("default_province","STRING"), SF("default_province_code","STRING"),
    SF("default_country","STRING"), SF("default_country_code","STRING"), SF("default_zip","STRING"),
]

PRODUCTS_SCHEMA = [
    SF("product_id","STRING"), SF("title","STRING"), SF("handle","STRING"),
    SF("product_type","STRING"), SF("vendor","STRING"), SF("status","STRING"),
    SF("tags","STRING"), SF("created_at","TIMESTAMP"), SF("updated_at","TIMESTAMP"),
    SF("published_at","TIMESTAMP"), SF("body_html","STRING"),
]

VARIANTS_SCHEMA = [
    SF("variant_id","STRING"), SF("product_id","STRING"), SF("title","STRING"),
    SF("option1","STRING"), SF("option2","STRING"), SF("option3","STRING"),
    SF("sku","STRING"), SF("price","FLOAT"), SF("compare_at_price","FLOAT"),
    SF("weight","FLOAT"), SF("weight_unit","STRING"), SF("inventory_quantity","INTEGER"),
    SF("inventory_management","STRING"), SF("inventory_policy","STRING"),
    SF("fulfillment_service","STRING"), SF("taxable","BOOLEAN"), SF("barcode","STRING"),
    SF("requires_shipping","BOOLEAN"), SF("created_at","TIMESTAMP"), SF("updated_at","TIMESTAMP"),
]

# ── REST paginator (customers, products, incremental orders) ──────────────────
def rest_paginate(endpoint, key, params=None):
    url, p, total = f"{REST_URL}/{endpoint}", {"limit": 250, **(params or {})}, 0
    while url:
        resp = session.get(url, params=p, timeout=30)
        if resp.status_code == 429:
            time.sleep(float(resp.headers.get("Retry-After", 4)))
            continue
        resp.raise_for_status()
        batch = resp.json().get(key, [])
        if not batch: break
        total += len(batch)
        print(f"\r  {total:,} {key}   ", end="", flush=True)
        yield from batch
        url = resp.links.get("next", {}).get("url")
        p = None
    print()

def xform_customer(c):
    da  = c.get("default_address") or {}
    emc = c.get("email_marketing_consent") or {}
    smc = c.get("sms_marketing_consent") or {}
    return {
        "customer_id": s(c.get("id")), "email": c.get("email"), "phone": c.get("phone"),
        "first_name": c.get("first_name"), "last_name": c.get("last_name"),
        "created_at": c.get("created_at"), "updated_at": c.get("updated_at"),
        "state": c.get("state"), "verified_email": c.get("verified_email"),
        "tax_exempt": c.get("tax_exempt"), "buyer_accepts_marketing": c.get("accepts_marketing"),
        "email_marketing_state": emc.get("state"), "sms_marketing_state": smc.get("state"),
        "tags": c.get("tags"), "currency": c.get("currency"), "note": c.get("note"),
        "default_address1": da.get("address1"), "default_city": da.get("city"),
        "default_province": da.get("province"), "default_province_code": da.get("province_code"),
        "default_country": da.get("country"), "default_country_code": da.get("country_code"),
        "default_zip": da.get("zip"),
    }

def xform_product(p):
    return {
        "product_id": s(p.get("id")), "title": p.get("title"), "handle": p.get("handle"),
        "product_type": p.get("product_type"), "vendor": p.get("vendor"),
        "status": p.get("status"), "tags": p.get("tags"),
        "created_at": p.get("created_at"), "updated_at": p.get("updated_at"),
        "published_at": p.get("published_at"), "body_html": p.get("body_html"),
    }

def xform_variant(v, pid):
    return {
        "variant_id": s(v.get("id")), "product_id": s(pid), "title": v.get("title"),
        "option1": v.get("option1"), "option2": v.get("option2"), "option3": v.get("option3"),
        "sku": v.get("sku"), "price": f(v.get("price")), "compare_at_price": f(v.get("compare_at_price")),
        "weight": f(v.get("weight")), "weight_unit": v.get("weight_unit"),
        "inventory_quantity": v.get("inventory_quantity"),
        "inventory_management": v.get("inventory_management"),
        "inventory_policy": v.get("inventory_policy"),
        "fulfillment_service": v.get("fulfillment_service"),
        "taxable": v.get("taxable"), "barcode": v.get("barcode"),
        "requires_shipping": v.get("requires_shipping"),
        "created_at": v.get("created_at"), "updated_at": v.get("updated_at"),
    }

# ── Runners ───────────────────────────────────────────────────────────────────
def run_orders_bulk():
    print("\n📦 Bulk op 1/2: orders + line items + discounts...")
    url = run_bulk_operation(ORDERS_BULK_QUERY, "Orders")
    objects = download_jsonl(url)
    orders, line_items, discounts = parse_orders_jsonl(objects)
    print(f"  orders: {len(orders):,} | line_items: {len(line_items):,} | discounts: {len(discounts):,}")
    print("\n💾 Loading to BigQuery...")
    load_to_bq("shopify_orders",     orders,     ORDERS_SCHEMA)
    load_to_bq("shopify_line_items", line_items, LINE_ITEMS_SCHEMA)
    load_to_bq("shopify_discounts",  discounts,  DISCOUNTS_SCHEMA)

def run_refunds_bulk():
    print("\n🔄 Bulk op 2/2: refunds...")
    url = run_bulk_operation(REFUNDS_BULK_QUERY, "Refunds")
    objects = download_jsonl(url)
    refunds = parse_refunds_jsonl(objects)
    print(f"  refunds: {len(refunds):,}")
    load_to_bq("shopify_refunds", refunds, REFUNDS_SCHEMA)

def run_customers():
    print("\n👥 Fetching customers (REST)...")
    rows = [xform_customer(c) for c in rest_paginate("customers.json", "customers")]
    load_to_bq("shopify_customers", rows, CUSTOMERS_SCHEMA)

def run_products():
    print("\n🛍️  Fetching products (REST)...")
    products, variants = [], []
    for p in rest_paginate("products.json", "products"):
        products.append(xform_product(p))
        for v in (p.get("variants") or []):
            variants.append(xform_variant(v, p.get("id")))
    print(f"  products: {len(products):,} | variants: {len(variants):,}")
    load_to_bq("shopify_products", products, PRODUCTS_SCHEMA)
    load_to_bq("shopify_variants", variants, VARIANTS_SCHEMA)

def run_orders_incremental():
    since = (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"\n📦 Incremental orders updated since {since}...")
    orders = []
    for o in rest_paginate("orders.json", "orders", {"status": "any", "updated_at_min": since}):
        ba, sa, c = o.get("billing_address") or {}, o.get("shipping_address") or {}, o.get("customer") or {}
        orders.append({
            "order_id": s(o.get("id")), "order_name": o.get("name"),
            "order_number": o.get("order_number"), "confirmation_number": o.get("confirmation_number"),
            "created_at": o.get("created_at"), "updated_at": o.get("updated_at"),
            "processed_at": o.get("processed_at"), "cancelled_at": o.get("cancelled_at"),
            "closed_at": o.get("closed_at"), "financial_status": o.get("financial_status"),
            "fulfillment_status": o.get("fulfillment_status"), "cancel_reason": o.get("cancel_reason"),
            "currency": o.get("currency"), "presentment_currency": o.get("presentment_currency"),
            "current_total_price": f(o.get("current_total_price")),
            "current_subtotal_price": f(o.get("current_subtotal_price")),
            "current_total_discounts": f(o.get("current_total_discounts")),
            "current_total_tax": f(o.get("current_total_tax")),
            "total_price": f(o.get("total_price")), "subtotal_price": f(o.get("subtotal_price")),
            "total_discounts": f(o.get("total_discounts")), "total_tax": f(o.get("total_tax")),
            "total_shipping_price": f((o.get("total_shipping_price_set") or {}).get("shop_money", {}).get("amount")),
            "total_weight": o.get("total_weight"),
            "customer_id": s(c.get("id")), "customer_email": c.get("email"),
            "contact_email": o.get("contact_email"), "customer_locale": o.get("customer_locale"),
            "customer_accepts_marketing": o.get("buyer_accepts_marketing"),
            "source_name": o.get("source_name"), "tags": o.get("tags"),
            "note": o.get("note"), "test": o.get("test"),
            "landing_site": o.get("landing_site"),
            "client_ip": o.get("browser_ip"),
            "payment_gateway_names": json.dumps(o.get("payment_gateway_names") or []),
            "billing_first_name": ba.get("first_name"), "billing_last_name": ba.get("last_name"),
            "billing_address1": ba.get("address1"), "billing_city": ba.get("city"),
            "billing_province": ba.get("province"), "billing_province_code": ba.get("province_code"),
            "billing_country": ba.get("country"), "billing_country_code": ba.get("country_code"),
            "billing_zip": ba.get("zip"), "billing_phone": ba.get("phone"),
            "shipping_first_name": sa.get("first_name"), "shipping_last_name": sa.get("last_name"),
            "shipping_address1": sa.get("address1"), "shipping_city": sa.get("city"),
            "shipping_province": sa.get("province"), "shipping_province_code": sa.get("province_code"),
            "shipping_country": sa.get("country"), "shipping_country_code": sa.get("country_code"),
            "shipping_zip": sa.get("zip"),
            "shipping_latitude": f(sa.get("latitude")), "shipping_longitude": f(sa.get("longitude")),
        })
    if orders:
        load_to_bq("shopify_orders", orders, ORDERS_SCHEMA, bigquery.WriteDisposition.WRITE_APPEND)

# ── Entry point ───────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["backfill", "incremental"], default="incremental")
    args = parser.parse_args()
    print(f"🚀 Terra Shopify ETL — mode: {args.mode} → {BQ_PROJECT}.{BQ_DATASET}.*")
    if args.mode == "backfill":
        run_orders_bulk()
        run_refunds_bulk()
    else:
        run_orders_incremental()
    run_customers()
    run_products()
    print("\n✅ ETL complete")

if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("❌ Fatal error:")
        traceback.print_exc()
        sys.exit(1)
