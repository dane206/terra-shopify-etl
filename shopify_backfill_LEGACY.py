#!/usr/bin/env python3
"""
Terra Health Essentials — Shopify Parallel Backfill → BigQuery
==============================================================
Pulls full order history using parallel monthly chunks via REST API.
Proven approach: 2,403 orders in 7.3s per month chunk.

Strategy:
  - Split history into monthly chunks (Apr 2024 → now)
  - Run chunks in parallel (4 workers)
  - Load each completed chunk directly to BQ (APPEND)
  - Separate passes for customers and products (small, fast)

Tables produced:
  terra-analytics-prod.sources.shopify_orders
  terra-analytics-prod.sources.shopify_line_items
  terra-analytics-prod.sources.shopify_refunds
  terra-analytics-prod.sources.shopify_discounts
  terra-analytics-prod.sources.shopify_customers
  terra-analytics-prod.sources.shopify_products
  terra-analytics-prod.sources.shopify_variants

Usage:
  python shopify_backfill.py
"""

import os, sys, json, time, traceback
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter, Retry
from google.cloud import bigquery

import configparser
config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), "config.ini"))

SHOPIFY_STORE = config["shopify"]["store"]
ACCESS_TOKEN  = config["shopify"]["access_token"]
API_VERSION   = config["shopify"].get("api_version", "2026-01")
BQ_PROJECT    = "terra-analytics-prod"
BQ_DATASET    = "sources"
REST_URL      = f"https://{SHOPIFY_STORE}/admin/api/{API_VERSION}"

# Start of order history (confirmed via API)
HISTORY_START = datetime(2024, 4, 1, tzinfo=timezone.utc)
PARALLEL_WORKERS = 4

bq = bigquery.Client(project=BQ_PROJECT)

# ── Helpers ───────────────────────────────────────────────────────────────────
def s(v): return str(v) if v is not None else None
def f(v):
    try: return float(v) if v is not None else None
    except: return None

def make_session():
    sess = requests.Session()
    sess.headers.update({"X-Shopify-Access-Token": ACCESS_TOKEN})
    sess.mount("https://", HTTPAdapter(max_retries=Retry(
        total=5, backoff_factor=2, status_forcelist=[500, 502, 503, 504])))
    return sess

def load_to_bq(table, rows, schema, mode=bigquery.WriteDisposition.WRITE_APPEND):
    if not rows:
        return 0
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{table}"
    job = bq.load_table_from_json(rows, table_id, job_config=bigquery.LoadJobConfig(
        schema=schema, write_disposition=mode, ignore_unknown_values=True))
    job.result()
    return len(rows)

# ── Schemas ───────────────────────────────────────────────────────────────────
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
    SF("referring_site","STRING"), SF("cart_token","STRING"),
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
    SF("fulfillment_status","STRING"),
]

REFUNDS_SCHEMA = [
    SF("refund_id","STRING"), SF("order_id","STRING"), SF("order_name","STRING"),
    SF("created_at","TIMESTAMP"), SF("note","STRING"),
    SF("refund_line_item_id","STRING"), SF("line_item_id","STRING"),
    SF("product_id","STRING"), SF("variant_id","STRING"), SF("sku","STRING"),
    SF("quantity","INTEGER"), SF("restock_type","STRING"),
    SF("subtotal","FLOAT"), SF("total_tax","FLOAT"),
]

DISCOUNTS_SCHEMA = [
    SF("order_id","STRING"), SF("order_name","STRING"), SF("order_created_at","TIMESTAMP"),
    SF("discount_type","STRING"), SF("code","STRING"), SF("title","STRING"),
    SF("value","FLOAT"), SF("value_type","STRING"), SF("allocation_method","STRING"),
    SF("target_type","STRING"), SF("target_selection","STRING"),
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

# ── Transform functions ───────────────────────────────────────────────────────
def transform_order(o):
    ba = o.get("billing_address") or {}
    sa = o.get("shipping_address") or {}
    c  = o.get("customer") or {}
    return {
        "order_id":                   s(o.get("id")),
        "order_name":                 o.get("name"),
        "order_number":               o.get("order_number"),
        "confirmation_number":        o.get("confirmation_number"),
        "created_at":                 o.get("created_at"),
        "updated_at":                 o.get("updated_at"),
        "processed_at":               o.get("processed_at"),
        "cancelled_at":               o.get("cancelled_at"),
        "closed_at":                  o.get("closed_at"),
        "financial_status":           o.get("financial_status"),
        "fulfillment_status":         o.get("fulfillment_status"),
        "cancel_reason":              o.get("cancel_reason"),
        "currency":                   o.get("currency"),
        "presentment_currency":       o.get("presentment_currency"),
        "current_total_price":        f(o.get("current_total_price")),
        "current_subtotal_price":     f(o.get("current_subtotal_price")),
        "current_total_discounts":    f(o.get("current_total_discounts")),
        "current_total_tax":          f(o.get("current_total_tax")),
        "total_price":                f(o.get("total_price")),
        "subtotal_price":             f(o.get("subtotal_price")),
        "total_discounts":            f(o.get("total_discounts")),
        "total_tax":                  f(o.get("total_tax")),
        "total_shipping_price":       f((o.get("total_shipping_price_set") or {}).get("shop_money", {}).get("amount")),
        "total_weight":               o.get("total_weight"),
        "customer_id":                s(c.get("id")),
        "customer_email":             c.get("email"),
        "contact_email":              o.get("contact_email"),
        "customer_locale":            o.get("customer_locale"),
        "customer_accepts_marketing": o.get("buyer_accepts_marketing"),
        "source_name":                o.get("source_name"),
        "tags":                       o.get("tags"),
        "note":                       o.get("note"),
        "test":                       o.get("test"),
        "landing_site":               o.get("landing_site"),
        "client_ip":                  o.get("browser_ip"),
        "payment_gateway_names":      json.dumps(o.get("payment_gateway_names") or []),
        "referring_site":             o.get("referring_site"),
        "cart_token":                 o.get("cart_token"),
        "billing_first_name":         ba.get("first_name"),
        "billing_last_name":          ba.get("last_name"),
        "billing_address1":           ba.get("address1"),
        "billing_city":               ba.get("city"),
        "billing_province":           ba.get("province"),
        "billing_province_code":      ba.get("province_code"),
        "billing_country":            ba.get("country"),
        "billing_country_code":       ba.get("country_code"),
        "billing_zip":                ba.get("zip"),
        "billing_phone":              ba.get("phone"),
        "shipping_first_name":        sa.get("first_name"),
        "shipping_last_name":         sa.get("last_name"),
        "shipping_address1":          sa.get("address1"),
        "shipping_city":              sa.get("city"),
        "shipping_province":          sa.get("province"),
        "shipping_province_code":     sa.get("province_code"),
        "shipping_country":           sa.get("country"),
        "shipping_country_code":      sa.get("country_code"),
        "shipping_zip":               sa.get("zip"),
        "shipping_latitude":          f(sa.get("latitude")),
        "shipping_longitude":         f(sa.get("longitude")),
    }

def transform_line_item(li, order_id, order_name, order_created_at):
    return {
        "line_item_id":       s(li.get("id")),
        "order_id":           order_id,
        "order_name":         order_name,
        "order_created_at":   order_created_at,
        "product_id":         s(li.get("product_id")),
        "variant_id":         s(li.get("variant_id")),
        "sku":                li.get("sku"),
        "title":              li.get("title"),
        "variant_title":      li.get("variant_title"),
        "name":               li.get("name"),
        "vendor":             li.get("vendor"),
        "quantity":           li.get("quantity"),
        "price":              f(li.get("price")),
        "total_discount":     f(li.get("total_discount")),
        "gift_card":          li.get("gift_card"),
        "taxable":            li.get("taxable"),
        "requires_shipping":  li.get("requires_shipping"),
        "fulfillment_status": li.get("fulfillment_status"),
    }

def transform_refund(r, o):
    order_id   = s(o.get("id"))
    order_name = o.get("name")
    refund_id  = s(r.get("id"))
    rows = []
    for rli in (r.get("refund_line_items") or []):
        li = rli.get("line_item") or {}
        rows.append({
            "refund_id":           refund_id,
            "order_id":            order_id,
            "order_name":          order_name,
            "created_at":          r.get("created_at"),
            "note":                r.get("note"),
            "refund_line_item_id": s(rli.get("id")),
            "line_item_id":        s(rli.get("line_item_id")),
            "product_id":          s(li.get("product_id")),
            "variant_id":          s(li.get("variant_id")),
            "sku":                 li.get("sku"),
            "quantity":            rli.get("quantity"),
            "restock_type":        rli.get("restock_type"),
            "subtotal":            f(rli.get("subtotal")),
            "total_tax":           f(rli.get("total_tax")),
        })
    return rows

def transform_discount(dc, order_id, order_name, order_created_at):
    return {
        "order_id":          order_id,
        "order_name":        order_name,
        "order_created_at":  order_created_at,
        "discount_type":     "discount_code",
        "code":              dc.get("code"),
        "title":             None,
        "value":             f(dc.get("amount")),
        "value_type":        dc.get("type"),
        "allocation_method": None,
        "target_type":       None,
        "target_selection":  None,
    }

# ── Monthly chunk fetcher ─────────────────────────────────────────────────────
def fetch_month(month_start, month_end, label):
    """Fetch all orders for a month, return (orders, line_items, refunds, discounts)."""
    sess = make_session()
    url = f"{REST_URL}/orders.json"
    params = {
        "status": "any",
        "limit": 250,
        "created_at_min": month_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "created_at_max": month_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    orders, line_items, refunds, discounts = [], [], [], []
    page = 0

    while url:
        resp = sess.get(url, params=params if page == 0 else None, timeout=30)
        if resp.status_code == 429:
            wait = float(resp.headers.get("Retry-After", 2))
            time.sleep(wait)
            continue
        resp.raise_for_status()
        batch = resp.json().get("orders", [])
        if not batch:
            break
        page += 1

        for o in batch:
            order_id   = s(o.get("id"))
            order_name = o.get("name")
            created_at = o.get("created_at")

            orders.append(transform_order(o))

            for li in (o.get("line_items") or []):
                line_items.append(transform_line_item(li, order_id, order_name, created_at))

            for r in (o.get("refunds") or []):
                refunds.extend(transform_refund(r, o))

            for dc in (o.get("discount_codes") or []):
                discounts.append(transform_discount(dc, order_id, order_name, created_at))

        url = resp.links.get("next", {}).get("url")
        params = None

    print(f"  ✓ {label}: {len(orders):,} orders, {len(line_items):,} items, {len(refunds):,} refunds")
    return orders, line_items, refunds, discounts

def process_month(args):
    """Worker function: fetch month then load to BQ."""
    month_start, month_end, label = args
    try:
        orders, line_items, refunds, discounts = fetch_month(month_start, month_end, label)
        if orders:
            load_to_bq("shopify_orders",     orders,     ORDERS_SCHEMA)
            load_to_bq("shopify_line_items", line_items, LINE_ITEMS_SCHEMA)
            load_to_bq("shopify_refunds",    refunds,    REFUNDS_SCHEMA)
            load_to_bq("shopify_discounts",  discounts,  DISCOUNTS_SCHEMA)
        return label, len(orders), None
    except Exception as e:
        return label, 0, str(e)

# ── REST paginator for customers/products ─────────────────────────────────────
def rest_paginate(endpoint, key):
    sess = make_session()
    url = f"{REST_URL}/{endpoint}"
    params = {"limit": 250}
    total = 0
    while url:
        resp = sess.get(url, params=params, timeout=30)
        if resp.status_code == 429:
            time.sleep(float(resp.headers.get("Retry-After", 2)))
            continue
        resp.raise_for_status()
        batch = resp.json().get(key, [])
        if not batch:
            break
        total += len(batch)
        print(f"\r  {total:,} {key}   ", end="", flush=True)
        yield from batch
        url = resp.links.get("next", {}).get("url")
        params = None
    print()

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print(f"🚀 Terra Shopify Parallel Backfill → {BQ_PROJECT}.{BQ_DATASET}.*")

    # ── Step 1: Truncate all tables first ────────────────────────────────────
    print("\n🗑️  Truncating existing tables...")
    for table, schema in [
        ("shopify_orders",     ORDERS_SCHEMA),
        ("shopify_line_items", LINE_ITEMS_SCHEMA),
        ("shopify_refunds",    REFUNDS_SCHEMA),
        ("shopify_discounts",  DISCOUNTS_SCHEMA),
    ]:
        table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{table}"
        try:
            bq.delete_table(table_id)
        except Exception:
            pass
        bq.create_table(bigquery.Table(table_id, schema=schema))
        print(f"  ✓ {table_id} ready")

    # ── Step 2: Build monthly chunks ──────────────────────────────────────────
    now = datetime.now(timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    chunks = []
    cursor = HISTORY_START
    while cursor <= now:
        next_month = cursor + relativedelta(months=1)
        label = cursor.strftime("%Y-%m")
        chunks.append((cursor, next_month, label))
        cursor = next_month

    print(f"\n📦 Fetching orders: {len(chunks)} monthly chunks, {PARALLEL_WORKERS} parallel workers")
    start = time.time()
    total_orders = 0
    errors = []

    with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as executor:
        futures = {executor.submit(process_month, chunk): chunk[2] for chunk in chunks}
        for future in as_completed(futures):
            label, count, error = future.result()
            if error:
                errors.append(f"{label}: {error}")
                print(f"  ✗ {label}: ERROR — {error}")
            else:
                total_orders += count

    elapsed = time.time() - start
    print(f"\n  Total: {total_orders:,} orders in {elapsed:.1f}s")

    if errors:
        print(f"\n⚠️  {len(errors)} chunk(s) failed:")
        for e in errors:
            print(f"  - {e}")

    # ── Step 3: Customers ─────────────────────────────────────────────────────
    print("\n👥 Fetching customers...")
    customers = []
    for c in rest_paginate("customers.json", "customers"):
        da  = c.get("default_address") or {}
        emc = c.get("email_marketing_consent") or {}
        smc = c.get("sms_marketing_consent") or {}
        customers.append({
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
        })
    load_to_bq("shopify_customers", customers, CUSTOMERS_SCHEMA, bigquery.WriteDisposition.WRITE_TRUNCATE)
    print(f"  ✅ {BQ_PROJECT}.{BQ_DATASET}.shopify_customers — {len(customers):,} rows")

    # ── Step 4: Products + Variants ───────────────────────────────────────────
    print("\n🛍️  Fetching products...")
    products, variants = [], []
    for p in rest_paginate("products.json", "products"):
        products.append({
            "product_id": s(p.get("id")), "title": p.get("title"), "handle": p.get("handle"),
            "product_type": p.get("product_type"), "vendor": p.get("vendor"),
            "status": p.get("status"), "tags": p.get("tags"),
            "created_at": p.get("created_at"), "updated_at": p.get("updated_at"),
            "published_at": p.get("published_at"), "body_html": p.get("body_html"),
        })
        for v in (p.get("variants") or []):
            variants.append({
                "variant_id": s(v.get("id")), "product_id": s(p.get("id")),
                "title": v.get("title"), "option1": v.get("option1"),
                "option2": v.get("option2"), "option3": v.get("option3"),
                "sku": v.get("sku"), "price": f(v.get("price")),
                "compare_at_price": f(v.get("compare_at_price")),
                "weight": f(v.get("weight")), "weight_unit": v.get("weight_unit"),
                "inventory_quantity": v.get("inventory_quantity"),
                "inventory_management": v.get("inventory_management"),
                "inventory_policy": v.get("inventory_policy"),
                "fulfillment_service": v.get("fulfillment_service"),
                "taxable": v.get("taxable"), "barcode": v.get("barcode"),
                "requires_shipping": v.get("requires_shipping"),
                "created_at": v.get("created_at"), "updated_at": v.get("updated_at"),
            })
    load_to_bq("shopify_products", products, PRODUCTS_SCHEMA, bigquery.WriteDisposition.WRITE_TRUNCATE)
    load_to_bq("shopify_variants", variants, VARIANTS_SCHEMA, bigquery.WriteDisposition.WRITE_TRUNCATE)
    print(f"  ✅ {BQ_PROJECT}.{BQ_DATASET}.shopify_products — {len(products):,} rows")
    print(f"  ✅ {BQ_PROJECT}.{BQ_DATASET}.shopify_variants — {len(variants):,} rows")

    print(f"\n✅ Backfill complete in {(time.time()-start)/60:.1f} minutes")

if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("❌ Fatal error:")
        traceback.print_exc()
        sys.exit(1)
