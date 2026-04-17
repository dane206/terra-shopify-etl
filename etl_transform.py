#!/usr/bin/env python3
"""
ETL transformer: reads Shopify orders JSONL and loads 3 clean tables to BigQuery.

Usage:
  python etl_transform.py --jsonl orders_2025_07.jsonl --config config/config.yaml

Tables produced:
  - orders
  - order_items
  - customers

Notes:
- This transforms the raw JSONL produced by shopify_orders_to_bigquery.py
- Requires: google-cloud-bigquery, PyYAML, pandas
- Install: pip install google-cloud-bigquery pyyaml pandas
"""
import argparse, json, os, sys
from pathlib import Path

try:
    import yaml
    import pandas as pd
    from google.cloud import bigquery
except Exception as e:
    print("Install dependencies: pip install google-cloud-bigquery pyyaml pandas")
    raise

def load_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)

def ensure_cols(df, cols):
    for c in cols:
        if c not in df.columns:
            df[c] = None
    return df[cols]

def parse_orders_jsonl(jsonl_path: str):
    orders = []
    items = []
    customers = {}

    with open(jsonl_path, "r") as f:
        for line in f:
            if not line.strip():
                continue
            o = json.loads(line)

            # --- orders ---
            orders.append({
                "order_id": str(o.get("id")),
                "order_name": o.get("name"),
                "customer_id": str((o.get("customer") or {}).get("id")) if o.get("customer") else None,
                "processed_at": o.get("processed_at"),
                "financial_status": (o.get("financial_status") or "").lower(),
                "cancel_reason": o.get("cancel_reason"),
                "cancelled_at": o.get("cancelled_at"),
                "current_total_price": o.get("current_total_price"),
                "current_total_tax": o.get("current_total_tax"),
                "current_total_discounts": o.get("current_total_discounts"),
                "total_shipping_price": (o.get("total_shipping_price_set") or {}).get("shop_money", {}).get("amount"),
                "currency": o.get("currency"),
                "is_test": bool(o.get("test")),
                "source_name": o.get("source_name"),
                "order_status_url": o.get("order_status_url"),
            })

            # --- customers ---
            c = o.get("customer")
            if c:
                cid = str(c.get("id"))
                if cid and cid not in customers:
                    customers[cid] = {
                        "customer_id": cid,
                        "email": c.get("email"),
                        "first_name": c.get("first_name"),
                        "last_name": c.get("last_name"),
                        "created_at": c.get("created_at"),
                        "is_subscriber": None,
                        "country_code": (o.get("billing_address") or {}).get("country_code"),
                        "state": (o.get("billing_address") or {}).get("province_code"),
                        "city": (o.get("billing_address") or {}).get("city"),
                        "postal_code": (o.get("billing_address") or {}).get("zip"),
                    }

            # --- line items ---
            for li in (o.get("line_items") or []):
                product_id = str(li.get("product_id")) if li.get("product_id") is not None else None
                variant_id = str(li.get("variant_id")) if li.get("variant_id") is not None else None
                sku = li.get("sku") or "unknown"
                item_id = f"shopify_US_{product_id}_{variant_id}" if product_id and variant_id else None
                item_group_id = f"shopify_US_{product_id}" if product_id else None

                disc = 0.0
                for da in (li.get("discount_allocations") or []):
                    try:
                        disc += float((da.get("amount_set") or {}).get("shop_money", {}).get("amount") or 0)
                    except Exception:
                        pass

                price = li.get("price")
                qty = li.get("quantity") or 0
                try:
                    price_f = float(price)
                except Exception:
                    price_f = None

                line_subtotal = None
                if price_f is not None:
                    try:
                        line_subtotal = price_f * float(qty) - float(disc or 0)
                    except Exception:
                        line_subtotal = None

                items.append({
                    "order_id": str(o.get("id")),
                    "line_id": str(li.get("id")) if li.get("id") is not None else None,
                    "product_id": product_id,
                    "variant_id": variant_id,
                    "sku": sku,
                    "item_id": item_id,
                    "item_group_id": item_group_id,
                    "item_name": li.get("name"),
                    "item_variant": li.get("variant_title"),
                    "quantity": qty,
                    "price": price,
                    "discount_allocations": disc,
                    "line_subtotal": line_subtotal,
                    "currency": o.get("currency")
                })

    return pd.DataFrame(list(customers.values())), pd.DataFrame(orders), pd.DataFrame(items)

def load_to_bigquery(cfg, df, table_name):
    client = bigquery.Client(project=cfg["bigquery"]["project_id"])
    dataset = cfg["bigquery"]["dataset"]
    table_id = f'{cfg["bigquery"]["project_id"]}.{dataset}.{table_name}'
    job = client.load_table_from_dataframe(df, table_id)
    job.result()
    print(f"Loaded {len(df)} rows into {table_id}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--jsonl", required=True, help="Path to Shopify orders JSONL")
    ap.add_argument("--config", default="config/config.yaml", help="Config YAML")
    args = ap.parse_args()

    if not os.path.exists(args.config):
        print("Missing config. Copy config/config.example.yaml to config/config.yaml and edit.")
        sys.exit(1)

    cfg = load_config(args.config)
    customers_df, orders_df, items_df = parse_orders_jsonl(args.jsonl)

    customers_cols = ["customer_id","email","first_name","last_name","created_at","is_subscriber",
                      "country_code","state","city","postal_code"]
    orders_cols = ["order_id","order_name","customer_id","processed_at","financial_status","cancel_reason",
                   "cancelled_at","current_total_price","current_total_tax","current_total_discounts",
                   "total_shipping_price","currency","is_test","source_name","order_status_url"]
    items_cols = ["order_id","line_id","product_id","variant_id","sku","item_id","item_group_id",
                  "item_name","item_variant","quantity","price","discount_allocations","line_subtotal","currency"]

    customers_df = ensure_cols(customers_df, customers_cols)
    orders_df    = ensure_cols(orders_df, orders_cols)
    items_df     = ensure_cols(items_df, items_cols)

    load_to_bigquery(cfg, customers_df, cfg["tables"]["customers"])
    load_to_bigquery(cfg, orders_df,    cfg["tables"]["orders"])
    load_to_bigquery(cfg, items_df,     cfg["tables"]["order_items"])

if __name__ == "__main__":
    main()
