#!/usr/bin/env python3
import os
import sys
import time
import json
import traceback
import requests
from datetime import datetime
import pytz
from urllib.parse import urlparse, parse_qs
from google.cloud import storage, bigquery
from requests.adapters import HTTPAdapter, Retry

def fail(msg):
    print(f"❌ {msg}", file=sys.stderr)
    sys.exit(1)

# ─── Load config.ini ───────────────────────────────────────
import configparser
config = configparser.ConfigParser()
config.read("config.ini")

SHOPIFY_STORE = config["shopify"]["store"]
ACCESS_TOKEN  = config["shopify"]["access_token"]
API_VERSION   = config["shopify"].get("api_version", "2025-04")

# Get store timezone from config or default to Pacific Time
STORE_TIMEZONE = config["shopify"].get("timezone", "America/Los_Angeles")
try:
    tz = pytz.timezone(STORE_TIMEZONE)
except pytz.exceptions.UnknownTimeZoneError:
    fail(f"Unknown timezone: {STORE_TIMEZONE}. Please use a valid timezone name.")

# Process date range from config
try:
    # Get raw dates from config
    local_start = config["dates"]["start_date"]  # Changed from "start" to "start_date"
    local_end = config["dates"]["end_date"]     # Changed from "end" to "end_date"

    # Handle different date formats
    def parse_local_datetime(dt_str):
        # Remove Z if present (we're treating config dates as local time)
        dt_str = dt_str.replace('Z', '')

        # Handle ISO format with or without T separator
        if 'T' in dt_str:
            dt = datetime.fromisoformat(dt_str)
        else:
            # If only date is provided, use start of day for start date
            # or end of day for end date
            if dt_str == local_start:
                dt = datetime.fromisoformat(f"{dt_str}T00:00:00")
            else:
                dt = datetime.fromisoformat(f"{dt_str}T23:59:59")
        return dt

    # Parse dates and make timezone-aware
    local_dt_start = parse_local_datetime(local_start)
    local_dt_end = parse_local_datetime(local_end)

    # Make datetimes timezone-aware
    local_dt_start = tz.localize(local_dt_start, is_dst=None)
    local_dt_end = tz.localize(local_dt_end, is_dst=None)

    # Convert to UTC
    utc_dt_start = local_dt_start.astimezone(pytz.UTC)
    utc_dt_end = local_dt_end.astimezone(pytz.UTC)

    # Format for the API
    START_DATE = utc_dt_start.strftime("%Y-%m-%dT%H:%M:%SZ")
    END_DATE = utc_dt_end.strftime("%Y-%m-%dT%H:%M:%SZ")

    print(f"📅 Store time ({STORE_TIMEZONE}): {local_dt_start.strftime('%Y-%m-%d %H:%M:%S')} to {local_dt_end.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🌐 UTC for API: {START_DATE} to {END_DATE}")

except Exception as e:
    fail(f"Date parsing error: {str(e)}. Use format like 2024-03-15 or 2024-03-15T00:00:00")

GCS_BUCKET    = config["gcs"]["bucket"]

BQ_PROJECT    = config["bigquery"]["project"]
BQ_DATASET    = config["bigquery"]["dataset"]
BQ_TABLE      = config["bigquery"]["table"]
# ───────────────────────────────────────────────────────────

# HTTP session with retries
session = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[429,500,502,503,504], allowed_methods=["GET"])
session.mount("https://", HTTPAdapter(max_retries=retries))
session.headers.update({
    "X-Shopify-Access-Token": ACCESS_TOKEN,
    "Content-Type": "application/json"
})

def get_expected_count(start, end):
    url = f"https://{SHOPIFY_STORE}/admin/api/{API_VERSION}/orders/count.json"
    params = {
        "status": "any",
        "created_at_min": start,
        "created_at_max": end
    }
    resp = session.get(url, params=params, timeout=30)
    resp.raise_for_status()
    count = resp.json().get("count", 0)
    print(f"📝 Expected order count: {count}")
    return count

def stream_orders():
    # Get the expected count from the global scope
    expected_count = get_expected_count(START_DATE, END_DATE)

    url = f"https://{SHOPIFY_STORE}/admin/api/{API_VERSION}/orders.json"
    params = {
        "limit": 250,
        "status": "any",
        "order": "created_at asc",
        "created_at_min": START_DATE,
        "created_at_max": END_DATE
    }

    # Add progress tracking
    page_count = 0
    order_count = 0
    start_time = time.time()
    last_update = start_time

    print(f"⏳ Starting order extraction from Shopify...")
    seen = set()

    while True:
        # Show spinner during API calls
        spinner = "|/-\\"
        print(f"\r🔄 Fetching page {page_count+1} {spinner[page_count % 4]}", end="")

        # Make the API call
        resp = session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        batch = resp.json().get("orders", [])

        if not batch:
            break

        # Update counters
        page_count += 1
        order_count += len(batch)
        current_time = time.time()

        # Clear spinner and show progress
        if current_time - last_update >= 2 or page_count == 1:
            elapsed = current_time - start_time
            rate = order_count / elapsed if elapsed > 0 else 0
            eta = ((expected_count - order_count) / rate) if rate > 0 and expected_count > 0 else "unknown"
            eta_str = f"{int(eta/60)}m {int(eta%60)}s" if isinstance(eta, (int, float)) else eta

            print(f"\r📄 Page {page_count}: {len(batch)} orders | Total: {order_count:,}/{expected_count:,} ({order_count/expected_count*100:.1f}%) | Rate: {rate:.1f} orders/sec | ETA: {eta_str}      ")
            last_update = current_time

        yield from batch

        # Check for next page
        next_url = resp.links.get("next", {}).get("url")
        if not next_url or next_url in seen:
            break
        seen.add(next_url)
        url = next_url
        params = None

        # Respect rate limits but don't slow down too much
        time.sleep(0.5)

    # Final progress update
    print(f"\r✅ Extraction complete: {order_count:,} orders across {page_count} pages in {time.time() - start_time:.1f} seconds                                ")

def main():
    try:
        # 1) Determine total
        expected = get_expected_count(START_DATE, END_DATE)

        # 2) Dump to JSONL - use table name for the file
        out = f"{BQ_TABLE}.jsonl"  # Use the table name from config
        count = 0
        print(f"⏱️  Starting dump of up to {expected} orders…")

        # Use a completely different approach for writing the file
        orders_list = list(stream_orders())[:expected]
        count = len(orders_list)

        # Clean the orders for BigQuery compatibility
        print("🔄 Sanitizing order data for BigQuery...")
        cleaned_orders = []
        for order in orders_list:
            # Create a clean copy, converting problematic values
            def sanitize(obj):
                if isinstance(obj, dict):
                    return {k: sanitize(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return [sanitize(item) for item in obj]
                # Convert large integers to strings - these often cause BigQuery parsing issues
                elif isinstance(obj, int) and (obj > 9007199254740991 or obj < -9007199254740991):
                    return str(obj)
                else:
                    return obj

            # Apply sanitization to the whole order object
            clean_order = sanitize(order)
            cleaned_orders.append(clean_order)

        # Write with precise control over the format
        with open(out, "w") as f:
            for i, order in enumerate(cleaned_orders):
                # Add newlines between objects but not at the end
                if i > 0:
                    f.write("\n")
                # Use strict JSON formatting
                f.write(json.dumps(order, ensure_ascii=True))

        print(f"✅ Dumped {count:,} orders to {out}")

        # 3) Ensure GCS bucket exists & upload
        storage_client = storage.Client(project=BQ_PROJECT)
        try:
            bucket = storage_client.get_bucket(GCS_BUCKET)
        except Exception:
            bucket = storage_client.create_bucket(GCS_BUCKET)
            print(f"✅ Created GCS bucket {GCS_BUCKET}")
        blob = bucket.blob(out)
        blob.upload_from_filename(out)
        print(f"✅ Uploaded {out} to gs://{GCS_BUCKET}/{out}")

        # 4) Ensure BigQuery dataset exists
        bq = bigquery.Client(project=BQ_PROJECT)
        ds_ref = bigquery.DatasetReference(BQ_PROJECT, BQ_DATASET)
        try:
            bq.get_dataset(ds_ref)
        except Exception:
            bq.create_dataset(bigquery.Dataset(ds_ref))
            print(f"✅ Created dataset {BQ_PROJECT}.{BQ_DATASET}")

        # 5) Load JSONL into BigQuery
        tbl_ref = ds_ref.table(BQ_TABLE)

        # Use explicit schema instead of autodetect
        schema = [
            bigquery.SchemaField("id", "STRING"),  # Use STRING instead of INTEGER for IDs
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("updated_at", "TIMESTAMP"),
            bigquery.SchemaField("number", "INTEGER"),
            bigquery.SchemaField("note", "STRING"),
            bigquery.SchemaField("token", "STRING"),
            bigquery.SchemaField("gateway", "STRING"),
            bigquery.SchemaField("test", "BOOLEAN"),
            bigquery.SchemaField("total_price", "FLOAT"),
            bigquery.SchemaField("subtotal_price", "FLOAT"),
            bigquery.SchemaField("total_tax", "FLOAT"),
            bigquery.SchemaField("currency", "STRING"),
            bigquery.SchemaField("financial_status", "STRING"),
            bigquery.SchemaField("confirmed", "BOOLEAN"),
            bigquery.SchemaField("total_discounts", "FLOAT"),
            bigquery.SchemaField("total_line_items_price", "FLOAT"),
            bigquery.SchemaField("order_number", "INTEGER"),
            # Fix discount_codes to be a RECORD type
            bigquery.SchemaField("discount_codes", "RECORD", mode="REPEATED", fields=[
                bigquery.SchemaField("code", "STRING"),
                bigquery.SchemaField("amount", "FLOAT"),
                bigquery.SchemaField("type", "STRING")
            ]),
            bigquery.SchemaField("line_items", "RECORD", mode="REPEATED", fields=[
                bigquery.SchemaField("id", "STRING"),
                bigquery.SchemaField("variant_id", "STRING"),
                bigquery.SchemaField("title", "STRING"),
                bigquery.SchemaField("quantity", "INTEGER"),
                bigquery.SchemaField("price", "FLOAT"),
                bigquery.SchemaField("sku", "STRING"),
                bigquery.SchemaField("name", "STRING")
            ])
        ]

        job_cfg = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            ignore_unknown_values=True,
            schema=schema,  # Use the explicit schema defined above
            autodetect=False  # Turn off autodetect since we're defining schema
        )

        # Load from GCS
        gcs_uri = f"gs://{GCS_BUCKET}/{out}"
        print(f"🔄 Loading from {gcs_uri} into BigQuery…")
        load_job = bq.load_table_from_uri(
            gcs_uri,
            tbl_ref,
            job_config=job_cfg
        )

        print("🔄 Loading into BigQuery…")
        load_job.result()
        print(f"✅ Loaded {load_job.output_rows:,} rows into {BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}")

        # Use output configuration to determine table name
        output_split = config["output"]["split_by"]
        output_prefix = config["output"]["prefix"]

        if output_split == "month":
            # Group orders by month and create separate tables
            orders_by_month = {}
            for order in cleaned_orders:
                # Extract month from created_at field
                created_at = order.get("created_at", "")
                if created_at:
                    month = created_at[:7]  # Gets YYYY-MM format
                    if month not in orders_by_month:
                        orders_by_month[month] = []
                    orders_by_month[month].append(order)

            # Process each month separately
            for month, month_orders in orders_by_month.items():
                month_table = f"{output_prefix}{month.replace('-', '_')}"  # e.g. orders_2024_06
                month_file = f"{month_table}.jsonl"
                print(f"📅 Processing {len(month_orders)} orders for {month}...")

                # Write this month's orders to JSONL
                with open(month_file, "w") as f:
                    for i, order in enumerate(month_orders):
                        if i > 0:
                            f.write("\n")
                        f.write(json.dumps(order, ensure_ascii=True))

                # Upload to GCS
                blob = bucket.blob(month_file)
                blob.upload_from_filename(month_file)
                print(f"✅ Uploaded {month_file} to gs://{GCS_BUCKET}/{month_file}")

                # Load into BigQuery with month-specific table name
                month_tbl_ref = ds_ref.table(month_table)
                month_gcs_uri = f"gs://{GCS_BUCKET}/{month_file}"

                load_job = bq.load_table_from_uri(
                    month_gcs_uri,
                    month_tbl_ref,
                    job_config=job_cfg
                )
                print(f"🔄 Loading {month} data into BigQuery table {month_table}...")
                load_job.result()
                print(f"✅ Loaded {load_job.output_rows:,} rows into {BQ_PROJECT}.{BQ_DATASET}.{month_table}")

    except Exception:
        print("❌ Fatal error:")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

