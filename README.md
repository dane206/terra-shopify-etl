# terra-shopify-etl

Pulls Shopify orders, line items, discounts, refunds, customers, products and variants → `terra-analytics-prod.sources.*`

## Tables produced

| Table | Method | Granularity |
|---|---|---|
| `shopify_orders` | Bulk GraphQL / REST incremental | one row per order |
| `shopify_line_items` | Bulk GraphQL | one row per line item |
| `shopify_discounts` | Bulk GraphQL | one row per discount application |
| `shopify_refunds` | Bulk GraphQL | one row per refund line item |
| `shopify_customers` | REST | one row per customer |
| `shopify_products` | REST | one row per product |
| `shopify_variants` | REST | one row per variant |

## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Add credentials to `config.ini`:

```ini
[shopify]
store        = terra-health-essentials.myshopify.com
access_token = YOUR_ACCESS_TOKEN
api_version  = 2026-01

[bigquery]
project = terra-analytics-prod
dataset = sources
```

## Usage

```bash
# Incremental — last 2 days of orders via REST + full customers/products refresh, APPEND
python shopify_to_bigquery.py --mode incremental

# Backfill — full history via Bulk GraphQL operations, TRUNCATE
python shopify_to_bigquery.py --mode backfill
```

Backfill runs two sequential bulk operations (orders + refunds are separate due to Shopify's 2-level nesting limit), then fetches customers and products via REST.

## Auth

Static access token in `config.ini`. BigQuery uses Application Default Credentials (ADC).
