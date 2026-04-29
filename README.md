# terra-shopify-etl

Pulls Shopify orders, line items, discounts, refunds, customers, products, variants, transactions, and discount catalog → `terra-analytics-dev.sources.*` (promote to `terra-analytics-prod` when ready).

Runs daily at 6am PT as a Cloud Run Job in `terra-analytics-dev`.

## Tables produced

| Table | Method | Mode |
|---|---|---|
| `shopify_orders` | Bulk GraphQL (backfill) / REST (incremental) | TRUNCATE / APPEND |
| `shopify_line_items` | Bulk GraphQL | TRUNCATE |
| `shopify_discounts` | Bulk GraphQL | TRUNCATE |
| `shopify_refunds` | Bulk GraphQL | TRUNCATE |
| `shopify_customers` | REST | TRUNCATE |
| `shopify_products` | REST | TRUNCATE |
| `shopify_variants` | REST | TRUNCATE |
| `shopify_transactions` | REST (per-order) | TRUNCATE / APPEND |
| `shopify_discount_catalog` | GraphQL Bulk | TRUNCATE |

## Local development

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Add credentials to `config.ini` (gitignored):

```ini
[shopify]
store        = terra-health-essentials.myshopify.com
access_token = YOUR_ACCESS_TOKEN
api_version  = 2025-04
```

```bash
# Incremental — last 2 days of orders + full refresh of everything else
python shopify_to_bigquery.py --mode incremental

# Backfill — full history via Bulk GraphQL, TRUNCATE
python shopify_to_bigquery.py --mode backfill
```

## Cloud Run deployment

Credentials come from Secret Manager (`shopify-store`, `shopify-access-token`). BigQuery uses the job's service account (`terra-etl-runner@terra-analytics-dev.iam.gserviceaccount.com`).

```bash
./deploy.sh
```

This builds the image via Cloud Build, creates/updates the Cloud Run Job, and creates/updates the Cloud Scheduler trigger (daily 6am PT).

## Auth

- **Local**: static access token in `config.ini`
- **Cloud Run**: secrets injected as env vars from Secret Manager; BigQuery via service account ADC
