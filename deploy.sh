#!/usr/bin/env bash
set -e

ENV=${1:-dev}

if [ "$ENV" == "dev" ]; then
  BQ_PROJECT="terra-analytics-dev"
elif [ "$ENV" == "prod" ]; then
  BQ_PROJECT="terra-analytics-prod"
else
  echo "❌ Unknown environment: $ENV"
  echo "Usage: ./deploy.sh [dev|prod]"
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG="$SCRIPT_DIR/config.ini"

echo "🌍 Environment: $ENV → $BQ_PROJECT"

sed -i '' "s/^project = .*/project = $BQ_PROJECT/" "$CONFIG"

echo "✅ config.ini updated: project = $BQ_PROJECT"
echo ""
echo "Run your script now, e.g.:"
echo "  python shopify_to_bigquery.py --mode incremental"
