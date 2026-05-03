#!/usr/bin/env bash
set -e

ENV=${1:-dev}

if [ "$ENV" == "dev" ]; then
  PROJECT="terra-analytics-dev"
elif [ "$ENV" == "prod" ]; then
  PROJECT="terra-analytics-prod"
else
  echo "❌ Unknown environment: $ENV"
  echo "Usage: ./deploy.sh [dev|prod]"
  exit 1
fi

SERVICE="terra-shopify-etl"
JOB="${SERVICE}-${ENV}"
IMAGE="gcr.io/${PROJECT}/${JOB}:latest"
REGION="us-central1"

# ── Git check ─────────────────────────────────────────────────────────────────
if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "⚠️  Uncommitted changes detected. Commit before deploying."
  git status --short
  exit 1
fi

echo "🌍 Environment : $ENV"
echo "📦 Project     : $PROJECT"
echo "🐳 Image       : $IMAGE"
echo "🚀 Job         : $JOB"
echo ""

echo "🔨 Building image..."
docker build -t "$IMAGE" .

echo "📤 Pushing image..."
docker push "$IMAGE"

echo "🚀 Deploying Cloud Run job..."
gcloud run jobs deploy "$JOB" \
  --image "$IMAGE" \
  --region "$REGION" \
  --project "$PROJECT" \
  --task-timeout 3600 \
  --memory 512Mi \
  --cpu 1 \
  --set-env-vars "BQ_PROJECT=${PROJECT}" \
  --set-secrets "SHOPIFY_ACCESS_TOKEN=shopify-access-token:latest,SHOPIFY_STORE=shopify-store:latest" \
  --quiet

echo ""
echo "✅ Deployed $JOB to Cloud Run ($ENV)"
echo ""
echo "To run now:"
echo "  gcloud run jobs execute $JOB --region $REGION --project $PROJECT"
