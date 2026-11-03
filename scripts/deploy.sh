#!/usr/bin/env bash
# Deploy the lakehouse to your AWS account and run it once.
#   ./scripts/deploy.sh                 # uses your default AWS CLI profile/region
#   ALERT_EMAIL=me@x.com ./scripts/deploy.sh
# Cost: a few cents per run with 12 months of data (see README "Cost").
set -euo pipefail
cd "$(dirname "$0")/.."

STACK=${STACK:-divvy-lakehouse}
MONTHS=${MONTHS:-2025-10:2026-09}

echo "1/5 Deploying CloudFormation stack $STACK"
aws cloudformation deploy --stack-name "$STACK" --template-file infra/template.yaml \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides ProjectName="$STACK" AlertEmail="${ALERT_EMAIL:-}"

out() { aws cloudformation describe-stacks --stack-name "$STACK" \
  --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" --output text; }
RAW=$(out RawBucketName); ART=$(out ArtifactsBucketName); JOB=$(out GlueJobName); CRAWLER=$(out CrawlerName)

echo "2/5 Uploading Glue script + shared library"
rm -f /tmp/divvy_lakehouse.zip && (cd src && zip -qr /tmp/divvy_lakehouse.zip divvy_lakehouse)
aws s3 cp glue_jobs/raw_to_curated.py "s3://$ART/glue/raw_to_curated.py"
aws s3 cp /tmp/divvy_lakehouse.zip "s3://$ART/glue/divvy_lakehouse.zip"

echo "3/5 Ingesting Divvy months $MONTHS into s3://$RAW"
PYTHONPATH=src python -m divvy_lakehouse.ingest --months "$MONTHS" --dest "s3://$RAW"

echo "4/5 Crawling raw zone + running the ETL job"
aws glue start-crawler --name "$CRAWLER"
RUN=$(aws glue start-job-run --job-name "$JOB" --query JobRunId --output text)
while true; do
  STATE=$(aws glue get-job-run --job-name "$JOB" --run-id "$RUN" --query JobRun.JobRunState --output text)
  echo "   job $RUN: $STATE"; [[ "$STATE" =~ ^(SUCCEEDED|FAILED|TIMEOUT|ERROR|STOPPED)$ ]] && break; sleep 30
done
[[ "$STATE" == SUCCEEDED ]] || { echo "Glue job did not succeed"; exit 1; }

echo "5/5 Done. Open Athena, pick workgroup '$STACK-wg', database 'divvy_lake', and run sql/athena/02_*.sql"
