#!/usr/bin/env bash
# Delete everything deploy.sh created. Buckets are emptied first (CloudFormation
# cannot delete a non-empty bucket). Irreversible.
set -euo pipefail
STACK=${STACK:-divvy-lakehouse}
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)

empty_bucket() {
  local b=$1
  aws s3api head-bucket --bucket "$b" 2>/dev/null || return 0
  echo "Emptying s3://$b"
  aws s3 rm "s3://$b" --recursive
  # Versioned buckets keep old versions and delete markers: remove them too.
  for kind in Versions DeleteMarkers; do
    while true; do
      batch=$(aws s3api list-object-versions --bucket "$b" --max-items 1000 \
        --query "{Objects: $kind[].{Key: Key, VersionId: VersionId}}" --output json)
      [[ "$batch" == *'"Key"'* ]] || break
      aws s3api delete-objects --bucket "$b" --delete "$batch" >/dev/null
    done
  done
}

for zone in raw curated athena-results artifacts; do
  empty_bucket "$STACK-$zone-$ACCOUNT"
done

aws cloudformation delete-stack --stack-name "$STACK"
aws cloudformation wait stack-delete-complete --stack-name "$STACK"
echo "Stack $STACK deleted."
