#!/usr/bin/env bash
# The thing S3 cannot do: snapshot a bucket, wreck it, and roll the WHOLE
# bucket back atomically. Snapshots are labeled Swarm commit roots
# (docs/DESIGN.md §5) — one 32-byte reference is the entire bucket state.
set -euo pipefail

: "${S3WARM_ENDPOINT:=http://localhost:8333}"
: "${AWS_ACCESS_KEY_ID:=s3warmdev}"
: "${AWS_SECRET_ACCESS_KEY:=s3warmdevsecret}"
: "${AWS_DEFAULT_REGION:=us-east-1}"
export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_DEFAULT_REGION
aws() { command aws --endpoint-url "$S3WARM_ENDPOINT" "$@"; }
sig() { curl -sf --aws-sigv4 "aws:amz:$AWS_DEFAULT_REGION:s3" \
        --user "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY" "$@"; }

BUCKET=rollback-demo-$RANDOM
WORK=$(mktemp -d) && trap 'rm -rf "$WORK"' EXIT

echo "==> A healthy bucket"
aws s3 mb "s3://$BUCKET"
echo "the good config"   > "$WORK/config.yaml"
echo "precious data v1"  > "$WORK/data.txt"
aws s3 cp "$WORK/config.yaml" "s3://$BUCKET/config.yaml"
aws s3 cp "$WORK/data.txt"    "s3://$BUCKET/data.txt"

echo "==> Snapshot it (label: golden)"
sig -X PUT "$S3WARM_ENDPOINT/$BUCKET?x-swarm-snapshot=golden"; echo

echo "==> Now break everything"
echo "oops, corrupted" > "$WORK/bad.txt"
aws s3 cp "$WORK/bad.txt" "s3://$BUCKET/config.yaml"
aws s3 rm "s3://$BUCKET/data.txt"
aws s3 ls "s3://$BUCKET"

echo "==> One call: atomic whole-bucket rollback"
sig -X POST "$S3WARM_ENDPOINT/$BUCKET?x-swarm-restore=golden"; echo

echo "==> Everything is back"
aws s3 ls "s3://$BUCKET"
aws s3 cp "s3://$BUCKET/config.yaml" - | head -1
aws s3 cp "s3://$BUCKET/data.txt" -    | head -1

echo "==> Clean up"
aws s3 rm "s3://$BUCKET" --recursive
aws s3 rb "s3://$BUCKET"
echo "done."
