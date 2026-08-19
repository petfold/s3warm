#!/usr/bin/env bash
# First contact: a bucket and an object through the AWS CLI, unchanged
# except for --endpoint-url. Along the way: the object's permanent Swarm
# reference.
set -euo pipefail

: "${S3WARM_ENDPOINT:=http://localhost:8333}"
: "${AWS_ACCESS_KEY_ID:=s3warmdev}"
: "${AWS_SECRET_ACCESS_KEY:=s3warmdevsecret}"
: "${AWS_DEFAULT_REGION:=us-east-1}"
export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_DEFAULT_REGION
aws() { command aws --endpoint-url "$S3WARM_ENDPOINT" "$@"; }

BUCKET=quickstart-$RANDOM
WORK=$(mktemp -d) && trap 'rm -rf "$WORK"' EXIT

echo "==> Create a bucket"
aws s3 mb "s3://$BUCKET"

echo "==> Upload a file"
echo "hello, swarm — $(date -u)" > "$WORK/hello.txt"
aws s3 cp "$WORK/hello.txt" "s3://$BUCKET/greetings/hello.txt"

echo "==> List it"
aws s3 ls "s3://$BUCKET/greetings/"

echo "==> Download it back and compare"
aws s3 cp "s3://$BUCKET/greetings/hello.txt" "$WORK/back.txt"
diff "$WORK/hello.txt" "$WORK/back.txt" && echo "content matches"

echo "==> The Swarm-native identity of the object (x-swarm-* headers)"
curl -s -I --aws-sigv4 "aws:amz:$AWS_DEFAULT_REGION:s3" \
     --user "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY" \
     "$S3WARM_ENDPOINT/$BUCKET/greetings/hello.txt" | grep -i '^x-swarm' || true

echo "==> Clean up"
aws s3 rm "s3://$BUCKET/greetings/hello.txt"
aws s3 rb "s3://$BUCKET"
echo "done."
