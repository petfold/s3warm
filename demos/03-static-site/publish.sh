#!/usr/bin/env bash
# Publish a static site with the S3 workflow you already have
# (`aws s3 sync`), then browse it Swarm-natively: the bucket's commit root
# is a real manifest any Bee node serves via /bzz — no gateway needed.
set -euo pipefail
cd "$(dirname "$0")"

: "${S3WARM_ENDPOINT:=http://localhost:8333}"
: "${BEE_ENDPOINT:=http://localhost:1633}"
: "${AWS_ACCESS_KEY_ID:=s3warmdev}"
: "${AWS_SECRET_ACCESS_KEY:=s3warmdevsecret}"
: "${AWS_DEFAULT_REGION:=us-east-1}"
export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_DEFAULT_REGION
aws() { command aws --endpoint-url "$S3WARM_ENDPOINT" "$@"; }

BUCKET=${SITE_BUCKET:-site-demo}

echo "==> Deploy the site (plain aws s3 sync)"
aws s3 mb "s3://$BUCKET" 2>/dev/null || true
aws s3 sync site "s3://$BUCKET" --delete

echo "==> Wait for the bucket commit (a Swarm manifest of the whole site)"
ROOT=""
for _ in $(seq 1 15); do
  ROOT=$(curl -s -I --aws-sigv4 "aws:amz:$AWS_DEFAULT_REGION:s3" \
       --user "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY" \
       "$S3WARM_ENDPOINT/$BUCKET" | tr -d '\r' | awk 'tolower($1)=="x-swarm-bucket-root:"{print $2}')
  [ -n "$ROOT" ] && break
  sleep 1
done
if [ -z "$ROOT" ]; then
  echo "no commit root yet — is the gateway running with -commit async (default)?" >&2
  exit 1
fi

echo
echo "site root: $ROOT"
echo "browse it on ANY Bee node or public Swarm gateway:"
echo "  $BEE_ENDPOINT/bzz/$ROOT/index.html"
echo "  https://gateway.ethswarm.org/bzz/$ROOT/index.html"
echo

if curl -sf -o /dev/null "$BEE_ENDPOINT/bzz/$ROOT/index.html"; then
  echo "==> Verified: index.html served natively via bzz://"
  curl -s -I "$BEE_ENDPOINT/bzz/$ROOT/index.html" | grep -i '^content-type' || true
else
  echo "(bzz check skipped: $BEE_ENDPOINT is not a full Bee node — the fake dev"
  echo " backend has no /bzz. Run against a real node to browse the site.)"
fi
