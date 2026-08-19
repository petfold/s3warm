#!/usr/bin/env bash
# Folder backup to Swarm with rclone, then a full restore — the classic
# S3 backup workflow, no rclone flags beyond the endpoint.
set -euo pipefail

: "${S3WARM_ENDPOINT:=http://localhost:8333}"
: "${AWS_ACCESS_KEY_ID:=s3warmdev}"
: "${AWS_SECRET_ACCESS_KEY:=s3warmdevsecret}"
export RCLONE_S3_PROVIDER=Other \
       RCLONE_S3_ENDPOINT="$S3WARM_ENDPOINT" \
       RCLONE_S3_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
       RCLONE_S3_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
       RCLONE_S3_REGION=us-east-1

BUCKET=backup-demo-$RANDOM
# Work under the current directory: sandboxed rclone installs (snap) cannot
# read /tmp.
WORK=$(mktemp -d -p "$PWD" .backup-demo-XXXXXX) && trap 'rm -rf "$WORK"' EXIT

echo "==> Make a working tree"
mkdir -p "$WORK/project/docs"
echo "important report"        > "$WORK/project/report.txt"
echo "meeting notes"           > "$WORK/project/docs/notes.md"
head -c 65536 /dev/urandom     > "$WORK/project/data.bin"

echo "==> Back it up"
rclone sync "$WORK/project" ":s3:$BUCKET/project"
rclone ls ":s3:$BUCKET"

echo "==> Keep working; back up again (only changes transfer)"
echo "updated report" > "$WORK/project/report.txt"
rclone sync "$WORK/project" ":s3:$BUCKET/project" -v 2>&1 | grep -E 'report.txt|Transferred' | head -4

echo "==> Disaster strikes: restore into a fresh directory"
rclone copy ":s3:$BUCKET/project" "$WORK/restored"
diff -r "$WORK/project" "$WORK/restored" && echo "restore is byte-identical"

echo "==> Clean up"
rclone purge ":s3:$BUCKET"
echo "done."
