#!/usr/bin/env python3
"""The one-line switch: take an existing boto3 app and point it at Swarm.

The ONLY change versus talking to AWS is the client construction — an
endpoint_url (and your s3warm credentials). Everything below it is the same
code you already run: upload, list, download, presigned share links.
"""

import os
import urllib.request

import boto3
from botocore.config import Config

# ---- the only part that changes when you move from AWS to Swarm ----------
s3 = boto3.client(
    "s3",
    endpoint_url=os.environ.get("S3WARM_ENDPOINT", "http://localhost:8333"),
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "s3warmdev"),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "s3warmdevsecret"),
    region_name="us-east-1",
    # boto3 falls back to legacy SigV2 presigned URLs on custom endpoints;
    # s3warm (like modern S3) speaks SigV4.
    config=Config(signature_version="s3v4"),
)
# ---------------------------------------------------------------------------

bucket = f"boto3-demo-{os.getpid()}"

print("==> create bucket")
s3.create_bucket(Bucket=bucket)

print("==> upload with metadata")
s3.put_object(
    Bucket=bucket,
    Key="reports/q3.txt",
    Body=b"quarterly numbers: all up and to the right\n",
    ContentType="text/plain",
    Metadata={"author": "demo"},
)

print("==> list")
for obj in s3.list_objects_v2(Bucket=bucket).get("Contents", []):
    print(f"    {obj['Key']}  {obj['Size']}B  {obj['ETag']}")

print("==> download + metadata round-trip")
got = s3.get_object(Bucket=bucket, Key="reports/q3.txt")
assert got["Metadata"] == {"author": "demo"}, got["Metadata"]
print("    body:", got["Body"].read().decode().strip())

print("==> presigned share link (works in any browser, expires in an hour)")
url = s3.generate_presigned_url(
    "get_object", Params={"Bucket": bucket, "Key": "reports/q3.txt"}, ExpiresIn=3600
)
print("   ", url)
with urllib.request.urlopen(url) as resp:  # no credentials — the URL carries them
    assert resp.status == 200
    print("    fetched via presigned URL without credentials: OK")

print("==> clean up")
s3.delete_object(Bucket=bucket, Key="reports/q3.txt")
s3.delete_bucket(Bucket=bucket)
print("done.")
