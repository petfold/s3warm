# S3 API compatibility matrix

Status legend: ✅ implemented (skeleton) · 🎯 planned, phase noted · 🪧 stubbed (accepted, fixed response) · ❌ deliberately unsupported

This file is the living checklist; update it with every change to the API surface.
Design rationale: [`DESIGN.md`](DESIGN.md).

## Service operations

| Operation | Status | Notes |
|---|---|---|
| ListBuckets | ✅ | |

## Bucket operations

| Operation | Status | Notes |
|---|---|---|
| CreateBucket | ✅ | S3/DNS name validation; `x-swarm-postage-batch-id` binds a bucket default batch (🎯 P1) |
| DeleteBucket | ✅ | 409 `BucketNotEmpty` when non-empty |
| HeadBucket | ✅ | |
| GetBucketLocation | ✅ | Returns configured region |
| ListObjectsV2 | ✅ | prefix, delimiter, max-keys, continuation-token, start-after, encoding-type |
| ListObjects (V1) | ✅ | marker/NextMarker semantics |
| GetBucketVersioning | 🪧 | Empty config (unversioned) until P3 |
| PutBucketVersioning | 🎯 P3 | |
| ListObjectVersions | 🎯 P3 | |
| GetBucketAcl / PutBucketAcl | 🪧 P2 | Canned `private` returned; mutation rejected |
| GetBucketPolicy / PutBucketPolicy | 🎯 P3 | Grant-subset only, not full IAM policy language |
| GetBucketCors / PutBucketCors | 🎯 P2 | Gateway-level CORS |
| GetBucketLifecycleConfiguration | 🎯 P2 | Read-only synthetic rule derived from postage batch TTL |
| PutBucketLifecycleConfiguration | ❌ | Expiry is postage TTL; extend by topping up the batch |
| GetBucketTagging / PutBucketTagging | 🎯 P3 | |
| ListMultipartUploads | 🎯 P2 | |
| Website / Replication / Inventory / Analytics / Metrics / Accelerate / Logging / Notification / RequestPayment / ObjectLock config | ❌ | Website = native `bzz://`; replication is inherent to Swarm |

## Object operations

| Operation | Status | Notes |
|---|---|---|
| PutObject | ✅ | Streaming; MD5 ETag; `Content-MD5` + `x-amz-content-sha256` enforced; `x-amz-meta-*`; zero-byte objects; `x-swarm-reference` response header |
| GetObject | ✅ | Range pass-through; conditional headers; composite stitching 🎯 P2 |
| HeadObject | ✅ | |
| DeleteObject | ✅ | Index removal; bytes expire with the postage batch |
| DeleteObjects (batch) | ✅ | Quiet mode supported |
| CopyObject | ✅ | O(1) — same Swarm reference; `x-amz-metadata-directive` COPY/REPLACE; `?versionId` source 🎯 P3 |
| CreateMultipartUpload | 🎯 P2 | |
| UploadPart | 🎯 P2 | Parts stream straight to `/bytes`, no staging |
| UploadPartCopy | 🎯 P2 | Whole-object O(1); byte-range re-streams |
| CompleteMultipartUpload | 🎯 P2 | Composite object + S3 multipart ETag; optional async consolidation |
| AbortMultipartUpload | 🎯 P2 | Abandoned parts expire with stamps |
| ListParts | 🎯 P2 | |
| GetObjectAttributes | 🎯 P2 | |
| GetObjectTagging / PutObjectTagging / DeleteObjectTagging | 🎯 P3 | |
| GetObjectAcl / PutObjectAcl | 🪧 P2 | Canned `private` |
| RestoreObject / SelectObjectContent / GetObjectTorrent | ❌ | |
| Conditional PUT (`If-None-Match: *` / `If-Match`) | 🎯 P2 | Trivial via index constraint |

## Authentication

| Mechanism | Status | Notes |
|---|---|---|
| SigV4, Authorization header | ✅ | AWS doc test vectors in unit tests; any region label accepted (service must be `s3`) |
| SigV4 presigned URLs | 🎯 P2 | |
| `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` (aws-chunked) | 🎯 P2 | Required by Java SDK v2 defaults |
| Trailing checksums (`x-amz-checksum-*`) | 🎯 P2 | |
| SigV2 | ❌ | Legacy |
| Anonymous mode | ✅ | Explicit opt-in, dev only |

## Semantics extensions (`x-swarm-*`)

| Header | Direction | Meaning |
|---|---|---|
| `x-swarm-reference` | response | Swarm reference of the object (omitted for encrypted objects) |
| `x-swarm-postage-batch-id` | request | Batch override for this write / bucket default at CreateBucket |
| `x-swarm-batch-ttl` | response | 🎯 P1 — seconds until the object's batch expires |
