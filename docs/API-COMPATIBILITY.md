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
| GetBucketAcl / PutBucketAcl | 🎯 P3 | Grants map to the bucket's ACT grantee list (grantee = Ethereum public key, design §8); canned `private` until then |
| GetBucketPolicy / PutBucketPolicy | 🎯 P3 | Grant-subset only, not full IAM policy language |
| GetBucketCors / PutBucketCors | 🎯 P2 | Gateway-level CORS |
| GetBucketLifecycleConfiguration | 🎯 P2 | Read-only synthetic rule derived from postage batch TTL |
| PutBucketLifecycleConfiguration | ❌ | Expiry is postage TTL; extend by topping up the batch |
| GetBucketTagging / PutBucketTagging | 🎯 P3 | |
| ListMultipartUploads | 🎯 P2 | |
| Get/PutBucketNotificationConfiguration | 🎯 research | Candidate mapping to Swarm's native pub-sub (PSS/GSOC), design §21 |
| Website / Replication / Inventory / Analytics / Metrics / Accelerate / Logging / RequestPayment / ObjectLock config | ❌ | Website = native `bzz://`; replication is inherent to Swarm |

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

| Extension | Direction | Meaning |
|---|---|---|
| `x-swarm-reference` | response | Swarm reference of the object (omitted for encrypted objects) |
| `x-swarm-postage-batch-id` | request + response | Request: batch override for this write / bucket default at CreateBucket. Response: the batch that stamped the object (PUT/GET/HEAD) |
| `x-swarm-batch-ttl` | response | ✅ Estimated seconds until the object's batch expires (PUT/GET/HEAD) |
| `x-swarm-bucket-root` | response | 🎯 P2 — current commit root of the bucket (HeadBucket); capture it to snapshot |
| `x-swarm-commit-seq` | response | 🎯 P2 — bucket commit sequence number (HeadBucket) |
| Snapshot / rollback | extension op | 🎯 P2 — label a commit root; restore the bucket head to one (bucket-resource extension + admin CLI, design §5) |
| `x-swarm-redundancy-strategy` | request | 🎯 P2 — erasure-coding fetch strategy/fallback override on GET |
| Grants API | extension op | 🎯 P3 — per-bucket ACT grantee management; grants readable off-gateway (design §8) |
