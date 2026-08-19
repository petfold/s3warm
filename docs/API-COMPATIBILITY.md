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
| Get/PutBucketVersioning | ✅ | Enabled/Suspended; never-versioned buckets return the empty config |
| ListObjectVersions | ✅ | Real version history: `Version` + `DeleteMarker` entries, newest-first per key, key/version-id-marker pagination, delimiter roll-up |
| GetBucketAcl / PutBucketAcl | 🎯 P3 | Grants map to the bucket's ACT grantee list (grantee = Ethereum public key, design §8); canned `private` until then |
| GetBucketPolicy / PutBucketPolicy | 🎯 P3 | Grant-subset only, not full IAM policy language |
| Get/Put/DeleteBucketCors | ✅ | Per-bucket rules with S3 wildcard-origin matching; preflights and response decoration handled before auth |
| Get/Put/DeleteBucketEncryption | ✅ | SSE-S3 (AES256) default-encryption config; KMS rejected |
| GetBucketLifecycleConfiguration | 🎯 P2 | Read-only synthetic rule derived from postage batch TTL |
| PutBucketLifecycleConfiguration | ❌ | Expiry is postage TTL; extend by topping up the batch |
| GetBucketTagging / PutBucketTagging | 🎯 P3 | |
| ListMultipartUploads | ✅ | Prefix filter; key/upload-id markers 🎯 |
| Get/PutBucketNotificationConfiguration | 🎯 research | Candidate mapping to Swarm's native pub-sub (PSS/GSOC), design §21 |
| Website / Replication / Inventory / Analytics / Metrics / Accelerate / Logging / RequestPayment / ObjectLock config | ❌ | Website = native `bzz://`; replication is inherent to Swarm |

## Object operations

| Operation | Status | Notes |
|---|---|---|
| PutObject | ✅ | Streaming; MD5 ETag; `Content-MD5` + `x-amz-content-sha256` enforced; `x-amz-meta-*`; zero-byte objects; `x-swarm-reference` response header |
| GetObject | ✅ | Range pass-through; conditional headers; composite (multipart) objects stitched from parts, including ranges across part boundaries; `?versionId` (a delete-marker version answers 405, a marker-shadowed key 404 + `x-amz-delete-marker`) |
| HeadObject | ✅ | |
| DeleteObject | ✅ | Versioned buckets get delete markers; `?versionId` removes one version permanently (latest promotion included); never-versioned keys are removed — bytes expire with the postage batch either way |
| DeleteObjects (batch) | ✅ | Quiet mode; per-entry `VersionId`, marker info in results |
| CopyObject | ✅ | O(1) — same Swarm reference; `x-amz-metadata-directive` COPY/REPLACE; `x-amz-copy-source-if-*` conditionals; `?versionId` sources with `x-amz-copy-source-version-id` |
| CreateMultipartUpload | ✅ | Metadata/content-type/storage-class captured; batch validated at initiate |
| UploadPart | ✅ | Parts stream straight to `/bytes`, no staging; 1–10000, integrity headers enforced |
| UploadPartCopy | ✅ | Whole-object simple source is O(1) (same reference); byte-range re-streams; composite source 🎯 |
| CompleteMultipartUpload | ✅ | Composite object + S3 multipart ETag; retry-idempotent; conditional (`If-Match`/`If-None-Match`); min part size enforced; async consolidation 🎯 |
| AbortMultipartUpload | ✅ | Abandoned parts expire with stamps — GC is automatic |
| ListParts | ✅ | With part-number-marker/max-parts pagination |
| GetObject/HeadObject `?partNumber` | ✅ | Part-ranged reads with `x-amz-mp-parts-count`; non-multipart objects are one part |
| GetObject `response-*` overrides | ✅ | content-type/-disposition/-encoding/-language, cache-control, expires — signature-covered, as used by presigned links |
| GetObjectAttributes | ✅ | ETag, Checksum, ObjectParts (with pagination), StorageClass, ObjectSize |
| GetObjectTagging / PutObjectTagging / DeleteObjectTagging | 🎯 P3 | |
| GetObjectAcl / PutObjectAcl | 🪧 P2 | Canned `private` |
| RestoreObject / SelectObjectContent / GetObjectTorrent | ❌ | |
| SSE-S3 (`x-amz-server-side-encryption: AES256`) | ✅ | Mapped to `swarm-encrypt`: Bee encrypts/decrypts; the key-bearing 64-byte reference stays private (design §12). Per-request, bucket-default, or gateway-wide. SSE-C/KMS rejected |
| Conditional PUT (`If-None-Match` / `If-Match`) | ✅ | Checked atomically with the index write; `If-Match` on a missing key is 404, as on AWS |

## Authentication

| Mechanism | Status | Notes |
|---|---|---|
| SigV4, Authorization header | ✅ | AWS doc test vectors in unit tests; any region label accepted (service must be `s3`) |
| SigV4 presigned URLs | ✅ | Query-string auth with `UNSIGNED-PAYLOAD`; expiry enforced (out-of-range `X-Amz-Expires` is 403, as on AWS); AWS doc vector in unit tests |
| `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` (aws-chunked) | ✅ | Chunk-signature chain + signed trailers verified; `STREAMING-UNSIGNED-PAYLOAD-TRAILER` too |
| Flexible checksums (`x-amz-checksum-*`) | ✅ | CRC32/CRC32C/CRC64NVME/SHA-1/SHA-256 via header or trailer; stored, echoed on PUT and on GET/HEAD with checksum mode (full responses only); multipart composite/FULL_OBJECT checksums 🎯 |
| SigV2 | ❌ | Legacy |
| Anonymous mode | ✅ | Explicit opt-in, dev only |

## Semantics extensions (`x-swarm-*`)

| Extension | Direction | Meaning |
|---|---|---|
| `x-swarm-reference` | response | Swarm reference of the object (omitted for encrypted objects) |
| `x-swarm-postage-batch-id` | request + response | Request: batch override for this write / bucket default at CreateBucket. Response: the batch that stamped the object (PUT/GET/HEAD) |
| `x-swarm-batch-ttl` | response | ✅ Estimated seconds until the object's batch expires (PUT/GET/HEAD) |
| `x-swarm-bucket-root` | response | ✅ Current commit root (HeadBucket): the whole bucket is browsable at any Bee via `GET /bzz/{root}/{key}` |
| `x-swarm-commit-seq` | response | ✅ Bucket commit sequence number (HeadBucket) |
| Snapshot / rollback | extension op | ✅ `PUT ?x-swarm-snapshot=<label>` (forces a commit, pins the root), `GET ?x-swarm-snapshot` (list), `POST ?x-swarm-restore=<label\|root>` — atomic whole-bucket rollback (design §5) |
| Feed checkpoint anchor | config | ✅ With `-feed-key`, every commit publishes to the bucket's sequence feed (`owner` = key, topic = keccak256("s3warm/1/"+bucket)) — resolvable at any Bee via `GET /feeds/{owner}/{topic}` |
| `x-swarm-redundancy-strategy` / `x-swarm-redundancy-fallback-mode` | request | ✅ Erasure-coding fetch strategy/fallback on GET (deployment default via `-fetch-strategy`) |
| Grants API | extension op | 🎯 P3 — per-bucket ACT grantee management; grants readable off-gateway (design §8) |
