# s3warm Reference Manual

The complete operational reference: configuration, authentication, the S3
dialect s3warm speaks, its Swarm-native extensions, errors, and operations.
Tutorial introduction: [User Guide](USER-GUIDE.md). Architecture and
rationale: [Design](DESIGN.md). Per-operation support:
[compatibility matrix](API-COMPATIBILITY.md).

---

## Configuration

One static binary, configured by flags with environment-variable defaults
(the flag wins when both are set).

| Flag | Env | Default | Meaning |
|---|---|---|---|
| `-listen` | `S3WARM_LISTEN` | `:8333` | S3 API listen address |
| `-bee-api` | `S3WARM_BEE_API` | `http://127.0.0.1:1633` | Bee node API endpoint |
| `-batch-id` | `S3WARM_BATCH_ID` | — | Default postage batch id (64 hex). Required for writes unless a bucket or request provides one |
| `-batch-id-file` | `S3WARM_BATCH_ID_FILE` | — | File to read the batch id from when `-batch-id` is empty (init-container handoff) |
| `-access-key` | `S3WARM_ACCESS_KEY` | — | Access key id. Empty enables **anonymous mode** (dev only; loud startup warning) |
| `-secret-key` | `S3WARM_SECRET_KEY` | — | Secret access key (must be set together with `-access-key`) |
| `-region` | `S3WARM_REGION` | `us-east-1` | Region label reported by GetBucketLocation (`us-east-1` renders as the empty LocationConstraint, as on AWS) |
| `-db` | `S3WARM_DB` | `s3warm.db` | SQLite metadata index path. Empty = in-memory index (dev only; metadata lost on restart). Schema migrates automatically on open |
| `-redundancy` | `S3WARM_REDUNDANCY` | `0` | Erasure-coding level (0–4) applied to `STANDARD`-class writes |
| `-encrypt` | `S3WARM_ENCRYPT` | `false` | Encrypt **all** uploads (gateway-wide SSE default) |
| `-ack` | `S3WARM_ACK` | `node` | PUT ack policy: `node` or `network` (see below) |
| `-commit` | `S3WARM_COMMIT` | `async` | Bucket commit chain: `async` (debounced on-Swarm manifests) or `off` |
| `-feed-key` | `S3WARM_FEED_KEY` | — | Hex secp256k1 private key; publishes each commit as a feed checkpoint (see commit chain) |
| `-fetch-strategy` | `S3WARM_FETCH_STRATEGY` | — | Default erasure-coding fetch strategy for reads (0–4; empty = node default) |
| `-chequebook-min` | `S3WARM_CHEQUEBOOK_MIN` | `0.2` | Auto top-up the node's chequebook when its available balance falls below this many xBZZ (`0` disables the keeper). Bandwidth is cheap on Swarm — a fraction of an xBZZ covers a lot of traffic |
| `-chequebook-target` | `S3WARM_CHEQUEBOOK_TARGET` | `1` | Top the chequebook up to this many xBZZ |
| `-chequebook-reserve` | `S3WARM_CHEQUEBOOK_RESERVE` | `1` | Wallet xBZZ never taken by chequebook top-ups — kept for postage, the expensive resource |
| `-domain` | `S3WARM_DOMAIN` | — | Base domain enabling virtual-host-style addressing (`bucket.<domain>`) |

### Ack policy

What a `200` on PUT means (design §6). The tier is explicit — durability is
never silently relaxed:

| `-ack` | 200 means |
|---|---|
| `node` (default) | The object is in the Bee node's local store; network push follows asynchronously. Read-after-write always holds |
| `network` | Chunks pushed to the network before the ack. Strongest, slowest |

### Storage classes → redundancy

`x-amz-storage-class` on PUT/CreateMultipartUpload maps to Swarm
erasure-coding levels; the class is echoed back on reads.

| Class | Level |
|---|---|
| `STANDARD` (default) | the `-redundancy` setting |
| `REDUCED_REDUNDANCY`, `SWARM_NONE` | 0 (none) |
| `SWARM_MEDIUM` | 1 |
| `SWARM_STRONG` | 2 |
| `SWARM_INSANE` | 3 |
| `SWARM_PARANOID` | 4 |

---

## Authentication

- **SigV4, Authorization header.** Canonical requests use S3's
  single-encoding path rules. Any region label is accepted in the credential
  scope (the scope's own region feeds key derivation); the service must be
  `s3`. Clock skew tolerance: ±15 minutes. Validated against the official
  AWS test vectors in the unit suite.
- **Presigned URLs (query-string SigV4).** `UNSIGNED-PAYLOAD` canonical
  requests; `X-Amz-Expires` must be 1…604800 seconds — out-of-range or
  missing is `403 AccessDenied`, expiry is checked against `X-Amz-Date`.
  Legacy SigV2 URLs are not accepted.
- **Streaming payloads (aws-chunked).** All three payload types:
  `STREAMING-AWS4-HMAC-SHA256-PAYLOAD`, its `-TRAILER` form (signed
  trailers), and `STREAMING-UNSIGNED-PAYLOAD-TRAILER`. Chunk signatures are
  verified as a chain seeded from the request signature;
  `x-amz-decoded-content-length` sizes the upload.
- **Payload integrity.** `x-amz-content-sha256` (hex form) and `Content-MD5`
  are verified while streaming; a mismatch fails the request and nothing is
  indexed.
- **Anonymous mode.** With no configured credentials every request is
  accepted; for development only.

### Flexible checksums

`CRC32`, `CRC32C`, `CRC64NVME`, `SHA1`, `SHA256` — supplied up-front
(`x-amz-checksum-<alg>`) or as an aws-chunked trailer (`x-amz-trailer`).
Verified during upload (`400 BadDigest` on mismatch), stored, echoed on the
PUT response, and returned on GET/HEAD under `x-amz-checksum-mode: ENABLED`
— **except on partial responses** (Range or `partNumber`), because the
stored checksum covers the full object and clients validate bodies against
it. Multipart composite/`FULL_OBJECT` checksums are not yet computed.

---

## Addressing

Path-style (`/bucket/key`) always works. With `-domain example.com`,
virtual-host-style (`bucket.example.com/key`) is also recognized. Bucket
names follow the S3/DNS rules (3–63 chars, lowercase, no `..`, not an IP).

Operational endpoints live under a prefix no valid bucket name can shadow:

| Path | Purpose |
|---|---|
| `/_s3warm/health` | Process liveness |
| `/_s3warm/ready` | Bee reachability |
| `/_s3warm/metrics` | Prometheus exposition (below) |

---

## S3 dialect notes

The per-operation matrix is
[API-COMPATIBILITY.md](API-COMPATIBILITY.md); these are the behavioral
details on top of it.

- **Listings.** ListObjectsV2 and V1 with prefix/delimiter roll-up,
  `max-keys` (clamped to 1000), continuation tokens/markers,
  `encoding-type=url`. Served entirely from the local index — listing never
  touches the network.
- **Conditional requests.** `If-Match`/`If-None-Match`/`If-(Un)Modified-Since`
  on reads; conditional **writes** (`If-Match`, `If-None-Match` incl. `*`)
  on PutObject and CompleteMultipartUpload, evaluated atomically with the
  index write. `If-Match` against a missing key is `404`, as on AWS.
- **CopyObject** is O(1): the destination points at the same Swarm
  reference; `x-amz-metadata-directive` COPY/REPLACE and all four
  `x-amz-copy-source-if-*` conditionals supported. Copying an object onto
  itself without REPLACE is rejected, as on AWS.
- **Multipart.** Parts 1–10000, minimum 5 MiB except the last (checked at
  complete); parts stream straight to Swarm with no staging; completion is
  retry-idempotent and produces the standard multipart ETag
  (`md5-of-md5s-N`). Completed objects are *composite*: reads stitch
  consecutive part ranges, including ranges across part boundaries;
  `GET/HEAD ?partNumber=N` serves single parts with `x-amz-mp-parts-count`.
  `UploadPartCopy`: whole-object simple sources are O(1); byte ranges
  (strict `bytes=first-last`) re-stream; composite sources not yet.
  Aborted/abandoned parts need no GC — their bytes expire with the batch.
- **GetObjectAttributes** with `ETag` (unquoted, per AWS), `Checksum`,
  `ObjectParts` (paginated via `x-amz-max-parts` /
  `x-amz-part-number-marker`), `StorageClass`, `ObjectSize`.
- **SSE-S3.** `x-amz-server-side-encryption: AES256` maps to Swarm's native
  encryption. Scope: per request, bucket default
  (`GET/PUT/DELETE ?encryption`, standard XML), or `-encrypt` gateway-wide.
  Encrypted objects never expose `x-swarm-reference` (the 64-byte reference
  embeds the decryption key). SSE-C and SSE-KMS are rejected with
  `501 NotImplemented` — never silently ignored.
- **CORS.** Per-bucket rules (`GET/PUT/DELETE ?cors`, ≤100 rules), S3's
  single-`*` origin patterns, method taken from
  `Access-Control-Request-Method` when present. Preflights are answered
  before authentication (400 without Origin+method, 403 on non-match);
  matching rules decorate responses — including error responses — with
  `Access-Control-Allow-Origin` (the literal `*` for wildcard rules,
  otherwise the echoed origin), `-Allow-Methods`, `-Expose-Headers`,
  `-Max-Age`, and `-Allow-Headers` on preflight.
- **GetObject response overrides**: `response-content-type`,
  `-content-language`, `-expires`, `-cache-control`, `-content-disposition`,
  `-content-encoding` (signature-covered; the presigned-URL staple).
- **Content-Encoding** is stored and returned with any `aws-chunked`
  transport token stripped.
- **Zero-byte objects** (directory markers) are indexed without a Swarm
  upload.
- **ListObjectVersions** is served with unversioned semantics (every object
  one `null` version), as S3 does for never-versioned buckets. Real
  versioning is roadmap phase 3.

---

## Swarm-native extensions

### Request headers

| Header | On | Meaning |
|---|---|---|
| `x-swarm-postage-batch-id` | PUT, CreateMultipartUpload, CreateBucket | Stamp this write from a specific batch / set the bucket's default batch |
| `x-swarm-redundancy-strategy` | GET | Erasure-coding fetch strategy override (0–4) |
| `x-swarm-redundancy-fallback-mode` | GET | Fetch fallback (`true`/`false`) |

### Response headers

| Header | On | Meaning |
|---|---|---|
| `x-swarm-reference` | PUT/GET/HEAD object | The object's Swarm reference — fetchable via `/bytes/{ref}` on any Bee. Omitted for encrypted objects |
| `x-swarm-postage-batch-id` | PUT/GET/HEAD object | The batch that stamped the object |
| `x-swarm-batch-ttl` | PUT/GET/HEAD object | Estimated seconds until that batch (and the object) expires |
| `x-swarm-bucket-root` | HeadBucket, snapshot/restore | The bucket's current commit root — browse via `/bzz/{root}/{key}` |
| `x-swarm-commit-seq` | HeadBucket | Commit sequence number |

### The commit chain

With `-commit async` (the default), every batch of mutations produces a
*commit* a few seconds later: a [mantaray](https://github.com/ethersphere/bee)
manifest on Swarm with one fork per object (entry = the object's reference,
`Content-Type` in metadata — which is what makes `GET /bzz/{root}/{key}`
work on any Bee node) plus a reserved `.s3warm/commit` fork holding a JSON
document:

```json
{
  "version": 1,
  "bucket": "…",
  "seq": 7,
  "parent": "<hex root of commit 6>",
  "timestamp": "…",
  "objects": [ { "Key": "…", "SwarmRef": "…", "ETag": "…", "...": "full index rows" } ]
}
```

Parent links make the chain walkable backwards from any root; the `objects`
array is the exact-restore source of truth. Composite (multipart) objects
appear in the manifest as JSON descriptors
(`{"s3warm":"composite/1","parts":[…]}`); zero-byte objects appear only in
the commit document.

**Feed checkpoints.** With `-feed-key <hex secp256k1>`, each commit is
also published as a sequence-feed update:
owner = the key's Ethereum address, topic = `keccak256("s3warm/1/" +
bucket)`, feed index = `seq − 1`, payload = the 32-byte commit root. Any
Bee resolves the bucket's latest root via
`GET /feeds/{owner}/{topic}?type=sequence` — the portable recovery anchor.

### Snapshots & rollback

| Call | Effect |
|---|---|
| `PUT /{bucket}?x-swarm-snapshot=<label>` | Forces a commit, records `<label>` → root, pins the root on the gateway's Bee node. Returns JSON `{bucket, label, root, seq, createdAt}` |
| `GET /{bucket}?x-swarm-snapshot` | Lists snapshots (JSON) |
| `POST /{bucket}?x-swarm-restore=<label or 64-hex root>` | Atomic whole-bucket rollback: replaces the entire object set from the commit document and points the head at that root. Returns JSON `{bucket, root, seq, objects}` |

Labels match `[A-Za-z0-9._-]{1,64}`. Restore accepts any commit root whose
document names the same bucket — including roots from before a restore, so
roll-forward works too. Snapshot lifetime is bounded by the postage batch
that stamped the commit; pinning protects it from the local node's GC only.

---

## Errors

Standard S3 XML error envelope (`Code`, `Message`, `Resource`,
`RequestId`). Codes beyond the usual S3 set:

| Code | HTTP | When |
|---|---|---|
| `SwarmPostageError` | 402 | Postage batch missing, expired, not yet usable, or (immutable batch) out of capacity — checked *before* bytes move, and 4xx so SDKs don't retry-loop |
| `NoSuchCORSConfiguration` / `ServerSideEncryptionConfigurationNotFoundError` | 404 | GET on unset bucket config |
| `NotImplemented` | 501 | Operation outside the [matrix](API-COMPATIBILITY.md) |
| `ServiceUnavailable` | 503 | Bee unreachable (SDK-retryable) |

A `NoSuchKey` whose message mentions non-retrievable data means the index
knows the object but Swarm no longer serves the bytes — typically an
expired batch.

---

## Operations

### Metrics (`/_s3warm/metrics`)

| Metric | Type | Labels |
|---|---|---|
| `s3warm_requests_total` | counter | `method`, `code` |
| `s3warm_request_duration_seconds` | histogram | `method` |
| `s3warm_object_bytes_in_total` / `_out_total` | counter | — |
| `s3warm_bee_requests_total` | counter | `op` (`bytes_upload`, `bytes_download`, `stamp`, `soc_upload`, `pin`, `health`), `code` (0 = transport error) |
| `s3warm_bee_request_duration_seconds` | histogram | `op` |
| `s3warm_stamp_ttl_seconds` | gauge | `batch` |
| `s3warm_stamp_utilization_ratio` | gauge | `batch` |
| `s3warm_chequebook_available_bzz` | gauge | — |
| `s3warm_wallet_bzz` | gauge | — |
| `s3warm_chequebook_deposits_total` | counter | — |

The stamp manager refreshes tracked batches every 5 minutes and logs
warnings at ≥80% utilization or <30 days TTL.

**Chequebook keeper.** Bee seeds its chequebook once at deployment and
never refills it; the gateway checks it daily (first check ~30 s after
start) and deposits from the node wallet when the available balance is
below `-chequebook-min`, topping up to `-chequebook-target`. Guards: the
deposit is skipped (with a warning) when the wallet lacks xDAI for gas,
and only wallet funds **above `-chequebook-reserve`** may be taken — the
wallet pays for postage batches, and storage outranks bandwidth. Every
automatic deposit is logged and counted in
`s3warm_chequebook_deposits_total`.

### Limits & defaults

| What | Value |
|---|---|
| Max keys per listing / batch delete | 1000 |
| Multipart part numbers / min part size | 1–10000 / 5 MiB (except last) |
| XML request bodies | 1 MiB (batch delete), 4 MiB (complete multipart), 64 KiB (bucket configs) |
| Presign lifetime | ≤ 604800 s (7 days) |
| SigV4 clock skew | ±15 min |
| Commit debounce | 3 s |
| Snapshot label | `[A-Za-z0-9._-]{1,64}` |

### Deployment

`docker compose up -d --build` runs the dev stack (gateway + `fakebee`, an
in-memory Bee-API stand-in). Production: run the binary as a sidecar next
to a Bee node; terminate TLS in front of it (SigV4 does not encrypt
payloads); persist `-db`; monitor the stamp gauges. The gateway only ever
dials its configured `-bee-api` — no user-influenced upstream URLs.

### Conformance harness

`test/s3tests/run.sh` runs the curated subset of Ceph's s3-tests —
[`passing.txt`](../test/s3tests/passing.txt) is the executable
compatibility claim; every listed test must pass, and CI enforces it on
each push. To reproduce locally: start the compose stack and run the
script (first run clones the suite and builds a virtualenv).
