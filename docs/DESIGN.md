# s3warm — an S3-compatible API gateway for Swarm

| | |
|---|---|
| **Status** | Draft v0.2 — adds key-based tenancy via ACT, commit-chain buckets with checkpointed feeds, snapshots/rollback |
| **Date** | 2026-08-19 |
| **Scope** | Design of an Amazon S3–compatible HTTP API layer on top of [Swarm](https://www.ethswarm.org/), served by a gateway that talks to a [Bee](https://github.com/ethersphere/bee) node |

---

## 1. Overview

Amazon S3's REST API is the de-facto standard for object storage. Thousands of tools speak it natively: the AWS CLI and SDKs, `rclone`, `restic`, `mc`, backup agents, CI systems, data pipelines, media servers. Swarm offers properties none of them can get from S3 — content addressing, censorship resistance, incentivized decentralized storage, cryptographic ownership — but is only reachable through its own Bee HTTP API.

**s3warm** is a translation gateway: a single Go binary that exposes the S3 REST API on one side and drives one or more Bee nodes on the other. Any S3 client pointed at s3warm's endpoint reads and writes Swarm.

```
S3 clients (aws cli, boto3, rclone, mc, restic, SDKs)
        │  S3 REST + AWS Signature V4
        ▼
┌──────────────────────────────────────────────┐
│                  s3warm gateway              │
│  ┌────────────┐  ┌───────────┐  ┌─────────┐  │
│  │ S3 API     │  │ SigV4     │  │ Metadata│  │
│  │ router/XML │  │ auth      │  │ index   │  │
│  ├────────────┤  ├───────────┤  ├─────────┤  │
│  │ Object     │  │ Multipart │  │ Stamp   │  │
│  │ service    │  │ service   │  │ manager │  │
│  ├────────────┴──┴───────────┴──┴─────────┤  │
│  │ Manifest / feed synchronizer (phase 2) │  │
│  └────────────────────┬────────────────────┘ │
│                       │ Bee HTTP API          │
└───────────────────────┼──────────────────────┘
                        ▼
                Bee node(s) ──► Swarm network
```

### Goals

1. **Drop-in compatibility** for the common S3 workflows: `aws s3 cp/sync/ls`, boto3, rclone, mc, presigned URLs, multipart upload.
2. **Preserve Swarm-native value**: every object keeps a public Swarm reference; a bucket is (optionally) a real Swarm manifest addressable via `bzz://`, so data written through s3warm is never locked into s3warm.
3. **Operational simplicity**: one static binary, config via flags/env, deployable as a sidecar next to a Bee node.
4. **Honest semantics**: where S3 and Swarm differ fundamentally (deletion, payment via postage stamps, expiry), surface the difference explicitly rather than pretend.

### Non-goals

- Full AWS control-plane surface (IAM, STS, S3 Select, Glacier restore, replication config, inventory, analytics).
- Being a general Swarm client library (that is bee-js / the Bee API itself).
- Hiding postage economics completely — storage on Swarm is paid and time-bound; the gateway manages it but does not pretend it doesn't exist.

---

## 2. Swarm primitives used

| Primitive | What it is | How s3warm uses it |
|---|---|---|
| **Chunk / reference** | 4 KB content-addressed unit; files are Merkle trees of chunks; the root hash is a 32-byte *reference* (64 hex chars; 64 bytes when encrypted) | The identity of every stored object |
| **`/bytes`** | Upload/download arbitrary data streams; `GET` supports HTTP `Range` | Object payload I/O (PUT/GET data path) |
| **`/bzz` + mantaray manifests** | Path-addressed collections (manifest = trie of forks with per-fork metadata) | Phase 2: canonical on-Swarm representation of a bucket |
| **Feeds / single-owner chunks (SOC)** | Mutable pointer owned by a private key: topic → latest reference | Phase 2: bucket head pointer (`feed(owner, topic=H(bucket)) → manifest root`), making buckets portable and recoverable |
| **Postage batches (`/stamps`)** | Prepaid storage rights: batch = (amount, depth); TTL derived from amount and storage price; uploads must be stamped | Every write is stamped; stamp manager tracks utilization/TTL |
| **Erasure coding** | Per-upload redundancy levels 0–4 (NONE, MEDIUM, STRONG, INSANE, PARANOID) | Mapped from `x-amz-storage-class` |
| **Encryption** | `swarm-encrypt: true` → Bee encrypts; the 64-byte reference embeds the decryption key | Mapped from SSE (`x-amz-server-side-encryption`) |
| **ACT** | Access Control Trie (Bee ≥ 2.2): per-content grantee lists with a key-wrapping history; grantees decrypt with their own keys | Authorization layer for private buckets — grants readable off-gateway; tenant = Ethereum key pair (§8) |
| **Local pinning (`/pins`)** | Pin chunks on a node so they survive local GC | Bucket head roots and labeled snapshots stay pinned on the gateway's Bee node (§5) |
| **`/stewardship`** | Check/repair retrievability of a reference | Ops tooling: periodic health sweep of indexed refs |

Bee endpoints consumed by the gateway: `POST/GET /bytes` (including GET-side redundancy fetch strategy/fallback parameters, §17), `GET /health`, `GET /stamps`, `GET /stamps/{id}`, `POST /stamps/{amount}/{depth}`, `PATCH /stamps/topup|dilute/...`; phase 2 adds `/chunks` (mantaray node upload), `/soc` (feed checkpoints), `/bzz` (manifest reads), `/pins`, `/tags`; phase 3 adds `/grantees` (ACT grants).

---

## 3. Concept mapping

| S3 concept | Swarm / s3warm construct | Notes |
|---|---|---|
| Bucket | Row in metadata index; phase 2: + mantaray manifest with a feed as head pointer | Bucket names validated to S3/DNS rules |
| Object key | Index row `(bucket, key) → reference, size, etag, metadata`; phase 2: manifest fork at path=key | Keys are opaque UTF-8 up to 1024 bytes, `/` has no special meaning |
| Object data | Swarm reference via `/bytes` | Content-addressed → identical payloads dedup naturally |
| ETag | MD5 computed by the gateway while streaming (S3-compatible); multipart uses S3's `md5(md5s)-N` scheme | Swarm's own hash exposed separately as `x-swarm-reference` |
| Last-Modified, Content-Type, `x-amz-meta-*` | Stored in the index (phase 2: also as manifest fork metadata) | |
| Version | Historical index rows; version id = ULID → reference | Old bytes persist on Swarm anyway while stamped — versioning is nearly free (phase 3) |
| DeleteObject | Index/manifest removal only | Swarm has no delete; bytes expire when their postage batch expires. Documented, not hidden |
| CopyObject | New index row pointing at the **same** reference | Server-side copy is O(1), zero data movement |
| Storage class | Erasure-coding redundancy level (§12) | |
| SSE-S3 (`AES256`) | `swarm-encrypt: true`; 64-byte ref held privately in the index | Ref is never exposed for encrypted objects (it contains the key) |
| Bucket ACL / grants | Per-bucket ACT grantee list; grantee = Ethereum public key | Grants extend to native off-gateway reads; revocation is forward-only (§8) |
| Bucket snapshot / rollback (extension) | Labeled manifest root from the bucket's commit chain | Atomic O(1) whole-bucket rollback — no S3 equivalent (§5) |
| Lifecycle / expiry | Postage batch TTL | Read-only mapping: expiry surfaced via headers; extending = topping up the batch |
| Region | Single configurable label (default `us-east-1` for client compatibility) | SigV4 scope accepted for any region string |
| Account / credentials | Access-key/secret pairs linked to a tenant Ethereum key pair (feed owner + ACT publisher) | Key-based multi-tenancy in phase 3 (§8) |
| Durability / replication config | Inherent (network-level redundancy + optional erasure coding) | Replication APIs intentionally unsupported |

**The killer property:** because every object is content-addressed, any object written through s3warm is *also* retrievable from any public Swarm gateway via its reference — and a phase-2 bucket is a browsable `bzz://` manifest. S3 compatibility without S3 lock-in.

---

## 4. Architecture

### Components

- **S3 front end** — HTTP router keyed on method + path shape + query subresource (S3 routes by query params like `?uploads`, `?list-type=2`); XML request/response codecs; S3 error envelope; path-style and virtual-host-style addressing.
- **Auth** — AWS Signature V4 verifier: header auth (phase 1), presigned URLs and `aws-chunked` streaming payloads with trailing checksums (phase 2). Credentials resolved through a provider interface (static config → file/DB-backed store later).
- **Object service** — put/get/copy/delete pipelines; streams everything; computes MD5/SHA-256 on the fly; enforces integrity headers (`Content-MD5`, `x-amz-content-sha256`).
- **Multipart service** — upload session + part bookkeeping; completion produces a *composite object* (§7).
- **Metadata index** — pluggable store behind a narrow interface. Default embedded (in-memory now → SQLite next); Postgres for HA/multi-gateway.
- **Bee client** — thin typed client over the Bee HTTP API with connection reuse, timeouts, error taxonomy (postage failures vs availability).
- **Stamp manager** — batch binding and health: utilization/TTL polling, warnings, optional auto-topup/dilute.
- **Manifest/feed synchronizer (phase 2)** — maintains the on-Swarm mantaray manifest per bucket and its feed head pointer, off the hot path.

### Deployment topologies

1. **Sidecar (default):** one s3warm + one Bee node, embedded index. Strong read-after-write consistency.
2. **HA:** N stateless s3warm instances + shared Postgres index + M Bee nodes behind the gateway's client pool. Consistency provided by the shared index (§10).

---

## 5. Metadata index and source of truth

S3 semantics that Swarm cannot serve directly — millisecond `HEAD`, ordered listings with prefixes/delimiters/pagination, ETags, user metadata queries — require a local index. The index is the **serving-path source of truth**; Swarm holds the **data and (phase 2) a canonical, portable representation**.

### Schema (logical)

```
buckets(name PK, created_at, batch_id, versioning, head_root, commit_seq,
        checkpoint_root, feed_topic)
snapshots(bucket, label, root_ref, commit_seq, created_at)
objects(bucket, key, version_id, swarm_ref, size, etag, content_type,
        user_meta JSONB, storage_class, encrypted, parts JSONB NULL,
        last_modified, is_latest, delete_marker)
        PK (bucket, key, version_id); index (bucket, is_latest, key)
multipart_uploads(upload_id PK, bucket, key, initiated_at, meta JSONB)
multipart_parts(upload_id, part_number, swarm_ref, size, etag)
credentials(access_key PK, secret_encrypted, tenant, created_at)
```

Ordered listing = a range scan over `(bucket, key)`; delimiter roll-up computed by the gateway (§6).

### Bucket state as a commit chain (phase 2)

The gateway maintains, per bucket, a mantaray manifest on Swarm. Manifest nodes are built gateway-side (Bee's own `manifest/mantaray` Go package) and uploaded as chunks — no server-side manifest editing required. Every manifest root is already an immutable snapshot of the whole bucket, and the design exploits that: a reserved fork, `/.s3warm/commit`, holds the **parent root reference**, a sequence number, a timestamp and an op summary. Each debounced write-batch produces a new root that links backward — a git-like chain in which unchanged forks are structurally shared, so a commit costs only the changed path's node chain. Commits are built per write-batch by default (`commit=async`, debounced) or per PUT (`commit=sync`) for buckets whose `bzz://` view must never lag.

### Feeds as checkpoint anchors

The mutable pointer (feed: `owner = tenant key`, `topic = keccak256("s3warm/1/" + bucket)`, updates as signed SOCs — the bee-js mechanism) is deliberately **not** advanced per write-batch. Feeds are the operationally weakest part of Swarm writes: every update is a signed, stamped SOC; lookups are the latency-sensitive, eventually-consistent path; and concurrent writers cannot share one safely. Because each commit links to its parent, any sufficiently recent root reconstructs the full history — so the feed only needs to advance on a slow cadence:

| Checkpoint policy | Feed advanced | Use |
|---|---|---|
| `every-commit` | On each commit | Small buckets; recovery anchor never lags |
| `interval` (default) | Every N commits or T seconds, plus on clean shutdown | General use — write amplification drops from O(write-batches) to O(checkpoints) |
| `manual` | Only on explicit flush or snapshot | Bulk loads, cost-minimal |

The honest trade-off: content addressing only points backward, so after a **total** index loss, head *discovery* is only as fresh as the last checkpoint — data is never lost, but discoverability of the tail delta is bounded by the checkpoint interval. The interval is the tunable.

### Snapshots & rollback

A snapshot is a labeled commit: record the root reference in `snapshots`, pin it on the gateway's Bee node, optionally re-stamp for retention (snapshot lifetime is otherwise bounded by its batch TTL). Restore is pointing the bucket head at an old root — an **atomic, O(1) rollback of the entire bucket**, something S3 itself cannot do. Exposed as an `x-swarm-*` extension on the bucket resource plus admin CLI rather than a contorted S3 verb; `HeadBucket` returns `x-swarm-bucket-root` and `x-swarm-commit-seq` so any client can capture a snapshot reference cheaply.

### Recovery

Given the feed owner key and bucket name, a fresh gateway rebuilds its index by resolving the feed → latest checkpointed root → walking the manifest (keys, refs, metadata are all in the forks), and enumerates history by following parent links. Commits after the last checkpoint are rediscoverable only from the index — hence the clean-shutdown checkpoint. The index remains a cache with a documented rebuild procedure; losing it is an inconvenience, not data loss.

---

## 6. Request flows

### PutObject (streaming, zero buffering)

```
client ──► SigV4 verify (headers only)
       ──► resolve bucket + postage batch (header override → bucket batch → global default)
       ──► body: TeeReader ──► MD5 + SHA-256 accumulators
                          └──► POST {bee}/bytes  (swarm-postage-batch-id,
                               swarm-redundancy-level, swarm-encrypt, swarm-deferred-upload)
       ◄── reference
       ──► verify x-amz-content-sha256 / Content-MD5 against accumulators
             mismatch → error, index row NOT written (stray stamped bytes simply expire)
       ──► index upsert (bucket, key) → ref, size, etag, metadata   [txn]
       ──► enqueue commit (phase 2, §5)
       ◄── 200, ETag:"<md5>", x-swarm-reference:<ref>
```

Latency note: `swarm-deferred-upload: true` (default) acks after local store + async push — S3-like latency, durability follows; `false` waits for network sync — slower, stronger. Configurable per deployment.

Zero-byte objects (directory markers created by consoles/clients) are indexed without a Bee upload.

### GetObject

Index lookup → conditional headers (`If-Match`/`If-None-Match`/`If-(Un)Modified-Since`) evaluated locally → `GET {bee}/bytes/{ref}` with pass-through `Range` and the configured erasure-coding fetch strategy/fallback (§17) → stream body; gateway sets `ETag`, `Last-Modified`, `Content-Type`, `x-amz-meta-*`, `Accept-Ranges`, `x-swarm-reference` (omitted for encrypted objects — the 64-byte ref embeds the key).

### ListObjectsV2 / V1

Served entirely from the index. Range scan from `(bucket, prefix, after)`; delimiter roll-up while scanning: a key containing the delimiter after the prefix collapses into a `CommonPrefix`, each unique prefix counts once toward `MaxKeys`. Continuation token encodes "resume after key K" or "resume after rolled-up prefix P" (opaque base64). `encoding-type=url` supported.

### DeleteObject / DeleteObjects

Index row(s) removed (versioned buckets: delete marker inserted); manifest fork removed on the next commit. Physical bytes remain until their batch expires — stated plainly in docs and in the `x-swarm-*` response headers, because it is a real difference from S3 and users making compliance decisions must know it.

### CopyObject

Source row read → new row written with the same `swarm_ref` (metadata `COPY` or `REPLACE` per directive). O(1) regardless of object size — dramatically cheaper than S3's own server-side copy. `UploadPartCopy` with a byte range is the exception: it must stream the range through the splitter (new ref for the sub-range).

---

## 7. Multipart uploads

S3 semantics: init → parallel parts (5 MiB–5 GiB, any order, retryable) → complete (ordered part list) → single object. Swarm has no server-side concatenation, and part boundaries almost never align with chunk-tree subtree boundaries, so a completed root hash cannot be assembled from part hashes. Design:

1. **UploadPart** streams each part straight to `/bytes` (no local staging, no disk): part → `(ref, size, md5)` recorded in the index.
2. **CompleteMultipartUpload** validates the part list (order, min sizes, ETags) and writes an object row with `parts = [(ref, size), ...]` — a **composite object**. ETag follows S3's multipart convention: `hex(md5(concat(part_md5_bytes))) + "-" + N`, which keeps `aws s3 sync` and friends happy.
3. **GetObject** on a composite maps the requested byte range onto the part list and streams consecutive sub-range reads from Bee — the join happens in the gateway, still fully streaming.
4. **Consolidation (optional, default on, async):** a background job re-streams the composite through `/bytes` to mint a single canonical reference, then swaps the object row. Buckets get manifest/`bzz://` parity for large objects at the cost of one internal re-upload; operators can turn it off.
5. **AbortMultipartUpload** deletes bookkeeping rows; already-uploaded part bytes simply expire with their stamps (Swarm's expiry model makes abandoned-part GC automatic — nice).

On Swarm (manifest view), an unconsolidated composite is represented as a small JSON descriptor chunk (`{"s3warm":"composite/1","parts":[...]}`) so the bucket manifest stays complete; direct `bzz://` fetchers see the descriptor, s3warm and consolidation see through it.

---

## 8. Authentication & authorization

Two complementary layers: SigV4 authenticates S3 clients at the gateway; ACT makes the resulting grants *portable* — enforceable by Swarm itself, off-gateway.

### Layer 1 — S3-dialect authentication (gateway-enforced)

- **AWS Signature V4, header-based** (phase 1): canonical request reconstruction with S3's single-encoding path rules; constant-time signature compare; ±15 min clock-skew window; any region label accepted in the credential scope (service must be `s3`).
- **Presigned URLs** (phase 2): query-string SigV4 (`X-Amz-*` params, `UNSIGNED-PAYLOAD`), expiry enforcement. Enables browser upload/download straight through the gateway.
- **Streaming signatures** (phase 2): `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` (`aws-chunked`) chunk-signature verification and trailing checksums (`x-amz-checksum-crc32/crc32c/sha1/sha256`) — required for full Java SDK v2 / newer SDK defaults.
- **Credentials**: provider interface; static pair via env/flags now, file/DB-backed multi-key store later. SigV4 requires the *actual* secret to derive signing keys, so secrets are stored recoverable — encrypted at rest with a gateway master key, never hashed.
- **Anonymous/dev mode**: explicit opt-in flag for local development; loud warning at startup.

### Layer 2 — key-based tenancy and ACT authorization (phase 3)

- **A tenant is an Ethereum key pair**, held by the gateway; the credentials table links S3 access keys to it. The same key owns the bucket's feed and acts as ACT publisher — so bucket ownership handoff is a key handoff.
- **Private buckets are uploaded with `swarm-act: true`** under the bucket owner's publisher key, with **one grantee list and ACT history per bucket**. Per-object ACT would multiply history lookups and stamp costs for no S3-semantic gain — S3 access patterns are overwhelmingly bucket-granular.
- **Grants map onto Bee's grantee endpoints** (`POST`/`PATCH /grantees`): grantee ID = Ethereum public key. The S3-facing surface is the `PutBucketAcl`-adjacent grants API; the gateway still checks grants on the fast path, ACT makes the same grants independently true on Swarm.
- **The payoff:** a grantee reads a private bucket directly from any Bee node with their own key — no s3warm in the path, no trust in the gateway after the grant. This extends the no-lock-in property (§3) from public objects to private data.
- **Caveats, stated plainly:** revocation is forward-only (ciphertext already readable stays readable — you cannot un-share); reads pay an ACT history lookup; ACT content has 64-byte encrypted references, so `x-swarm-reference` stays suppressed and native access needs the publisher/history headers rather than a bare URL.

---

## 9. Postage stamp management

Storage on Swarm is prepaid via postage batches; this is the largest semantic gap vs S3 and s3warm makes it a managed, visible concern:

- **Binding:** per-request header `x-swarm-postage-batch-id` → bucket default (set at `CreateBucket` via the same header) → gateway default batch. 
- **Monitoring:** stamp manager polls `GET /stamps/{id}`: utilization (bucket-fullness across the 2^depth collision buckets) and `batchTTL`. Prometheus gauges + log warnings at configurable thresholds (e.g. 80% utilization, TTL < 30 days).
- **Autopilot (opt-in, spends funds):** auto `topup` when TTL sinks below target, auto `dilute` (or roll to a new batch) when utilization approaches capacity.
- **Surfacing:** responses carry `x-swarm-batch-id`; object expiry exposed via an `x-amz-expiration`-style header derived from batch TTL; bucket lifecycle `GET` returns a read-only synthetic rule mirroring the TTL.
- **Failure mapping:** batch expired/overflowing/not-found → HTTP **402** with S3-style error code `SwarmPostageError` — a 4xx so SDKs fail fast instead of retry-looping, with a message telling the operator exactly which batch needs attention.

Capacity planning cheat-sheet (documented for operators): batch capacity ≈ 2^depth × 4 KB theoretical; effective usable capacity is lower due to stochastic bucket filling — the manager treats Bee's reported utilization as authoritative.

---

## 10. Consistency & concurrency

- **Single gateway:** strong read-after-write and list-after-write — every mutation is an index transaction; reads go through the index. This matches (indeed exceeds) S3's current consistency model.
- **Concurrent PUTs to one key:** last committed transaction wins, same as S3.
- **Conditional writes:** `If-None-Match: *` and `If-Match` on PUT (S3 2024 additions) are trivial index constraints — phase 2, cheap, and valuable for coordination-hungry clients.
- **Multi-gateway:** all instances share one Postgres index → same guarantees. The feed/manifest layer is *not* used for serving reads and is allowed to lag (eventual); it exists for portability and recovery.
- **External writers:** data written to Swarm outside s3warm is not visible in buckets (by design); an `import` admin command can graft an existing reference into a bucket as a new key (O(1), it's a copy).

---

## 11. Versioning (phase 3)

Content addressing makes S3 versioning almost free: every overwrite already creates a new reference and the old bytes remain retrievable while stamped. Enabling versioning on a bucket = keep the old index rows (`version_id` = ULID, monotonic, sortable) instead of superseding them, insert delete markers on DELETE, expose `GET ?versionId=`, `ListObjectVersions`. Version lifetime is bounded by stamp TTL, not by S3 lifecycle rules — surfaced in docs and headers.

The commit chain (§5) is the *portable* representation of history: bucket-level, recoverable by walking parent links from any root. The index still materializes per-object versions, because S3 versioning semantics — interleaved version ids, delete markers, `ListObjectVersions` — need ordered per-key access that a per-request DAG walk cannot serve. The two compose: per-object versions for S3 clients, whole-bucket snapshots (§5) for operators.

---

## 12. Storage classes & encryption mapping

| `x-amz-storage-class` | Swarm redundancy level |
|---|---|
| `STANDARD` (default) | configured default (ships as `MEDIUM`) |
| `REDUCED_REDUNDANCY` | `NONE` (0) |
| `STANDARD_IA`, `ONEZONE_IA` | `MEDIUM` (1) |
| `SWARM_NONE/MEDIUM/STRONG/INSANE/PARANOID` (extension) | explicit 0–4 |
| `GLACIER*`, `DEEP_ARCHIVE` | rejected — `InvalidStorageClass` (no restore semantics to honor) |

HEAD/GET/List report the class the client wrote.

**SSE:** `x-amz-server-side-encryption: AES256` → `swarm-encrypt: true`. The 64-byte reference (which embeds the decryption key) lives only in the index; `x-swarm-reference` is suppressed for encrypted objects. SSE-KMS/SSE-C rejected (phase 3 may map SSE-C to gateway-side AES-GCM). Responses echo `x-amz-server-side-encryption: AES256`.

---

## 13. Error mapping

S3 XML error envelope (`Code`, `Message`, `Resource`, `RequestId`) throughout. Highlights:

| Condition | HTTP | Code |
|---|---|---|
| Unknown bucket / key / upload id | 404 | `NoSuchBucket` / `NoSuchKey` / `NoSuchUpload` |
| Bucket exists / not empty | 409 | `BucketAlreadyOwnedByYou` / `BucketNotEmpty` |
| Bad signature / unknown key / skew | 403 | `SignatureDoesNotMatch` / `InvalidAccessKeyId` / `RequestTimeTooSkewed` |
| Malformed auth header | 400 | `AuthorizationHeaderMalformed` |
| Body hash mismatch | 400 | `XAmzContentSHA256Mismatch` / `BadDigest` |
| Invalid part / order (MPU) | 400 | `InvalidPart` / `InvalidPartOrder` |
| Range not satisfiable | 416 | `InvalidRange` |
| Conditional failure | 412 / 304 | `PreconditionFailed` / — |
| **Postage batch invalid/expired/full** | **402** | **`SwarmPostageError`** (extension) |
| Bee unreachable / timeout | 503 | `ServiceUnavailable` (SDK-retryable) |
| Unimplemented operation | 501 | `NotImplemented` |

---

## 14. API compatibility

Full per-operation matrix: [`docs/API-COMPATIBILITY.md`](API-COMPATIBILITY.md). Summary:

- **Phase 1 (MVP):** ListBuckets, Create/Head/Delete-Bucket, GetBucketLocation, PutObject, GetObject (+Range), HeadObject, DeleteObject(s), CopyObject, ListObjectsV2/V1, SigV4 header auth.
- **Phase 2:** full multipart set, presigned URLs, streaming signatures + trailing checksums, GetObjectAttributes, conditional PUT, commit-chain manifests + checkpointed feeds, snapshots/rollback, SSE, CORS.
- **Phase 3:** versioning suite, tagging, ACT-backed grants + key-based multi-tenancy, stamp autopilot, Postgres HA, PSS/GSOC notification research.
- **Deliberately unsupported:** ACL mutation (canned `private` only), website hosting (that's native `bzz://`), replication/inventory/analytics/Glacier/S3 Select/Object Lock/Torrent.

---

## 15. Interoperability & testing

**Interop targets (tested in CI, roughly in priority order):** AWS CLI v2, boto3, aws-sdk-go-v2, aws-sdk-js-v3, rclone, MinIO `mc`, restic, Cyberduck.

- **Unit:** SigV4 against the official AWS test vectors (in repo); list pagination/delimiter edge cases; multipart ETag algebra; bucket-name validation.
- **Integration:** `httptest`-level suite with a fake Bee today; docker-compose harness with `bee dev` (in-memory dev-mode Bee with purchasable fake batches) for real end-to-end runs.
- **Conformance:** curated subset of Ceph's `s3-tests` (the industry compatibility suite), tracked in a pass/fail manifest so compatibility claims are backed by data; MinIO Mint later.

---

## 16. Observability & operations

- Structured logs (slog) with request id, access key, bucket/key, Bee latency.
- Prometheus `/metrics`: request rates/latencies by op, Bee upstream latencies/errors, stamp utilization + TTL gauges, bytes in/out, in-flight multipart sessions.
- `/_s3warm/health` (process) and `/_s3warm/ready` (Bee reachability + index ping) — an underscore prefix no valid bucket name can shadow.
- The gateway's Bee node pins current bucket head roots and labeled snapshots, so the portable representation survives local garbage collection.
- Admin surface (`s3warmctl` or `/admin` behind separate auth): credential and grantee management, batch binding, checkpoint flush, snapshot/rollback, index rebuild-from-feed, reference import.

---

## 17. Performance notes

- Everything streams; per-request memory is O(hash state + copy buffer). No spooling to disk, including multipart.
- Bee connection pooling with keep-alive; per-op timeouts; upstream parallelism is Bee's chunking pipeline, not the gateway's problem.
- Listing never touches Bee. HEAD never touches Bee.
- Reads pass Bee's erasure-coding fetch strategy/fallback parameters through (configurable), trading latency for retrieval robustness.
- Commits are debounced; feed checkpoints are rarer still (§5) — both off the hot path.
- Optional read-through LRU (ref → recent bytes) for hot small objects — deferred until measured need.

---

## 18. Security considerations

- TLS termination at the gateway or an fronting proxy; SigV4 alone does not protect payload confidentiality.
- Constant-time signature comparison; bounded clock skew; presigned expiry enforced.
- Credential secrets encrypted at rest (master key via env/KMS-file); never logged.
- Request hygiene: bucket-name validation, bounded XML bodies (1 MiB), `max-keys` clamped to 1000, upload size limits configurable.
- The gateway talks only to its configured Bee endpoint — no user-influenced upstream URLs (no SSRF surface).
- Anonymous mode refuses to start unless explicitly flagged, and logs a warning banner.

---

## 19. Roadmap

Rationale and exit criteria only; current progress is tracked with checkboxes in [`ROADMAP.md`](../ROADMAP.md).

| Phase | Contents | Exit criterion |
|---|---|---|
| **0 — skeleton** (this repo) | Design; compiling gateway: routing, SigV4, errors, in-memory index, Bee client, MVP object/bucket ops | `aws s3 cp/ls/rm` round-trips against `bee dev` |
| **1 — MVP** | SQLite index, stamp manager v1, conditional requests, hardening, docker-compose, CI conformance harness | rclone sync of a real tree; s3-tests subset green |
| **2 — compatibility** | Multipart, presigned, streaming sigs + checksums, SSE, commit-chain manifests + checkpointed feeds, snapshots/rollback, GetObjectAttributes, CORS | restic + Java SDK v2 work; buckets browsable via `bzz://`; snapshot → rollback round-trip |
| **3 — production** | Versioning, tagging, ACT-backed grants + key-based multi-tenancy, Postgres HA, stamp autopilot, PSS/GSOC notification research | Multi-gateway behind LB; a grantee reads a private bucket off-gateway |

---

## 20. Alternatives considered

- **Implement S3 inside Bee** — rejected: couples release cycles, bloats the node; a gateway iterates independently and composes with any Bee.
- **Fork MinIO** — rejected: AGPL implications, and the MinIO gateway framework was removed upstream in 2022. Its test corpus and routing patterns remain useful references.
- **Zenko CloudServer custom backend** — rejected: heavyweight Node.js stack; the surrounding ecosystem (Bee, tooling) is Go.
- **Serve straight from manifests, no index** — rejected for the serving path: Bee has no efficient manifest iteration/pagination API, and S3 listing semantics (delimiter roll-up, tokens) demand an ordered local structure. Kept as the *portability* layer instead.
- **Per-write-batch feed updates (draft v0.1)** — superseded by the commit chain: history now lives in the immutable manifests themselves, demoting feeds to rare checkpoint anchors (§5). Recorded here because it is the obvious first design, and its costs — a signed, stamped SOC per batch, feed-lookup latency, single-writer feeds — are why it lost.

---

## 21. Open questions

1. Bucket ownership handoff = handing over the tenant key (feed owner + ACT publisher). Is a re-key-and-copy flow needed for handoffs that must not share the old key?
2. Should consolidation of composite objects default on (bzz-parity) or off (cheaper)? Needs cost measurement on real Bee.
3. Global bucket namespace vs per-access-key namespaces for multi-tenant deployments.
4. Best surfacing of stamp economics: synthetic lifecycle rules vs `x-swarm-*` headers only.
5. Integrity for `UNSIGNED-PAYLOAD` writes: require trailing CRC checksums when signature doesn't cover the body?
6. `bee dev` fidelity limits for CI (postage semantics differ from mainnet in places) — how much needs a testnet job?
7. Checkpoint cadence defaults: what N commits / T seconds balances write cost against recovery freshness, and is the clean-shutdown flush enough for most operators?
8. S3 Event Notifications over Swarm's native pub-sub (PSS/GSOC): the mechanism exists — is the demand worth the API surface?
9. ACT revocation vs S3 expectations: forward-only revocation must be surfaced loudly — in docs, headers, or the grants API response?

---

## Appendix A — example session (target UX)

```console
$ export AWS_ACCESS_KEY_ID=dev AWS_SECRET_ACCESS_KEY=devsecret AWS_DEFAULT_REGION=us-east-1
$ aws --endpoint-url http://localhost:8333 s3 mb s3://photos
make_bucket: photos
$ aws --endpoint-url http://localhost:8333 s3 cp holiday.jpg s3://photos/2026/holiday.jpg
upload: ./holiday.jpg to s3://photos/2026/holiday.jpg
$ aws --endpoint-url http://localhost:8333 s3api head-object --bucket photos --key 2026/holiday.jpg
{ "ETag": "\"9c1185a5c5e9fc54612808977ee8f548\"", ... }
# the same bytes, straight from any Swarm gateway, no s3warm involved:
$ curl https://gateway.ethswarm.org/bytes/<x-swarm-reference> -o holiday.jpg
```
