# Roadmap

Progress tracking only. Rationale and exit criteria: [DESIGN.md §19](docs/DESIGN.md#19-roadmap).
Per-operation API status: [API-COMPATIBILITY.md](docs/API-COMPATIBILITY.md).
Rule: items link to the docs, they never re-describe them.

## Phase 0 — skeleton

Exit: `aws s3 cp/ls/rm` round-trips against `bee dev`.

- [x] Design document (docs/DESIGN.md, v0.2)
- [x] S3 routing, XML codecs, error envelope (§4, §13)
- [x] SigV4 header auth, AWS doc test vectors passing (§8)
- [x] Metadata index interface + in-memory implementation (§5)
- [x] Bee client: /bytes upload/download, /health (§2)
- [x] Bucket ops: Create/Head/Delete/List, GetBucketLocation
- [x] Object ops: Put/Get(+Range)/Head/Delete/DeleteObjects/Copy, conditional GET headers
- [x] ListObjectsV2/V1 with delimiter roll-up and pagination
- [x] CI (fmt, vet, test, build)
- [x] Verified round-trip against a live Bee node (2.8.1, rclone: mkdir/copy/ls/cat/delete; object also fetched directly from Bee via its `x-swarm-reference`)

## Phase 1 — MVP

Exit: rclone syncs a real tree; s3-tests subset green.

- [x] SQLite index (§5) — cgo-free (modernc.org/sqlite), WAL mode, conformance suite shared with the in-memory store; persistence verified across a gateway restart against a live Bee node
- [x] Stamp manager v1 (§9): cached batch state, synchronous pre-upload validation (402 `SwarmPostageError` per the §6 ack rule), `x-swarm-postage-batch-id`/`x-swarm-batch-ttl` on PUT/GET/HEAD, background capacity/TTL warnings — Prometheus gauges land with the metrics item
- [x] Conditional-request hardening (§10): conditional PUT (`If-Match`/`If-None-Match`, atomic at the index) and `x-amz-copy-source-if-*`; +6 s3-tests in the manifest (208 total)
- [x] Explicit ack-policy surface, `network`/`node` tiers (§6): `-ack` flag replaces `-deferred`
- [x] docker-compose harness (§15) — fakebee (in-memory Bee-API stand-in; Bee ≥2.x removed `dev` mode) + batch-init + gateway
- [x] s3-tests subset in CI with pass manifest (§15) — 202 Ceph s3-tests green in `test/s3tests/passing.txt`, curated against a live Bee node, run in CI against fakebee; required implementing ListObjectVersions with unversioned (null-version) semantics
- [x] Prometheus metrics (§16): `/_s3warm/metrics` — request rates/latencies, Bee upstream ops, object bytes in/out, stamp TTL + utilization gauges

## Phase 2 — compatibility

Exit: restic + Java SDK v2 work; buckets browsable via `bzz://`; snapshot → rollback round-trip.

- [x] Multipart uploads (§7): full API set, composite objects with range-stitched reads, retry-idempotent + conditional complete, `?partNumber` reads; manifest at 250 s3-tests. Still open: async consolidation, UploadPartCopy from composite sources
- [x] Presigned URLs (§8): AWS doc vector in unit tests; expiry semantics match AWS
- [x] Streaming signatures (aws-chunked) + flexible checksums (§8): all three payload variants, chunk/trailer signature chains, five checksum algorithms; multipart composite/FULL_OBJECT checksums still open
- [x] SSE via `swarm-encrypt` (§12): per-request AES256, bucket default-encryption config, gateway-wide flag; key-bearing references stay private; verified against a live Bee node (encrypt on PUT, transparent decrypt on GET)
- [x] Commit-chain manifests + feed checkpoints (§5): debounced mantaray commits (Bee's own mantaray package over /bytes), commit document with parent links, `-feed-key` sequence-feed anchors; verified live — bucket browsable via `GET /bzz/{root}/{key}` on the node, feed resolves to the commit root. Still open: interval/manual checkpoint policies (currently every-commit), incremental manifest builds
- [x] Snapshots & rollback (§5): `?x-swarm-snapshot` create/list (pinned roots), `?x-swarm-restore` atomic whole-bucket rollback by label or raw root; HeadBucket exposes `x-swarm-bucket-root`/`x-swarm-commit-seq`
- [x] Read-side erasure-coding fetch strategies (§17): `-fetch-strategy` default + per-request `x-swarm-redundancy-strategy`/`-fallback-mode`
- [x] GetObjectAttributes: ETag/Checksum/ObjectParts/StorageClass/ObjectSize with part pagination
- [x] CORS: per-bucket config (Get/Put/Delete ?cors), S3 wildcard-origin matching, pre-auth preflights and response decoration (full s3-tests CORS family remains gated on anonymous/ACL reads)

## Phase 3 — production

Exit: multi-gateway behind LB; a grantee reads a private bucket off-gateway.

- [x] Versioning suite (§11): Enabled/Suspended modes, version rows with delete markers and latest-promotion, `?versionId` on GET/HEAD/DELETE/Copy/Attributes, real ListObjectVersions with pagination; index-only — old bytes were already retained by content addressing. Bucket restore flattens version history (noted)
- [ ] ACT-backed grants + key-based multi-tenancy (§8)
- [x] Object/bucket tagging: per-version object tag sets and bucket tag sets, `x-amz-tagging` header on PUT/initiate-multipart, copy `x-amz-tagging-directive`, `x-amz-tagging-count` on reads
- [ ] Postgres index for HA (§10)
- [x] Chequebook auto top-up: daily check, deposits wallet→chequebook below `-chequebook-min`, never touching the postage reserve (first slice of the money autopilot)
- [ ] Stamp autopilot (§9)
- [ ] PSS/GSOC notification research (§21)
