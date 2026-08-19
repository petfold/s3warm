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
- [ ] Conditional-request hardening (§10)
- [ ] Explicit ack-policy surface, `network`/`node` tiers (§6)
- [ ] docker-compose harness with `bee dev` (§15)
- [ ] s3-tests subset in CI with pass/fail manifest (§15)
- [ ] Prometheus metrics + structured logging polish (§16)

## Phase 2 — compatibility

Exit: restic + Java SDK v2 work; buckets browsable via `bzz://`; snapshot → rollback round-trip.

- [ ] Multipart uploads: composite objects + optional consolidation (§7)
- [ ] Presigned URLs (§8)
- [ ] Streaming signatures (aws-chunked) + trailing checksums (§8)
- [ ] SSE via `swarm-encrypt` (§12)
- [ ] Commit-chain manifests + checkpointed feeds (§5)
- [ ] Snapshots & rollback, `x-swarm-bucket-root`/`x-swarm-commit-seq` (§5)
- [ ] Read-side erasure-coding fetch strategies (§17)
- [ ] GetObjectAttributes, CORS

## Phase 3 — production

Exit: multi-gateway behind LB; a grantee reads a private bucket off-gateway.

- [ ] Versioning suite (§11)
- [ ] ACT-backed grants + key-based multi-tenancy (§8)
- [ ] Object/bucket tagging
- [ ] Postgres index for HA (§10)
- [ ] Stamp autopilot (§9)
- [ ] PSS/GSOC notification research (§21)
