# s3warm

**An Amazon S3–compatible API gateway for [Swarm](https://www.ethswarm.org/).**

s3warm exposes the S3 REST API (AWS Signature V4, XML, the whole dialect) and translates it onto a [Bee](https://github.com/ethersphere/bee) node — so the enormous ecosystem of S3 tooling (`aws` CLI, boto3, rclone, `mc`, restic, every SDK) can read and write decentralized, content-addressed, censorship-resistant storage without knowing Swarm exists.

And without lock-in in the other direction either: every object written through s3warm has a public Swarm reference (returned as `x-swarm-reference`) and remains retrievable from *any* Swarm gateway.

> **Status: design + early skeleton.** The [design document](docs/DESIGN.md) is the primary artifact; the Go code implements the phase-0 skeleton (routing, SigV4, S3 errors, in-memory index, Bee client, core bucket/object operations). See the [API compatibility matrix](docs/API-COMPATIBILITY.md).

```
S3 clients (aws cli, boto3, rclone, mc, restic, SDKs)
        │  S3 REST + SigV4
        ▼
   s3warm gateway  ── metadata index (listings, ETags, metadata)
        │  Bee HTTP API (/bytes, /stamps, /feeds, …)
        ▼
   Bee node ──► Swarm network
```

## Quickstart (local dev)

Requires Go ≥ 1.22 and a Bee binary.

```bash
# 1. Run a dev-mode Bee node (in-memory, fake chain)
bee dev

# 2. Buy a (dev) postage batch
BATCH=$(curl -s -X POST http://localhost:1633/stamps/100000000/24 | sed -n 's/.*"batchID":"\([^"]*\)".*/\1/p')

# 3. Run s3warm
S3WARM_BATCH_ID=$BATCH \
S3WARM_ACCESS_KEY=dev S3WARM_SECRET_KEY=devsecret \
go run ./cmd/s3warm

# 4. Use any S3 client
export AWS_ACCESS_KEY_ID=dev AWS_SECRET_ACCESS_KEY=devsecret AWS_DEFAULT_REGION=us-east-1
aws --endpoint-url http://localhost:8333 s3 mb s3://demo
aws --endpoint-url http://localhost:8333 s3 cp README.md s3://demo/docs/readme.md
aws --endpoint-url http://localhost:8333 s3 ls s3://demo/docs/
```

With rclone: `rclone config` → type `s3`, provider `Other`, endpoint `http://localhost:8333`.

## Configuration

| Flag | Env | Default | Meaning |
|---|---|---|---|
| `-listen` | `S3WARM_LISTEN` | `:8333` | S3 API listen address |
| `-db` | `S3WARM_DB` | `s3warm.db` | SQLite metadata index path (empty = in-memory, dev only) |
| `-bee-api` | `S3WARM_BEE_API` | `http://127.0.0.1:1633` | Bee node API endpoint |
| `-batch-id` | `S3WARM_BATCH_ID` | — | Default postage batch id (required for writes) |
| `-access-key` | `S3WARM_ACCESS_KEY` | — | Access key id (empty = anonymous dev mode) |
| `-secret-key` | `S3WARM_SECRET_KEY` | — | Secret access key |
| `-region` | `S3WARM_REGION` | `us-east-1` | Region label reported to clients |
| `-redundancy` | `S3WARM_REDUNDANCY` | `0` | Default erasure-coding level 0–4 (`STANDARD` storage class) |
| `-encrypt` | `S3WARM_ENCRYPT` | `false` | Encrypt uploads on Swarm (SSE) |
| `-ack` | `S3WARM_ACK` | `node` | PUT ack policy (design §6): `node` (Bee local store, network push follows) or `network` (pushed before ack) |
| `-domain` | `S3WARM_DOMAIN` | — | Base domain for virtual-host-style addressing (`bucket.domain`) |

Operational endpoints (never shadowed by buckets): `/_s3warm/health`, `/_s3warm/ready`,
and Prometheus metrics at `/_s3warm/metrics` (requests, Bee upstream latencies,
object bytes in/out, postage batch TTL and utilization gauges).

## Docker / compose

```bash
docker compose up -d --build   # fakebee (in-memory Bee stand-in) + s3warm on :8333
```

Bee removed its `dev` mode, so the compose stack ships `fakebee` — an in-memory
stand-in for the Bee API subset s3warm uses. To run against a real Bee node,
drop the `bee`/`batch-init` services and set `S3WARM_BEE_API` + `S3WARM_BATCH_ID`.

## Conformance (Ceph s3-tests)

The compatibility claim is executable: [`test/s3tests/passing.txt`](test/s3tests/passing.txt)
lists every test from [Ceph's s3-tests](https://github.com/ceph/s3-tests) suite
that s3warm passes, and CI runs exactly that manifest against the gateway
(backed by fakebee). The manifest itself is curated against a real Bee node.

```bash
docker compose up -d --build
test/s3tests/run.sh            # clones s3-tests, sets up a venv, runs the manifest
```

## Development

```bash
make build   # go build ./...
make test    # go test ./... (includes AWS SigV4 test vectors + API round-trip vs a fake Bee)
make run     # run the gateway
```

Repository layout:

```
cmd/s3warm/        entry point
internal/api/      S3 REST routing, XML types, error envelope, handlers
internal/auth/     AWS Signature V4 verification
internal/bee/      Bee HTTP API client
internal/store/    metadata index (interface + SQLite and in-memory implementations)
internal/config/   configuration
docs/              design document + compatibility matrix
```

## Documents

- [Design](docs/DESIGN.md) — architecture, S3↔Swarm concept mapping, multipart strategy, postage stamp management, consistency model, roadmap rationale.
- [Roadmap](ROADMAP.md) — phase-by-phase progress with tickmarks.
- [API compatibility matrix](docs/API-COMPATIBILITY.md) — per-operation status.

## License

[BSD 3-Clause](LICENSE).
