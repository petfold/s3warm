# s3warm

**An Amazon S3–compatible API gateway for [Swarm](https://www.ethswarm.org/).**

s3warm exposes the S3 REST API (AWS Signature V4, XML, the whole dialect) and
translates it onto a [Bee](https://github.com/ethersphere/bee) node — so the
enormous ecosystem of S3 tooling (`aws` CLI, boto3, rclone, `mc`, every SDK)
can read and write decentralized, content-addressed, censorship-resistant
storage without knowing Swarm exists.

And without lock-in in the other direction either: every object has a public
Swarm reference (`x-swarm-reference`), and every bucket becomes a real Swarm
manifest — browsable via `bzz://{root}/{key}` on **any** Bee node, no gateway
in the path.

```
S3 clients (aws cli, boto3, rclone, mc, restic, SDKs)
        │  S3 REST + SigV4
        ▼
   s3warm gateway  ── metadata index (listings, ETags, snapshots)
        │  Bee HTTP API (/bytes, /stamps, /soc, …)
        ▼
   Bee node ──► Swarm network
```

**Status:** 307 tests of Ceph's s3-tests conformance suite green in CI
([the executable claim](test/s3tests/passing.txt)); objects, listings,
multipart, conditional writes, presigned URLs, streaming signatures,
checksums, SSE-S3, CORS, versioning, tagging and multi-tenant credentials —
plus Swarm-native commit chains with atomic whole-bucket snapshot/rollback,
and private buckets whose grants are enforced by Swarm itself (ACT): a
grantee reads them from their own Bee node with no gateway in the path. See
the [compatibility matrix](docs/API-COMPATIBILITY.md) and
[roadmap](ROADMAP.md).

## Try it in ten minutes

```bash
docker compose up -d --build     # gateway + in-memory dev backend on :8333

export AWS_ACCESS_KEY_ID=s3warmdev AWS_SECRET_ACCESS_KEY=s3warmdevsecret AWS_DEFAULT_REGION=us-east-1
aws --endpoint-url http://localhost:8333 s3 mb s3://demo
aws --endpoint-url http://localhost:8333 s3 cp README.md s3://demo/
```

Then follow the **[User Guide](docs/USER-GUIDE.md)** — written for people who
know S3 and don't know Swarm: the three things that genuinely differ
(prepaid storage, delete semantics, public content addresses), pointing your
existing tools at the gateway, and running against a real node.

## Demos

Runnable walkthroughs in [`demos/`](demos/):

| | Popular use case |
|---|---|
| [01](demos/01-quickstart.sh) | First bucket with the unchanged AWS CLI |
| [02](demos/02-backup-restore.sh) | Folder backup + byte-identical restore with rclone |
| [03](demos/03-static-site/publish.sh) | Deploy a website with `aws s3 sync`, browse it Swarm-natively via `bzz://` |
| [04](demos/04-boto3-switch/app.py) | Move an existing boto3 app with one constructor change; presigned share links |
| [05](demos/05-snapshot-rollback.sh) | Snapshot a bucket, wreck it, roll it back atomically — the thing S3 can't do |

## Documentation

- **[User Guide](docs/USER-GUIDE.md)** — tutorial for S3 users moving to Swarm.
- **[Reference Manual](docs/REFERENCE.md)** — every flag, header, extension, error, metric and limit.
- **[Design](docs/DESIGN.md)** — architecture, S3↔Swarm concept mapping, consistency model, rationale.
- **[API compatibility matrix](docs/API-COMPATIBILITY.md)** — per-operation status.
- **[Roadmap](ROADMAP.md)** — phase-by-phase progress.

## Development

```bash
make build   # go build ./...
make test    # unit tests: AWS SigV4 vectors, store conformance, API round-trips
test/s3tests/run.sh   # the 307-test conformance manifest (needs the compose stack)
```

Repository layout:

```
cmd/s3warm/        the gateway
cmd/fakebee/       in-memory Bee-API stand-in for dev/CI
internal/api/      S3 REST routing, XML, handlers, CORS, snapshots, grants
internal/auth/     SigV4: header, presigned, streaming (aws-chunked)
internal/bee/      Bee HTTP API client
internal/manifest/ commit chain: mantaray manifests, feeds, committer
internal/stamp/    postage batch manager
internal/store/    metadata index (SQLite + in-memory)
demos/             runnable walkthroughs
docs/              user guide, reference, design, matrix
test/s3tests/      Ceph s3-tests conformance harness + passing manifest
```

## License

[BSD 3-Clause](LICENSE).
