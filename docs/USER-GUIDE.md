# s3warm User Guide

*For people who know Amazon S3 and want their buckets on Swarm — no Swarm
knowledge required.*

s3warm is a gateway: it speaks the S3 API you already use (AWS CLI, boto3,
rclone, any SDK) and stores your objects on [Swarm](https://www.ethswarm.org/),
a decentralized storage network. For your tools, moving to Swarm is a
configuration change — usually just the endpoint URL.

This guide gets you from zero to working buckets, explains the handful of
things that genuinely work differently, and walks through five hands-on
demos. The exhaustive option-by-option documentation lives in the
[Reference Manual](REFERENCE.md).

---

## 1. The mental model

Everything you know still applies: buckets, keys, ETags, multipart uploads,
presigned URLs, SigV4 credentials. Three things are genuinely different
underneath, and they're features once you know them:

**1. Storage is prepaid rent, not a monthly bill.**
On Swarm you buy a *postage batch* — a prepaid allowance of storage with a
size and a lifetime (TTL). Every upload is stamped from a batch. When a
batch expires, its data expires. s3warm manages the batch for you (you
configure one default), tells you the remaining lifetime on every response
(`x-swarm-batch-ttl`, in seconds), and warns in its logs and metrics as a
batch runs low. Topping up a batch extends the life of everything stamped
with it. If a batch is dead or full, writes fail fast with a clear
`402 SwarmPostageError` instead of pretending.

**2. Delete removes the listing, not the bytes.**
Swarm has no delete. `DeleteObject` removes the key from your bucket
immediately (it disappears from listings and GETs), but the underlying bytes
remain on the network until their postage batch expires. If you handle data
with deletion-compliance requirements, encrypt it (see SSE below) — deleting
an encrypted object orphans ciphertext.

**3. Every object has a permanent public address.**
Content on Swarm is content-addressed: each object you upload gets a
*Swarm reference* — returned as the `x-swarm-reference` header — and can be
fetched through **any** Swarm gateway on the planet, with no s3warm and no
credentials in the path. Whole buckets get an address too (more below).
That's the point of the exercise: S3 compatibility without S3 lock-in.
The flip side: treat unencrypted objects as public-if-the-reference-leaks.
For private data, enable server-side encryption — then the reference embeds
the decryption key and s3warm never exposes it.

---

## 2. Ten-minute start (no Swarm node needed)

The repo ships a dev stack — the gateway plus an in-memory stand-in for a
Swarm node — so you can try everything on a laptop:

```bash
git clone https://github.com/petfold/s3warm && cd s3warm
docker compose up -d --build
```

The S3 endpoint is now `http://localhost:8333` with credentials
`s3warmdev` / `s3warmdevsecret`. Point the AWS CLI at it:

```bash
export AWS_ACCESS_KEY_ID=s3warmdev
export AWS_SECRET_ACCESS_KEY=s3warmdevsecret
export AWS_DEFAULT_REGION=us-east-1

aws --endpoint-url http://localhost:8333 s3 mb s3://hello
echo swarm > hi.txt
aws --endpoint-url http://localhost:8333 s3 cp hi.txt s3://hello/
aws --endpoint-url http://localhost:8333 s3 ls s3://hello/
```

That's a working S3 dialect. Data in the dev stack is not on the real Swarm
network — for that, keep reading.

## 3. Running against a real Swarm node

### Getting a Bee node

If you don't have a node yet, two good paths:

- **Easiest:** [Swarm Desktop](https://github.com/ethersphere/swarm-desktop)
  wraps a Bee node in a point-and-click app — install, click, you have a
  node with its API on `localhost:1633`.
- **For servers** (where a gateway usually lives): the official
  [Bee quick-start](https://docs.ethswarm.org/docs/bee/installation/quick-start/)
  — a one-line installer plus a short config walkthrough.

**Fund it before it can store anything.** A fresh node has its own wallet
on [Gnosis Chain](https://www.gnosis.io/) (an Ethereum sidechain — that's
where Swarm's payments happen, because fees there cost fractions of a
cent). It needs two things in that wallet:

- **xDAI** — DAI is a USD-pegged stablecoin; xDAI is its Gnosis Chain
  form, used there as the native coin that pays transaction gas. A few
  dollars' worth is plenty.
- **xBZZ** — BZZ is the Swarm network's own token (issued on Ethereum);
  xBZZ is the same token bridged to Gnosis Chain. It's what actually buys
  storage (postage batches) and settles bandwidth.

The easiest way to get both: the official
[Swarm funding tool](https://fund.ethswarm.org/) takes ETH, DAI, or most
any Ethereum-side coin or token and delivers xDAI + xBZZ straight to your
node's wallet in one go. (Manual route: acquire them on an exchange or
bridge and send them yourself.)

One transfer of each, to the wallet address the node prints on first start
(also at `curl localhost:1633/addresses`), and the money part is
bootstrapped: on its next start the node automatically deploys its
*chequebook* (the payment contract it settles bandwidth with) and seeds it
with an initial xBZZ deposit from that wallet, and the same wallet pays
for the postage batch below.

One caveat to know: that chequebook seeding is a **one-time** event — Bee
does not top the chequebook up from the wallet later, however full the
wallet is. Light and moderate traffic rarely exhausts the initial deposit,
but if your gateway serves heavy download traffic, glance at
`curl localhost:1633/chequebook/balance` occasionally and refill with
`curl -X POST "localhost:1633/chequebook/deposit?amount=…"` if the
available balance runs low. (Postage batches and top-ups keep drawing from
the wallet directly, so those are unaffected.)

### Buying a postage batch

With a funded node running, buy the batch your uploads will be stamped
from:

```bash
# Buy a batch on your node (size/lifetime tradeoffs: see the Bee docs).
curl -X POST "http://localhost:1633/stamps/100000000000/24"
# → {"batchID":"<64 hex chars>"}
```

### Starting the gateway

Then run s3warm next to the node:

```bash
s3warm \
  -bee-api    http://localhost:1633 \
  -batch-id   <your batch id> \
  -access-key <choose one> -secret-key <choose one> \
  -db         /var/lib/s3warm/index.db
```

Check readiness: `curl http://localhost:8333/_s3warm/ready`. Every flag has
an `S3WARM_*` environment twin — the full table is in the
[Reference](REFERENCE.md#configuration).

**Keeping an eye on your batch.** Every PUT/GET/HEAD response carries
`x-swarm-postage-batch-id` and `x-swarm-batch-ttl` (seconds of life left).
The gateway logs warnings when a batch passes 80% capacity or drops under
30 days, and exports both as Prometheus gauges at `/_s3warm/metrics`.
Extend a batch's life anytime with a top-up on your node:
`curl -X PATCH "http://localhost:1633/stamps/topup/<batch>/<amount>"`.

## 4. Pointing your existing tools at s3warm

The pattern is always the same three settings: **endpoint URL**, **your
s3warm credentials**, **region `us-east-1`** (any region string works — the
gateway accepts them all — but `us-east-1` is what tools default to).

**AWS CLI** — either `--endpoint-url` per command, or once in
`~/.aws/config`:

```ini
[profile swarm]
region = us-east-1
endpoint_url = s3=http://localhost:8333
```
(older CLIs without `endpoint_url` support: keep using `--endpoint-url`,
or `export AWS_ENDPOINT_URL_S3=http://localhost:8333` on CLI v2 ≥ 2.13.)

**boto3 / Python:**

```python
import boto3
from botocore.config import Config

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:8333",
    aws_access_key_id="...", aws_secret_access_key="...",
    region_name="us-east-1",
    config=Config(signature_version="s3v4"),  # needed for presigned URLs
)
```

The `signature_version` line matters: on custom endpoints boto3 silently
generates *legacy SigV2* presigned URLs, which s3warm (like modern S3
regions) rejects.

**rclone** — `rclone config`: type `s3`, provider `Other`,
endpoint `http://localhost:8333`, plus your keys. Or purely via environment:

```bash
export RCLONE_S3_PROVIDER=Other RCLONE_S3_ENDPOINT=http://localhost:8333 \
       RCLONE_S3_ACCESS_KEY_ID=... RCLONE_S3_SECRET_ACCESS_KEY=... \
       RCLONE_S3_REGION=us-east-1
rclone ls :s3:mybucket
```

**Any other SDK** — the checklist: set the endpoint; use path-style
addressing if the SDK asks (most use it automatically on custom endpoints);
SigV4 signing (the default everywhere modern); region `us-east-1`.

## 5. Hands-on demos

Runnable scripts for all of these live in [`demos/`](../demos/); each works
against the dev stack unless noted. What they cover, in ascending order of
"you can't do this on S3":

**Demo 1 — [first bucket with the AWS CLI](../demos/01-quickstart.sh).**
The full round trip, plus your first look at the Swarm identity every
object gets:

```text
==> The Swarm-native identity of the object (x-swarm-* headers)
X-Swarm-Batch-Ttl: 31535979
X-Swarm-Postage-Batch-Id: cdcd…cdcd
X-Swarm-Reference: f8548b4fd758e48b8fcaa6a6191c085fe368e484bc4331b277f780b13fa7dad3
```

That reference is fetchable from any Swarm gateway:
`curl https://gateway.ethswarm.org/bytes/<reference>`.

**Demo 2 — [folder backup and restore with rclone](../demos/02-backup-restore.sh).**
The most common S3 workload of all. `rclone sync` up, keep working (only
changes transfer), `rclone copy` back, `diff -r` proves the restore is
byte-identical.

**Demo 3 — [publish a website, browse it without the gateway](../demos/03-static-site/publish.sh).**
Deploy with plain `aws s3 sync site s3://site-demo`. A few seconds later the
bucket has a *commit root* (`x-swarm-bucket-root` on HeadBucket) — a real
Swarm manifest of the whole site, servable by **any** Bee node:

```text
site root: 9682810ac221194a6e731c47b49b9587c8a8276b69096b6128c200a7637bc969
  http://localhost:1633/bzz/9682…c969/index.html
  https://gateway.ethswarm.org/bzz/9682…c969/index.html
==> Verified: index.html served natively via bzz://
```

An S3 deploy workflow, decentralized hosting output. (This step needs a real
Bee node; the dev stack prints the URLs and skips the fetch.)

**Demo 4 — [the one-line boto3 switch](../demos/04-boto3-switch/app.py).**
An existing Python app moved to Swarm by changing only the client
constructor — uploads, metadata round-trips, and a presigned share link
that works in any browser with no credentials.

**Demo 5 — [snapshot and rollback](../demos/05-snapshot-rollback.sh).**
The one S3 can't match. Snapshot a bucket, wreck it, restore everything
with one call:

```text
==> Snapshot it (label: golden)
{"bucket":"…","label":"golden","root":"837fc9c0…","seq":1}
==> Now break everything
==> One call: atomic whole-bucket rollback
{"bucket":"…","objects":2,"root":"837fc9c0…","seq":1}
==> Everything is back
```

Because every bucket commit is an immutable Swarm manifest, a snapshot is
just a label on a 32-byte root — and restore is pointing the bucket back at
it. Details and the API: [Reference §snapshots](REFERENCE.md#snapshots--rollback).

## 6. Server-side encryption

Add the standard S3 header and Swarm's native encryption takes over:

```bash
aws --endpoint-url http://localhost:8333 s3 cp secret.txt s3://vault/ \
    --sse AES256
```

Bee encrypts on write and decrypts transparently on read; the
key-embedding reference never leaves the gateway (no `x-swarm-reference` on
encrypted objects). Set it per request, as a bucket default
(`put-bucket-encryption`, exactly as on AWS), or gateway-wide (`-encrypt`).
SSE-C and SSE-KMS are rejected rather than silently ignored.

## 7. What's supported, what isn't (yet)

The executable answer is [`test/s3tests/passing.txt`](../test/s3tests/passing.txt)
— every Ceph s3-tests conformance test s3warm passes, run in CI — and the
per-operation [compatibility matrix](API-COMPATIBILITY.md). The short
version: objects, listings, multipart, conditional requests, presigned
URLs, streaming signatures, checksums, SSE-S3 and CORS all work. The
common 501s you might hit: ACL mutation, versioning, object tagging and
bucket policies (all on the [roadmap](../ROADMAP.md)).

## 8. Troubleshooting

| Symptom | Cause & fix |
|---|---|
| `402 SwarmPostageError` on writes | Your postage batch is missing, expired, not yet usable, or full. Check `curl http://<bee>/stamps/<batch>`; top up or buy a new batch and update `-batch-id` |
| `403 SignatureDoesNotMatch` | Wrong secret key, or clock skew over 15 minutes — check `date` |
| Presigned URL gives `AccessDenied: anonymous access is disabled` and contains `AWSAccessKeyId=` | Your SDK generated a legacy SigV2 URL. boto3: `Config(signature_version="s3v4")` |
| `501 NotImplemented` | That operation isn't supported yet — see the [matrix](API-COMPATIBILITY.md) |
| `503 ServiceUnavailable`, "bee unreachable" | The gateway can't reach the Bee node — check `-bee-api` and `curl http://<bee>/health` |
| No `x-swarm-bucket-root` on HeadBucket | The first commit hasn't run yet (debounced a few seconds after the first write), or the gateway runs with `-commit off` |
| Writes slow | You're on `-ack network` (waits for network sync). The default `node` acks after the local store — see [Reference §ack policy](REFERENCE.md#ack-policy) |
