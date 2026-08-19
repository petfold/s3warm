# Demos

Runnable walkthroughs for the [User Guide](../docs/USER-GUIDE.md). Each one
is a popular S3 use case, pointed at Swarm.

## Setup

Every demo talks to an s3warm gateway at `$S3WARM_ENDPOINT`
(default `http://localhost:8333`) with the dev credentials. The quickest
gateway is the repo's compose stack:

```bash
docker compose up -d --build
```

To run against a real Swarm node instead, start the gateway yourself
(see the [User Guide](../docs/USER-GUIDE.md#running-against-a-real-swarm-node))
— demo 3's `bzz://` browsing needs a real node.

Override defaults via environment: `S3WARM_ENDPOINT`, `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`.

| Demo | Use case | Needs |
|---|---|---|
| [`01-quickstart.sh`](01-quickstart.sh) | First bucket and object with the AWS CLI | `aws` |
| [`02-backup-restore.sh`](02-backup-restore.sh) | Folder backup + restore | `rclone` |
| [`03-static-site/publish.sh`](03-static-site/publish.sh) | Publish a website, browse it Swarm-natively via `bzz://` | `aws`, a real Bee node for the bzz step |
| [`04-boto3-switch/app.py`](04-boto3-switch/app.py) | Point an existing Python app at Swarm; presigned share links | `python3` + `boto3` |
| [`05-snapshot-rollback.sh`](05-snapshot-rollback.sh) | Snapshot a bucket, break it, roll it back atomically | `aws`, `curl` ≥ 7.75 |
