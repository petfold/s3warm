#!/usr/bin/env bash
# Runs the curated subset of Ceph's s3-tests (the industry S3 conformance
# suite) against a local s3warm gateway. The manifest test/s3tests/passing.txt
# is the compatibility claim: every test in it must pass.
#
# Prerequisites: the gateway reachable per s3tests.conf (e.g. the repo's
# docker-compose stack), python3 + venv, git, network for the first run.
set -euo pipefail
cd "$(dirname "$0")"

REPO_DIR=${S3TESTS_DIR:-.s3-tests}
S3TESTS_URL=https://github.com/ceph/s3-tests

if [ ! -d "$REPO_DIR" ]; then
    git clone --quiet "$S3TESTS_URL" "$REPO_DIR"
fi
# Pin: the revision the passing manifest was curated against.
S3TESTS_PIN=${S3TESTS_PIN:-5522d1c351f75bc00ae0f64f742f3f095f5939d9}
git -C "$REPO_DIR" checkout --quiet "$S3TESTS_PIN" 2>/dev/null ||
    { git -C "$REPO_DIR" fetch --quiet && git -C "$REPO_DIR" checkout --quiet "$S3TESTS_PIN"; }

VENV="$REPO_DIR/.venv"
if [ ! -x "$VENV/bin/pytest" ]; then
    python3 -m venv "$VENV"
    "$VENV/bin/pip" install --quiet --upgrade pip
    "$VENV/bin/pip" install --quiet -r "$REPO_DIR/requirements.txt" pytest
fi

export S3TEST_CONF="$PWD/s3tests.conf"

mapfile -t tests < <(grep -vE '^\s*(#|$)' passing.txt)
echo "running ${#tests[@]} tests from passing.txt against $(grep -m1 '^port' s3tests.conf | tr -d ' ')"

cd "$REPO_DIR"
exec .venv/bin/python -m pytest -q "${tests[@]}"
