#!/usr/bin/env bash
set -euo pipefail

source_repo=/mnt/c/Users/yuech/source/repos/yuechen-li-dev/OctetDB
work=$(mktemp -d -p /home/yuech octetdb-group-m0.XXXXXX)
case "$work" in
  /home/yuech/octetdb-group-m0.*) ;;
  *) exit 90 ;;
esac
cleanup() { rm -rf -- "$work"; }
trap cleanup EXIT

cp -a "$source_repo/." "$work/repo"
mkdir "$work/tmp"
cd "$work/repo"
TMPDIR="$work/tmp" go test -run TestGroupCommitDevHarness -count=1 -args \
  -group-dev-output "$source_repo/docs/product/evidence/GROUP_COMMIT_M0/dev-harness/current-wsl-ext4.json"
