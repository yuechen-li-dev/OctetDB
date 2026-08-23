#!/usr/bin/env bash
set -euo pipefail

source_repo=/mnt/c/Users/yuech/source/repos/yuechen-li-dev/OctetDB
mode=${1:-current}
case "$mode" in current|v0.2.0) ;; *) echo "usage: $0 [current|v0.2.0]" >&2; exit 2 ;; esac
output_dir="$source_repo/docs/product/evidence/GROUP_COMMIT_M0/raw/wsl-$mode"
work=$(mktemp -d -p /home/yuech octetdb-group-formal.XXXXXX)
case "$work" in
  /home/yuech/octetdb-group-formal.*) ;;
  *) exit 90 ;;
esac
cleanup() { rm -rf -- "$work"; }
trap cleanup EXIT

cp -a "$source_repo/." "$work/repo"
mkdir -p "$work/tmp" "$work/data" "$output_dir"
cd "$work/repo/docs/product/evidence/PERF_M4/harness"
if [[ "$mode" == current ]]; then
  go mod edit -replace github.com/yuechen-li-dev/octetdb="$work/repo"
fi
TMPDIR="$work/tmp" go build -o "$work/perf-m4" .
for workload in w1 w2 w3 w4; do
  for concurrency in 1 8 32; do
    "$work/perf-m4" \
      -lane default -workload "$workload" -population 1000 -operations 384 \
      -concurrency "$concurrency" -warmup 32 \
      -data "$work/data/$workload-c$concurrency" \
      -output "$output_dir/$workload-c$concurrency.json"
  done
done
