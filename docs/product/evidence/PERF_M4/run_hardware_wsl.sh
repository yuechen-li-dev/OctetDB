#!/usr/bin/env bash
set -euo pipefail

repo=/mnt/c/Users/yuech/source/repos/yuechen-li-dev/OctetDB
binary=/home/yuech/tigercompare/perfm4-harness
data_root=/home/yuech/tigercompare/perfm4-hardware
evidence="$repo/docs/product/evidence/PERF_M4/profiles/wsl"
events=cycles,instructions,cache-references,cache-misses,branches,branch-misses

mkdir -p "$data_root" "$evidence"
cd "$repo/docs/product/evidence/PERF_M4/harness"
go build -trimpath -o "$binary" .

run_case() {
    local name="$1" lane="$2" workload="$3" population="$4" operations="$5"
    shift 5
    local data="$data_root/$name"
    test "${data#"$data_root"/}" != "$data"
    mkdir -p "$data"
    "$binary" -lane "$lane" -workload "$workload" -population "$population" \
        -operations 1 -warmup 0 -concurrency 1 -data "$data" "$@" \
        -output "$evidence/$name-seed.json"
    perf stat -x, -e "$events" -o "$evidence/$name.perf.csv" -- \
        "$binary" -lane "$lane" -workload "$workload" -population "$population" \
        -operations "$operations" -warmup 100 -concurrency 1 -data "$data" -skip-seed "$@" \
        -output "$evidence/$name.json"
    rm -rf -- "$data"
}

run_case w1-default default w1 1000 10000 -contention uniform
run_case w5-default default w5 10000 1000 -query-op filter -selectivity 25
run_case w5-specialized specialized w5 10000 100000 -query-op filter -selectivity 25
