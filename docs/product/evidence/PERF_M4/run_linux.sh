#!/usr/bin/env bash
set -euo pipefail

suite="${1:-all}"
repetitions="${2:-3}"
repo=/mnt/c/Users/yuech/source/repos/yuechen-li-dev/OctetDB
root="$repo/docs/product/evidence/PERF_M4"
harness="$root/harness"
raw="$root/raw/wsl"
binary=/home/yuech/tigercompare/perfm4-harness
data_root=/home/yuech/tigercompare/perfm4-linux-data
postgres='postgres://postgres@127.0.0.1:54330/tigercompare?sslmode=disable'

mkdir -p "$raw" "$data_root"
cd "$harness"
go build -trimpath -o "$binary" .

run_one() {
    local name="$1" lane="$2" workload="$3" population="$4" operations="$5" concurrency="$6" rep="$7"
    shift 7
    local output="$raw/$name-$lane-r$rep.json"
    local data="$data_root/$name-$lane-r$rep"
    test ! -e "$output"
    test "${data#"$data_root"/}" != "$data"
    mkdir -p "$data"
    "$binary" -lane "$lane" -workload "$workload" -population "$population" \
        -operations "$operations" -concurrency "$concurrency" \
        -warmup "$(( operations / 10 > 100 ? 100 : operations / 10 ))" \
        -postgres "$postgres" -data "$data" -output "$output" "$@"
    rm -rf -- "$data"
}

run_case() {
    local name="$1" workload="$2" population="$3" operations="$4" concurrency="$5"
    shift 5
    for rep in $(seq 1 "$repetitions"); do
        case "$rep" in
            1) lanes=(postgres default specialized) ;;
            2) lanes=(default specialized postgres) ;;
            *) lanes=(specialized postgres default) ;;
        esac
        for lane in "${lanes[@]}"; do
            run_one "$name" "$lane" "$workload" "$population" "$operations" "$concurrency" "$rep" "$@"
        done
    done
}

if [[ "$suite" == primary || "$suite" == all ]]; then
    for workload in w1 w2 w3 w4; do run_case "primary-$workload-c8" "$workload" 1000 1000 8; done
    run_case primary-w5-c8 w5 10000 1000 8
    run_case primary-w6-c8-70r20w10q w6 1000 5000 8
fi

if [[ "$suite" == query || "$suite" == all ]]; then
    for query_op in point filter take map count; do
        run_case "w5-$query_op-s25-p10000-c1" w5 10000 500 1 -query-op "$query_op" -selectivity 25
    done
    for selectivity in early 1 10 50 100 none; do
        run_case "w5-take-s$selectivity-p10000-c1" w5 10000 500 1 -query-op take -selectivity "$selectivity"
    done
fi

if [[ "$suite" == dimensions || "$suite" == all ]]; then
    run_case w1-uniform-c1 w1 1000 1000 1 -contention uniform
    run_case w1-hotset-c8 w1 1000 1000 8 -contention hotset
    run_case w1-hotkey-c32 w1 1000 1000 32 -contention hotkey
    run_case w5-filter-s25-p1000-c1 w5 1000 500 1 -query-op filter -selectivity 25
    run_case w5-filter-s25-p100000-c1 w5 100000 100 1 -query-op filter -selectivity 25
    run_case w6-c8-50r40w10q w6 1000 5000 8 -mix 50r40w10q
fi

echo "PERF-M4 Linux suite complete: $raw"
