#!/usr/bin/env bash
set -euo pipefail

repo=/mnt/c/Users/yuech/source/repos/yuechen-li-dev/OctetDB
root=/home/yuech/tigercompare/perfm4-runs
binary=/home/yuech/tigercompare/perfm4-tigercompare
tb=/home/yuech/tigercompare/tools/tigerbeetle
evidence="$repo/docs/product/evidence/PERF_M4/raw/tiger-wsl"

mkdir -p "$root" "$evidence"
cd "$repo"
go build -trimpath -o "$binary" ./cmd/tigercompare

for rep in 1 2 3; do
    run="$root/w1-b1-r$rep"
    test "${run#"$root"/}" != "$run"
    mkdir -p "$run"
    "$tb" format --cluster=0 --replica=0 --replica-count=1 "$run/0_0.tigerbeetle" >"$run/server.log" 2>&1
    "$tb" start --addresses=127.0.0.1:3001 --memory=1GiB --limit-storage=10GiB --experimental \
        "$run/0_0.tigerbeetle" >>"$run/server.log" 2>&1 &
    pid=$!
    trap 'kill -TERM "$pid" 2>/dev/null || true' EXIT
    for _ in $(seq 1 100); do
        if bash -c '</dev/tcp/127.0.0.1/3001' 2>/dev/null; then break; fi
        sleep 0.05
    done
    "$binary" -lane tiger -workload independent -batch 1 -operations 1000 -warmup 100 \
        -accounts 1000 -seed "$((6400+rep))" -server-pid "$pid" \
        -tiger-address 127.0.0.1:3001 \
        -topology 'WSL2 loopback TCP; one production-mode TigerBeetle 0.17.9 replica' \
        -out "$evidence/w1-b1-tiger-r$rep.json"
    stat --format='logical_bytes=%s allocated_blocks=%b block_bytes=512' "$run/0_0.tigerbeetle" \
        >"$evidence/w1-b1-tiger-r$rep.storage.txt"
    kill -TERM "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    trap - EXIT
    rm -rf -- "$run"
done
