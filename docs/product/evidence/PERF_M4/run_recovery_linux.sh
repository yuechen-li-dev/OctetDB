#!/usr/bin/env bash
set -euo pipefail
repo=/mnt/c/Users/yuech/source/repos/yuechen-li-dev/OctetDB
root="$repo/docs/product/evidence/PERF_M4"
binary=/home/yuech/tigercompare/perfm4-recovery
data_root=/home/yuech/tigercompare/perfm4-recovery-data
raw="$root/raw/wsl"
mkdir -p "$data_root" "$raw"
cd "$root/harness"
go build -trimpath -o "$binary" ./recoveryprobe
for rep in 1 2 3; do
    cold="$data_root/cold-r$rep"
    "$binary" -mode cold -data "$cold" -output "$raw/recovery-cold-default-r$rep.json"
    rm -rf -- "$cold"
    for workload in w1 w3; do
        for preparation in snapshot wal; do
            data="$data_root/$workload-$preparation-r$rep"
            test "${data#"$data_root"/}" != "$data"
            "$binary" -mode "prepare-$preparation" -workload "$workload" -population 10000 -data "$data"
            "$binary" -mode measure -workload "$workload" -population 10000 -data "$data" \
                -output "$raw/recovery-$workload-$preparation-default-r$rep.json"
            rm -rf -- "$data"
        done
    done
done
