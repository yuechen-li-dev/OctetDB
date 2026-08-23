#!/usr/bin/env bash
set -euo pipefail
repo=/mnt/c/Users/yuech/source/repos/yuechen-li-dev/OctetDB
root="$repo/docs/product/evidence/PERF_M4"
binary=/home/yuech/tigercompare/perfm4-harness
data_root=/home/yuech/tigercompare/perfm4-resource-data
raw="$root/raw/wsl"
postgres='postgres://postgres@127.0.0.1:54330/tigercompare?sslmode=disable'
mkdir -p "$data_root" "$raw"
cd "$root/harness"
go build -trimpath -o "$binary" .

usage() {
    local ticks=0 rss=0 p value
    for p in /proc/[0-9]*; do
        if [[ -r "$p/comm" ]] && [[ "$(cat "$p/comm" 2>/dev/null || true)" == postgres ]] && [[ -r "$p/stat" ]]; then
            set -- $(cat "$p/stat" 2>/dev/null || continue)
			(( $# >= 15 )) || continue
            ticks=$((ticks+${14}+${15}))
            value=$(awk '/VmRSS/ {print $2}' "$p/status" 2>/dev/null || true)
            rss=$((rss+${value:-0}))
        fi
    done
    echo "$ticks $rss"
}

for workload in w1 w2 w3 w4 w5 w6; do
    population=1000; operations=10000
    [[ "$workload" == w5 ]] && population=10000
    [[ "$workload" == w6 ]] && operations=50000
    data="$data_root/$workload"
    test "${data#"$data_root"/}" != "$data"
    read -r before_ticks before_rss < <(usage)
    "$binary" -lane postgres -workload "$workload" -population "$population" \
        -operations "$operations" -concurrency 8 -warmup 100 -postgres "$postgres" \
        -data "$data" -output "$raw/resource-postgres-$workload.json" &
    pid=$!
    peak_ticks=$before_ticks; peak_rss=$before_rss
    while kill -0 "$pid" 2>/dev/null; do
        read -r ticks rss < <(usage)
        (( ticks > peak_ticks )) && peak_ticks=$ticks
        (( rss > peak_rss )) && peak_rss=$rss
        sleep 0.05
    done
    wait "$pid"
    printf '{\n  "workload": "%s",\n  "topology": "native WSL PostgreSQL; sum of postgres process RSS",\n  "clock_ticks_per_second": 100,\n  "server_cpu_seconds": %.2f,\n  "server_peak_rss_bytes": %d\n}\n' \
        "$workload" "$((peak_ticks-before_ticks))e-2" "$((peak_rss*1024))" \
        >"$raw/resource-postgres-$workload-server.json"
    rm -rf -- "$data"
done
