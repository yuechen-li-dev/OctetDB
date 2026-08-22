#!/usr/bin/env bash
set -euo pipefail

# Run from the native-Linux copy of Database-Scheduler, not /mnt/c. The optional
# first argument selects: smoke, primary, diagnostics, contention, population,
# profiles, hardware, storage, recovery, or longrun.
mode="${1:-smoke}"
repo="$(cd "$(dirname "$0")/../.." && pwd)"
case "$repo" in /home/*) ;; *) echo "benchmark repository must be on native Linux storage" >&2; exit 2;; esac

root=/home/yuech/tigercompare
run_root="$root/runs"
tool_root="$root/tools"
evidence="$repo/experiments/TigerCompareM0/evidence"
mkdir -p "$run_root" "$evidence" "$repo/experiments/TigerCompareM0/environment" "$repo/experiments/TigerCompareM0/profiles"

tb="$tool_root/tigerbeetle"
bench="$root/tigercompare"
postgres_dsn='postgres://postgres@127.0.0.1:54330/tigercompare?sslmode=disable'
topology_oct='in-process Go call; one Engine; harness concurrent burst'
topology_tb='Go 0.17.9 client over loopback TCP; one production-mode TigerBeetle 0.17.9 replica'
topology_pg='Go pgx over loopback TCP; native PostgreSQL 18.6; one transaction per transfer'

go build -o "$bench" ./cmd/tigercompare

environment() {
  {
    date --iso-8601=seconds
    uname -a
    cat /etc/os-release
    lscpu
    free -h
    findmnt -T "$repo" -o SOURCE,FSTYPE,OPTIONS
    lsblk -o NAME,MODEL,TYPE,SIZE,FSTYPE,MOUNTPOINTS
    go version
    git rev-parse HEAD
    "$tb" version --verbose
    psql --version
    psql "$postgres_dsn" -Atc 'select version(); show synchronous_commit; show fsync; show full_page_writes; show data_checksums;'
    getconf CLK_TCK
    cat /proc/sys/kernel/perf_event_paranoid
  } > "$repo/experiments/TigerCompareM0/environment/linux.txt"
}

safe_clean_run() {
  local dir="$1"
  case "$dir" in "$run_root"/*) ;; *) echo "refusing cleanup outside $run_root" >&2; exit 2;; esac
  # Every target is an exact child of the dedicated generated-run root.
  rm -rf -- "$dir"
}

run_local() {
  local lane="$1" workload="$2" batch="$3" operations="$4" accounts="$5" seed="$6" name="$7"
  local dir="$run_root/$name"
  mkdir -p "$dir"
  "$bench" -lane "$lane" -durability group -workload "$workload" -batch "$batch" \
    -operations "$operations" -accounts "$accounts" -seed "$seed" -storage-dir "$dir" \
    -topology "$topology_oct" -out "$evidence/$name.json" >/dev/null
  safe_clean_run "$dir"
}

run_tiger() {
  local workload="$1" batch="$2" operations="$3" accounts="$4" seed="$5" name="$6"
  local dir="$run_root/$name"
  mkdir -p "$dir"
  "$tb" format --cluster=0 --replica=0 --replica-count=1 "$dir/0_0.tigerbeetle" >"$dir/server.log" 2>&1
  "$tb" start --addresses=127.0.0.1:3000 --memory=1GiB --limit-storage=10GiB --experimental \
    "$dir/0_0.tigerbeetle" >>"$dir/server.log" 2>&1 &
  local pid=$!
  trap "kill -TERM $pid 2>/dev/null || true" EXIT
  for _ in $(seq 1 100); do
    if bash -c '</dev/tcp/127.0.0.1/3000' 2>/dev/null; then break; fi
    sleep 0.05
  done
  local before_size before_blocks after_size after_blocks
  before_size=$(stat -c %s "$dir/0_0.tigerbeetle")
  before_blocks=$(stat -c %b "$dir/0_0.tigerbeetle")
  "$bench" -lane tiger -workload "$workload" -batch "$batch" -operations "$operations" \
    -accounts "$accounts" -seed "$seed" -server-pid "$pid" -topology "$topology_tb" \
    -out "$evidence/$name.json" >/dev/null
  after_size=$(stat -c %s "$dir/0_0.tigerbeetle")
  after_blocks=$(stat -c %b "$dir/0_0.tigerbeetle")
  printf 'logical_before=%s\nblocks_before=%s\nlogical_after=%s\nblocks_after=%s\nblock_unit=512\n' \
    "$before_size" "$before_blocks" "$after_size" "$after_blocks" > "$evidence/$name.storage.txt"
  kill -TERM "$pid" 2>/dev/null || true
  for _ in {1..50}; do
    if ! kill -0 "$pid" 2>/dev/null; then break; fi
    sleep 0.1
  done
  kill -KILL "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  trap - EXIT
  safe_clean_run "$dir"
}

run_postgres() {
  local workload="$1" batch="$2" operations="$3" accounts="$4" seed="$5" name="$6"
  local pid
  pid=$(pgrep -u postgres -f 'postgres -D /var/lib/postgres/tigercompare' | head -n 1)
  "$bench" -lane postgres -postgres-dsn "$postgres_dsn" -workload "$workload" -batch "$batch" \
    -operations "$operations" -accounts "$accounts" -seed "$seed" -server-pid "$pid" \
    -topology "$topology_pg" -out "$evidence/$name.json" >/dev/null
}

run_rotated_three() {
  local rep="$1" workload="$2" batch="$3" operations="$4" accounts="$5" seed="$6" stem="$7"
  run_named() {
    local lane="$1"
    if [[ "$lane" == tiger ]]; then
      run_tiger "$workload" "$batch" "$operations" "$accounts" "$seed" "${stem}-tiger-r${rep}"
    else
      run_local "$lane" "$workload" "$batch" "$operations" "$accounts" "$seed" "${stem}-${lane}-r${rep}"
    fi
  }
  case "$rep" in
    1) run_named oct; run_named go; run_named tiger ;;
    2) run_named go; run_named tiger; run_named oct ;;
    3) run_named tiger; run_named oct; run_named go ;;
  esac
}

environment

case "$mode" in
  smoke)
    run_local oct independent 8 100 100 8128 smoke-oct
    run_local go independent 8 100 100 8129 smoke-go
    run_tiger independent 8 100 100 8130 smoke-tiger
    run_postgres independent 2 20 10 8131 smoke-postgres
    ;;
  primary)
    rm -f "$evidence"/batch-*.json "$evidence"/batch-*.storage.txt "$evidence"/primary-postgres-*.json
    for batch in 1 4 8 16 32 64 128 256 512; do
      operations=$((batch * 200)); if (( operations < 5000 )); then operations=5000; fi
      for rep in 1 2 3; do
        seed=$((8128 + rep + batch * 10))
        run_rotated_three "$rep" independent "$batch" "$operations" 1000 "$seed" "batch-b${batch}"
      done
    done
    for rep in 1 2 3; do
      run_postgres independent 64 1000 1000 "$((9100+rep))" "primary-postgres-r${rep}"
    done
    ;;
  diagnostics)
    for lane in oct go; do
      for rep in 1 2 3; do
        name="memory-${lane}-r${rep}"
        dir="$run_root/$name"; mkdir -p "$dir"
        "$bench" -lane "$lane" -durability memory -workload independent -batch 128 -operations 100000 \
          -accounts 1000 -seed "$((10000+rep))" -storage-dir "$dir" -topology "$topology_oct" \
          -out "$evidence/$name.json" >/dev/null
        safe_clean_run "$dir"
      done
      for gogc in 50 100 200; do
        for rep in 1 2 3; do
          name="gc-${lane}-gogc${gogc}-r${rep}"
          dir="$run_root/$name"; mkdir -p "$dir"
          GOGC="$gogc" "$bench" -lane "$lane" -durability group -workload independent -batch 64 \
            -operations 30000 -accounts 1000 -seed "$((11000+rep+gogc))" -storage-dir "$dir" \
            -topology "$topology_oct" -out "$evidence/$name.json" >/dev/null
          safe_clean_run "$dir"
        done
      done
    done
    ;;
  contention)
    rm -f "$evidence"/contention-*.json "$evidence"/contention-*.storage.txt
    for workload in independent hot_source hot_destination hotset; do
      for rep in 1 2 3; do
        seed=$((12000+rep))
        run_rotated_three "$rep" "$workload" 64 5000 1000 "$seed" "contention-${workload}"
      done
    done
    ;;
  population)
    rm -f "$evidence"/population-*.json "$evidence"/population-*.storage.txt
    for accounts in 1000 100000; do
      for rep in 1 2 3; do
        seed=$((14000+rep+accounts))
        run_rotated_three "$rep" independent 64 5000 "$accounts" "$seed" "population-a${accounts}"
      done
    done
    ;;
  largegc)
    run_local oct independent 128 500000 100000 14501 largegc-oct-a100000
    run_local go independent 128 500000 100000 14502 largegc-go-a100000
    ;;
  profiles)
    for lane in oct go; do
      for durability in memory group; do
        name="profile-${lane}-${durability}"
        dir="$run_root/$name"; mkdir -p "$dir"
        "$bench" -lane "$lane" -durability "$durability" -workload independent -batch 128 \
          -operations 100000 -accounts 1000 -seed 15000 -storage-dir "$dir" -topology "$topology_oct" \
          -cpu-profile "$repo/experiments/TigerCompareM0/profiles/$name.cpu.pprof" \
          -heap-profile "$repo/experiments/TigerCompareM0/profiles/$name.heap.pprof" \
          -out "$evidence/$name.json" >/dev/null
        safe_clean_run "$dir"
      done
    done
    ;;
  hardware)
    for lane in oct go; do
      for durability in memory group; do
        name="perf-${lane}-${durability}"
        dir="$run_root/$name"; mkdir -p "$dir"
        perf stat -x, -e cycles,instructions,cache-misses,branch-misses -o "$evidence/$name.csv" -- \
          "$bench" -lane "$lane" -durability "$durability" -workload independent -batch 128 \
          -operations 100000 -accounts 1000 -seed 16000 -storage-dir "$dir" -topology "$topology_oct" \
          -out "$evidence/$name.json" >/dev/null
        safe_clean_run "$dir"
      done
    done
    name=profile-tiger
    dir="$run_root/$name"; mkdir -p "$dir"
    "$tb" format --cluster=0 --replica=0 --replica-count=1 "$dir/0_0.tigerbeetle" >"$dir/server.log" 2>&1
    "$tb" start --addresses=127.0.0.1:3000 --memory=1GiB --limit-storage=10GiB --experimental "$dir/0_0.tigerbeetle" >>"$dir/server.log" 2>&1 &
    pid=$!; trap "kill -TERM $pid 2>/dev/null || true" EXIT
    sleep 2
    perf record -F 199 -g -p "$pid" -o "$repo/experiments/TigerCompareM0/profiles/$name.data" >/dev/null 2>&1 & perf_pid=$!
    "$bench" -lane tiger -workload independent -batch 512 -operations 500000 -accounts 1000 -seed 16001 \
      -server-pid "$pid" -topology "$topology_tb" -out "$evidence/$name.json" >/dev/null
    kill -INT "$perf_pid" 2>/dev/null || true; wait "$perf_pid" 2>/dev/null || true
    perf report --stdio -i "$repo/experiments/TigerCompareM0/profiles/$name.data" > "$repo/experiments/TigerCompareM0/profiles/$name.txt"
    perf stat -x, -e cycles,instructions,cache-misses,branch-misses -p "$pid" -o "$evidence/perf-tiger.csv" & perf_pid=$!
    "$bench" -lane tiger -skip-setup -workload independent -batch 512 -operations 500000 -accounts 1000 -seed 16002 \
      -server-pid "$pid" -topology "$topology_tb" -out "$evidence/perf-tiger.json" >/dev/null
    kill -INT "$perf_pid" 2>/dev/null || true; wait "$perf_pid" 2>/dev/null || true
    kill -TERM "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; trap - EXIT
    safe_clean_run "$dir"
    ;;
  debugprofile)
    name=profile-tiger-debug
    dir="$run_root/$name"; mkdir -p "$dir"
    tb_debug="$tool_root/tigerbeetle-debug"
    "$tb_debug" format --cluster=0 --replica=0 --replica-count=1 "$dir/0_0.tigerbeetle" >"$dir/server.log" 2>&1
    "$tb_debug" start --addresses=127.0.0.1:3000 --memory=1GiB --limit-storage=10GiB --experimental "$dir/0_0.tigerbeetle" >>"$dir/server.log" 2>&1 &
    pid=$!; trap "kill -TERM $pid 2>/dev/null || true" EXIT; sleep 8
    perf record -F 199 -g -p "$pid" -o "$repo/experiments/TigerCompareM0/profiles/$name.data" >/dev/null 2>&1 & perf_pid=$!
    "$bench" -lane tiger -workload independent -batch 512 -operations 1000000 -accounts 1000 -seed 16500 \
      -server-pid "$pid" -topology 'debug server used only for symbol classification' -out "$evidence/$name.json" >/dev/null
    kill -INT "$perf_pid" 2>/dev/null || true; wait "$perf_pid" 2>/dev/null || true
    perf report --stdio -i "$repo/experiments/TigerCompareM0/profiles/$name.data" > "$repo/experiments/TigerCompareM0/profiles/$name.txt"
    kill -TERM "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; trap - EXIT
    safe_clean_run "$dir"
    ;;
  storage)
    run_tiger independent 512 100000 1000 17001 storage-tiger-100k
    run_tiger independent 512 1000000 1000 17002 storage-tiger-1m
    ;;
  recovery)
    name=recovery-tiger-100k
    dir="$run_root/$name"; mkdir -p "$dir"
    "$tb" format --cluster=0 --replica=0 --replica-count=1 "$dir/0_0.tigerbeetle" >"$dir/server.log" 2>&1
    "$tb" start --addresses=127.0.0.1:3000 --memory=1GiB --limit-storage=10GiB --experimental "$dir/0_0.tigerbeetle" >>"$dir/server.log" 2>&1 &
    pid=$!; sleep 2
    "$bench" -lane tiger -workload independent -batch 512 -operations 100000 -accounts 1000 -seed 18001 \
      -server-pid "$pid" -topology "$topology_tb" -out "$evidence/recovery-tiger-before-crash.json" >/dev/null
    kill -KILL "$pid"; wait "$pid" 2>/dev/null || true
    start_ns=$(date +%s%N)
    "$tb" start --addresses=127.0.0.1:3000 --memory=1GiB --limit-storage=10GiB --experimental "$dir/0_0.tigerbeetle" >>"$dir/server.log" 2>&1 &
    pid=$!; trap "kill -TERM $pid 2>/dev/null || true" EXIT
    "$bench" -lane tiger -skip-setup -warmup 0 -workload independent -batch 1 -operations 1 -accounts 1000 -seed 18002 \
      -server-pid "$pid" -topology "$topology_tb" -out "$evidence/recovery-tiger-after-crash.json" >/dev/null
    end_ns=$(date +%s%N)
    printf 'time_to_accept_traffic_ns=%s\ndata_file_bytes=%s\nallocated_blocks=%s\n' \
      "$((end_ns-start_ns))" "$(stat -c %s "$dir/0_0.tigerbeetle")" "$(stat -c %b "$dir/0_0.tigerbeetle")" > "$evidence/recovery-tiger.txt"
    kill -TERM "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; trap - EXIT
    cp "$dir/server.log" "$evidence/recovery-tiger-server.log"
    safe_clean_run "$dir"
    ;;
  longrun)
    name=longrun-oct-300s
    dir="$run_root/$name"; mkdir -p "$dir"
    "$bench" -lane oct -durability group -workload hotset -batch 64 -operations 1 -duration 300s \
      -accounts 1000 -seed 13000 -storage-dir "$dir" -topology "$topology_oct" \
      -out "$evidence/$name.json" >/dev/null
    safe_clean_run "$dir"
    ;;
  *) echo "unknown mode: $mode" >&2; exit 2;;
esac
