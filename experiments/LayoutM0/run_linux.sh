#!/usr/bin/env bash
set -euo pipefail

# Run from a native-Linux copy of Database-Scheduler. Modes: primary, c2-only,
# profiles, hardware, hardware-c2, recovery, lookup. This is intentionally the pressure subset from
# TIGER-COMPARE-M0, not another broad sweep.
mode="${1:-primary}"
repo="$(cd "$(dirname "$0")/../.." && pwd)"
case "$repo" in /home/*) ;; *) echo "benchmark repository must be on native Linux storage" >&2; exit 2;; esac

root=/home/yuech/tigercompare
run_root="$root/runs-layout-m0"
tool_root="$root/tools"
evidence="$repo/experiments/LayoutM0/evidence"
profiles="$repo/experiments/LayoutM0/profiles"
bench="$root/tigercompare-layout"
tb="$tool_root/tigerbeetle"
mkdir -p "$run_root" "$evidence" "$profiles"
go build -o "$bench" ./cmd/tigercompare

safe_clean() {
  local target="$1"
  case "$target" in "$run_root"/*) rm -rf -- "$target" ;; *) echo "refusing cleanup outside $run_root" >&2; exit 2 ;; esac
}

run_local() {
  local lane="$1" workload="$2" batch="$3" operations="$4" accounts="$5" seed="$6" name="$7"
  local dir="$run_root/$name"
  mkdir -p "$dir"
  "$bench" -lane "$lane" -durability group -workload "$workload" -batch "$batch" \
    -operations "$operations" -accounts "$accounts" -seed "$seed" -storage-dir "$dir" \
    -topology "in-process Go; lane=$lane" -out "$evidence/$name.json" >/dev/null
  safe_clean "$dir"
}

run_tiger() {
  local workload="$1" batch="$2" operations="$3" accounts="$4" seed="$5" name="$6"
  local dir="$run_root/$name"
  mkdir -p "$dir"
  "$tb" format --cluster=0 --replica=0 --replica-count=1 "$dir/0_0.tigerbeetle" >"$dir/server.log" 2>&1
  "$tb" start --addresses=127.0.0.1:3000 --memory=1GiB --limit-storage=10GiB --experimental "$dir/0_0.tigerbeetle" >>"$dir/server.log" 2>&1 &
  local pid=$!
  trap 'kill -TERM '"$pid"' 2>/dev/null || true' EXIT
  for _ in $(seq 1 100); do bash -c '</dev/tcp/127.0.0.1/3000' 2>/dev/null && break; sleep 0.05; done
  "$bench" -lane tiger -workload "$workload" -batch "$batch" -operations "$operations" -accounts "$accounts" \
    -seed "$seed" -server-pid "$pid" -topology "loopback client; one production TigerBeetle 0.17.9 replica" \
    -out "$evidence/$name.json" >/dev/null
  kill -TERM "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  trap - EXIT
  safe_clean "$dir"
}

run_lane() {
  local lane="$1" workload="$2" batch="$3" operations="$4" accounts="$5" seed="$6" stem="$7" rep="$8"
  if [[ "$lane" == tiger ]]; then
    run_tiger "$workload" "$batch" "$operations" "$accounts" "$seed" "${stem}-${lane}-r${rep}"
  else
    run_local "$lane" "$workload" "$batch" "$operations" "$accounts" "$seed" "${stem}-${lane}-r${rep}"
  fi
}

run_rotated() {
  local rep="$1" workload="$2" batch="$3" operations="$4" accounts="$5" seed="$6" stem="$7"
  local order
  case "$rep" in
    1) order="oct go c2 tiger" ;;
    2) order="go c2 tiger oct" ;;
    3) order="c2 tiger oct go" ;;
  esac
  for lane in $order; do run_lane "$lane" "$workload" "$batch" "$operations" "$accounts" "$seed" "$stem" "$rep"; done
}

case "$mode" in
  primary)
    for rep in 1 2 3; do
      run_rotated "$rep" independent 64 12800 1000 "$((21000+rep))" b64
      run_rotated "$rep" independent 512 102400 1000 "$((22000+rep))" b512
      run_rotated "$rep" hot_source 64 5000 1000 "$((23000+rep))" hot-source
      run_rotated "$rep" independent 64 5000 100000 "$((24000+rep))" population-100k
    done
    ;;
  c2-only)
    for rep in 1 2 3; do
      run_local c2 independent 64 12800 1000 "$((21000+rep))" "b64-c2-r${rep}"
      run_local c2 independent 128 25600 1000 "$((21500+rep))" "b128-c2-r${rep}"
      run_local c2 independent 256 51200 1000 "$((21750+rep))" "b256-c2-r${rep}"
      run_local c2 independent 512 102400 1000 "$((22000+rep))" "b512-c2-r${rep}"
      run_local c2 hot_source 64 5000 1000 "$((23000+rep))" "hot-source-c2-r${rep}"
      run_local c2 independent 64 5000 100000 "$((24000+rep))" "population-100k-c2-r${rep}"
    done
    ;;
  profiles)
    for batch in 64 512; do
      name="profile-c2-b${batch}"
      dir="$run_root/$name"; mkdir -p "$dir"
      "$bench" -lane c2 -durability group -workload independent -batch "$batch" -operations 200000 \
        -accounts 1000 -seed "$((25000+batch))" -storage-dir "$dir" -topology "in-process C2" \
        -cpu-profile "$profiles/$name.cpu.pprof" -heap-profile "$profiles/$name.heap.pprof" \
        -out "$evidence/$name.json" >/dev/null
      go tool pprof -top "$profiles/$name.cpu.pprof" >"$profiles/$name.cpu.txt"
      go tool pprof -top -alloc_space "$profiles/$name.heap.pprof" >"$profiles/$name.alloc.txt"
      go tool pprof -top -inuse_space "$profiles/$name.heap.pprof" >"$profiles/$name.inuse.txt"
      safe_clean "$dir"
    done
    ;;
  hardware)
    for batch in 64 512; do
      operations=500000
      for lane in go c2; do
        name="perf-${lane}-b${batch}"
        dir="$run_root/$name"; mkdir -p "$dir"
        perf stat -x, -e cycles,instructions,cache-misses,branch-misses -o "$evidence/$name.csv" -- \
          "$bench" -lane "$lane" -durability group -workload independent -batch "$batch" -operations "$operations" \
          -accounts 1000 -seed "$((26000+batch))" -storage-dir "$dir" -topology "in-process $lane" \
          -out "$evidence/$name.json" >/dev/null
        safe_clean "$dir"
      done
      name="perf-tiger-b${batch}"; dir="$run_root/$name"; mkdir -p "$dir"
      "$tb" format --cluster=0 --replica=0 --replica-count=1 "$dir/0_0.tigerbeetle" >"$dir/server.log" 2>&1
      "$tb" start --addresses=127.0.0.1:3000 --memory=1GiB --limit-storage=10GiB --experimental "$dir/0_0.tigerbeetle" >>"$dir/server.log" 2>&1 &
      pid=$!; trap 'kill -TERM '"$pid"' 2>/dev/null || true' EXIT
      for _ in $(seq 1 100); do bash -c '</dev/tcp/127.0.0.1/3000' 2>/dev/null && break; sleep 0.05; done
      perf stat -x, -e cycles,instructions,cache-misses,branch-misses -p "$pid" -o "$evidence/$name.csv" & perf_pid=$!
      "$bench" -lane tiger -workload independent -batch "$batch" -operations "$operations" -accounts 1000 \
        -seed "$((27000+batch))" -server-pid "$pid" -topology "loopback; Tiger server counters only" \
        -out "$evidence/$name.json" >/dev/null
      kill -INT "$perf_pid" 2>/dev/null || true; wait "$perf_pid" 2>/dev/null || true
      kill -TERM "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; trap - EXIT
      safe_clean "$dir"
    done
    ;;
  hardware-c2)
    for batch in 64 512; do
      name="perf-c2-b${batch}"; dir="$run_root/$name"; mkdir -p "$dir"
      perf stat -x, -e cycles,instructions,cache-misses,branch-misses -o "$evidence/$name.csv" -- \
        "$bench" -lane c2 -durability group -workload independent -batch "$batch" -operations 500000 \
        -accounts 1000 -seed "$((26000+batch))" -storage-dir "$dir" -topology "in-process c2" \
        -out "$evidence/$name.json" >/dev/null
      safe_clean "$dir"
    done
    ;;
  lookup)
    go test ./internal/m7write -run '^$' -bench 'Benchmark(AccountLookup|WALRecord)' -benchmem -count 5 | tee "$evidence/components.txt"
    ;;
  recovery)
    name=recovery-c2
    dir="$run_root/$name"; mkdir -p "$dir"
    go run ./cmd/layoutprobe -dir "$dir" -accounts 1000 -operations 100000 -tail 10000 \
      -out "$evidence/$name.json" >/dev/null
    safe_clean "$dir"
    ;;
  *) echo "unknown mode: $mode" >&2; exit 2 ;;
esac
