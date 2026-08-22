#!/usr/bin/env python3
import glob
import json
import os
import statistics

HERE = os.path.dirname(os.path.abspath(__file__))
EVIDENCE = os.path.join(HERE, "evidence")


def series(stem, lane):
    paths = sorted(glob.glob(os.path.join(EVIDENCE, f"{stem}-{lane}-r*.json")))
    return [json.load(open(path, encoding="utf-8")) for path in paths]


def metric(items, *path):
    values = []
    for item in items:
        value = item
        for key in path:
            value = value[key]
        values.append(value)
    return {"median": statistics.median(values), "min": min(values), "max": max(values)}


def summarize_run(stem, lane):
    items = series(stem, lane)
    result = {"repetitions": len(items)}
    for name, path in {
        "operations_per_second": ("result", "operations_per_second"),
        "p99_us": ("result", "p99_us"),
        "alloc_bytes_per_op": ("result", "gc", "bytes_per_op"),
        "allocs_per_op": ("result", "gc", "allocs_per_op"),
        "gc_cpu_percent_capacity": ("result", "gc", "gc_cpu_percent_of_go_capacity"),
        "gc_cycles": ("result", "gc", "gc_cycles"),
        "max_pause_us": ("result", "gc", "max_pause_us"),
        "live_heap_bytes": ("result", "gc", "live_heap_bytes"),
        "rss_bytes": ("result", "gc", "rss_bytes"),
        "wal_bytes_per_op": ("result", "storage", "wal_bytes_per_op"),
    }.items():
        result[name] = metric(items, *path)
    process_gc = []
    for item in items:
        process = item["result"]["process_cpu_seconds"]
        gc = item["result"]["gc"]["gc_cpu_seconds"]
        process_gc.append(100 * gc / process if process else 0)
    result["gc_cpu_percent_process"] = {
        "median": statistics.median(process_gc), "min": min(process_gc), "max": max(process_gc)
    }
    if lane == "tiger":
        rss = [item["result"]["gc"]["rss_bytes"] + item["result"]["external_process"]["rss_bytes"] for item in items]
        result["rss_bytes"] = {"median": statistics.median(rss), "min": min(rss), "max": max(rss)}
    result["all_correct"] = all(
        item["result"]["correctness"]["conserved"]
        and item["result"]["correctness"]["duplicate_suppressed"]
        and item["result"]["correctness"]["rejected"] == 0
        for item in items
    )
    return result


def counters(lane, batch):
    values = {}
    path = os.path.join(EVIDENCE, f"perf-{lane}-b{batch}.csv")
    for line in open(path, encoding="utf-8"):
        if not line.strip() or line.startswith("#"):
            continue
        fields = line.split(",")
        values[fields[2].split(":")[0]] = int(fields[0])
    instructions = values["instructions"]
    return {
        "ipc": instructions / values["cycles"],
        "cache_misses_per_1k_instructions": 1000 * values["cache-misses"] / instructions,
        "branch_misses_per_1k_instructions": 1000 * values["branch-misses"] / instructions,
        "raw": values,
    }


summary = {
    "milestone": "OCTETDB-LAYOUT-M0",
    "verdict": "Success",
    "revisions": {
        "octetdb_experiment_base": "7e7db384850ef48d818b1406005340f8452a2dcd",
        "frozen_tiger_compare_octetdb": "3bfa3a7f3a71ec51a9cd3432f18c7b10c6e29f29",
        "tigerbeetle": "cc1c06a924e49b11089c521b2209d34c92caaf18",
        "tigerbeetle_version": "0.17.9",
        "copeland_inspected": "2b404befdd0aa29bbcacd3dda693f2d6fb2970a6",
        "oct_inspected": "1ee679c6d6967e9c56f98334e4e81e0420722b58",
    },
    "primary": {
        stem: {lane: summarize_run(stem, lane) for lane in ("tiger", "oct", "go", "c2")}
        for stem in ("b64", "b512", "hot-source", "population-100k")
    },
    "c2_scaling": {
        stem: summarize_run(stem, "c2")
        for stem in ("b64", "b128", "b256", "b512")
    },
    "hardware": {
        f"{lane}_b{batch}": counters(lane, batch)
        for lane in ("tiger", "go", "c2") for batch in (64, 512)
    },
    "recovery": json.load(open(os.path.join(EVIDENCE, "recovery-c2.json"), encoding="utf-8")),
}

digests_match = True
for stem in ("b64", "b512", "hot-source", "population-100k"):
    for rep in (1, 2, 3):
        digests = []
        for lane in ("oct", "go", "c2"):
            path = os.path.join(EVIDENCE, f"{stem}-{lane}-r{rep}.json")
            digests.append(json.load(open(path, encoding="utf-8"))["result"]["correctness"]["state_digest"])
        digests_match = digests_match and len(set(digests)) == 1
summary["cross_lane_final_digests_match"] = digests_match

encoded = json.dumps(summary, indent=2) + "\n"
with open(os.path.join(HERE, "summary.json"), "w", encoding="utf-8", newline="\n") as output:
    output.write(encoded)
print(encoded, end="")
