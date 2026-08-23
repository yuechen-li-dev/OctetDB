#!/usr/bin/env python3
"""Normalize PERF-M4 repetitions without deleting outliers."""

from __future__ import annotations

import json
import re
import statistics
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PLATFORM = sys.argv[1] if len(sys.argv) > 1 else "wsl"
if PLATFORM not in {"wsl", "windows"}:
    raise SystemExit("usage: summarize.py [wsl|windows]")
RAW = ROOT / "raw" / PLATFORM
OUT = ROOT / "summaries"
RUN_RE = re.compile(r"^(?P<case>.+)-(?P<lane>postgres|default|specialized)-r(?P<rep>\d+)\.json$")
FIELDS = (
    "throughput_ops_s", "p50_ns", "p95_ns", "p99_ns", "max_ns",
    "cpu_user_ns", "cpu_kernel_ns", "rss_bytes", "heap_alloc_bytes",
    "total_alloc_delta_bytes", "malloc_delta", "gc_cycles_delta",
    "gc_pause_delta_ns", "storage_after_bytes", "storage_delta_bytes_per_op",
    "wal_bytes_per_op", "records_examined",
)


def median(values: list[float]) -> float:
    return float(statistics.median(values))


groups: dict[tuple[str, str], list[dict]] = {}
for path in sorted(RAW.glob("*.json")):
    match = RUN_RE.match(path.name)
    if not match:
        continue
    data = json.loads(path.read_text(encoding="utf-8"))
    if "config" not in data:
        continue
    groups.setdefault((match["case"], match["lane"]), []).append(data)

summary_groups = []
lookup: dict[tuple[str, str], dict] = {}
all_correct = True
for (case, lane), runs in sorted(groups.items()):
    row = {
        "case": case,
        "lane": lane,
        "repetitions": len(runs),
        "config": runs[0]["config"],
        "spread": {},
        "correctness": all(all(run["correctness"].values()) for run in runs),
    }
    all_correct = all_correct and row["correctness"]
    for field in FIELDS:
        values = [float(run.get(field, 0)) for run in runs]
        row[field] = median(values)
        row["spread"][field] = {"min": min(values), "max": max(values)}
    summary_groups.append(row)
    lookup[(case, lane)] = row

derived = []
for case in sorted({case for case, _ in groups}):
    pg = lookup.get((case, "postgres"))
    default = lookup.get((case, "default"))
    specialized = lookup.get((case, "specialized"))
    if not default or not specialized:
        continue
    row = {
        "case": case,
        "specialization_multiplier": specialized["throughput_ops_s"] / default["throughput_ops_s"],
        "specialized_default_p99_ratio": specialized["p99_ns"] / default["p99_ns"] if default["p99_ns"] else None,
    }
    if pg:
        row.update({
            "default_postgres_throughput_ratio": default["throughput_ops_s"] / pg["throughput_ops_s"],
            "default_postgres_p99_ratio": default["p99_ns"] / pg["p99_ns"] if pg["p99_ns"] else None,
            "specialized_postgres_throughput_ratio": specialized["throughput_ops_s"] / pg["throughput_ops_s"],
        })
    derived.append(row)

tiger_runs = []
for path in sorted((ROOT / "raw" / "tiger-wsl").glob("*.json")):
    tiger_runs.append(json.loads(path.read_text(encoding="utf-8")))
tiger = None
if tiger_runs:
    tiger = {
        "repetitions": len(tiger_runs),
        "throughput_ops_s": median([run["result"]["operations_per_second"] for run in tiger_runs]),
        "p50_ns": median([run["result"]["p50_us"] * 1000 for run in tiger_runs]),
        "p95_ns": median([run["result"]["p95_us"] * 1000 for run in tiger_runs]),
        "p99_ns": median([run["result"]["p99_us"] * 1000 for run in tiger_runs]),
        "max_ns": median([run["result"]["max_us"] * 1000 for run in tiger_runs]),
        "rss_bytes": median([run["result"]["external_process"]["rss_bytes"] for run in tiger_runs]),
        "correctness": all(run["result"]["correctness"]["conserved"] and run["result"]["correctness"]["duplicate_suppressed"] for run in tiger_runs),
        "topology": tiger_runs[0]["config"]["topology"],
    }

OUT.mkdir(parents=True, exist_ok=True)
payload = json.dumps({
    "schema_version": 1,
    "platform": PLATFORM,
    "included_repetition_files": sum(len(runs) for runs in groups.values()),
    "all_correct": all_correct,
    "groups": summary_groups,
    "derived": derived,
    "tigerbeetle_w1": tiger,
}, indent=2) + "\n"
(OUT / f"summary-{PLATFORM}.json").write_text(payload, encoding="utf-8")
if PLATFORM == "wsl":
    (OUT / "summary.json").write_text(payload, encoding="utf-8")
print(f"groups={len(summary_groups)} runs={sum(len(runs) for runs in groups.values())} all_correct={all_correct}")
