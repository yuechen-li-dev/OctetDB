# DBSCHED-M0 retained evidence

Two normalized, lane-order-rotated runs are retained under `evidence/run-1`
and `evidence/run-2`. Each contains:

- `config.json`
- `correctness.json`
- `baseline-results.json`
- `admission-results.json`
- `scheduled-results.json`

The verdict is **Success**: the full scheduled lane completed all 135,000
attempts at baseline throughput in both runs and held overload p99 at
2.58–2.59 ms, compared with baseline 2.76 and 3.84 ms. Admission-only was
inconsistent, so the result is attributed to the simple batching/worker-shaping
combination rather than rejection alone.

See `docs/experiments/DBSCHED_M0.md` for methodology, ablation, limitations,
and the single next recommendation. Scratch and pilot runs remain ignored under
`runs/`.
