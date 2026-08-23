# Profile evidence

`windows/` contains CPU and heap profiles for default W1, default W5 full
filter, and specialized W5 full filter. `wsl/` contains raw `perf stat` CSV
for cycles, instructions, cache references/misses, and branches/misses. WSL
hardware throughput is diagnostic only; primary throughput comes from the
rotated matrix.

Headline observations:

- W1 default: 91.6% of sampled CPU in the filesystem synchronization call.
- W5 default: JSON validation/decoding plus allocation dominate.
- W5 specialized: 79.0% in one generated FLOW `__octStep`; query-path
  allocation is approximately 9–10 bytes/query in WSL harness accounting.
- The hardware runs used different operation counts to obtain stable samples;
  compare normalized rates/direction, not raw totals.

