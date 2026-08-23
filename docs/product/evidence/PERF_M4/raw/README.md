# Raw evidence policy

- `wsl/`: primary rotated same-OS/ext4 runs (207 repetition JSON files), WSL
  recovery probes, and PostgreSQL resource sidecars.
- `windows/`: Windows/NTFS portability cross-check, profiles, temporal
  controls, recovery probes, and corrected W4 runs.
- `tiger-wsl/`: current W1-only TigerBeetle batch-1 runs and allocated-storage
  sidecars.
- `invalidated_concurrent_profile_20260822/`: 34 files produced while an
  accidental profile run overlapped the dimension suite. Excluded.
- `invalidated_cpu_field_bug_20260822/`: two PostgreSQL resource-sampler
  attempts with invalid CPU accounting. Excluded.
- `invalidated_webhook_result_semantics_20260822/`: W4 runs made before the
  adapter retrieved and validated the original durable duplicate result.
  Excluded.
- `superseded_resources_before_webhook_fix_20260822/`: otherwise useful
  resource runs superseded after the W4 semantic correction. Excluded.

No inconvenient statistical outlier was deleted. Invalidations have a
specific methodological or semantic reason and remain auditable.

