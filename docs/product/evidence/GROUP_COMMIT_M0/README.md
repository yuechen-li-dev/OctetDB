# GROUP-COMMIT-M0 evidence

This directory contains bounded evidence for the invisible durable group-commit milestone. `dev-harness/` holds the fast architectural loop, `raw/` and `summaries/` hold bounded benchmark output, `faults/` maps deterministic failure tests, `profiles/` is reserved for focused profiles, and `environment/` records host details.

Run the smoke harness:

```text
go test -run TestGroupCommitDevHarness -count=1 -args -group-dev-smoke -group-dev-output docs/product/evidence/GROUP_COMMIT_M0/dev-harness/current-smoke.json
```

Run the normal harness by omitting `-group-dev-smoke`. Both compare internal one-command-per-sync baseline mode with the public-default group-commit mode. The control is test-only and adds no public durability option.
