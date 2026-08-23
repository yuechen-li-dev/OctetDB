# OCT-QUERY-M0 fresh-agent legibility check

Date: 2026-08-22

## Inputs supplied

The fresh coding agent was restricted to `README.md`,
`docs/GETTING_STARTED.md`, and `doc.go`. It was told only to open an OctetDB
catalog, open `workers/jobs`, and find the first ten jobs whose Status is Ready.
It was explicitly denied implementation, tests, reports, history, and
maintainer explanation.

## API shape returned

```go
db, err := octetdb.OpenCatalog(ctx, "./data/myapp", octetdb.DefaultKeyedOptions())
if err != nil { return err }
defer db.Close()

workers, err := db.Bucket(ctx, "workers")
if err != nil { return err }
jobs, err := workers.Dataset(ctx, "jobs", octetdb.DatasetOptions{TypeIdentity: "example.Job/v1"})
if err != nil { return err }

ready := make([]Job, 0, 10)
err = octetdb.ScanDataset(ctx, jobs, func(_ string, job Job) (octetdb.ScanAction, error) {
    if job.Status != Ready {
        return octetdb.ScanContinue, nil
    }
    ready = append(ready, job)
    if len(ready) == 10 {
        return octetdb.ScanStop, nil
    }
    return octetdb.ScanContinue, nil
})
```

The agent also stated that the result is in ascending record-key order.

## Findings

- API hallucinations: none. Every OctetDB symbol used is in the supplied docs.
- SQL invention: none. The agent explicitly selected a Go predicate over a
  deterministic Dataset scan.
- Filesystem/catalog confusion: none. It identified `./data/myapp` as the
  owned database directory and `workers`/`jobs` as logical catalog names, not
  filesystem components.
- Human correction: only application-owned inputs—the real database path,
  actual TypeIdentity/options, concrete `Job` and `Ready` symbols, and optional
  shutdown-error handling.

No maintainer correction of the OctetDB API or topology was required.
