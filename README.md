# OctetDB

OctetDB is an embeddable specialized OLTP engine whose transactional behavior
is defined semantically and materialized as a safe-Go execution and storage
system. The v0.1 model is intentionally narrow: bounded accounts, deposits,
withdrawals, transfers, account freezing, and a two-step transfer workflow.

It is not a PostgreSQL replacement, general SQL database, network daemon,
TigerBeetle clone, cache, or ORM.

## Status

The first public release is `v0.1.0`. It is intentionally pre-1.0: the API and
database format may change between minor releases, incompatible data is
detected and rejected, and automatic migration is not promised.

## Install

```sh
go get github.com/yuechen-li-dev/octetdb@v0.1.0
```

OctetDB requires Go 1.23 or newer. The release is tested on Windows and Linux
under WSL2; macOS is not yet verified. Oct and PostgreSQL are not required to
build or use the public package.

## 30-second example

```go
ctx := context.Background()
db, err := octetdb.Open(ctx, octetdb.Options{Path: "./account-data"})
if err != nil {
    log.Fatal(err)
}
defer db.Close()

result, err := db.Submit(ctx, octetdb.Command{
    ID: "create-1", Kind: octetdb.Create, AccountID: 1, Amount: 100,
})
if err != nil {
    log.Fatal(err)
}
account, ok := db.Get(1)
fmt.Println(result.Accepted, account.Balance, ok)
```

Runnable examples cover the [minimal lifecycle](examples/minimal/main.go) and
[batch idempotency across restart](examples/restart/main.go).

## Core model

One `DB` owns one directory and authoritative keyed account state. Commands
carry stable IDs. `SubmitBatch` evaluates commands serially in offered order,
including overlapping hot keys, and writes all new decisions in one WAL frame.
Accepted and rejected decisions receive a sequence. Duplicate IDs within the
configured exact bounded horizon return the original decision without applying
it again.

`Options` exposes only product bounds: path, maximum accounts, dedupe horizon,
and maximum offered batch size. Defaults are 100,000 accounts, 100,000 retained
command results, and 512 commands per batch.

## Durability

A successful submission means its complete WAL frame has been written and
synchronized. Recovery validates an optional snapshot, validates and replays
the complete WAL tail, and truncates an incomplete final append. Corruption and
incompatible formats fail closed. See [the precise durability contract](docs/DURABILITY.md)
and [recovery/storage layout](docs/RECOVERY.md).

## Format compatibility

`FormatVersion` identifies the database format independently of the Go module
version. Before 1.0, the format may change between minor releases. OctetDB does
not yet promise automatic migration, but it does promise that incompatible data
is detected and rejected rather than silently interpreted as the current
format.

## Limitations

- Account/transfer domain only; there is no generic schema or user-defined model.
- Single process and single replica; v0.1 has no directory lock, replication, or failover.
- No SQL, network server, secondary indexes, migrations, or online backup API.
- Idempotency is exact only inside `DedupeHorizon`; expired IDs may apply again.
- Cancellation is honored before admission, not after durable processing begins.
- Snapshot rename power-loss guarantees are weaker on Windows than POSIX.

## Performance evidence

In a bounded single-replica local OLTP experiment, the specialized safe-Go
engine that became the v0.1 production engine exceeded the measured
single-replica TigerBeetle control in the tested configurations. The systems
were not guarantee-equivalent, and this is neither a general database benchmark
nor a claim that OctetDB is generally faster than TigerBeetle. See the
[TigerCompareM0 report](docs/experiments/TIGER_COMPARE_M0.md) for methodology,
environment, and limitations.

## License

OctetDB is licensed under the [GNU General Public License v3.0](LICENSE).

## Development and experiments

```sh
go test ./...
go vet ./...
```

The root package and `internal/core` are production OctetDB. `cmd/`, the other
`internal/` packages, `postgres/`, and `experiments/` retain benchmark controls,
fixtures, generated artifacts, and historical research. Start with the
[experiment archive index](docs/experiments/README.md), not those packages, when
looking for historical evidence.
