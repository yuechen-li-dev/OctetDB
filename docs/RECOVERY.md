# Recovery and storage ownership

A user selects one directory:

```go
db, err := octetdb.OpenCatalog(ctx, "./data/shop", octetdb.DefaultKeyedOptions())
```

OctetDB owns every file beneath that directory. Applications name only the
directory, Bucket, Dataset, record key, and command ID. They do not choose
catalog, WAL, or snapshot filenames.

The durable semantic topology is Database → Bucket → Dataset → Records. It is
not mirrored as directories. Physical record encoding and files are backend-
owned and may change in a future incompatible pre-1.0 format.

## Canonical v0.2 recovery

`OpenCatalog`:

1. validates the format marker;
2. loads and verifies an optional snapshot;
3. validates and replays the complete WAL tail, truncating only an incomplete
   final append;
4. validates the catalog checksum, database identity, unique dataset IDs,
   topology, kinds, type identities, and bounds;
5. verifies that every recovered backend record belongs to a catalog Dataset;
6. rebuilds the non-durable ascending primary-key scan cursor from records.

Malformed checksummed data, impossible sequences, missing dataset identities,
or corrupt JSON fail with `ErrorCorruption`. Unsupported model/format versions
or a mismatched opener fail with `ErrorIncompatible`. Filesystem failures return
`ErrorStorage`; configured bounds that are smaller than recovered durable state
return `ErrorCapacity`.

An incomplete temporary catalog or snapshot file is not authoritative.
Automatic migration and best-effort salvage are intentionally absent.

## Format inventory and policy

| Model | Status in v0.2 | Opener | Migration |
| --- | --- | --- | --- |
| `accounts-v1` from v0.1 | supported public compatibility | `Open` | none required |
| `keyed-json-v1` pre-v0.2 candidate | deprecated compatibility only | `OpenKeyed` | none; export/recreate manually |
| `catalog-keyed-json-v1` | canonical new v0.2 format | `OpenCatalog` | n/a |

`OpenKeyed` retains its old distinct format; it is not implemented through the
catalog format. `OpenCatalog` never invents a default Dataset around global
keys. Each opener rejects the other formats, so old directories are never
silently reinterpreted.

## Operational recovery

Copy the entire directory only while the database is closed, or after the
application has coordinated quiescence and a snapshot. Do not copy or replace
individual files as an application workflow. There is no online backup,
directory lock, repair tool, or schema migration in v0.2.

The corruption contract covers catalog damage, snapshot damage, complete WAL
damage, incomplete final WAL append, format mismatch, and Dataset
kind/type/bound mismatch. Detected failures use public `ErrorKind` categories
rather than diagnostic string matching.
