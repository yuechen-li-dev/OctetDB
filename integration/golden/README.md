# PRODUCT-M2B / QUERY-M0 golden integrations

This is a separate Go module so its four applications can consume only exported
OctetDB APIs. It uses `chi`, `net/http`, `encoding/json`, `slog`, `context`, plain
constructors, and application-specific service/store interfaces.

The module requires the candidate version `v0.2.0` and temporarily replaces it
with the repository root because no release is made by this milestone. The clean
v0.1.0 baseline had no replacement and is recorded in
`docs/product/evidence/PRODUCT_M2_V01_BASELINE.md`.

The persistence adapters declare these logical leaves:

| Application | Bucket | Dataset |
| --- | --- | --- |
| inventory | `inventory` | `items` |
| webhook | `events` | `webhooks` |
| order | `commerce` | `orders` |
| job | `workers` | `jobs` |

They use no key prefixes, internal imports, filesystem topology, SQL, or Oct.
QUERY-M0 adds two conventional exported-API reads:

- job: `GET /jobs/ready?limit=10` scans `workers/jobs`, selects `Ready`, and
  stops at the limit; `POST /jobs/{id}/requeue` proves failed/requeued behavior;
- inventory: `GET /items/low-stock?threshold=5&limit=10` scans
  `inventory/items`, selects low-stock items, and stops at the limit.

Both preserve deterministic record-key order and results across restart. The
store adapters use `ScanDataset`; service and HTTP layers remain ordinary Go.

Run:

```sh
go test -race ./...
go vet ./...
```

Each application has `cmd/server`, `internal/httpapi`, `internal/service`, and
`internal/store`. Store tests prove restart and idempotency behavior.
