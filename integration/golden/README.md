# PRODUCT-M2 golden integrations

This is a separate Go module so its four applications can consume only exported
OctetDB APIs. It uses `chi`, `net/http`, `encoding/json`, `slog`, `context`, plain
constructors, and application-specific service/store interfaces.

The module requires the candidate version `v0.2.0` and temporarily replaces it
with the repository root because no release is made by this milestone. The clean
v0.1.0 baseline had no replacement and is recorded in
`docs/product/evidence/PRODUCT_M2_V01_BASELINE.md`.

Run:

```sh
go test -race ./...
go vet ./...
```

Each application has `cmd/server`, `internal/httpapi`, `internal/service`, and
`internal/store`. Store tests prove restart and idempotency behavior.
