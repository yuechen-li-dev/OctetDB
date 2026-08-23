# Optional Oct query example

This advanced example demonstrates Oct's `query`, `filter`, `map`, `take`, and
`Query.First`/`Any`/`Count` syntax. The syntax lowers to Oct's existing FLOW
state-machine runtime. It does not connect to OctetDB storage and is not needed
to use Go `Dataset.Scan` or `ScanDataset`.

The candidate was verified with Oct revision
`ca22ab8dfc20ac6d6c59dd34976789cd2c84ad2e` (`v1.0.0-61-gca22ab8d`). Generated
FLOW host-facade ABI is revision-scoped; OctetDB does not promise compatibility
for generated artifacts across other Oct revisions.

From that Oct checkout:

```sh
go run ./cmd/oct test ../OctetDB/examples/oct-query/query.octest --execution auto --json
```
