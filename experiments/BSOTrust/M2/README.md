# BSO-TRUST-M2

This experiment adds relationship-local, role-scoped continuity proofs around
the unchanged TRUST-M1 admission and TRUST-M0 financial paths. Checks use
synthetic continuity references, never reserve funds, and are amortized outside
ordinary payment handling.

```powershell
go test ./experiments/BSOTrust/M2
go run ./cmd/bso-trust-m2
go run ./cmd/bso-trust-m2 --json
```

Compile the typed Oct contract through the adjacent compiler checkout:

```powershell
go run ./cmd/oct test C:\Users\yuech\source\repos\yuechen-li-dev\OctetDB\experiments\BSOTrust\M2\trust_continuity.octest --execution compiled --json
```
