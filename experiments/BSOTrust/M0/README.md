# BSO-TRUST-M0

This bounded experiment inserts federated, role-scoped trust admission before the unchanged BSO-SIM-M2 reserve/settlement protocol. BSOs persist their own trust policies and remain the only objects with OctetDB balance mutation methods. Deterministic providers expose only a metadata request/attestation API.

Run the experiment:

```powershell
go run ./cmd/bso-trust-m0 --mode smoke
go run ./cmd/bso-trust-m0 --mode normal --migrate
go test ./experiments/BSOTrust/M0
```

Run the Oct language contract from the adjacent `oct` repository:

```powershell
go run ./cmd/oct test C:\Users\yuech\source\repos\yuechen-li-dev\OctetDB\experiments\BSOTrust\M0\trust_policy.octest --execution compiled --json
```

The simulation models identity, risk, and authorization providers. Escrow and dispute DTOs are declared for a bounded semantic extension, but custody and adjudication workflows are intentionally absent.
