# BSO-TRUST-M1

This bounded research experiment subjects the M0 federated trust model to
deterministic provider preference, capacity, outages, recurring cache reuse,
network effects, trust islands, bridging, and bundled-provider pressure.
Providers remain data-only trust authorities. The unchanged M0 suite is run as
the financial settlement/conservation baseline.

```powershell
go test ./experiments/BSOTrust/M1
go run ./cmd/bso-trust-m1
go run ./cmd/bso-trust-m1 --json
```

Run the typed Oct contract through the adjacent compiler checkout:

```powershell
go run ./cmd/oct test C:\Users\yuech\source\repos\yuechen-li-dev\OctetDB\experiments\BSOTrust\M1\trust_federation.octest --execution compiled --json
```

