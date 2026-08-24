# BSO-TRUST-M3

This experiment separates pure, bounded policy decisions from durable
financial mutation owned by a BSO authority. It contains no contract VM,
callbacks, generic policy storage, gas, blockchain, or global rollback.

```powershell
go test ./experiments/BSOTrust/M3
go run ./cmd/bso-trust-m3
go run ./cmd/bso-trust-m3 --json
go run ./cmd/oct test C:\Users\yuech\source\repos\yuechen-li-dev\OctetDB\experiments\BSOTrust\M3 --execution compiled --json
```
