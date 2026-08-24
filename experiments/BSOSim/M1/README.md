# BSO-SIM-M1

M1 represents every cross-BSO transfer as a compact `TransactionAgent` record and schedules those records over bounded worker queues. BSOs remain the only durable financial authorities. The coordinator places and migrates agents but is absent from protocol-message routing after placement.

Machine protocol envelopes and migration checkpoints use deterministic, typed, versioned data-only Octagon records. JSON is retained only for optional human-readable CLI results.

```powershell
go test ./experiments/BSOSim/M1 -count=1
go run ./cmd/bso-sim-m1 --mode smoke
go run ./cmd/bso-sim-m1 --mode normal
```

The companion Oct contract is under `Experiments/BSOSim/M1` in the `oct` repository.
