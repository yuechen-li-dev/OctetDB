# BSO-SIM-M2

M2 runs each already-partitioned scheduler worker in exactly one long-lived goroutine. Each worker owns its explicit queue and agent set, including delivered protocol work. BSOs remain the only durable financial authorities; the coordinator only places and migrates agents.

Machine protocol envelopes and migration checkpoints use deterministic, typed, versioned data-only Octagon records. JSON is retained only for optional human-readable CLI results.

```powershell
go test ./experiments/BSOSim/M2 -count=1
go run ./cmd/bso-sim-m2 --mode smoke
go run ./cmd/bso-sim-m2 --mode normal
```

The unchanged companion Oct contract is under `Experiments/BSOSim/M1` in the `oct` repository.
