# BSO-SIM-M0

BSO-SIM-M0 is a small deterministic simulation of independently durable Bank-Shaped Objects coordinating integer-valued transfers. It is an architectural and educational experiment, not a bank, payment network, cryptocurrency, custody product, compliance system, or production financial protocol.

Each `bso:NNNNNN` owns a separate OctetDB catalog directory. Its single durable state record contains its spendable balance, reservations, incoming/outgoing transfer views, exact seen-message IDs, audit entries, and protocol version. Cross-BSO work happens only by authenticated envelopes in a logical-time transport; no mutation callback spans two databases.

The protocol is:

1. sender durably reserves value and sends `Offer`;
2. receiver durably accepts and sends `Accept`;
3. sender finalizes the debit and sends `Commit`;
4. receiver applies the credit once and sends `Acknowledge`;
5. sender records acknowledgement;
6. either side may replay its last message during reconciliation.

Retries retain stable transfer and message IDs. OctetDB's durable keyed-command result returns the original response for an exact replay without rerunning the callback. The deterministic transport can drop, duplicate, delay, and reorder envelopes. Configured crash points close and reopen the participant from its OctetDB directory before protocol delivery continues.

## Run

From the OctetDB repository root:

```text
go run ./cmd/bso-sim --mode smoke
go run ./cmd/bso-sim --mode normal
go run ./cmd/bso-sim --mode fun-large
```

Useful bounded overrides are `--seed`, `--bsos`, `--transfers`, `--fault-profile`, and `--workload`. `--json` emits machine-readable results. Normal is the default and includes 10/100/1,000-BSO scaling, hot merchant, hot payer, and stronger fault lanes.

## Verify

```text
go test ./experiments/BSOSim/M0 -count=1
go test ./cmd/bso-sim
```

The companion Oct protocol contract is in the `oct` repository at `Experiments/BSOSim/M0`:

```text
go run ./cmd/oct test Experiments/BSOSim/M0 --execution interpreted
go run ./cmd/oct test Experiments/BSOSim/M0 --execution compiled
```

The correctness digest covers sorted BSO balances, both participant transfer states, and unresolved IDs. It excludes timestamps and performance numbers. Tests also check the stronger in-flight equation after delivery rounds:

```text
spendable + reserved + committed-but-not-yet-credited = initial total
```

Results are architectural shape measurements from a toy, in-process simulator. They are not comparisons with real payment systems or distributed ledgers.
