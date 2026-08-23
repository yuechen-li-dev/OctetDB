# Frozen task set

All tasks target only `github.com/yuechen-li-dev/octetdb v0.2.0`. Participants
received no hidden-test details and no maintainer explanation of the API.

## Task 1: durable inventory

Topology `inventory/items`. Create and read items, reserve and release stock,
reject over-reservation, survive restart, and make reserve retries idempotent.

## Task 2: idempotent webhook processor

Process an event by external event ID, store result/status, retry safely without
rerunning the callback, and preserve the behavior across restart. Explain the
record-key/command-ID distinction.

## Task 3: cross-dataset order transaction

Topology `commerce/orders` plus `inventory/items`. `PlaceOrder` verifies and
decrements stock and writes the order atomically. Failures must not partially
mutate either dataset. Retry, restart, and conflicting writes are exercised.

## Task 4: job discovery

Topology `workers/jobs`. Create, claim, complete/fail, and deterministically
list the first N ready jobs while excluding claimed jobs and stopping early.

## Task 5: durable domain rejection

Implement Created/Paid/Shipped/Cancelled transitions. Reject double pay and
ship-before-pay durably; ordinary callback errors must not consume identity.
Send payment email only after the durable decision and handle concurrency.

## Task 6: mixed point-read and scan workload

Across orders and items, list the first 20 low-stock items and Paid orders,
point-read one order, then mutate normally. Results must be deterministic and
survive restart; identical keys in distinct datasets remain independent.

