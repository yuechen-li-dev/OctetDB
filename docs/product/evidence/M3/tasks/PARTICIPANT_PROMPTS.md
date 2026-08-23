# Participant prompts

Each participant received this common instruction in a clean context:

> You are a participant in a measured fresh-user benchmark of released Go
> module `github.com/yuechen-li-dev/octetdb@v0.2.0`. Create an independent Go
> module in your assigned directory that depends exactly on v0.2.0. Do not use
> a replace directive, local checkout import, unreleased code, internal/research
> package, or inspect `docs/product/PRODUCT_M*` or another participant. You may
> consult only README, GETTING_STARTED, DURABILITY, RECOVERY, CHANGELOG, v0.2.0
> release notes, exported GoDoc/pkg.go.dev, public examples, and downloaded
> exported source. Discover the API from that evidence without architecture
> guidance. Preserve tests and detailed notes covering times, documents and
> source consulted, failures, hallucinations, interventions, cycles, LOC,
> concepts, workarounds/feature requests, and operational/schema answers. Run
> `go test ./...`. Do not modify product source.

## Participant A assignment

> Task 1: durable inventory at `inventory/items`: create/read, reserve/release,
> reject over-reservation, restart, and idempotent reserve retry. Catch double
> reservation, negative stock, retry reapplication, and restart loss.
>
> Task 4: job discovery at `workers/jobs`: create, claim, complete/fail, list
> the first N Ready jobs deterministically, exclude claimed jobs, use public
> scan early-stop/Take where applicable, and preserve restart correctness. Do
> not maintain a hidden in-memory ready list unless justified as durable state.

## Participant B assignment

> Task 2: process a webhook by external event ID, store result/status, retry
> safely without rerunning the callback, and retry after restart. Choose a
> stable command-ID strategy and distinguish record key from command ID.
>
> Task 5: implement Created/Paid/Shipped/Cancelled. Reject double pay and
> ship-before-pay; repeat rejected commands durably; let unexpected callback
> error leave retry identity available. Send an email after successful payment,
> placing it where you think correct. Test conflicting pays and restart.

## Participant C assignment

> Task 3: at `commerce/orders` and `inventory/items`, make `PlaceOrder`
> atomically verify stock, decrement inventory, create/update the order, and
> return a result. Failure must not partially mutate either Dataset. Include
> idempotent retry, restart, and bounded conflicting-write tests.
>
> Task 6: list the first 20 low-stock items, list the first 20 Paid orders,
> point-read one order, then mutate normally across those two Datasets. Results
> must be deterministic; test restart and the same key in different Datasets.
> Use the public query surface rather than inventing generic SQL.

The coordinator's original dispatch also named each absolute assigned
directory. That environmental path is omitted above; the preserved source
directories unambiguously identify each target.

