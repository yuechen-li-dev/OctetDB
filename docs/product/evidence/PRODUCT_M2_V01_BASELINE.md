# PRODUCT-M2 v0.1.0 external baseline

This evidence was recorded before PRODUCT-M2 product changes.

## Method

A clean module was created outside the OctetDB checkout with this requirement:

```text
github.com/yuechen-li-dev/octetdb v0.1.0
```

There was no `replace` directive. `go list -m` resolved the dependency from the
public module cache at `github.com/yuechen-li-dev/octetdb@v0.1.0`. The probes
used only the root public package. `go test -v ./...` passed four executable
tests, including close/reopen checks. The two state-machine tests pass by
demonstrating the invalid states that v0.1 cannot prevent.

## Golden probes

| App | Public mapping attempted | Restart/idempotency result | Fundamental result |
| --- | --- | --- | --- |
| Inventory reservation | item ID → `AccountID`; stock → `Balance`; reserve/release → `Withdraw`/`Deposit` | correct across restart; exact retry returns the prior result | usable only by adopting financial names and integer-only stock |
| Webhook processor | hash external event ID into `AccountID`; account existence → processed | duplicate and restart-safe | cannot store the external ID, processing status, or result; hash collision becomes a correctness concern |
| Order lifecycle | order state encoded in `Balance`; transition → `Deposit` | command retry works; state restarts | two callers can both observe Created and both apply Paid, producing Shipped without a ship command |
| Job state | job status encoded in `Balance`; claim → `Deposit` | command retry works; state restarts | two distinct workers can both claim; owner, failure reason, and retry count have no representation |

All four probes used zero internal imports and required no Oct source, generated
code, layout contract, WAL filename, snapshot filename, or compiler concept.

## Friction recorded before fixes

| Friction | Apps | Classification | Evidence |
| --- | ---: | --- | --- |
| Public model names and validation are financial | 4 | missing product feature | only `Account`, `Balance`, and nine fixed account command kinds exist |
| Application-defined fields cannot be stored | 3 | missing product feature | webhook status/result, order state details, and job ownership have no public representation |
| Read-validate-write is not atomic | 2 | missing product feature | order double-pay and job double-claim are both accepted when expressed as separate `Get` and `Deposit` operations |
| External string IDs require an application hash/table | 3 | API ergonomics | the only entity key is a nonzero `uint64` |
| Exact retries require stable caller IDs | 4 | intentional advanced requirement | correctness requires the caller to reuse identity; silent ID generation would break retries |
| Product bounds have safe zero-value defaults | 4 | not friction | no capacity, dedupe, or batch tuning was required |
| Snapshot calls are optional but WAL remains until explicitly snapshotted | 4 | missing default | basic callers must either accept unbounded WAL growth or learn `Snapshot` maintenance |
| `Get` lacks context and typed operational errors | 4 | API ergonomics | it returns only `(Account, bool)`, conflating closed with missing |

## Metrics

These counts cover direct OctetDB calls/types in each small persistence adapter,
not HTTP/service/domain code. Concepts count distinct public OctetDB types or
operations a developer must understand.

| App | OctetDB-specific integration LOC | Public concepts | Configuration values chosen | Restart/idempotency | Internal imports | Oct required |
| --- | ---: | ---: | ---: | --- | ---: | --- |
| Inventory | 12 | 7 | 1 (`Path`) | correct / correct | 0 | no |
| Webhook | 11 | 7 | 1 (`Path`) | marker correct / correct; result unavailable | 0 | no |
| Order | 11 | 7 | 1 (`Path`) | restart correct / retry correct; transition correctness impossible | 0 | no |
| Job | 11 | 7 | 1 (`Path`) | restart correct / retry correct; claim correctness impossible | 0 | no |

## Baseline conclusion

Documentation alone cannot repair the repeated blocker. v0.1 is honestly
account-specific. A default product path needs application-defined keyed state
and one durable, idempotent, atomic validated mutation boundary. Opaque blob CRUD
alone would store the missing fields but would not solve the double-transition
and double-claim failures.
