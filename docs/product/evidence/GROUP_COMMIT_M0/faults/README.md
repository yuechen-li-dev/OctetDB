# Fault evidence

Deterministic pre-append, pre-Sync, and post-Sync/pre-response seams are internal fields on `KeyedDB`. Focused tests in `group_commit_test.go` cover Sync failure poisoning, append failure after staged duplicate fanout, response-loss recovery, cancellation, panic isolation, and close/drain behavior.
