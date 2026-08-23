# Hidden-test policy

The coordinator applies black-box tests only after participant code is
submitted. Adapters may translate each participant's public application API,
but must not repair its semantics. Tests cover close/reopen, duplicate accepted
and rejected decisions, callback abort/retry, bounded conflicts, conservation,
deterministic scans, early stop where observable, and dataset-scoped identity.

