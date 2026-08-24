# JobQueue proof

Profile summary: repeated Ready scans dominate a read-only/rebuild-published job snapshot. The application explicitly selects `StableIdentity`, `BoundedExtent`, `BoundedKeyedDataset`, `ReadMostlyDataset`, `MaterializedFilter`, `FiniteStateDataset`, `FilteredView`, and the shallow `JobQueue` starting point. It imports `DatabaseTemplateContracts`, uses `.ID`, `.Status`, `IsReady`, and three Concept-admitted `with` overrides; no backend mechanism is named.

The fact proves the 100,000-record bound, exact key selector, state typing, source ordering, Ready predicate, two-result early stop, and compiled/interpreted parity.
