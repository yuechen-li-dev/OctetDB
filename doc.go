// Package octetdb provides an embeddable, single-process OLTP engine with a
// conventional catalog/keyed-state workflow and specialized domain paths.
//
// A successful Submit or SubmitBatch call means the command decisions and
// resulting authoritative account state have been written to the WAL and the
// WAL has been synchronized. Open recovers the latest installed snapshot and
// then replays the complete WAL tail. OctetDB rejects detected corruption and
// incompatible formats rather than attempting a best-effort open.
//
// A successful SubmitKeyed call likewise means its exact decision and all
// resulting record mutations have been written and synchronized. OpenKeyed
// recovers a validated snapshot and complete WAL tail.
//
// New applications can use OpenCatalog with DefaultKeyedOptions. Bucket and
// Dataset calls durably declare a shallow Database/Bucket/Dataset topology;
// Dataset.Get and Dataset.Mutate store ordinary Go values as keyed JSON without
// application string prefixes. Dataset.Scan and ScanDataset visit detached
// logical records in key order while holding one stable committed state; a
// callback can stop synchronously without a goroutine or channel pipeline.
// CatalogDB.Mutate can atomically access several datasets. Stable KeyedCommand
// IDs remain database-wide and give exact retry decisions inside the configured
// bounded dedupe horizon.
//
// The catalog is semantic topology, not physical nesting. M2B has no arbitrary
// child datasets, SQL/query language, secondary index, rename, or destructive
// delete. Query-M0 scans KeyedJSON datasets and may block writes for the scan
// duration. OpenKeyed remains the candidate PRODUCT-M2 compatibility
// path, and the v0.1 Open and DB APIs retain the account/transfer model.
//
// OctetDB is not a SQL database, network service, replicated system, ORM, or
// dynamic query engine. A directory must be opened by only one process.
package octetdb
