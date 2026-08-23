// Package octetdb provides a durable embedded Go OLTP database.
//
// New applications open one user-selected directory with OpenCatalog, then
// declare the durable logical topology:
//
//	Database
//	└── Bucket
//	    └── Dataset
//	        └── Records
//
// Record identity is a Dataset plus an application key. KeyedCommand identity
// is database-wide because one Database.Mutate callback can atomically read and
// write records in several datasets. A successful mutation has one durable
// accepted or rejected decision and is exactly retryable while its command ID
// remains inside the configured dedupe horizon.
//
// Dataset.Get decodes one record into an ordinary Go value. Dataset.Scan and
// ScanDataset visit detached values in ascending record-key order from one
// stable logical snapshot. Scans are read-only, honor context cancellation
// between records, and stop synchronously when a callback returns ScanStop.
// The current serialized snapshot implementation blocks mutations for the
// duration of a scan.
//
// Oct is optional. Go applications can use every database and scan capability
// in this package without the Oct compiler or runtime. Oct's separate
// filter/map/take query syntax is an advanced authoring path that lowers to its
// FLOW runtime; Oct compiler internals are not dependencies of this package.
//
// The v0.1 Open and DB account API remains supported. OpenKeyed is a deprecated
// compatibility path for the distinct, unreleased pre-v0.2 global-key format;
// new code should use OpenCatalog. Formats are detected fail-closed and are
// never silently reinterpreted or migrated.
//
// OctetDB is single-process and single-replica. It is not SQL, a network
// service, an ORM, or a replicated system. The application must ensure that at
// most one process or handle opens a database directory.
package octetdb
