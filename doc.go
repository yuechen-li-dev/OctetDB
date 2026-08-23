// Package octetdb provides an embeddable, single-process OLTP engine with a
// conventional keyed-state workflow and specialized domain paths.
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
// New applications can use OpenKeyed with DefaultKeyedOptions, store ordinary
// Go values as JSON, and express atomic validated changes with SubmitKeyed.
// Stable KeyedCommand IDs give exact retry decisions inside the configured
// bounded dedupe horizon. The v0.1 Open and DB APIs retain the narrower
// account/transfer model.
//
// OctetDB is not a SQL database, network service, replicated system, ORM, or
// dynamic query engine. A directory must be opened by only one process.
package octetdb
