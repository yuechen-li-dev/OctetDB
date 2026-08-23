// Package octetdb provides an embeddable, single-process OLTP engine for a
// bounded account and transfer domain.
//
// A successful Submit or SubmitBatch call means the command decisions and
// resulting authoritative account state have been written to the WAL and the
// WAL has been synchronized. Open recovers the latest installed snapshot and
// then replays the complete WAL tail. OctetDB rejects detected corruption and
// incompatible formats rather than attempting a best-effort open.
//
// OctetDB v0.1 is not a SQL database, a network service, a replicated system,
// or a generic command store. Applications open one DB for a directory, submit
// uniquely identified commands, read accounts by ID, optionally create a
// snapshot, and close the DB before opening that directory again.
package octetdb
