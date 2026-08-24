# Negative specialization proof: webhook ingest

Decision: use default OctetDB; select no template.

The PERF-M4 webhook/event shape is dominated by durable synchronized command handling, exact durable replay results, and point access. OctetDB already owns those semantics through `Database.Mutate`, database-wide keyed command identity, the dedupe horizon, and Dataset point lookup. A filtered read materialization would neither remove the synchronization bottleneck nor add semantic value, while creating a rebuild/coherence obligation.

No `EventDedupeDataset` is published. This is an intentional catalog result, not a missing implementation. Default Go remains the complete application path.
