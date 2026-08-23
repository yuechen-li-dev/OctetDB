# PRODUCT-M3 evidence

This directory contains the frozen prompts, isolated participant applications,
hidden-test adapters, and factual summaries for the first OctetDB v0.2.0
human/LLM integration benchmark round.

## Frozen subject

- Module: `github.com/yuechen-li-dev/octetdb v0.2.0`
- Tag commit: `76a0659ae9125c8c9b689fe155f2ff30a4b30fc9`
- Product features remained frozen throughout the round.
- Participant modules must not contain `replace` directives or import internal,
  research, or unreleased packages.

## Isolation

Participants A, B, and C started in clean model contexts. Each received two
task descriptions and the allowlist of public product documents. They were
instructed not to inspect other participant directories or internal milestone
reports. Hidden tests were authored and applied separately by the benchmark
coordinator after participant implementation.

No real independent human participant was available for this round. The
`human/README.md` artifact records that fact; no proxy sentiment is fabricated.

