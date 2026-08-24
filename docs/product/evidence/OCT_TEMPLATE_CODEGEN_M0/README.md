# OCT-TEMPLATE-CODEGEN-M0 — W5 codegen parity

Verdict: Success.

- Same source semantics: template and bespoke W5 return identical counts/order at limits 1, 10, 2500, and 5000.
- Same normalized MIR: Oct's focused compiler regression performs structural equality after nominal FLOW-name normalization.
- Same emitted Go: independently emitted FLOW cores and public facades are byte-identical after normalizing nominal symbols and the intentionally identity-bearing checkpoint fingerprint.
- Same compiler profile: Go reports the same constructor/core/facade inlining and escape decisions; both benchmark lanes allocate zero bytes.
- Stable order/provenance: compiler declaration lowering is name-sorted, repeated generation is byte-identical, and provenance remains comments only.

The original benchmark layout reported template 11.7% slower. After changing only benchmark helper placement, five one-second samples gave these medians:

| Control | Bespoke | Template | Apparent difference |
| --- | ---: | ---: | ---: |
| Forward order | 66,754 ns/op | 60,708 ns/op | template 9.1% faster |
| Reverse order | 66,398 ns/op | 60,630 ns/op | template 8.7% faster |

This reversal invalidates the old causal attribution. It is evidence of Go code/benchmark-layout sensitivity, not that templates are faster. The durable conclusion is no runtime cost attributable to template codegen.

Reproduction commands are in `../OCT_DB_TEMPLATES_M0/README.md`; the compiler-side proof is in Oct at `docs/internal/evidence/OCT_TEMPLATE_CODEGEN_M0/README.md`.
