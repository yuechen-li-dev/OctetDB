# OCT-DB-TEMPLATES-M0 evidence

This evidence records why the milestone ends in **Honest stop** rather than
shipping a metadata-only template catalog that cannot type-check its central
`Record`, `Key`, and predicate overrides.

## Reproduction

From the Oct repository at commit `5a77e76801cdd3e7c8dbdeb272ce04ba4930d5dd`:

```powershell
go run ./cmd/oct run <OctetDB>/docs/product/evidence/OCT_DB_TEMPLATES_M0/compiler-probes/convention/Main.oct
go run ./cmd/oct build <OctetDB>/docs/product/evidence/OCT_DB_TEMPLATES_M0/compiler-probes/parametric-record/ParametricRecord.template.oct
go run ./cmd/oct build <OctetDB>/docs/product/evidence/OCT_DB_TEMPLATES_M0/compiler-probes/nominal-predicate/Main.oct
go run ./cmd/oct build <OctetDB>/docs/product/evidence/OCT_DB_TEMPLATES_M0/compiler-probes/field-selector/Main.oct
```

The first probe proves `.template.oct` already participates in normal package
discovery and `with` customization without a parser mode. The remaining probes
isolate the missing language capabilities: user-defined type parameters,
predicate retargeting across nominal record types, and typed field selectors.

`probe-results.txt` contains the exact command results. No generated Go was
hand-edited and no runtime/template subsystem was added.
