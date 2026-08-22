# M5 Octagon/record-table reconnaissance

Oct revision: `a997285fc006b29143c31f6391ca6f21a950adb2`.

## Primitive record-table probe

`reconnaissance/octagon_record_table_probe.octest` loads a one-row
`Products { ID: Int Name: String }` table from Octagon. Compiled execution
passes and preserves nominal table row/cell behavior.

## Artifact interpreter mismatch

Using the same direct record-table root through `oct artifact` fails before
artifact code runs:

```text
LoadOctagon .../catalog.octagon: record field ID mismatch:
expected Int, got ast.ArrayLiteralExpr
```

The artifact command reports `Execution: build-time-interpreted`. Its Octagon
materializer treats a declared record-table field as a scalar cell and does not
apply the table's implicit column-array storage rule.

## Compiled enum/refinement mismatch

The compiled loader accepts primitive Int/String table columns, but the
realistic table with enum-valued columns (and independently a refined Int
price column) panics during typed reflection materialization:

```text
panic: reflect: call of reflect.Value.SetInt on struct Value
main.__octMaterialize(...)
main.__octLoadOctagon_DBSchedulerM5_Products(...)
```

M5's bounded workaround is a typed `CatalogData` record containing column
arrays. Both compiled Facts and the artifact interpreter load that shape. Oct
then constructs and validates the `Products` record table. Enum columns are
represented as integer codes with range Facts until the compiled loader is
fixed. No Oct compiler change was made for M5.
