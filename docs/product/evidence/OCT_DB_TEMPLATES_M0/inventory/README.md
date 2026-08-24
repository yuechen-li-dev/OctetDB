# Inventory proof

Profile summary: repeated low-stock scans dominate an explicitly rebuilt inventory snapshot. The application selects the bounded-keyed and materialized-filter patterns plus the shallow `Inventory` starting point and imports `DatabaseTemplateContracts` for refined configuration values. `.SKU` and `IsLowStock` remain exact to `InventoryItem`; they cannot be reused for `Job`.

The fact proves the 50,000-record bound, exact SKU selector, low-stock result, explicit publication source/version, and compiled/interpreted parity.
