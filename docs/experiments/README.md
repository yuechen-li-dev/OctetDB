# Experiment archive

Everything in this directory and the repository-level `experiments/` directory
is retained research evidence. These reports explain what was tested and why;
they are not OctetDB API or operational documentation.

Historical names such as M7, M8, M9, C2, LayoutM0, and TigerCompareM0 describe
the lanes that produced evidence at the time. They may no longer match current
production package names. Do not rewrite old reports to make them appear as if
they used the product API.

The supported product starts at the root `octetdb` package and depends only on
`internal/core`. Deleting `experiments/`, historical reports, and benchmark
commands does not remove a production dependency.
