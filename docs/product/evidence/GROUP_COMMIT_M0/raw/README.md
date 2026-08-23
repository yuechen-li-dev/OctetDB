# Raw evidence

Bounded formal W1-W4/c1/c8/c32 output is retained for pinned v0.2.0 and the current checkout on WSL/ext4 and Windows/NTFS. The `*-current` files were built with a temporary Go module `replace` to the checkout. PERF-M4's legacy harness writes a hard-coded `octetdb@v0.2.0` metadata label, so that one label remains stale in `*-current`; directory identity and the reproducible run scripts distinguish the binaries.
