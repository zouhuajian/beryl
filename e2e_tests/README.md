# Vecton E2E Tests

This crate owns full-system Vecton E2E tests for the current supported runtime path.

The tests start local metadata and worker services with temporary state, then use the public `FsClient` for user-visible operations. Run them directly with:

```bash
cargo test -p vecton-e2e-tests
```

`cargo test --workspace` includes this crate by default.

The first test set does not cover active UFS read/write paths, QUIC/RDMA, replication, multi-group metadata, physical block free, or restart recovery.
