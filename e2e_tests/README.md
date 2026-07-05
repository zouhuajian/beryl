# Vecton E2E Tests

This crate owns full-system Vecton E2E tests for the current supported runtime path.

The tests start local metadata and worker services with temporary state, then use the public `FsClient` for user-visible operations. Run them directly with:

```bash
cargo test -p e2e_tests
```

`cargo test --workspace` includes this crate by default.

The current suite covers the supported Rust client -> metadata -> worker path, metadata restart fail-closed behavior for active writes, and worker restart/full-report convergence.

It does not cover active UFS read/write paths, worker peer transfer, QUIC/RDMA, replication, multi-group metadata, metadata peer RPC, admin API, physical block free, or complete repair/rebalance product behavior.
