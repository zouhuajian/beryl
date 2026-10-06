# Beryl

Beryl is a distributed storage system written in Rust.

## Rust

- Prefer concrete types and clear ownership. Introduce traits, generics, and shared state only when needed.
- Use `Result` for expected failures and preserve error sources. Justify any use of `panic` or `unsafe`.
- Do not hold synchronous lock guards across `.await`. Keep blocking operations off async executor threads. Define shutdown and error handling for background tasks.

## Storage Semantics

- When changing read, write, replication, or recovery paths, define success conditions, durability boundaries, and the state after failures.
- Specify encoding, versioning, and corruption handling for protocols and on-disk formats. Do not depend on Rust memory layout.
- Discuss unconfirmed architecture and consistency semantics before implementing them.

## Engineering Conventions

- Use the toolchain in `rust-toolchain.toml`. Inherit shared workspace settings in member crates and maintain only the root `Cargo.lock`.
- Use `license_header.txt` for new Rust source files.
- Match validation to the scope of the change. The full checks must match CI:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```
