# beryl-cli Agent Instructions

Follow the repository root `AGENTS.md`. This file adds crate-specific
constraints.

## Crate Boundary

`beryl-cli` owns the public command contract, installed-package path
resolution, and routing to package-internal Metadata and Worker processes.

## Required Behavior

- Resolve role binaries from the installed `libexec` directory, never `PATH`.
- Replace the CLI process for long-running roles so PID and signal ownership are
  preserved.
- Keep static configuration validation separate from runtime startup.
- Keep the public command surface explicit and fail closed on invalid layout or
  child process failures.

## Prohibited Changes

- Do not depend on `beryl-metadata` or `beryl-worker` production crates.
- Do not add daemonization, process supervision, service discovery, shell
  evaluation, or arbitrary command passthrough.
- Do not duplicate role configuration parsing or runtime policy.

## Focused Validation

```bash
cargo test -p beryl-cli
```
