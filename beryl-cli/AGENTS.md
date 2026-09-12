# beryl-cli Agent Instructions

Follow the repository root instructions.

## Responsibility

Own the public command contract, installed-package resolution, and routing to
the appropriate runtime role.

## Invariants

- Resolve runtime roles within the installed package, independently of ambient
  command lookup.
- Preserve the public process identity and signal behavior of long-running
  roles.
- Keep static configuration validation separate from service startup.
- Reject invalid installation state and report role failures explicitly.
- Leave configuration semantics and runtime policy with the owning role.
  Process supervision and arbitrary command execution are outside this crate's
  current responsibility.

## Validation Focus

Verify affected command behavior through process boundaries, including package
resolution, configuration validation, failure reporting, and signal behavior
when relevant.
