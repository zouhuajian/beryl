# types/AGENTS.md

This file applies to `types/`.

## 1. Directory purpose

`types/` contains authoritative domain model types shared across the Vecton workspace.

This directory should define:

- strongly typed identifiers
- domain enums and value objects
- Block / Chunk / Stream-related domain models
- metadata-facing shared domain structs
- state/version/epoch wrappers when they are part of domain semantics
- request/response-adjacent domain objects that are not protobuf-generated code

`types/` is the semantic model layer for the workspace.

## 2. What must NOT live here

Do not put the following into `types/`:

- protobuf-generated structs as the authoritative model
- service implementation logic
- persistence engine details
- transport codec logic
- client retry algorithms
- metadata state machine code
- worker-local execution helpers
- generic utilities that belong in `common/`

If a type is only meaningful inside one crate’s implementation, keep it in that crate.

## 3. Identity is strict here

This directory is the main place where identity separation must remain explicit.

Rules:

- `inode_id` is filesystem authority identity
- `data_handle_id` is stable data-plane/data-version identity
- `file_handle` is session-scoped open-write identity
- `block_id` must remain aligned with data-plane identity plus block index semantics
- do not create umbrella IDs that blur these roles

Never collapse multiple identities into one integer wrapper “for convenience”.

## 4. Block / Chunk / Stream modeling rules

The distinction is mandatory.

Rules:

- Block is the sole management / reporting / replication / relocation unit
- Chunk is the physical IO / repair / checksum granularity
- Stream is the continuous read/write abstraction with negotiated runtime context
- block size, storage chunk size, and transport frame size are different concepts
- model them as different concepts even when they are numerically equal in some deployments

Do not create APIs or structs that flatten Block/Chunk/Stream into a single generic range abstraction unless that abstraction is proven semantically correct.

## 5. Domain modeling rules

- use newtypes / wrappers for semantic safety where confusion is plausible
- use enums for variant-bearing concepts
- keep expected vs actual version/epoch/state distinctions explicit
- prefer typed structs over `HashMap<String, String>`
- avoid ambiguous names like `id`, `version`, `epoch`, `state`, `meta`
- use units in names when needed: `_bytes`, `_ms`, `_index`

A `types/` API should make illegal states harder to represent.

## 6. Persistence and self-description awareness

Types that participate in persisted or cross-node state must preserve interpretation semantics.

Rules:

- persisted layout-relevant fields must be explicit
- do not assume runtime defaults are enough to interpret existing data
- tail block / tail chunk semantics must stay representable where relevant
- if a type encodes layout/version/checksum meaning, document that clearly

## 7. Conversion rules

Conversions between domain types and proto/storage/runtime representations must be deliberate.

Rules:

- avoid lossy `From`/`Into` implementations unless the loss is obvious and safe
- if conversion can fail semantically, use `TryFrom`
- do not hide identity collapse or version dropping inside convenience conversions
- keep conversion boundaries visible in code review

## 8. Dependency discipline

`types/` should stay highly reusable.

Rules:

- keep dependencies light
- avoid depending on high-level implementation crates
- do not import generated proto code as the authoritative shape of the domain
- do not let `types/` become coupled to a single runtime path

## 9. Coding rules for this directory

- prioritize semantic precision over brevity
- derive common traits intentionally, not automatically everywhere
- use `Copy` only for truly small immutable value types where the semantic cost is low
- implement `Display` only when there is a stable human-meaningful representation
- implement `Default` only when a default value is semantically valid

Avoid placeholder or catch-all variants like `Unknown` unless the protocol/domain truly requires them.

## 10. Tests required for changes here

Changes in `types/` should usually include:

- identity separation tests
- serialization / conversion tests where applicable
- equality / ordering tests when semantics depend on them
- version/epoch comparison tests where relevant
- negative tests for invalid construction or failed conversion

Tests should validate semantics, not just derive behavior.

## 11. Pre-merge checklist

Before submitting a change in `types/`, verify:

- did I preserve inode/data/session identity separation?
- did I keep Block / Chunk / Stream distinct?
- did I accidentally model runtime defaults as persisted truth?
- did I add a type that actually belongs in `common/`?
- did I add crate-local implementation detail that should stay outside `types/`?
- are conversions explicit and semantically safe?