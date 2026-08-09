# Internal Release Procedure

Beryl release artifacts are built only from an annotated version tag on the
pinned Anolis 8 builder. Normal pull requests, pushes, and merges run validation
but do not publish binaries.

## Prepare the tag

Start from the intended release commit with a clean worktree. The tag must be
`v` followed by the workspace version from `Cargo.toml`:

```bash
git tag -a v0.1.0-alpha.1 -m "Beryl v0.1.0-alpha.1"
```

Do not push the tag yet. A failed local release can be corrected without
changing a published tag.

## Build and package on Anolis 8

Run as the non-root release user with rootless Podman:

```bash
./scripts/build-anolis8-release.sh --require-tag
./scripts/package-release.sh --build-root <build-root-printed-above>
```

`--require-tag` fails unless HEAD has the annotated tag matching the workspace
version. The build records the tag, full source revision, toolchain, builder
image, and binary hashes. Packaging produces one deterministic tarball and its
SHA-256 checksum.

## Acceptance and publication

Before publication:

1. Verify the checksum and archive allowlist.
2. Confirm `VERSION` contains the expected tag and full source revision.
3. Clean-install the extracted package on the Anolis 8 host.
4. Format new storage, start one Metadata and one Worker, and wait for both
   readiness endpoints.
5. Run the same-tag Rust Client CRUD example and one same-version restart cycle.
6. Confirm both services remain ready and application logs contain no fatal
   errors.

Copy only the accepted `.tar.gz` and `.tar.gz.sha256` to the approved internal
artifact directory. Do not create a public GitHub Release.

Push the tag only after the artifact has passed acceptance:

```bash
git push origin v0.1.0-alpha.1
```

Installation, service, logging, metrics, and maintenance commands are documented
in [OPERATIONS.md](OPERATIONS.md).
