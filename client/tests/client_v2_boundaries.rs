// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

#[test]
fn client_crate_does_not_depend_on_removed_transport_or_worker_crates() {
    let manifest = include_str!("../Cargo.toml");

    assert!(
        !has_path_dependency(manifest, "worker"),
        "client crate must not depend on the worker crate"
    );
    assert!(
        !has_path_dependency(manifest, "transport"),
        "client crate must not depend on a removed global transport crate"
    );
    assert!(
        !manifest.contains(concat!("worker", "-rpc")) && !manifest.contains(concat!("worker", "_rpc")),
        "client crate must not depend on removed worker RPC crates"
    );
}

#[test]
fn client_v2_module_roots_exist_without_legacy_worker_module_export() {
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let src = manifest_dir.join("src");
    let lib = std::fs::read_to_string(src.join("lib.rs")).expect("read client/src/lib.rs");

    for module in ["api", "metadata", "data", "runtime", "cache", "session", "planner"] {
        assert!(
            src.join(module).join("mod.rs").exists(),
            "client v2 module root {module}/mod.rs must exist"
        );
    }

    assert!(
        !src.join("control").exists(),
        "client refactor uses metadata/ rather than control/"
    );
    assert!(
        !src.join("worker").exists(),
        "client/src/worker must not exist as a public or implied worker boundary"
    );
    assert!(
        !src.join("legacy").exists(),
        concat!(
            "client/src/",
            "legacy must not remain after the active FsClient path owns the public client surface"
        )
    );
    assert!(
        !lib.contains(concat!("pub mod ", "worker;")),
        "legacy worker module must not remain a public client boundary"
    );
    assert!(
        !declares_public_module(&lib, "legacy"),
        "legacy client module must not remain a public client boundary"
    );
    assert!(
        !declares_public_module(&lib, "worker"),
        "client/src/lib.rs must not expose a public worker module"
    );
    assert!(
        !declares_public_module(&lib, "data"),
        "worker data-plane internals must not be a public client module"
    );
}

#[test]
fn top_level_public_facade_is_v2_without_legacy_hcfs_or_meta_exports() {
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let src = manifest_dir.join("src");
    let lib = std::fs::read_to_string(src.join("lib.rs")).expect("read client/src/lib.rs");
    let api = std::fs::read_to_string(src.join("api").join("mod.rs")).expect("read client/src/api/mod.rs");

    for name in [
        "FsClient",
        "FileReader",
        "FileWriter",
        "OpenOptions",
        "CreateOptions",
        "AppendOptions",
        "ListOptions",
        "CreateDisposition",
    ] {
        assert!(
            exports_public_name(&lib, name) || exports_public_name(&api, name),
            "{name} must remain part of the v2 public facade"
        );
    }

    for name in [
        "FileHandle",
        "CreateMode",
        "Client",
        "Handle",
        concat!("Open", "Flags"),
        concat!("Action", "Machine"),
        "ReplayPolicy",
        concat!("Rpc", "Op"),
        concat!("Tonic", "FileSystemRpc"),
        concat!("Metadata", "RpcHelper"),
    ] {
        assert!(
            !exports_public_name(&lib, name),
            "{name} must not be a top-level public client export"
        );
        assert!(
            !exports_public_name(&api, name),
            "{name} must not be re-exported by the primary api module"
        );
    }

    assert!(
        !declares_public_module(&lib, "meta"),
        "old meta action-machine module must not be a public top-level module"
    );
    assert!(
        !declares_public_module(&lib, "metadata"),
        "metadata internals must not be a public top-level module"
    );
}

#[test]
fn crate_root_exposes_only_stable_facade_modules_and_types() {
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let src = manifest_dir.join("src");
    let lib = std::fs::read_to_string(src.join("lib.rs")).expect("read client/src/lib.rs");

    for name in [
        "FsClient",
        "FileReader",
        "FileWriter",
        "OpenOptions",
        "CreateOptions",
        "AppendOptions",
        "ListOptions",
        "CreateDisposition",
        "ClientConfig",
        "ClientError",
    ] {
        assert!(
            exports_public_name(&lib, name),
            "{name} must remain a root-level public facade export"
        );
    }

    for module in [
        "api",
        "cache",
        "canonical",
        "data",
        "metadata",
        "planner",
        "routing",
        "runtime",
        "session",
    ] {
        assert!(
            !declares_public_module(&lib, module),
            "client::{module} must not be a public crate-root module"
        );
    }
}

#[test]
fn public_api_sources_do_not_expose_worker_stream_handles() {
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let src = manifest_dir.join("src");
    let lib = std::fs::read_to_string(src.join("lib.rs")).expect("read client/src/lib.rs");
    let api = std::fs::read_to_string(src.join("api").join("mod.rs")).expect("read client/src/api/mod.rs");

    assert!(
        !declares_public_module(&lib, "data"),
        "client::data must stay internal until public read/write APIs are wired"
    );

    for name in [
        "ReadHandle",
        "WorkerWriteHandle",
        "WorkerReadOp",
        "WorkerWriteOp",
        "CommitWriteOp",
        "WorkerDataClient",
        "TonicWorkerDataClient",
    ] {
        assert!(
            !exports_public_name(&lib, name) && !exports_public_name(&api, name),
            "{name} must not be exported through the public facade"
        );
    }
}

#[test]
fn public_facade_does_not_export_metadata_layout_or_worker_route_details() {
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let src = manifest_dir.join("src");
    let lib = std::fs::read_to_string(src.join("lib.rs")).expect("read client/src/lib.rs");
    let api = std::fs::read_to_string(src.join("api").join("mod.rs")).expect("read client/src/api/mod.rs");

    for name in [
        "LayoutSnapshot",
        "WriteTarget",
        "WorkerEndpoint",
        "WorkerEndpointInfoProto",
        "FileBlockLocationProto",
        "ReadStream",
        "WriteStream",
    ] {
        assert!(
            !exports_public_name(&lib, name) && !exports_public_name(&api, name),
            "{name} must not be exported through the public client facade"
        );
    }

    for needle in ["worker_endpoint", "block_location", "stream_id"] {
        assert!(
            !lib.contains(needle) && !api.contains(needle),
            "{needle} must not appear in the public client facade sources"
        );
    }
}

#[test]
fn client_sources_do_not_use_process_evolution_wording() {
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let src = manifest_dir.join("src");
    let mut offenders = Vec::new();
    collect_rs_files(&src, &mut offenders);
    let banned = [
        "phase",
        "Phase",
        "v2",
        "V2",
        "staged migration",
        "staged unsupported",
        "temporary phase",
        "next phase",
    ];
    let matches = offenders
        .into_iter()
        .flat_map(|path| {
            let source = std::fs::read_to_string(&path).expect("read source");
            banned
                .iter()
                .filter(move |needle| source.contains(**needle))
                .map(move |needle| format!("{} contains {needle}", path.display()))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    assert!(matches.is_empty(), "banned wording found: {matches:?}");
}

#[test]
fn client_source_tree_has_no_orphan_rust_files() {
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let src = manifest_dir.join("src");
    let expected = [
        "api/fs_client.rs",
        "api/handle.rs",
        "api/mod.rs",
        "api/options.rs",
        "api/status.rs",
        "api/tests.rs",
        "cache/layout.rs",
        "cache/mod.rs",
        "cache/state_id.rs",
        "cache/worker_endpoint.rs",
        "canonical.rs",
        "config.rs",
        "consistency.rs",
        "context.rs",
        "data/mod.rs",
        "data/worker.rs",
        "error.rs",
        "lib.rs",
        "metadata/gateway.rs",
        "metadata/header.rs",
        "metadata/mod.rs",
        "metadata/ops.rs",
        "metadata/snapshot.rs",
        "metrics.rs",
        "modes.rs",
        "planner/mod.rs",
        "planner/read_planner.rs",
        "planner/write_planner.rs",
        "runtime/backoff.rs",
        "runtime/classify.rs",
        "runtime/context.rs",
        "runtime/decision.rs",
        "runtime/executor.rs",
        "runtime/mod.rs",
        "runtime/policy.rs",
        "runtime/refresh.rs",
        "runtime/singleflight.rs",
        "session/mod.rs",
        "session/write_session.rs",
    ]
    .into_iter()
    .map(str::to_string)
    .collect::<std::collections::BTreeSet<_>>();

    let mut files = Vec::new();
    collect_rs_files(&src, &mut files);
    let actual = files
        .into_iter()
        .map(|path| {
            path.strip_prefix(&src)
                .expect("source file under client/src")
                .to_string_lossy()
                .replace('\\', "/")
        })
        .collect::<std::collections::BTreeSet<_>>();

    let unexpected = actual.difference(&expected).collect::<Vec<_>>();
    let missing = expected.difference(&actual).collect::<Vec<_>>();

    assert!(
        unexpected.is_empty() && missing.is_empty(),
        "client/src Rust file set drifted; unexpected={unexpected:?}, missing={missing:?}"
    );
}

fn has_path_dependency(manifest: &str, crate_name: &str) -> bool {
    let prefix = format!("{crate_name} =");
    manifest
        .lines()
        .map(str::trim)
        .any(|line| line.starts_with(&prefix) && line.contains("path"))
}

fn declares_public_module(source: &str, module: &str) -> bool {
    let pub_mod = format!("pub mod {module};");
    source.lines().map(str::trim).any(|line| line == pub_mod)
}

fn exports_public_name(source: &str, name: &str) -> bool {
    let compact = source
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with("pub use "))
        .collect::<Vec<_>>()
        .join(" ");
    let normalized = compact.replace(['{', '}', ',', ';'], " ");

    normalized
        .split_whitespace()
        .any(|token| token == name || token.ends_with(&format!("::{name}")))
}

fn collect_rs_files(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
    for entry in std::fs::read_dir(dir).expect("read source dir") {
        let path = entry.expect("dir entry").path();
        if path.is_dir() {
            collect_rs_files(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}
