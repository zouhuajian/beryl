// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Service-layer error contract guardrails.

use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind, RecoveryAction, RefreshHint, RpcErrorDetail};
use beryl_metadata::service::header_from_rpc_error;
use beryl_proto::convert::rpc_error_from_proto;
use beryl_types::GroupName;
use std::fs;
use std::path::{Path, PathBuf};

const SCAN_ROOTS: &[&str] = &["src/service", "src/worker", "../beryl-worker/src"];
const REQUIRED_SCAN_FILES: &[&str] = &[
    "src/service/rpc.rs",
    "src/worker/service.rs",
    "../beryl-worker/src/net/server/grpc.rs",
];
const ALLOWLISTED_STATUS_FILES: &[&str] = &["../beryl-worker/src/net/server/grpc.rs"];
const SERVER_IMPL_MARKERS: &[&str] = &[
    "impl FileSystemServiceProto for",
    "impl MetadataWorkerServiceProto for",
    "impl WorkerDataService for",
];
const FORBIDDEN_PATTERNS: &[&str] = &["Status::from_error", "return Err(Status", "Err(Status"];

fn collect_rs_files_recursive(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).unwrap_or_else(|e| panic!("failed to read {}: {}", dir.display(), e)) {
        let entry = entry.expect("failed to read directory entry");
        let path = entry.path();
        if path.is_dir() {
            collect_rs_files_recursive(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

fn scan_root_paths() -> Vec<PathBuf> {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    SCAN_ROOTS.iter().map(|root| manifest_dir.join(root)).collect()
}

fn rel_from_manifest(path: &Path) -> String {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    path.strip_prefix(manifest_dir)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn grpc_server_impl_files() -> Vec<PathBuf> {
    let mut files = Vec::new();
    for root in scan_root_paths() {
        collect_rs_files_recursive(&root, &mut files);
    }
    files.sort();
    files.dedup();
    files
        .into_iter()
        .filter(|path| {
            let content = fs::read_to_string(path).expect("failed to read source file");
            SERVER_IMPL_MARKERS.iter().any(|marker| content.contains(marker))
        })
        .collect()
}

fn coverage_report(scanned_files: &[PathBuf]) -> String {
    let roots = SCAN_ROOTS.join(", ");
    let files = scanned_files
        .iter()
        .map(|p| format!("  - {}", rel_from_manifest(p)))
        .collect::<Vec<_>>()
        .join("\n");
    let allowlist = ALLOWLISTED_STATUS_FILES
        .iter()
        .map(|p| format!("  - {}", p))
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "scan_roots: [{}]\nscanned_files ({}):\n{}\nallowlist ({}):\n{}",
        roots,
        scanned_files.len(),
        files,
        ALLOWLISTED_STATUS_FILES.len(),
        allowlist
    )
}

#[test]
fn test_server_grpc_impl_scan_coverage() {
    let scanned_files = grpc_server_impl_files();
    let scanned_rel = scanned_files.iter().map(|p| rel_from_manifest(p)).collect::<Vec<_>>();
    let report = coverage_report(&scanned_files);

    assert!(
        !scanned_files.is_empty(),
        "gRPC server implementation scan is empty\n{}",
        report
    );

    for required in REQUIRED_SCAN_FILES {
        assert!(
            scanned_rel.iter().any(|f| f == required),
            "required server gRPC implementation file not scanned: {}\n{}",
            required,
            report
        );
    }
}

#[test]
fn test_server_grpc_impls_have_no_status_business_error_shortcuts() {
    let mut violations = Vec::new();
    let scanned_files = grpc_server_impl_files();
    let report = coverage_report(&scanned_files);

    for path in scanned_files {
        let rel = rel_from_manifest(&path);
        if ALLOWLISTED_STATUS_FILES.iter().any(|allowed| rel == *allowed) {
            continue;
        }

        let content = fs::read_to_string(&path).expect("failed to read service source");
        for (line_no, line) in content.lines().enumerate() {
            let trimmed = line.trim_start();
            if trimmed.starts_with("//") {
                continue;
            }
            for pattern in FORBIDDEN_PATTERNS {
                if line.contains(pattern) {
                    violations.push(format!("{}:{} => {}", rel, line_no + 1, pattern));
                }
            }
        }
    }

    assert!(
        violations.is_empty(),
        "forbidden Status business-error patterns found:\n{}\n{}",
        violations.join("\n"),
        report
    );
}

mod rpc_header_invariant_tests {
    use super::*;

    #[test]
    fn refresh_metadata_header_carries_kind_recovery_and_hint() {
        let err = RpcErrorDetail::refresh_metadata(
            ErrorKind::Metadata(MetadataErrorKind::MountEpochMismatch),
            RefreshHint {
                group_name: Some("root".to_string()),
                mount_epoch: Some(7),
                ..Default::default()
            },
            "mount epoch mismatch",
        );
        let header = header_from_rpc_error(&None, Some(GroupName::parse("root").unwrap()), Some(7), &err);
        let detail = header.error.expect("refresh failure must carry header.error");
        let rpc_error = rpc_error_from_proto(&detail);

        assert_eq!(
            rpc_error.kind,
            ErrorKind::Metadata(MetadataErrorKind::MountEpochMismatch)
        );
        match rpc_error.recovery {
            RecoveryAction::RefreshMetadata { hint } => {
                assert_eq!(hint.group_name.as_deref(), Some("root"));
                assert_eq!(hint.mount_epoch, Some(7));
            }
            other => panic!("expected RefreshMetadata recovery, got {other:?}"),
        }
    }
}
