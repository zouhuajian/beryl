// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Service-layer error contract guardrails.

use common::error::canonical::{CanonicalError, RefreshReason};
use common::header::RpcErrorCode;
use metadata::service::header_from_canonical_error;
use proto::common::{ErrorClassProto, RefreshReasonProto};
use std::fs;
use std::path::{Path, PathBuf};

const SCAN_ROOTS: &[&str] = &["src/service", "src/worker", "../worker/src"];
const REQUIRED_SCAN_FILES: &[&str] = &[
    "src/service/path_service.rs",
    "src/worker/service.rs",
    "../worker/src/net/server/grpc.rs",
];
const ALLOWLISTED_STATUS_FILES: &[&str] = &["src/worker/service.rs", "../worker/src/net/server/grpc.rs"];
const SERVER_IMPL_MARKERS: &[&str] = &[
    "impl FileSystemServiceProto for",
    "impl MetadataWorkerServiceProto for",
    "impl WorkerDataService for",
];
const FORBIDDEN_PATTERNS: &[&str] = &["Status::from_error", "return Err(Status", "Err(Status"];
const FS_HANDLER_FILES: &[&str] = &["src/service/path_service.rs"];
const FS_RPC_APPLICATION_ALLOWLIST: &[&str] = &[];
const FORBIDDEN_FS_APPLICATION_PATTERN: &str = "RpcErrorCode::Application";

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

fn fs_handler_application_report(findings: &[String]) -> String {
    let allowlist = FS_RPC_APPLICATION_ALLOWLIST
        .iter()
        .map(|p| format!("  - {}", p))
        .collect::<Vec<_>>()
        .join("\n");
    let files = FS_HANDLER_FILES
        .iter()
        .map(|p| format!("  - {}", p))
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "fs_handler_files:\n{}\napplication_allowlist ({}):\n{}\nfindings:\n{}",
        files,
        FS_RPC_APPLICATION_ALLOWLIST.len(),
        allowlist,
        findings.join("\n")
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

#[test]
fn test_fs_handlers_do_not_emit_rpc_application() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut findings = Vec::new();

    for rel in FS_HANDLER_FILES {
        if FS_RPC_APPLICATION_ALLOWLIST.contains(rel) {
            continue;
        }
        let path = manifest_dir.join(rel);
        let content = fs::read_to_string(&path).unwrap_or_else(|e| panic!("failed to read {}: {}", path.display(), e));
        for (line_no, line) in content.lines().enumerate() {
            let trimmed = line.trim_start();
            if trimmed.starts_with("//") {
                continue;
            }
            if line.contains(FORBIDDEN_FS_APPLICATION_PATTERN) {
                findings.push(format!(
                    "{}:{} => {}",
                    rel,
                    line_no + 1,
                    FORBIDDEN_FS_APPLICATION_PATTERN
                ));
            }
        }
    }

    assert!(
        findings.is_empty(),
        "filesystem handlers must not emit RpcCode::Application; use errno mapping via to_canonical_fs()\n{}",
        fs_handler_application_report(&findings)
    );
}

mod canonical_header_invariant_tests {
    use super::*;

    #[test]
    fn ok_header_has_no_error() {
        let ok = CanonicalError::ok("success");
        let header = header_from_canonical_error(&None, Some(1), Some(7), &ok);

        assert!(header.error.is_none(), "Ok must not carry header.error");
    }

    #[test]
    fn need_refresh_header_must_include_specific_refresh_reason() {
        let err = CanonicalError::need_refresh(
            RpcErrorCode::MountEpochMismatch,
            RefreshReason::MountEpochMismatch,
            "mount epoch mismatch",
        );
        let header = header_from_canonical_error(&None, Some(1), Some(7), &err);
        let detail = header.error.expect("NeedRefresh must carry header.error");

        assert_eq!(detail.error_class, ErrorClassProto::ErrorClassNeedRefresh as i32);
        assert_eq!(
            detail.refresh_reason,
            RefreshReasonProto::RefreshReasonMountEpochMismatch as i32
        );
        assert_ne!(
            detail.refresh_reason,
            RefreshReasonProto::RefreshReasonUnknown as i32,
            "NeedRefresh must include a specific refresh reason"
        );
    }
}
