// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output};
use std::thread;
use std::time::{Duration, Instant};

use tempfile::TempDir;

#[test]
fn help_and_version_do_not_require_an_installed_layout() {
    let help = run_cli(["--help"]);
    assert!(help.status.success(), "{}", String::from_utf8_lossy(&help.stderr));
    assert!(String::from_utf8_lossy(&help.stdout).contains("validate-conf"));

    let version = run_cli(["--version"]);
    assert!(version.status.success(), "{}", String::from_utf8_lossy(&version.stderr));
    let version = String::from_utf8_lossy(&version.stdout);
    for field in ["beryl 0.1.0-alpha.1", "source-revision:", "rustc:", "target:"] {
        assert!(version.contains(field), "missing {field}: {version}");
    }

    let version_command = run_cli(["version"]);
    assert!(version_command.status.success());
    assert_eq!(version_command.stdout, version.as_bytes());

    let missing_command = run_cli(std::iter::empty::<&str>());
    assert_eq!(missing_command.status.code(), Some(2));
}

#[test]
fn role_execution_uses_fixed_layout_arguments_and_preserves_pid() {
    let install = TestInstall::new();
    let pid_file = install.root.path().join("metadata.pid");
    let args_file = install.root.path().join("metadata.args");
    install.write_role(
        "beryl-metadata",
        r#"#!/bin/sh
printf '%s\n' "$$" > "$BERYL_TEST_PID_FILE"
printf '%s\n' "$@" > "$BERYL_TEST_ARGS_FILE"
"#,
    );
    let conf_dir = install.root.path().join("custom-conf");

    let mut child = Command::new(&install.cli)
        .arg("--conf-dir")
        .arg(&conf_dir)
        .arg("metadata")
        .env("BERYL_TEST_PID_FILE", &pid_file)
        .env("BERYL_TEST_ARGS_FILE", &args_file)
        .spawn()
        .unwrap();
    let launched_pid = child.id();
    let status = child.wait().unwrap();

    assert!(status.success());
    assert_eq!(fs::read_to_string(pid_file).unwrap().trim(), launched_pid.to_string());
    assert_eq!(
        fs::read_to_string(args_file).unwrap(),
        format!("start\n--config\n{}\n", conf_dir.join("metadata.yaml").display())
    );
}

#[test]
fn aggregate_validation_runs_both_roles_before_failing() {
    let install = TestInstall::new();
    let metadata_marker = install.root.path().join("metadata.checked");
    let worker_marker = install.root.path().join("worker.checked");
    install.write_role(
        "beryl-metadata",
        r#"#!/bin/sh
touch "$BERYL_TEST_METADATA_MARKER"
exit 7
"#,
    );
    install.write_role(
        "beryl-worker",
        r#"#!/bin/sh
touch "$BERYL_TEST_WORKER_MARKER"
"#,
    );

    let output = Command::new(&install.cli)
        .arg("validate-conf")
        .env("BERYL_TEST_METADATA_MARKER", &metadata_marker)
        .env("BERYL_TEST_WORKER_MARKER", &worker_marker)
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(1));
    assert!(metadata_marker.exists());
    assert!(
        worker_marker.exists(),
        "Worker validation was incorrectly short-circuited"
    );
}

#[test]
fn sigterm_reaches_the_execed_role_with_the_original_pid() {
    let install = TestInstall::new();
    let ready_file = install.root.path().join("worker.ready");
    install.write_role(
        "beryl-worker",
        r#"#!/bin/sh
trap 'exit 0' TERM INT
printf '%s\n' "$$" > "$BERYL_TEST_READY_FILE"
while true; do
  sleep 1
done
"#,
    );

    let mut child = Command::new(&install.cli)
        .arg("worker")
        .env("BERYL_TEST_READY_FILE", &ready_file)
        .spawn()
        .unwrap();
    wait_for_file(&mut child, &ready_file);
    let launched_pid = child.id();
    assert_eq!(
        fs::read_to_string(&ready_file).unwrap().trim(),
        launched_pid.to_string()
    );

    let kill_result = unsafe { libc::kill(launched_pid as i32, libc::SIGTERM) };
    assert_eq!(kill_result, 0);
    let status = wait_for_exit(&mut child);
    assert!(status.success(), "role did not handle SIGTERM successfully: {status}");
}

fn run_cli<I, S>(args: I) -> Output
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    Command::new(env!("CARGO_BIN_EXE_beryl")).args(args).output().unwrap()
}

struct TestInstall {
    root: TempDir,
    cli: PathBuf,
}

impl TestInstall {
    fn new() -> Self {
        let root = TempDir::new().unwrap();
        let bin_dir = root.path().join("bin");
        fs::create_dir_all(&bin_dir).unwrap();
        fs::create_dir(root.path().join("libexec")).unwrap();
        fs::create_dir(root.path().join("conf")).unwrap();
        let cli = bin_dir.join("beryl");
        fs::copy(env!("CARGO_BIN_EXE_beryl"), &cli).unwrap();
        make_executable(&cli);
        Self { root, cli }
    }

    fn write_role(&self, name: &str, script: &str) {
        let path = self.root.path().join("libexec").join(name);
        fs::write(&path, script).unwrap();
        make_executable(&path);
    }
}

fn make_executable(path: &Path) {
    let mut permissions = fs::metadata(path).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).unwrap();
}

fn wait_for_file(child: &mut Child, path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        assert!(child.try_wait().unwrap().is_none(), "role exited before becoming ready");
        if path.exists() {
            return;
        }
        thread::sleep(Duration::from_millis(10));
    }
    let _ = child.kill();
    let _ = child.wait();
    panic!("role did not become ready before timeout");
}

fn wait_for_exit(child: &mut Child) -> std::process::ExitStatus {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        thread::sleep(Duration::from_millis(10));
    }
    let _ = child.kill();
    let status = child.wait().unwrap();
    panic!("role did not exit before timeout: {status}");
}
