// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Immutable identity embedded in every production binary from one build.

/// Build fields that must agree across the public CLI and both process roles.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BuildInfo {
    /// Lockstep Cargo package version.
    pub package_version: &'static str,
    /// Full source revision injected by the release build.
    pub source_revision: &'static str,
    /// Compiler version used for the artifact.
    pub rustc_version: &'static str,
    /// Rust target triple used for the artifact.
    pub target: &'static str,
}

impl BuildInfo {
    /// Renders the fields clap appends after its automatically printed binary name.
    pub fn version_details(self) -> String {
        format!(
            "{}\nsource-revision: {}\nrustc: {}\ntarget: {}",
            self.package_version, self.source_revision, self.rustc_version, self.target
        )
    }

    /// Renders the stable human-readable version contract for one binary.
    pub fn version_text(self, binary_name: &str) -> String {
        format!("{binary_name} {}", self.version_details())
    }
}

/// Identity compiled once in `beryl-common` and reused by every binary.
pub const BUILD_INFO: BuildInfo = BuildInfo {
    package_version: env!("CARGO_PKG_VERSION"),
    source_revision: env!("BERYL_BUILD_SOURCE_REVISION"),
    rustc_version: env!("BERYL_BUILD_RUSTC_VERSION"),
    target: env!("BERYL_BUILD_TARGET"),
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_text_contains_every_build_identity_field() {
        let text = BUILD_INFO.version_text("beryl-test");

        assert!(text.starts_with(&format!("beryl-test {}\n", env!("CARGO_PKG_VERSION"))));
        assert!(text.contains("\nsource-revision: "));
        assert!(text.contains("\nrustc: rustc "));
        assert!(text.contains("\ntarget: "));
    }
}
