// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Capability flags for UFS backends.

use std::fmt;

/// Capability flags indicating what operations a UFS backend supports.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Capability {
    /// Backend supports native rename/move operation.
    pub supports_rename: bool,
    /// Backend supports recursive delete.
    pub supports_recursive_delete: bool,
    /// Backend supports directory operations (mkdir, list directories).
    pub supports_dir: bool,
    /// Rename fallback (copy + delete) is enabled.
    pub rename_fallback_enabled: bool,
}

impl Capability {
    /// Creates a new capability with conservative defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a capability for a filesystem backend (typically supports most operations).
    pub fn for_filesystem() -> Self {
        Self {
            supports_rename: true,
            supports_recursive_delete: true,
            supports_dir: true,
            rename_fallback_enabled: false,
        }
    }

    /// Creates a capability for an object storage backend (limited operations).
    pub fn for_object_storage() -> Self {
        Self {
            supports_rename: false,
            supports_recursive_delete: false,
            supports_dir: false,
            rename_fallback_enabled: false,
        }
    }

    /// Creates a capability for HDFS (supports most operations).
    pub fn for_hdfs() -> Self {
        Self {
            supports_rename: true,
            supports_recursive_delete: true,
            supports_dir: true,
            rename_fallback_enabled: false,
        }
    }

    /// Applies capability overrides from spec.
    pub fn with_overrides(mut self, overrides: &super::spec::CapabilityOverrides) -> Self {
        self.rename_fallback_enabled = overrides.rename_fallback_enabled;
        self
    }
}

impl fmt::Display for Capability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "rename={},recursive_delete={},dir={},fallback={}",
            self.supports_rename, self.supports_recursive_delete, self.supports_dir, self.rename_fallback_enabled
        )
    }
}
