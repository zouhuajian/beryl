// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! UFS registry for dynamic instance management.

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{info, warn};

use crate::error::UfsError;
use crate::opendal_impl::OpendalUfs;
use crate::spec::{UfsId, UfsSpec};
use crate::traits::UfsAccess;

/// Registry for managing UFS instances dynamically.
///
/// # Concurrency Semantics
///
/// The registry uses `RwLock` to allow concurrent reads and exclusive writes:
///
/// - `get()`: Acquires a read lock, clones an `Arc<dyn UfsAccess>`, and releases the lock.
///   The returned `Arc` is a snapshot that remains valid even if the registry is updated
///   later. This ensures that ongoing operations are not affected by registry updates.
///
/// - `upsert()` / `remove()` / `apply()`: Acquire a write lock, perform the update atomically,
///   and release the lock. These operations are serialized but do not block readers that
///   have already obtained an `Arc` snapshot.
///
/// # Example
///
/// ```ignore
/// use ufs::{UfsRegistry, UfsSpec, BackendKind, BackendConfig};
///
/// let registry = UfsRegistry::new();
///
/// // Add a new UFS instance
/// let spec = UfsSpec::new("s3-1", BackendKind::S3, BackendConfig::S3(...));
/// registry.upsert(spec)?;
///
/// // Get an instance (returns Arc snapshot)
/// if let Some(ufs) = registry.get("s3-1") {
///     // This Arc remains valid even if registry is updated
///     let data = ufs.read_all("path/to/file").await?;
/// }
/// ```
pub struct UfsRegistry {
    /// Map of UFS ID to UFS instance.
    ///
    /// Uses `Arc` to allow sharing instances across threads and to provide
    /// snapshot semantics for concurrent operations.
    instances: RwLock<HashMap<UfsId, Arc<dyn UfsAccess>>>,
}

impl UfsRegistry {
    /// Creates a new empty registry.
    pub fn new() -> Self {
        Self {
            instances: RwLock::new(HashMap::new()),
        }
    }

    /// Creates a registry from a list of specifications.
    ///
    /// This is useful for initialization from configuration or master-provided specs.
    pub fn from_specs(specs: Vec<UfsSpec>) -> Result<Self, UfsError> {
        let registry = Self::new();
        for spec in specs {
            registry.upsert(spec)?;
        }
        Ok(registry)
    }

    /// Upserts (inserts or updates) a UFS instance.
    ///
    /// If an instance with the same ID exists, it is replaced with the new one.
    /// Existing `Arc` references to the old instance remain valid until dropped.
    ///
    /// Returns `true` if an existing instance was replaced, `false` if it's a new instance.
    pub fn upsert(&self, spec: UfsSpec) -> Result<bool, UfsError> {
        let ufs = Arc::new(OpendalUfs::from_spec(&spec)?);
        let id = spec.id.clone();

        let mut instances = self.instances.write();
        let replaced = instances.contains_key(&id);
        instances.insert(id.clone(), ufs);

        if replaced {
            info!(ufs_id = %id, "updated UFS instance");
        } else {
            info!(ufs_id = %id, "added new UFS instance");
        }

        Ok(replaced)
    }

    /// Removes a UFS instance by ID.
    ///
    /// Returns `true` if the instance existed and was removed, `false` otherwise.
    /// Existing `Arc` references remain valid until dropped.
    pub fn remove(&self, id: &UfsId) -> bool {
        let mut instances = self.instances.write();
        let removed = instances.remove(id).is_some();

        if removed {
            info!(ufs_id = %id, "removed UFS instance");
        } else {
            warn!(ufs_id = %id, "attempted to remove non-existent UFS instance");
        }

        removed
    }

    /// Applies a full set of specifications, replacing all existing instances.
    ///
    /// This operation is atomic: the entire registry is replaced in one write lock acquisition.
    /// Useful for bulk updates from master or configuration reload.
    ///
    /// # Implementation Note
    ///
    /// This builds all new instances first, then atomically replaces the entire map.
    /// If any spec is invalid, the operation fails and the registry remains unchanged.
    pub fn apply(&self, specs: Vec<UfsSpec>) -> Result<(), UfsError> {
        // Build all instances first (fail fast if any spec is invalid)
        let mut new_instances: HashMap<UfsId, Arc<dyn UfsAccess>> = HashMap::new();
        for spec in specs {
            let ufs: Arc<dyn UfsAccess> = Arc::new(OpendalUfs::from_spec(&spec)?);
            let id = spec.id.clone();
            new_instances.insert(id, ufs);
        }

        // Atomically replace the entire registry
        let mut instances = self.instances.write();
        let old_count = instances.len();
        *instances = new_instances;
        let new_count = instances.len();

        info!(
            old_count = old_count,
            new_count = new_count,
            "applied full registry update"
        );

        Ok(())
    }

    /// Gets a UFS instance by ID.
    ///
    /// Returns an `Arc` snapshot that remains valid even if the registry is updated.
    /// This allows ongoing operations to complete without being affected by registry changes.
    ///
    /// # Concurrency
    ///
    /// This method acquires a read lock, clones the `Arc`, and releases the lock immediately.
    /// The returned `Arc` is independent of the registry's internal state.
    pub fn get(&self, id: &UfsId) -> Option<Arc<dyn UfsAccess>> {
        let instances = self.instances.read();
        instances.get(id).map(|arc| Arc::clone(arc))
    }

    /// Lists all registered UFS IDs.
    pub fn list_ids(&self) -> Vec<UfsId> {
        let instances = self.instances.read();
        instances.keys().cloned().collect()
    }

    /// Gets the number of registered instances.
    pub fn len(&self) -> usize {
        let instances = self.instances.read();
        instances.len()
    }

    /// Checks if the registry is empty.
    pub fn is_empty(&self) -> bool {
        let instances = self.instances.read();
        instances.is_empty()
    }
}

impl Default for UfsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{BackendConfig, BackendKind, FsConfig};

    #[test]
    fn test_registry_upsert_and_get() {
        let registry = UfsRegistry::new();

        let spec = UfsSpec::new(
            "test-fs",
            BackendKind::Fs,
            BackendConfig::Fs(FsConfig {
                root: "/tmp".to_string(),
            }),
        );

        assert!(!registry.upsert(spec).unwrap());
        assert!(registry.get(&UfsId::new("test-fs")).is_some());
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn test_registry_remove() {
        let registry = UfsRegistry::new();

        let spec = UfsSpec::new(
            "test-fs",
            BackendKind::Fs,
            BackendConfig::Fs(FsConfig {
                root: "/tmp".to_string(),
            }),
        );

        registry.upsert(spec).unwrap();
        assert!(registry.remove(&UfsId::new("test-fs")));
        assert!(registry.get(&UfsId::new("test-fs")).is_none());
    }

    #[test]
    #[ignore = "requires filesystem permissions not available in default test env"]
    fn test_registry_apply() {
        let registry = UfsRegistry::new();

        let spec1 = UfsSpec::new(
            "fs1",
            BackendKind::Fs,
            BackendConfig::Fs(FsConfig {
                root: "/tmp1".to_string(),
            }),
        );
        let spec2 = UfsSpec::new(
            "fs2",
            BackendKind::Fs,
            BackendConfig::Fs(FsConfig {
                root: "/tmp2".to_string(),
            }),
        );

        registry.upsert(spec1).unwrap();
        assert_eq!(registry.len(), 1);

        registry.apply(vec![spec2]).unwrap();
        assert_eq!(registry.len(), 1);
        assert!(registry.get(&UfsId::new("fs1")).is_none());
        assert!(registry.get(&UfsId::new("fs2")).is_some());
    }
}
