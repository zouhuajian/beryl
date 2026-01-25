// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! VolumeManager: Manages multiple storage directories (volumes).

use parking_lot::RwLock;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{error, info};
use types::ids::ShardGroupId;

/// Volume state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VolumeState {
    /// Healthy and ready to serve.
    Healthy,
    /// Read-only (e.g., disk full).
    ReadOnly,
    /// Degraded (some errors, but still usable).
    Degraded,
    /// Failed (unusable).
    Failed,
    /// Recovering (e.g., after failure).
    Recovering,
}

/// Volume information.
#[derive(Clone, Debug)]
pub struct VolumeInfo {
    /// Volume path.
    pub path: PathBuf,
    /// Current state.
    pub state: VolumeState,
    /// Total capacity in bytes.
    pub total_bytes: u64,
    /// Used capacity in bytes.
    pub used_bytes: u64,
    /// Available capacity in bytes.
    pub available_bytes: u64,
}

impl VolumeInfo {
    /// Calculate usage percentage.
    pub fn usage_percent(&self) -> f64 {
        if self.total_bytes == 0 {
            0.0
        } else {
            (self.used_bytes as f64 / self.total_bytes as f64) * 100.0
        }
    }
}

/// VolumeManager: manages multiple storage directories.
pub struct VolumeManager {
    volumes: Arc<RwLock<Vec<VolumeInfo>>>,
}

impl VolumeManager {
    /// Create a new VolumeManager.
    pub fn new() -> Self {
        Self {
            volumes: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Open volumes from storage directories.
    pub fn open_volumes(&self, storage_dirs: &[PathBuf]) -> Result<(), String> {
        let mut volumes = Vec::new();

        for dir in storage_dirs {
            match self.probe_volume(dir) {
                Ok(info) => {
                    info!(
                        path = %dir.display(),
                        state = ?info.state,
                        total_bytes = info.total_bytes,
                        available_bytes = info.available_bytes,
                        usage_percent = info.usage_percent(),
                        "Volume opened"
                    );
                    volumes.push(info);
                }
                Err(e) => {
                    error!(
                        path = %dir.display(),
                        error = %e,
                        "Failed to open volume"
                    );
                    // Continue with other volumes
                }
            }
        }

        if volumes.is_empty() {
            return Err("No volumes could be opened".to_string());
        }

        *self.volumes.write() = volumes;
        Ok(())
    }

    /// Probe a volume (check capacity, state, etc.).
    fn probe_volume(&self, path: &Path) -> Result<VolumeInfo, String> {
        // Ensure directory exists
        if !path.exists() {
            std::fs::create_dir_all(path).map_err(|e| format!("Failed to create volume directory: {}", e))?;
        }

        // Check if it's a directory
        let metadata = std::fs::metadata(path).map_err(|e| format!("Failed to access volume directory: {}", e))?;

        if !metadata.is_dir() {
            return Err(format!("Volume path is not a directory: {}", path.display()));
        }

        // Get filesystem capacity (using statfs equivalent)
        let (total_bytes, available_bytes, used_bytes) = self.get_capacity(path)?;

        // Determine initial state
        let state = if available_bytes == 0 {
            VolumeState::ReadOnly
        } else {
            VolumeState::Healthy
        };

        Ok(VolumeInfo {
            path: path.to_path_buf(),
            state,
            total_bytes,
            used_bytes,
            available_bytes,
        })
    }

    /// Get filesystem capacity (cross-platform using statfs/statvfs).
    fn get_capacity(&self, path: &Path) -> Result<(u64, u64, u64), String> {
        use std::ffi::CString;
        use std::os::raw::c_char;

        let path_str = path
            .to_str()
            .ok_or_else(|| format!("Path contains invalid UTF-8: {}", path.display()))?;
        let c_path = CString::new(path_str).map_err(|e| format!("Failed to convert path to CString: {}", e))?;

        #[cfg(any(target_os = "linux", target_os = "android"))]
        {
            use libc::{statfs, statfs as Statfs};
            let mut stat: Statfs = unsafe { std::mem::zeroed() };

            let result = unsafe { statfs(c_path.as_ptr() as *const c_char, &mut stat) };

            if result != 0 {
                return Err(format!("statfs failed: {}", std::io::Error::last_os_error()));
            }

            // statfs fields:
            // f_blocks: total blocks
            // f_bavail: available blocks for non-root
            // f_bfree: free blocks
            // f_frsize: fragment size (block size)
            let block_size = stat.f_frsize as u64;
            let total_blocks = stat.f_blocks as u64;
            let available_blocks = stat.f_bavail as u64;
            let free_blocks = stat.f_bfree as u64;

            let total_bytes = total_blocks * block_size;
            let available_bytes = available_blocks * block_size;
            let used_bytes = (total_blocks - free_blocks) * block_size;

            Ok((total_bytes, available_bytes, used_bytes))
        }

        #[cfg(target_os = "macos")]
        {
            use libc::{statfs, statfs as Statfs};
            let mut stat: Statfs = unsafe { std::mem::zeroed() };

            let result = unsafe { statfs(c_path.as_ptr() as *const c_char, &mut stat) };

            if result != 0 {
                return Err(format!("statfs failed: {}", std::io::Error::last_os_error()));
            }

            // macOS statfs fields:
            // f_blocks: total blocks
            // f_bavail: available blocks for non-root
            // f_bfree: free blocks
            // f_bsize: block size
            let block_size = stat.f_bsize as u64;
            let total_blocks = stat.f_blocks as u64;
            let available_blocks = stat.f_bavail as u64;
            let free_blocks = stat.f_bfree as u64;

            let total_bytes = total_blocks * block_size;
            let available_bytes = available_blocks * block_size;
            let used_bytes = (total_blocks - free_blocks) * block_size;

            Ok((total_bytes, available_bytes, used_bytes))
        }

        #[cfg(target_os = "windows")]
        {
            // On Windows, we would use GetDiskFreeSpaceExW
            // For now, return placeholder values
            // TODO: Implement GetDiskFreeSpaceExW when winapi is added as dependency
            use tracing::warn;
            warn!(
                path = %path.display(),
                "Windows filesystem capacity query not fully implemented, using placeholder"
            );
            let total = 100_000_000_000; // 100GB
            let available = 50_000_000_000; // 50GB
            let used = total - available;
            Ok((total, available, used))
        }

        #[cfg(target_os = "windows")]
        {
            // On Windows, use GetDiskFreeSpaceEx
            use std::os::windows::ffi::OsStrExt;
            use winapi::um::fileapi::GetDiskFreeSpaceExW;
            use winapi::um::winnt::ULARGE_INTEGER;

            let wide_path: Vec<u16> = path.as_os_str().encode_wide().chain(std::iter::once(0)).collect();

            let mut free_bytes: ULARGE_INTEGER = unsafe { std::mem::zeroed() };
            let mut total_bytes: ULARGE_INTEGER = unsafe { std::mem::zeroed() };

            let result = unsafe {
                GetDiskFreeSpaceExW(
                    wide_path.as_ptr(),
                    &mut free_bytes,
                    &mut total_bytes,
                    std::ptr::null_mut(),
                )
            };

            if result == 0 {
                return Err(format!(
                    "GetDiskFreeSpaceExW failed: {}",
                    std::io::Error::last_os_error()
                ));
            }

            // Calculate used bytes
            let total = total_bytes.QuadPart() as u64;
            let available = free_bytes.QuadPart() as u64;
            let used = total.saturating_sub(available);

            Ok((total, available, used))
        }

        #[cfg(not(any(
            target_os = "linux",
            target_os = "android",
            target_os = "macos",
            target_os = "windows"
        )))]
        {
            // Fallback: try to use statvfs if available
            use libc::{statvfs, statvfs as Statvfs};
            let mut stat: Statvfs = unsafe { std::mem::zeroed() };

            let result = unsafe { statvfs(c_path.as_ptr() as *const c_char, &mut stat) };

            if result == 0 {
                let block_size = stat.f_frsize as u64;
                let total_blocks = stat.f_blocks as u64;
                let available_blocks = stat.f_bavail as u64;
                let free_blocks = stat.f_bfree as u64;

                let total_bytes = total_blocks * block_size;
                let available_bytes = available_blocks * block_size;
                let used_bytes = (total_blocks - free_blocks) * block_size;

                Ok((total_bytes, available_bytes, used_bytes))
            } else {
                // Last resort: return placeholder
                warn!(
                    path = %path.display(),
                    "statvfs not available, using placeholder values"
                );
                let total = 100_000_000_000; // 100GB
                let available = 50_000_000_000; // 50GB
                let used = total - available;
                Ok((total, available, used))
            }
        }
    }

    /// Get all volumes.
    pub fn volumes(&self) -> Vec<VolumeInfo> {
        self.volumes.read().clone()
    }

    /// Get a volume by index.
    pub fn get_volume(&self, index: usize) -> Option<VolumeInfo> {
        self.volumes.read().get(index).cloned()
    }

    /// Select a volume for a group_id (round-robin or based on capacity).
    pub fn select_volume(&self, group_id: ShardGroupId) -> Option<VolumeInfo> {
        let volumes = self.volumes.read();
        if volumes.is_empty() {
            return None;
        }

        // Simple round-robin based on group_id
        let index = (group_id.as_raw() as usize) % volumes.len();
        volumes.get(index).cloned()
    }

    /// Update volume state.
    pub fn update_volume_state(&self, path: &Path, state: VolumeState) {
        let mut volumes = self.volumes.write();
        for vol in volumes.iter_mut() {
            if vol.path == path {
                vol.state = state;
                info!(
                    path = %path.display(),
                    state = ?state,
                    "Volume state updated"
                );
                break;
            }
        }
    }

    /// Refresh volume capacity.
    pub fn refresh_volume(&self, path: &Path) -> Result<(), String> {
        let mut volumes = self.volumes.write();
        for vol in volumes.iter_mut() {
            if vol.path == path {
                let (total, available, used) = self.get_capacity(path)?;
                vol.total_bytes = total;
                vol.available_bytes = available;
                vol.used_bytes = used;

                // Update state based on capacity
                if available == 0 {
                    vol.state = VolumeState::ReadOnly;
                } else if vol.state == VolumeState::ReadOnly && available > 0 {
                    vol.state = VolumeState::Healthy;
                }

                return Ok(());
            }
        }
        Err(format!("Volume not found: {}", path.display()))
    }

    /// Get total capacity across all volumes.
    pub fn total_capacity(&self) -> u64 {
        self.volumes.read().iter().map(|v| v.total_bytes).sum()
    }

    /// Get total available capacity across all volumes.
    pub fn total_available(&self) -> u64 {
        self.volumes.read().iter().map(|v| v.available_bytes).sum()
    }
}

impl Default for VolumeManager {
    fn default() -> Self {
        Self::new()
    }
}
