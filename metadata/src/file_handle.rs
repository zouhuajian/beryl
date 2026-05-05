// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! File handle management for metadata service.
//!
//! This module provides proper file handle management, tracking open files
//! and their associated metadata.

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use types::ids::{ClientId, DataHandleId};

/// File handle identifier.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct FileHandle(u64);

impl FileHandle {
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn as_raw(&self) -> u64 {
        self.0
    }
}

/// Open file entry.
pub struct OpenFileEntry {
    pub handle: FileHandle,
    pub data_handle_id: DataHandleId,
    pub client_id: ClientId,
    pub opened_at: u64, // Unix timestamp in milliseconds
}

/// File handle manager.
pub struct FileHandleManager {
    /// Map from handle to open file entry.
    handles: Arc<RwLock<HashMap<FileHandle, OpenFileEntry>>>,
    /// Map from data_handle_id to handles (for tracking multiple opens of same file).
    file_to_handles: Arc<RwLock<HashMap<DataHandleId, Vec<FileHandle>>>>,
    /// Next handle ID.
    next_handle_id: Arc<RwLock<u64>>,
}

impl FileHandleManager {
    pub fn new() -> Self {
        Self {
            handles: Arc::new(RwLock::new(HashMap::new())),
            file_to_handles: Arc::new(RwLock::new(HashMap::new())),
            next_handle_id: Arc::new(RwLock::new(1)),
        }
    }

    /// Open a file and return a handle.
    pub fn open_file(&self, data_handle_id: DataHandleId, client_id: ClientId) -> FileHandle {
        let mut next_id = self.next_handle_id.write();
        let handle = FileHandle::new(*next_id);
        *next_id += 1;

        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        let entry = OpenFileEntry {
            handle,
            data_handle_id,
            client_id,
            opened_at: now,
        };

        // Add to handles map
        self.handles.write().insert(handle, entry);

        // Add to file_to_handles map
        let mut file_map = self.file_to_handles.write();
        file_map.entry(data_handle_id).or_default().push(handle);

        handle
    }

    /// Close a file handle.
    pub fn close_file(&self, handle: FileHandle) -> Result<(), String> {
        let mut handles = self.handles.write();
        let entry = handles
            .remove(&handle)
            .ok_or_else(|| format!("File handle not found: {}", handle.as_raw()))?;

        // Remove from file_to_handles map
        let mut file_map = self.file_to_handles.write();
        if let Some(handles_for_file) = file_map.get_mut(&entry.data_handle_id) {
            handles_for_file.retain(|&h| h != handle);
            if handles_for_file.is_empty() {
                file_map.remove(&entry.data_handle_id);
            }
        }

        Ok(())
    }

    /// Get file ID from handle.
    pub fn get_data_handle_id(&self, handle: FileHandle) -> Option<DataHandleId> {
        self.handles.read().get(&handle).map(|e| e.data_handle_id)
    }

    /// Check if a handle is valid.
    pub fn is_valid(&self, handle: FileHandle) -> bool {
        self.handles.read().contains_key(&handle)
    }

    /// Get all open handles for a file.
    pub fn get_handles_for_file(&self, data_handle_id: DataHandleId) -> Vec<FileHandle> {
        self.file_to_handles
            .read()
            .get(&data_handle_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Get number of open handles.
    pub fn open_handle_count(&self) -> usize {
        self.handles.read().len()
    }

    /// Clean up handles for a specific client (e.g., on client disconnect).
    pub fn cleanup_client_handles(&self, client_id: ClientId) -> usize {
        let mut handles = self.handles.write();
        let mut file_map = self.file_to_handles.write();

        let handles_to_remove: Vec<FileHandle> = handles
            .iter()
            .filter(|(_, entry)| entry.client_id == client_id)
            .map(|(handle, _)| *handle)
            .collect();

        let mut removed = 0;
        for handle in handles_to_remove {
            if let Some(entry) = handles.remove(&handle) {
                removed += 1;
                if let Some(handles_for_file) = file_map.get_mut(&entry.data_handle_id) {
                    handles_for_file.retain(|&h| h != handle);
                    if handles_for_file.is_empty() {
                        file_map.remove(&entry.data_handle_id);
                    }
                }
            }
        }

        removed
    }
}

impl Default for FileHandleManager {
    fn default() -> Self {
        Self::new()
    }
}
