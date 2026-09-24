// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Strict snapshot v1 framing for replicated metadata column families.

use super::super::{ROCKSDB_SCHEMA_VERSION, STATE_CFS};
use beryl_types::{GroupName, RaftLogId};
use sha2::{Digest, Sha256};
use std::io::{self, Write};
use thiserror::Error;

const MAGIC: &[u8; 4] = b"BRYL";
const SNAPSHOT_FORMAT_VERSION: u16 = 1;
const TAG_CF_START: u8 = 1;
const TAG_KV: u8 = 2;
const TAG_CF_END: u8 = 3;
const TAG_TRAILER: u8 = 4;
const TAG_END: u8 = 0xff;

const MAX_KEY_BYTES: usize = 1024 * 1024;
const MAX_RECORD_BYTES: u64 = 64 * 1024 * 1024;
const MAX_RECORDS_PER_CF: u64 = 10_000_000;
const MAX_TOTAL_UNCOMPRESSED_BYTES: u64 = 64 * 1024 * 1024 * 1024;
const RESERVED_META_KEYS: &[&[u8]] = &[b"rocksdb_schema_version", b"storage_identity"];

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SnapshotIdentity {
    pub(crate) group_name: GroupName,
    pub(crate) last_applied_log_id: Option<RaftLogId>,
}

impl SnapshotIdentity {
    pub(crate) fn current(group_name: GroupName, last_applied_log_id: Option<RaftLogId>) -> Self {
        Self {
            group_name,
            last_applied_log_id,
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum SnapshotCodecError {
    #[error("snapshot IO failed: {0}")]
    Io(#[from] io::Error),
    #[error("invalid snapshot: {0}")]
    Invalid(String),
}

fn invalid(message: impl Into<String>) -> SnapshotCodecError {
    SnapshotCodecError::Invalid(message.into())
}

fn checked_total(current: u64, added: u64, maximum: u64, label: &str) -> Result<u64, SnapshotCodecError> {
    let next = current + added;
    if next > maximum {
        return Err(invalid(format!("{label} exceeds limit {maximum}")));
    }
    Ok(next)
}

pub(crate) fn is_node_local_meta_key(cf_name: &str, key: &[u8]) -> bool {
    cf_name == "meta" && RESERVED_META_KEYS.contains(&key)
}

/// Snapshot v1 framing for the fixed column-family traversal in `write_snapshot`.
/// Persisted records retain size and count limits independent of that traversal.
pub(crate) struct SnapshotWriter<W> {
    writer: W,
    hasher: Sha256,
    cf_records: u64,
    total_records: u64,
    total_bytes: u64,
}

impl<W: Write> SnapshotWriter<W> {
    pub(crate) fn new(mut writer: W, identity: &SnapshotIdentity) -> Result<Self, SnapshotCodecError> {
        let group = identity.group_name.as_str().as_bytes();
        let mut hasher = Sha256::new();
        write_hashed(&mut writer, &mut hasher, MAGIC)?;
        write_hashed(&mut writer, &mut hasher, &SNAPSHOT_FORMAT_VERSION.to_be_bytes())?;
        write_hashed(&mut writer, &mut hasher, &ROCKSDB_SCHEMA_VERSION.to_be_bytes())?;
        write_hashed(&mut writer, &mut hasher, &(group.len() as u16).to_be_bytes())?;
        write_hashed(&mut writer, &mut hasher, group)?;
        match identity.last_applied_log_id {
            Some(log_id) => {
                write_hashed(&mut writer, &mut hasher, &[1])?;
                write_hashed(&mut writer, &mut hasher, &log_id.term.to_be_bytes())?;
                write_hashed(&mut writer, &mut hasher, &log_id.leader_node_id.to_be_bytes())?;
                write_hashed(&mut writer, &mut hasher, &log_id.index.to_be_bytes())?;
            }
            None => write_hashed(&mut writer, &mut hasher, &[0])?,
        }
        write_hashed(&mut writer, &mut hasher, &(STATE_CFS.len() as u16).to_be_bytes())?;

        Ok(Self {
            writer,
            hasher,
            cf_records: 0,
            total_records: 0,
            total_bytes: 0,
        })
    }

    pub(crate) fn start_column_family(&mut self, name: &str) -> Result<(), SnapshotCodecError> {
        self.write_hashed(&[TAG_CF_START])?;
        self.write_hashed(&(name.len() as u16).to_be_bytes())?;
        self.write_hashed(name.as_bytes())?;
        self.cf_records = 0;
        Ok(())
    }

    pub(crate) fn write_record(&mut self, key: &[u8], value: &[u8]) -> Result<(), SnapshotCodecError> {
        if key.len() > MAX_KEY_BYTES {
            return Err(invalid(format!(
                "key length {} exceeds limit {MAX_KEY_BYTES}",
                key.len()
            )));
        }
        let record_bytes = (key.len() as u64) + (value.len() as u64);
        if record_bytes > MAX_RECORD_BYTES {
            return Err(invalid(format!(
                "record byte count {record_bytes} exceeds limit {MAX_RECORD_BYTES}"
            )));
        }
        let next_cf_records = checked_total(self.cf_records, 1, MAX_RECORDS_PER_CF, "column-family record count")?;
        self.total_records += 1;
        self.total_bytes = checked_total(
            self.total_bytes,
            record_bytes,
            MAX_TOTAL_UNCOMPRESSED_BYTES,
            "total uncompressed byte count",
        )?;
        self.cf_records = next_cf_records;
        self.write_hashed(&[TAG_KV])?;
        self.write_hashed(&(key.len() as u32).to_be_bytes())?;
        self.write_hashed(&(value.len() as u32).to_be_bytes())?;
        self.write_hashed(key)?;
        self.write_hashed(value)
    }

    pub(crate) fn end_column_family(&mut self) -> Result<(), SnapshotCodecError> {
        self.write_hashed(&[TAG_CF_END])?;
        self.write_hashed(&self.cf_records.to_be_bytes())
    }

    pub(crate) fn finish(mut self) -> Result<W, SnapshotCodecError> {
        self.write_hashed(&[TAG_TRAILER])?;
        self.write_hashed(&self.total_records.to_be_bytes())?;
        self.write_hashed(&self.total_bytes.to_be_bytes())?;
        let checksum = self.hasher.finalize();
        self.writer.write_all(&checksum)?;
        self.writer.write_all(&[TAG_END])?;
        self.writer.flush()?;
        Ok(self.writer)
    }

    fn write_hashed(&mut self, bytes: &[u8]) -> Result<(), SnapshotCodecError> {
        write_hashed(&mut self.writer, &mut self.hasher, bytes)
    }
}

fn write_hashed(writer: &mut impl Write, hasher: &mut Sha256, bytes: &[u8]) -> Result<(), SnapshotCodecError> {
    writer.write_all(bytes)?;
    hasher.update(bytes);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_writer_bounds_persisted_records() {
        let identity = SnapshotIdentity::current(GroupName::parse("root").unwrap(), None);
        let mut writer = SnapshotWriter::new(std::io::sink(), &identity).unwrap();
        writer.start_column_family("inodes").unwrap();
        let key = vec![0; MAX_KEY_BYTES + 1];
        assert!(writer.write_record(&key, b"value").is_err());
        let value = vec![0; MAX_RECORD_BYTES as usize];
        assert!(writer.write_record(b"key", &value).is_err());
    }
}
