// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Strict snapshot v2 framing for replicated metadata column families.

use super::super::{ROCKSDB_SCHEMA_VERSION, STATE_CFS};
use beryl_types::{GroupName, RaftLogId};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::io::{self, Read, Write};
use thiserror::Error;

const MAGIC: &[u8; 4] = b"BRYL";
const SNAPSHOT_FORMAT_VERSION: u16 = 2;
const TAG_CF_START: u8 = 1;
const TAG_KV: u8 = 2;
const TAG_CF_END: u8 = 3;
const TAG_TRAILER: u8 = 4;
const TAG_END: u8 = 0xff;

const MAX_GROUP_NAME_BYTES: usize = 256;
const MAX_CF_NAME_BYTES: usize = 64;
const MAX_KEY_BYTES: usize = 1024 * 1024;
const MAX_VALUE_BYTES: usize = 64 * 1024 * 1024;
const MAX_RECORD_BYTES: u64 = 64 * 1024 * 1024;
const MAX_RECORDS_PER_CF: u64 = 10_000_000;
const MAX_TOTAL_RECORDS: u64 = 100_000_000;
const MAX_TOTAL_UNCOMPRESSED_BYTES: u64 = 64 * 1024 * 1024 * 1024;
const RESERVED_META_KEYS: &[&[u8]] = &[b"rocksdb_schema_version", b"storage_identity"];

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SnapshotIdentity {
    pub(crate) storage_schema_version: u64,
    pub(crate) group_name: GroupName,
    pub(crate) last_applied_log_id: Option<RaftLogId>,
}

impl SnapshotIdentity {
    pub(crate) fn current(group_name: GroupName, last_applied_log_id: Option<RaftLogId>) -> Self {
        Self {
            storage_schema_version: ROCKSDB_SCHEMA_VERSION,
            group_name,
            last_applied_log_id,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SnapshotSummary {
    pub(crate) total_records: u64,
    pub(crate) total_uncompressed_bytes: u64,
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

struct HashingReader<R> {
    inner: R,
    hasher: Sha256,
}

impl<R: Read> HashingReader<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            hasher: Sha256::new(),
        }
    }

    fn read_hashed(&mut self, bytes: &mut [u8]) -> Result<(), SnapshotCodecError> {
        read_exact(&mut self.inner, bytes)?;
        self.hasher.update(bytes);
        Ok(())
    }

    fn read_u8(&mut self) -> Result<u8, SnapshotCodecError> {
        let mut bytes = [0; 1];
        self.read_hashed(&mut bytes)?;
        Ok(bytes[0])
    }

    fn read_u16(&mut self) -> Result<u16, SnapshotCodecError> {
        let mut bytes = [0; 2];
        self.read_hashed(&mut bytes)?;
        Ok(u16::from_be_bytes(bytes))
    }

    fn read_u32(&mut self) -> Result<u32, SnapshotCodecError> {
        let mut bytes = [0; 4];
        self.read_hashed(&mut bytes)?;
        Ok(u32::from_be_bytes(bytes))
    }

    fn read_u64(&mut self) -> Result<u64, SnapshotCodecError> {
        let mut bytes = [0; 8];
        self.read_hashed(&mut bytes)?;
        Ok(u64::from_be_bytes(bytes))
    }
}

fn read_exact(reader: &mut impl Read, bytes: &mut [u8]) -> Result<(), SnapshotCodecError> {
    reader.read_exact(bytes).map_err(|error| {
        if error.kind() == io::ErrorKind::UnexpectedEof {
            invalid("premature EOF before snapshot END marker")
        } else {
            SnapshotCodecError::Io(error)
        }
    })
}

fn checked_total(current: u64, added: u64, maximum: u64, label: &str) -> Result<u64, SnapshotCodecError> {
    let next = current
        .checked_add(added)
        .ok_or_else(|| invalid(format!("{label} overflow")))?;
    if next > maximum {
        return Err(invalid(format!("{label} exceeds limit {maximum}")));
    }
    Ok(next)
}

pub(crate) fn is_node_local_meta_key(cf_name: &str, key: &[u8]) -> bool {
    cf_name == "meta" && RESERVED_META_KEYS.contains(&key)
}

fn validate_record_key(cf_name: &str, key: &[u8]) -> Result<(), SnapshotCodecError> {
    if is_node_local_meta_key(cf_name, key) {
        return Err(invalid(format!(
            "node-local meta key {:?} is forbidden in replicated snapshots",
            String::from_utf8_lossy(key)
        )));
    }
    Ok(())
}

fn read_bounded_bytes<R: Read>(
    reader: &mut HashingReader<R>,
    length: usize,
    maximum: usize,
    label: &str,
) -> Result<Vec<u8>, SnapshotCodecError> {
    if length > maximum {
        return Err(invalid(format!("{label} length {length} exceeds limit {maximum}")));
    }
    let mut bytes = vec![0; length];
    reader.read_hashed(&mut bytes)?;
    Ok(bytes)
}

/// Decode and validate one snapshot while streaming records to `on_record`.
///
/// The callback may observe records before the final checksum is validated.
/// Callers must therefore write into a disposable staged generation and discard
/// it on every decoder error; callback effects are not assumed to be reversible.
pub(crate) fn decode_snapshot<R, F>(
    reader: R,
    expected: &SnapshotIdentity,
    mut on_record: F,
) -> Result<SnapshotSummary, SnapshotCodecError>
where
    R: Read,
    F: FnMut(&str, &[u8], &[u8]) -> Result<(), SnapshotCodecError>,
{
    let mut reader = HashingReader::new(reader);

    let mut magic = [0; MAGIC.len()];
    reader.read_hashed(&mut magic)?;
    if &magic != MAGIC {
        return Err(invalid("magic mismatch"));
    }
    let version = reader.read_u16()?;
    if version != SNAPSHOT_FORMAT_VERSION {
        return Err(invalid(format!(
            "unsupported snapshot format version {version}; expected {SNAPSHOT_FORMAT_VERSION}"
        )));
    }
    let schema = reader.read_u64()?;
    if schema != ROCKSDB_SCHEMA_VERSION || schema != expected.storage_schema_version {
        return Err(invalid(format!(
            "storage schema mismatch: snapshot={schema}, runtime={}, expected={}",
            ROCKSDB_SCHEMA_VERSION, expected.storage_schema_version
        )));
    }
    let group_length = reader.read_u16()? as usize;
    let group_bytes = read_bounded_bytes(&mut reader, group_length, MAX_GROUP_NAME_BYTES, "group name")?;
    let group_raw = std::str::from_utf8(&group_bytes).map_err(|_| invalid("group name is not UTF-8"))?;
    let group_name = GroupName::parse(group_raw).map_err(|error| invalid(format!("invalid group name: {error}")))?;
    if group_name != expected.group_name {
        return Err(invalid(format!(
            "group identity mismatch: snapshot={}, expected={}",
            group_name, expected.group_name
        )));
    }
    let last_applied_log_id = match reader.read_u8()? {
        0 => None,
        1 => Some(RaftLogId::new(
            reader.read_u64()?,
            reader.read_u64()?,
            reader.read_u64()?,
        )),
        value => return Err(invalid(format!("invalid last applied presence tag {value}"))),
    };
    if last_applied_log_id != expected.last_applied_log_id {
        return Err(invalid(format!(
            "last applied log id mismatch: snapshot={last_applied_log_id:?}, expected={:?}",
            expected.last_applied_log_id
        )));
    }
    let expected_cf_count = reader.read_u16()? as usize;
    if expected_cf_count != STATE_CFS.len() {
        return Err(invalid(format!(
            "column-family count mismatch: snapshot={expected_cf_count}, expected={}",
            STATE_CFS.len()
        )));
    }

    let expected_cfs: HashSet<&str> = STATE_CFS.iter().copied().collect();
    let mut seen = HashSet::with_capacity(STATE_CFS.len());
    let mut total_records = 0u64;
    let mut total_bytes = 0u64;
    loop {
        let tag = reader.read_u8()?;
        if tag == TAG_TRAILER {
            break;
        }
        if tag != TAG_CF_START {
            return Err(invalid(format!(
                "unknown snapshot tag {tag:#04x} outside column family"
            )));
        }

        let name_length = reader.read_u16()? as usize;
        let name_bytes = read_bounded_bytes(&mut reader, name_length, MAX_CF_NAME_BYTES, "column-family name")?;
        let name = std::str::from_utf8(&name_bytes).map_err(|_| invalid("column-family name is not UTF-8"))?;
        if !expected_cfs.contains(name) {
            return Err(invalid(format!("unknown column family {name:?}")));
        }
        if !seen.insert(name.to_string()) {
            return Err(invalid(format!("duplicate column family {name:?}")));
        }

        let mut cf_records = 0u64;
        loop {
            match reader.read_u8()? {
                TAG_KV => {
                    let key_length = reader.read_u32()? as usize;
                    let value_length = reader.read_u32()? as usize;
                    if key_length > MAX_KEY_BYTES {
                        return Err(invalid(format!(
                            "key length {key_length} exceeds limit {MAX_KEY_BYTES}"
                        )));
                    }
                    if value_length > MAX_VALUE_BYTES {
                        return Err(invalid(format!(
                            "value length {value_length} exceeds limit {MAX_VALUE_BYTES}"
                        )));
                    }
                    let record_bytes = (key_length as u64)
                        .checked_add(value_length as u64)
                        .ok_or_else(|| invalid("record byte count overflow"))?;
                    if record_bytes > MAX_RECORD_BYTES {
                        return Err(invalid(format!(
                            "record byte count {record_bytes} exceeds limit {MAX_RECORD_BYTES}"
                        )));
                    }
                    cf_records = checked_total(cf_records, 1, MAX_RECORDS_PER_CF, "column-family record count")?;
                    total_records = checked_total(total_records, 1, MAX_TOTAL_RECORDS, "total record count")?;
                    total_bytes = checked_total(
                        total_bytes,
                        record_bytes,
                        MAX_TOTAL_UNCOMPRESSED_BYTES,
                        "total uncompressed byte count",
                    )?;
                    let key = read_bounded_bytes(&mut reader, key_length, MAX_KEY_BYTES, "key")?;
                    let value = read_bounded_bytes(&mut reader, value_length, MAX_VALUE_BYTES, "value")?;
                    validate_record_key(name, &key)?;
                    on_record(name, &key, &value)?;
                }
                TAG_CF_END => {
                    let declared_records = reader.read_u64()?;
                    if declared_records > MAX_RECORDS_PER_CF {
                        return Err(invalid(format!(
                            "column-family record count {declared_records} exceeds limit {MAX_RECORDS_PER_CF}"
                        )));
                    }
                    if declared_records != cf_records {
                        return Err(invalid(format!(
                            "column-family record count mismatch for {name:?}: declared={declared_records}, actual={cf_records}"
                        )));
                    }
                    break;
                }
                tag => return Err(invalid(format!("unknown snapshot tag {tag:#04x} inside column family"))),
            }
        }
    }

    let missing = STATE_CFS
        .iter()
        .copied()
        .filter(|name| !seen.contains(*name))
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        return Err(invalid(format!("missing column families: {}", missing.join(", "))));
    }

    let declared_total_records = reader.read_u64()?;
    if declared_total_records > MAX_TOTAL_RECORDS {
        return Err(invalid(format!(
            "total record count {declared_total_records} exceeds limit {MAX_TOTAL_RECORDS}"
        )));
    }
    if declared_total_records != total_records {
        return Err(invalid(format!(
            "total record count mismatch: declared={declared_total_records}, actual={total_records}"
        )));
    }
    let declared_total_bytes = reader.read_u64()?;
    if declared_total_bytes > MAX_TOTAL_UNCOMPRESSED_BYTES {
        return Err(invalid(format!(
            "total uncompressed byte count {declared_total_bytes} exceeds limit {MAX_TOTAL_UNCOMPRESSED_BYTES}"
        )));
    }
    if declared_total_bytes != total_bytes {
        return Err(invalid(format!(
            "total uncompressed byte count mismatch: declared={declared_total_bytes}, actual={total_bytes}"
        )));
    }

    let calculated_checksum = reader.hasher.finalize();
    let mut stored_checksum = [0; 32];
    read_exact(&mut reader.inner, &mut stored_checksum)?;
    if calculated_checksum.as_slice() != stored_checksum {
        return Err(invalid("checksum mismatch"));
    }
    let mut end = [0; 1];
    read_exact(&mut reader.inner, &mut end)?;
    if end[0] != TAG_END {
        return Err(invalid(format!("missing explicit END marker; got {:#04x}", end[0])));
    }
    let mut trailing = [0; 1];
    match reader.inner.read(&mut trailing) {
        Ok(0) => {}
        Ok(_) => return Err(invalid("trailing bytes after snapshot END marker")),
        Err(error) => return Err(SnapshotCodecError::Io(error)),
    }

    Ok(SnapshotSummary {
        total_records,
        total_uncompressed_bytes: total_bytes,
    })
}

/// Streaming snapshot v2 writer with fixed format and resource limits.
pub(crate) struct SnapshotWriter<W> {
    writer: W,
    hasher: Sha256,
    seen: HashSet<String>,
    active_cf: Option<(String, u64)>,
    total_records: u64,
    total_bytes: u64,
}

impl<W: Write> SnapshotWriter<W> {
    pub(crate) fn new(mut writer: W, identity: &SnapshotIdentity) -> Result<Self, SnapshotCodecError> {
        let group = identity.group_name.as_str().as_bytes();
        if group.len() > MAX_GROUP_NAME_BYTES {
            return Err(invalid(format!(
                "group name length {} exceeds limit {MAX_GROUP_NAME_BYTES}",
                group.len()
            )));
        }
        if identity.storage_schema_version != ROCKSDB_SCHEMA_VERSION {
            return Err(invalid(format!(
                "storage schema mismatch: writer={}, runtime={ROCKSDB_SCHEMA_VERSION}",
                identity.storage_schema_version
            )));
        }

        let mut hasher = Sha256::new();
        write_hashed(&mut writer, &mut hasher, MAGIC)?;
        write_hashed(&mut writer, &mut hasher, &SNAPSHOT_FORMAT_VERSION.to_be_bytes())?;
        write_hashed(&mut writer, &mut hasher, &identity.storage_schema_version.to_be_bytes())?;
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
            seen: HashSet::with_capacity(STATE_CFS.len()),
            active_cf: None,
            total_records: 0,
            total_bytes: 0,
        })
    }

    pub(crate) fn start_column_family(&mut self, name: &str) -> Result<(), SnapshotCodecError> {
        if self.active_cf.is_some() {
            return Err(invalid("cannot start a column family before ending the current one"));
        }
        if !STATE_CFS.contains(&name) {
            return Err(invalid(format!("unknown column family {name:?}")));
        }
        if name.len() > MAX_CF_NAME_BYTES {
            return Err(invalid(format!(
                "column-family name length {} exceeds limit {MAX_CF_NAME_BYTES}",
                name.len()
            )));
        }
        if !self.seen.insert(name.to_string()) {
            return Err(invalid(format!("duplicate column family {name:?}")));
        }
        self.write_hashed(&[TAG_CF_START])?;
        self.write_hashed(&(name.len() as u16).to_be_bytes())?;
        self.write_hashed(name.as_bytes())?;
        self.active_cf = Some((name.to_string(), 0));
        Ok(())
    }

    pub(crate) fn write_record(&mut self, key: &[u8], value: &[u8]) -> Result<(), SnapshotCodecError> {
        let (cf_name, current_cf_records) = self
            .active_cf
            .as_ref()
            .map(|(name, records)| (name.clone(), *records))
            .ok_or_else(|| invalid("cannot write a record outside a column family"))?;
        if key.len() > MAX_KEY_BYTES {
            return Err(invalid(format!(
                "key length {} exceeds limit {MAX_KEY_BYTES}",
                key.len()
            )));
        }
        if value.len() > MAX_VALUE_BYTES {
            return Err(invalid(format!(
                "value length {} exceeds limit {MAX_VALUE_BYTES}",
                value.len()
            )));
        }
        validate_record_key(&cf_name, key)?;
        let record_bytes = (key.len() as u64)
            .checked_add(value.len() as u64)
            .ok_or_else(|| invalid("record byte count overflow"))?;
        if record_bytes > MAX_RECORD_BYTES {
            return Err(invalid(format!(
                "record byte count {record_bytes} exceeds limit {MAX_RECORD_BYTES}"
            )));
        }
        let next_cf_records = checked_total(current_cf_records, 1, MAX_RECORDS_PER_CF, "column-family record count")?;
        self.total_records = checked_total(self.total_records, 1, MAX_TOTAL_RECORDS, "total record count")?;
        self.total_bytes = checked_total(
            self.total_bytes,
            record_bytes,
            MAX_TOTAL_UNCOMPRESSED_BYTES,
            "total uncompressed byte count",
        )?;
        self.active_cf.as_mut().expect("checked above").1 = next_cf_records;
        self.write_hashed(&[TAG_KV])?;
        self.write_hashed(&(key.len() as u32).to_be_bytes())?;
        self.write_hashed(&(value.len() as u32).to_be_bytes())?;
        self.write_hashed(key)?;
        self.write_hashed(value)
    }

    pub(crate) fn end_column_family(&mut self) -> Result<(), SnapshotCodecError> {
        let (_, records) = self
            .active_cf
            .take()
            .ok_or_else(|| invalid("cannot end a column family when none is active"))?;
        self.write_hashed(&[TAG_CF_END])?;
        self.write_hashed(&records.to_be_bytes())
    }

    pub(crate) fn finish(mut self) -> Result<W, SnapshotCodecError> {
        if self.active_cf.is_some() {
            return Err(invalid("cannot finish snapshot with an active column family"));
        }
        let missing = STATE_CFS
            .iter()
            .copied()
            .filter(|name| !self.seen.contains(*name))
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            return Err(invalid(format!("missing column families: {}", missing.join(", "))));
        }
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

use super::super::*;

impl super::super::RocksDBStorage {
    /// Get current snapshot metadata.
    pub(crate) fn get_snapshot_meta(&self) -> MetadataResult<Option<Vec<u8>>> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf = db
            .cf_handle(super::super::CF_RAFT_SNAPSHOT)
            .ok_or_else(|| MetadataError::Internal("RaftSnapshot CF not found".to_string()))?;

        match db.get_cf(cf, b"snapshot_meta") {
            Ok(Some(value)) => Ok(Some(value)),
            Ok(None) => Ok(None),
            Err(e) => Err(MetadataError::Internal(format!("RocksDB error: {}", e))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[derive(Clone)]
    struct FixtureCf {
        name: String,
        records: Vec<(Vec<u8>, Vec<u8>)>,
        declared_records: Option<u64>,
    }

    fn identity() -> SnapshotIdentity {
        SnapshotIdentity::current(GroupName::parse("root").unwrap(), Some(RaftLogId::new(3, 7, 11)))
    }

    fn state_cfs() -> Vec<FixtureCf> {
        STATE_CFS
            .iter()
            .enumerate()
            .map(|(index, name)| FixtureCf {
                name: (*name).to_string(),
                records: if index == 0 {
                    vec![(b"key".to_vec(), b"value".to_vec())]
                } else {
                    Vec::new()
                },
                declared_records: None,
            })
            .collect()
    }

    fn push_hashed(bytes: &mut Vec<u8>, hasher: &mut Sha256, data: &[u8]) {
        bytes.extend_from_slice(data);
        hasher.update(data);
    }

    fn push_u16(bytes: &mut Vec<u8>, hasher: &mut Sha256, value: u16) {
        push_hashed(bytes, hasher, &value.to_be_bytes());
    }

    fn push_u32(bytes: &mut Vec<u8>, hasher: &mut Sha256, value: u32) {
        push_hashed(bytes, hasher, &value.to_be_bytes());
    }

    fn push_u64(bytes: &mut Vec<u8>, hasher: &mut Sha256, value: u64) {
        push_hashed(bytes, hasher, &value.to_be_bytes());
    }

    fn encode_fixture(
        identity: &SnapshotIdentity,
        cfs: &[FixtureCf],
        total_records_override: Option<u64>,
        total_bytes_override: Option<u64>,
    ) -> Vec<u8> {
        let mut bytes = Vec::new();
        let mut hasher = Sha256::new();
        push_hashed(&mut bytes, &mut hasher, MAGIC);
        push_u16(&mut bytes, &mut hasher, SNAPSHOT_FORMAT_VERSION);
        push_u64(&mut bytes, &mut hasher, identity.storage_schema_version);
        push_u16(&mut bytes, &mut hasher, identity.group_name.as_str().len() as u16);
        push_hashed(&mut bytes, &mut hasher, identity.group_name.as_str().as_bytes());
        match identity.last_applied_log_id {
            Some(log_id) => {
                push_hashed(&mut bytes, &mut hasher, &[1]);
                push_u64(&mut bytes, &mut hasher, log_id.term);
                push_u64(&mut bytes, &mut hasher, log_id.leader_node_id);
                push_u64(&mut bytes, &mut hasher, log_id.index);
            }
            None => push_hashed(&mut bytes, &mut hasher, &[0]),
        }
        push_u16(&mut bytes, &mut hasher, STATE_CFS.len() as u16);

        let mut total_records = 0u64;
        let mut total_bytes = 0u64;
        for cf in cfs {
            push_hashed(&mut bytes, &mut hasher, &[TAG_CF_START]);
            push_u16(&mut bytes, &mut hasher, cf.name.len() as u16);
            push_hashed(&mut bytes, &mut hasher, cf.name.as_bytes());
            for (key, value) in &cf.records {
                push_hashed(&mut bytes, &mut hasher, &[TAG_KV]);
                push_u32(&mut bytes, &mut hasher, key.len() as u32);
                push_u32(&mut bytes, &mut hasher, value.len() as u32);
                push_hashed(&mut bytes, &mut hasher, key);
                push_hashed(&mut bytes, &mut hasher, value);
                total_records += 1;
                total_bytes += (key.len() + value.len()) as u64;
            }
            push_hashed(&mut bytes, &mut hasher, &[TAG_CF_END]);
            push_u64(
                &mut bytes,
                &mut hasher,
                cf.declared_records.unwrap_or(cf.records.len() as u64),
            );
        }

        push_hashed(&mut bytes, &mut hasher, &[TAG_TRAILER]);
        push_u64(&mut bytes, &mut hasher, total_records_override.unwrap_or(total_records));
        push_u64(&mut bytes, &mut hasher, total_bytes_override.unwrap_or(total_bytes));
        bytes.extend_from_slice(&hasher.finalize());
        bytes.push(TAG_END);
        bytes
    }

    fn decode(bytes: &[u8], expected: &SnapshotIdentity) -> Result<SnapshotSummary, SnapshotCodecError> {
        decode_snapshot(Cursor::new(bytes), expected, |_cf, _key, _value| Ok(()))
    }

    #[test]
    fn valid_snapshot_streams_records_and_returns_totals() {
        let expected = identity();
        let bytes = encode_fixture(&expected, &state_cfs(), None, None);
        let mut records = Vec::new();

        let summary = decode_snapshot(Cursor::new(bytes), &expected, |cf, key, value| {
            records.push((cf.to_string(), key.to_vec(), value.to_vec()));
            Ok(())
        })
        .unwrap();

        assert_eq!(summary.total_records, 1);
        assert_eq!(summary.total_uncompressed_bytes, 8);
        assert_eq!(
            records,
            vec![(STATE_CFS[0].to_string(), b"key".to_vec(), b"value".to_vec())]
        );
    }

    #[test]
    fn every_truncation_is_rejected() {
        let expected = identity();
        let bytes = encode_fixture(&expected, &state_cfs(), None, None);
        for length in 0..bytes.len() {
            assert!(
                decode(&bytes[..length], &expected).is_err(),
                "accepted truncation at {length}"
            );
        }
    }

    #[test]
    fn checksum_corruption_and_extra_bytes_are_rejected() {
        let expected = identity();
        let mut corrupt = encode_fixture(&expected, &state_cfs(), None, None);
        let value_offset = corrupt.windows(5).position(|window| window == b"value").unwrap();
        corrupt[value_offset] ^= 0x01;
        assert!(decode(&corrupt, &expected)
            .unwrap_err()
            .to_string()
            .contains("checksum"));

        let mut extra = encode_fixture(&expected, &state_cfs(), None, None);
        extra.push(0);
        assert!(decode(&extra, &expected).unwrap_err().to_string().contains("trailing"));
    }

    #[test]
    fn duplicate_missing_and_unknown_column_families_are_rejected() {
        let expected = identity();
        let mut duplicate = state_cfs();
        duplicate[1].name = duplicate[0].name.clone();
        assert!(decode(&encode_fixture(&expected, &duplicate, None, None), &expected)
            .unwrap_err()
            .to_string()
            .contains("duplicate"));

        let mut missing = state_cfs();
        missing.pop();
        assert!(decode(&encode_fixture(&expected, &missing, None, None), &expected)
            .unwrap_err()
            .to_string()
            .contains("missing"));

        let mut unknown = state_cfs();
        unknown[0].name = "unknown".to_string();
        assert!(decode(&encode_fixture(&expected, &unknown, None, None), &expected)
            .unwrap_err()
            .to_string()
            .contains("unknown"));
    }

    #[test]
    fn oversized_key_is_rejected_before_payload_allocation() {
        let expected = identity();
        let mut oversized_key = encode_fixture(&expected, &state_cfs(), None, None);
        let key_offset = oversized_key.windows(3).position(|window| window == b"key").unwrap();
        oversized_key[key_offset - 8..key_offset - 4].copy_from_slice(&((MAX_KEY_BYTES + 1) as u32).to_be_bytes());
        assert!(decode(&oversized_key, &expected)
            .unwrap_err()
            .to_string()
            .contains("key length"));

        let mut oversized_value = encode_fixture(&expected, &state_cfs(), None, None);
        let key_offset = oversized_value.windows(3).position(|window| window == b"key").unwrap();
        oversized_value[key_offset - 4..key_offset].copy_from_slice(&((MAX_VALUE_BYTES + 1) as u32).to_be_bytes());
        assert!(decode(&oversized_value, &expected)
            .unwrap_err()
            .to_string()
            .contains("value length"));

        let mut oversized_record = encode_fixture(&expected, &state_cfs(), None, None);
        let key_offset = oversized_record.windows(3).position(|window| window == b"key").unwrap();
        oversized_record[key_offset - 4..key_offset].copy_from_slice(&(MAX_RECORD_BYTES as u32).to_be_bytes());
        assert!(decode(&oversized_record, &expected)
            .unwrap_err()
            .to_string()
            .contains("record byte count"));
    }

    #[test]
    fn identity_version_schema_group_and_last_applied_mismatches_are_rejected() {
        let expected = identity();

        let mut wrong_schema = expected.clone();
        wrong_schema.storage_schema_version += 1;
        assert!(
            decode(&encode_fixture(&wrong_schema, &state_cfs(), None, None), &expected)
                .unwrap_err()
                .to_string()
                .contains("schema")
        );

        let mut wrong_group = expected.clone();
        wrong_group.group_name = GroupName::parse("other").unwrap();
        assert!(
            decode(&encode_fixture(&wrong_group, &state_cfs(), None, None), &expected)
                .unwrap_err()
                .to_string()
                .contains("group")
        );

        let mut wrong_applied = expected.clone();
        wrong_applied.last_applied_log_id = Some(RaftLogId::new(3, 7, 12));
        assert!(
            decode(&encode_fixture(&wrong_applied, &state_cfs(), None, None), &expected)
                .unwrap_err()
                .to_string()
                .contains("last applied")
        );

        let mut wrong_version = encode_fixture(&expected, &state_cfs(), None, None);
        wrong_version[MAGIC.len() + 1] = 3;
        assert!(decode(&wrong_version, &expected)
            .unwrap_err()
            .to_string()
            .contains("version"));
    }

    #[test]
    fn record_and_byte_count_mismatches_are_rejected() {
        let expected = identity();
        let mut wrong_cf_count = state_cfs();
        wrong_cf_count[0].declared_records = Some(2);
        assert!(
            decode(&encode_fixture(&expected, &wrong_cf_count, None, None), &expected)
                .unwrap_err()
                .to_string()
                .contains("record count")
        );
        assert!(
            decode(&encode_fixture(&expected, &state_cfs(), Some(2), None), &expected)
                .unwrap_err()
                .to_string()
                .contains("total record")
        );
        assert!(
            decode(&encode_fixture(&expected, &state_cfs(), None, Some(9)), &expected)
                .unwrap_err()
                .to_string()
                .contains("uncompressed byte")
        );
        let mut excessive_cf_count = state_cfs();
        excessive_cf_count[0].declared_records = Some(MAX_RECORDS_PER_CF + 1);
        assert!(
            decode(&encode_fixture(&expected, &excessive_cf_count, None, None), &expected)
                .unwrap_err()
                .to_string()
                .contains("exceeds limit")
        );
        assert!(decode(
            &encode_fixture(&expected, &state_cfs(), Some(MAX_TOTAL_RECORDS + 1), None),
            &expected
        )
        .unwrap_err()
        .to_string()
        .contains("exceeds limit"));
        assert!(decode(
            &encode_fixture(&expected, &state_cfs(), None, Some(MAX_TOTAL_UNCOMPRESSED_BYTES + 1)),
            &expected
        )
        .unwrap_err()
        .to_string()
        .contains("exceeds limit"));
    }

    #[test]
    fn streaming_writer_round_trips_through_decoder() {
        let expected = identity();
        let mut writer = SnapshotWriter::new(Vec::new(), &expected).unwrap();
        for (index, name) in STATE_CFS.iter().enumerate() {
            writer.start_column_family(name).unwrap();
            if index == 0 {
                writer.write_record(b"key", b"value").unwrap();
            }
            writer.end_column_family().unwrap();
        }
        let bytes = writer.finish().unwrap();
        let mut records = Vec::new();

        let summary = decode_snapshot(Cursor::new(bytes), &expected, |cf, key, value| {
            records.push((cf.to_string(), key.to_vec(), value.to_vec()));
            Ok(())
        })
        .unwrap();

        assert_eq!(summary.total_records, 1);
        assert_eq!(summary.total_uncompressed_bytes, 8);
        assert_eq!(records.len(), 1);
    }

    #[test]
    fn writer_rejects_unknown_duplicate_missing_and_oversized_records() {
        let expected = identity();
        let mut unknown = SnapshotWriter::new(Vec::new(), &expected).unwrap();
        assert!(unknown
            .start_column_family("unknown")
            .unwrap_err()
            .to_string()
            .contains("unknown"));

        let mut duplicate = SnapshotWriter::new(Vec::new(), &expected).unwrap();
        duplicate.start_column_family(STATE_CFS[0]).unwrap();
        duplicate.end_column_family().unwrap();
        assert!(duplicate
            .start_column_family(STATE_CFS[0])
            .unwrap_err()
            .to_string()
            .contains("duplicate"));

        let missing = SnapshotWriter::new(Vec::new(), &expected).unwrap();
        assert!(missing.finish().unwrap_err().to_string().contains("missing"));

        let mut oversized = SnapshotWriter::new(Vec::new(), &expected).unwrap();
        oversized.start_column_family(STATE_CFS[0]).unwrap();
        assert!(oversized
            .write_record(&vec![0; MAX_KEY_BYTES + 1], b"")
            .unwrap_err()
            .to_string()
            .contains("key length"));

        let mut outside = SnapshotWriter::new(Vec::new(), &expected).unwrap();
        assert!(outside
            .write_record(b"key", b"value")
            .unwrap_err()
            .to_string()
            .contains("outside"));
        assert!(outside
            .end_column_family()
            .unwrap_err()
            .to_string()
            .contains("none is active"));

        let mut nested = SnapshotWriter::new(Vec::new(), &expected).unwrap();
        nested.start_column_family(STATE_CFS[0]).unwrap();
        assert!(nested
            .start_column_family(STATE_CFS[1])
            .unwrap_err()
            .to_string()
            .contains("before ending"));
        assert!(nested
            .finish()
            .unwrap_err()
            .to_string()
            .contains("active column family"));
    }

    #[test]
    fn unknown_tag_and_missing_end_marker_are_rejected() {
        let expected = identity();
        let mut unknown_tag = encode_fixture(&expected, &state_cfs(), None, None);
        let name_offset = unknown_tag
            .windows(STATE_CFS[0].len())
            .position(|window| window == STATE_CFS[0].as_bytes())
            .unwrap();
        unknown_tag[name_offset - 3] = 0x7f;
        assert!(decode(&unknown_tag, &expected)
            .unwrap_err()
            .to_string()
            .contains("unknown snapshot tag"));

        let mut illegal_inside = encode_fixture(&expected, &state_cfs(), None, None);
        let key_offset = illegal_inside.windows(3).position(|window| window == b"key").unwrap();
        illegal_inside[key_offset - 9] = TAG_CF_START;
        assert!(decode(&illegal_inside, &expected)
            .unwrap_err()
            .to_string()
            .contains("inside column family"));

        let mut bad_trailer = encode_fixture(&expected, &state_cfs(), None, None);
        let trailer_offset = bad_trailer.len() - (1 + 8 + 8 + 32 + 1);
        bad_trailer[trailer_offset] = 0x7e;
        assert!(decode(&bad_trailer, &expected)
            .unwrap_err()
            .to_string()
            .contains("outside column family"));

        let mut missing_end = encode_fixture(&expected, &state_cfs(), None, None);
        *missing_end.last_mut().unwrap() = 0;
        assert!(decode(&missing_end, &expected)
            .unwrap_err()
            .to_string()
            .contains("END marker"));
    }

    #[test]
    fn node_local_meta_keys_are_rejected_by_writer_and_decoder() {
        let expected = identity();
        for reserved in RESERVED_META_KEYS {
            let mut cfs = state_cfs();
            cfs.iter_mut()
                .find(|cf| cf.name == "meta")
                .unwrap()
                .records
                .push((reserved.to_vec(), b"local".to_vec()));
            assert!(decode(&encode_fixture(&expected, &cfs, None, None), &expected)
                .unwrap_err()
                .to_string()
                .contains("node-local"));

            let mut writer = SnapshotWriter::new(Vec::new(), &expected).unwrap();
            writer.start_column_family("meta").unwrap();
            assert!(writer
                .write_record(reserved, b"local")
                .unwrap_err()
                .to_string()
                .contains("node-local"));
        }
    }
}
