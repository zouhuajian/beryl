use types::layout::{BlockFormatId, FileLayout};

#[test]
fn block_format_id_exposes_canonical_constants() {
    assert_eq!(BlockFormatId::FULL_EFFECTIVE.as_raw(), 1);
    assert_eq!(BlockFormatId::CURRENT_FOR_NEW_FILE, BlockFormatId::FULL_EFFECTIVE);
    assert_eq!(
        BlockFormatId::from_raw(1).expect("known format"),
        BlockFormatId::FULL_EFFECTIVE
    );
    assert!(BlockFormatId::from_raw(2).is_err());
    assert!(BlockFormatId::from_raw(0).is_err());
}

#[test]
fn file_layout_records_current_or_explicit_block_format_id() {
    let current = FileLayout::try_new(4096, 1024, 1).unwrap();
    assert_eq!(current.block_format_id(), BlockFormatId::CURRENT_FOR_NEW_FILE);

    let explicit = FileLayout::try_with_block_format(8192, 2048, 1, BlockFormatId::FULL_EFFECTIVE).unwrap();
    assert_eq!(explicit.block_size(), 8192);
    assert_eq!(explicit.chunk_size(), 2048);
    assert_eq!(explicit.replication(), 1);
    assert_eq!(explicit.block_format_id(), BlockFormatId::FULL_EFFECTIVE);
}

#[test]
fn file_layout_construction_rejects_invalid_shape() {
    assert!(FileLayout::try_new(0, 1024, 1).is_err());
    assert!(FileLayout::try_new(4096, 0, 1).is_err());
    assert!(FileLayout::try_new(4096, 8192, 1).is_err());
    assert!(FileLayout::try_new(4097, 1024, 1).is_err());
    assert!(FileLayout::try_new(4096, 1024, 0).is_err());
}
