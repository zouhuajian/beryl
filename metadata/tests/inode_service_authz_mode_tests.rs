// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

use std::sync::Arc;

use metadata::mount::MountTable;
use metadata::service::{MetadataInodeServiceImpl, StubRangerAuthz};
use metadata::state::MemoryStateStore;

#[test]
#[should_panic(expected = "InodeService does not support RangerPath")]
fn inode_service_builder_rejects_ranger_path_authz() {
    let state_store: Arc<dyn metadata::state::StateStore> = Arc::new(MemoryStateStore::new());
    let mount_table = Arc::new(MountTable::new());

    let _ = MetadataInodeServiceImpl::new(state_store, mount_table).with_authz_provider(Arc::new(StubRangerAuthz));
}
