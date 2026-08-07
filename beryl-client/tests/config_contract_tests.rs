// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use std::path::PathBuf;

use beryl_client::{ClientConfig, FsClient};
use beryl_types::GroupName;

#[tokio::test]
async fn repository_client_configs_load() {
    let config = ClientConfig::load(repo_root().join("conf/client.yaml")).expect("client config loads");

    assert_eq!(config.client_name(), "default-client");
    assert_eq!(config.metadata_groups.len(), 1);
    assert_eq!(config.metadata_groups[0].group_name, GroupName::parse("root").unwrap());
    assert_eq!(config.metadata_groups[0].endpoints, vec!["127.0.0.1:18080".to_string()]);

    let client = FsClient::try_new(config).expect("FsClient construction must stay lazy");
    assert_eq!(client.config().metadata_groups.len(), 1);
    assert_eq!(
        client.config().metadata_groups[0].group_name,
        GroupName::parse("root").unwrap()
    );
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("client lives under workspace root")
        .to_path_buf()
}
