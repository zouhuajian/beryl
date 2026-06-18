// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

use std::path::{Path, PathBuf};

use client::{ClientConfig, FsClient};
use types::GroupName;

#[tokio::test]
async fn repository_client_site_loads_bootstrap_contract() {
    assert_client_bootstrap_contract(&repo_root().join("conf/client-site.yaml"));
    assert_client_bootstrap_contract(&repo_root().join("conf/local/client-site.yaml"));
}

fn assert_client_bootstrap_contract(config_path: &Path) {
    let config = ClientConfig::load(config_path).expect("client config loads");

    assert_eq!(config.client_name(), "default_client");
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
