// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Host configuration validation and address formatting.

use std::net::IpAddr;

use crate::error::{CommonError, CommonErrorKind};

/// Validate a host or IP that will be published to other processes.
pub fn validate_public_host(key: &str, host: &str) -> Result<(), CommonError> {
    if host.is_empty() || host != host.trim() || host.chars().any(char::is_whitespace) {
        return Err(invalid_config(key, "must be a host or IP without whitespace"));
    }
    if let Ok(ip) = host.parse::<IpAddr>() {
        return if ip.is_unspecified() {
            Err(invalid_config(key, "must be a routable host or IP"))
        } else {
            Ok(())
        };
    }
    if host.contains("://") || host.contains([':', '/', '\\']) {
        return Err(invalid_config(key, "must not include a scheme, port, or path"));
    }
    Ok(())
}

/// Format a host or IP with a port, adding brackets around IPv6 addresses.
pub fn format_host_port(host: &str, port: u16) -> String {
    match host.parse::<IpAddr>() {
        Ok(IpAddr::V6(_)) => format!("[{host}]:{port}"),
        _ => format!("{host}:{port}"),
    }
}

fn invalid_config(key: &str, detail: &str) -> CommonError {
    CommonError::new(CommonErrorKind::InvalidArgument, format!("{key} {detail}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_host_validation_accepts_routable_hosts_and_rejects_endpoint_syntax() {
        for host in ["metadata-01", "127.0.0.1", "::1"] {
            validate_public_host("host", host).unwrap();
        }

        for host in [
            "",
            " metadata-01",
            "metadata 01",
            "0.0.0.0",
            "::",
            "http://metadata-01",
            "metadata-01:18080",
        ] {
            assert!(validate_public_host("host", host).is_err());
        }
    }

    #[test]
    fn host_port_formatting_brackets_ipv6_addresses() {
        assert_eq!(format_host_port("metadata-01", 18080), "metadata-01:18080");
        assert_eq!(format_host_port("127.0.0.1", 18080), "127.0.0.1:18080");
        assert_eq!(format_host_port("::1", 18080), "[::1]:18080");
    }
}
