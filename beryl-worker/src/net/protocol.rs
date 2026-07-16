// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Worker data-plane network protocol selection.

use std::fmt;

/// Worker data-plane network protocol.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum WorkerNetProtocol {
    #[default]
    Grpc,
}

impl fmt::Display for WorkerNetProtocol {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Grpc => formatter.write_str("grpc"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_name_is_grpc() {
        assert_eq!(WorkerNetProtocol::Grpc.to_string(), "grpc");
    }
}
