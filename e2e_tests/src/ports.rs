// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

use std::net::SocketAddr;

use tokio::net::TcpListener;

use crate::TestResult;

pub struct PortReservation {
    listener: TcpListener,
    addr: SocketAddr,
}

impl PortReservation {
    pub async fn reserve_localhost() -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        Ok(Self { listener, addr })
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn into_listener(self) -> TcpListener {
        self.listener
    }
}
