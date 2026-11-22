// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io::ErrorKind;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::net::TcpListener;

use databend_meta_test_harness::reserve_port;
use tokio::net::TcpSocket;

/// A reserved port stays in use until the service it is reserved for binds it.
///
/// A socket without `SO_REUSEADDR` cannot bind the port: the kernel still
/// counts it as in use, and so hands it out neither to a bind on port 0 nor to
/// an outgoing connection. The raft service binds it all the same: tonic's
/// `TcpIncoming::bind` goes through `std::net::TcpListener::bind`, which sets
/// `SO_REUSEADDR` on Unix.
#[test]
fn test_reserve_port_keeps_the_port_for_its_service() -> anyhow::Result<()> {
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, reserve_port()));

    let socket = TcpSocket::new_v4()?;
    socket.set_reuseaddr(false)?;
    let in_use = socket
        .bind(addr)
        .expect_err("a reserved port must stay in use");
    assert_eq!(in_use.kind(), ErrorKind::AddrInUse);

    let _service = TcpListener::bind(addr)?;
    Ok(())
}
