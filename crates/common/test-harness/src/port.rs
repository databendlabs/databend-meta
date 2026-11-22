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

use std::net::Ipv4Addr;
use std::net::TcpListener;
use std::net::TcpStream;

/// Reserve a port on `127.0.0.1` for a service that a test starts later.
///
/// The port is left with one connection in `TIME_WAIT`, which keeps it in use:
/// the kernel hands it out neither to a bind on port 0 nor to an outgoing
/// connection, of this test or of another test process. A listener that sets
/// `SO_REUSEADDR` still binds it, and every listener built on
/// `std::net::TcpListener::bind`, such as tonic's `TcpIncoming::bind`, sets it
/// on Unix. `TIME_WAIT` lasts 60 s on Linux and 30 s on macOS: a service that
/// binds the port later finds it free, but no longer reserved.
///
/// Binding port 0 and closing the listener, as
/// `databend_base::testutil::next_port` does, leaves the port free at once:
/// either kind of allocation can take it before the service binds it.
pub fn reserve_port() -> u16 {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind a port to reserve");
    let port = listener
        .local_addr()
        .expect("get the address of the port to reserve")
        .port();

    let client =
        TcpStream::connect((Ipv4Addr::LOCALHOST, port)).expect("connect to the port to reserve");
    let (accepted, _) = listener
        .accept()
        .expect("accept a connection on the port to reserve");

    // The side that closes first enters `TIME_WAIT`: the accepted one, whose
    // local port is the reserved one.
    drop(accepted);
    drop(client);

    port
}
