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

use std::fmt;
use std::net::Ipv6Addr;

use anyerror::AnyError;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq, Eq, deepsize::DeepSizeOf)]
pub struct Endpoint {
    addr: String,
    port: u16,
}

impl Endpoint {
    pub fn new(addr: impl ToString, port: u16) -> Self {
        Self {
            addr: addr.to_string(),
            port,
        }
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    /// Parse `1.2.3.4:5555`, `localhost:5555` or `[::1]:5555` into `Endpoint`.
    ///
    /// An IPv6 address has to be bracketed, the way a URL authority writes it.
    /// Unbracketed, there is no telling the colons inside the address from the
    /// one in front of the port, so `::1:5555` is refused rather than guessed
    /// at.
    pub fn parse(address: &str) -> Result<Self, AnyError> {
        let invalid = || AnyError::error(format!("Failed to parse address: {}", address));

        let (addr, port) = match address.strip_prefix('[') {
            Some(bracketed) => {
                let (addr, rest) = bracketed.split_once(']').ok_or_else(invalid)?;

                // Brackets mean an IPv6 literal and nothing else. A name
                // accepted inside them would lose its brackets in
                // `to_string()` and stop parsing back to the same value.
                let is_ipv6 = addr.parse::<Ipv6Addr>().is_ok();
                if !is_ipv6 {
                    return Err(invalid());
                }

                let port = rest.strip_prefix(':').ok_or_else(invalid)?;

                (addr, port)
            }
            None => address.split_once(':').ok_or_else(invalid)?,
        };

        let port = port.parse::<u16>().map_err(|e| {
            AnyError::error(format!("Failed to parse port: {}; address: {}", e, address))
        })?;

        Ok(Self::new(addr, port))
    }
}

impl fmt::Display for Endpoint {
    /// Write `addr:port`, bracketing an IPv6 address so that the result parses
    /// back into the same `Endpoint`.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let is_ipv6 = self.addr.parse::<Ipv6Addr>().is_ok();

        if is_ipv6 {
            write!(f, "[{}]:{}", self.addr, self.port)
        } else {
            write!(f, "{}:{}", self.addr, self.port)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::Endpoint;

    #[test]
    fn test_endpoint_parse() -> anyhow::Result<()> {
        assert!(Endpoint::parse("1.2.3.4").is_err());
        assert!(Endpoint::parse("1.2.3.4:88888").is_err());

        assert_eq!("1.2.3.4", Endpoint::parse("1.2.3.4:1234")?.addr());
        assert_eq!(1234, Endpoint::parse("1.2.3.4:1234")?.port());

        Ok(())
    }

    /// An IPv6 address, which only a bracketed form can carry: unbracketed,
    /// the colons inside the address are indistinguishable from the one in
    /// front of the port.
    #[test]
    fn test_endpoint_parse_ipv6() -> anyhow::Result<()> {
        let endpoint = Endpoint::parse("[::1]:1234")?;
        assert_eq!("::1", endpoint.addr());
        assert_eq!(1234, endpoint.port());

        // Whatever `parse` accepts, `to_string` writes back unchanged, so an
        // address that made one round trip survives every later one.
        assert_eq!("[::1]:1234", Endpoint::parse("[::1]:1234")?.to_string());
        assert_eq!("1.2.3.4:1234", Endpoint::parse("1.2.3.4:1234")?.to_string());
        assert_eq!(
            "localhost:1234",
            Endpoint::parse("localhost:1234")?.to_string()
        );

        // The brackets are added by `Display` rather than stored, so an
        // endpoint built from a bare IPv6 host also prints a parsable form.
        assert_eq!("[::1]:1234", Endpoint::new("::1", 1234).to_string());
        // Ambiguous or malformed bracket forms are refused, not guessed at.
        assert!(Endpoint::parse("::1:1234").is_err());
        assert!(Endpoint::parse("[::1]1234").is_err());
        assert!(Endpoint::parse("[::1]:").is_err());
        assert!(Endpoint::parse("[::1:1234").is_err());
        assert!(Endpoint::parse("[localhost]:1234").is_err());

        Ok(())
    }
}
