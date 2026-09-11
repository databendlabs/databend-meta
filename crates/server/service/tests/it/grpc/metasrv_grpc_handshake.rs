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

//! Test metasrv SchemaApi by writing to one node and then reading from another,
//! on a restarted cluster.

use std::time::Duration;

use databend_meta::version::MIN_CLIENT_VERSION;
use databend_meta_client::MetaChannelManager;
use databend_meta_client::handshake;
use databend_meta_raft_config::Secret;
use databend_meta_runtime_api::SpawnApi;
use databend_meta_runtime_api::TokioRuntime;
use databend_meta_test_harness::MetaSrvTestContext;
use databend_meta_version::Version;
use databend_meta_version::version;
use log::debug;
use log::info;
use test_harness::test;

use crate::testing::meta_service_test_harness;
use crate::tests::start_metasrv;
use crate::tests::start_metasrv_with_context;

const PASSWORD_HASH: &str = "9246aa9be8de7b40d64eb664986430793b6cc13a19d2a456981e44f28303f9cf";

async fn try_handshake(addr: &str, username: &str, password: &str) -> anyhow::Result<()> {
    let timeout = Some(Duration::from_millis(1000));
    let channel = TokioRuntime::connect(addr.to_string(), timeout, None).await?;
    let (mut client, _once) =
        MetaChannelManager::<TokioRuntime>::new_real_client_for_testing(channel);
    handshake(&mut client, version(), &Version::min(), username, password).await?;
    Ok(())
}

async fn start_auth_server(
    strict: bool,
) -> anyhow::Result<(MetaSrvTestContext<TokioRuntime>, String)> {
    let mut context = MetaSrvTestContext::<TokioRuntime>::new(0);
    context.config.grpc.auth_username = Some("meta".to_string());
    context.config.grpc.auth_password_hash = Some(Secret::new(PASSWORD_HASH));
    context.config.grpc.auth_strict = Some(strict);
    start_metasrv_with_context(&mut context).await?;
    let address = context.config.grpc.api_address().unwrap();
    Ok((context, address))
}

fn assert_unauthenticated(result: anyhow::Result<()>, expected: &str) {
    let error = result.unwrap_err();
    let message = error.to_string();
    assert!(
        message.contains("valid authentication credentials"),
        "{message}"
    );
    assert!(message.contains(expected), "{message}");
}

fn scraped_counter(reason: &str) -> u64 {
    let scraped = databend_meta::metrics::meta_metrics_to_prometheus_string();
    let prefix = format!(
        "metasrv_meta_network_unauthenticated_passed_total{{reason=\"{}\"}} ",
        reason
    );
    let value = scraped
        .lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .unwrap_or("0");
    value.parse().unwrap()
}

async fn assert_permissive_counted(
    address: &str,
    password: &str,
    reason: &str,
) -> anyhow::Result<()> {
    let before = scraped_counter(reason);
    try_handshake(address, "meta", password).await?;
    let after = scraped_counter(reason);
    assert_eq!(after, before + 1, "{reason} password was not counted");
    Ok(())
}

/// - Test client version < serverside min-compatible-client-ver.
/// - Test metasrv version < client min-compatible-metasrv-ver.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_metasrv_handshake() -> anyhow::Result<()> {
    fn smaller_ver(v: &Version) -> Version {
        if v.major() > 0 {
            Version::new(v.major() - 1, v.minor(), v.patch())
        } else if v.minor() > 0 {
            Version::new(0, v.minor() - 1, v.patch())
        } else if v.patch() > 0 {
            Version::new(0, 0, v.patch() - 1)
        } else {
            unreachable!("can not build a semver smaller than {:?}", v)
        }
    }

    let (_tc, addr) = start_metasrv::<TokioRuntime>().await?;

    let c =
        TokioRuntime::connect(addr.to_string(), Some(Duration::from_millis(1000)), None).await?;
    let (mut client, _once) = MetaChannelManager::<TokioRuntime>::new_real_client_for_testing(c);

    info!("--- client has smaller ver than S.min_cli_ver");
    {
        let min_client_ver = &MIN_CLIENT_VERSION;
        let cli_ver = smaller_ver(min_client_ver);

        let res = handshake(&mut client, &cli_ver, &Version::min(), "root", "xxx").await;

        debug!("handshake res: {:?}", res);
        let e = res.unwrap_err();

        let want = format!(
            "meta-client protocol_version({}) < metasrv min-compatible({})",
            cli_ver, MIN_CLIENT_VERSION
        );
        assert!(e.to_string().contains(&want), "handshake err: {:?}", e);
    }

    info!("--- server has smaller ver than C.min_srv_ver");
    {
        let current = version();
        let required = Version::new(current.major() + 1, current.minor(), current.patch());

        let res = handshake(&mut client, version(), &required, "root", "xxx").await;

        debug!("handshake res: {:?}", res);
        let e = res.unwrap_err();

        let server_ver = version().as_tuple();

        let want = format!(
            "Invalid: server protocol_version({:?}) < client required({:?})",
            server_ver,
            required.as_tuple(),
        );
        assert!(
            e.to_string().contains(&want),
            "handshake err: {} contains: {}",
            e,
            want
        );
    }

    info!("--- old client using ver==0 is allowed");
    {
        let zero = Version::min();

        let res = handshake(&mut client, &zero, &Version::min(), "root", "xxx").await;

        debug!("handshake res: {:?}", res);
        assert!(res.is_ok());
    }

    Ok(())
}

#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
async fn test_unconfigured_server_accepts_old_client() -> anyhow::Result<()> {
    let (_server, address) = start_metasrv::<TokioRuntime>().await?;

    try_handshake(&address, "root", "").await
}

#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
async fn test_permissive_grpc_password_auth() -> anyhow::Result<()> {
    let (_server, address) = start_auth_server(false).await?;

    try_handshake(&address, "meta", "correct-password").await?;
    assert_permissive_counted(&address, "", "missing").await?;
    assert_permissive_counted(&address, "wrong-password", "incorrect").await?;
    let unknown = try_handshake(&address, "unknown", "correct-password").await;
    assert_unauthenticated(unknown, "Unknown user");
    Ok(())
}

#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
async fn test_strict_grpc_password_auth() -> anyhow::Result<()> {
    let (_server, address) = start_auth_server(true).await?;

    try_handshake(&address, "meta", "correct-password").await?;
    assert_unauthenticated(
        try_handshake(&address, "meta", "").await,
        "Invalid password",
    );
    let wrong = try_handshake(&address, "meta", "wrong-password").await;
    assert_unauthenticated(wrong, "Invalid password");
    let unknown = try_handshake(&address, "unknown", "correct-password").await;
    assert_unauthenticated(unknown, "Unknown user");
    Ok(())
}
