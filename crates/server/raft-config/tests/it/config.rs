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

use databend_meta_raft_config::MetaStartupError;
use databend_meta_raft_config::Secret;
use databend_meta_raft_config::config::RaftConfig;
use databend_meta_raft_config::config::get_default_raft_advertise_host;
use databend_meta_raft_config::data_version::DATA_VERSION;
use databend_meta_runtime_api::TokioRuntime;
use databend_meta_types::Endpoint;
use pretty_assertions::assert_eq;

/// A config with every host related field pinned, so rendered addresses do not
/// depend on the machine running the test.
fn config() -> RaftConfig {
    RaftConfig {
        raft_listen_host: "0.0.0.0".to_string(),
        raft_advertise_host: "meta-1".to_string(),
        raft_api_port: 9191,
        ..Default::default()
    }
}

#[test]
fn test_raft_config() -> anyhow::Result<()> {
    {
        let raft_config = &RaftConfig {
            single: true,
            join: vec!["j1".to_string()],
            ..Default::default()
        };
        let r = raft_config.check();

        assert_eq!(
            r,
            Err(MetaStartupError::InvalidConfig(String::from(
                "at least one of `single` and `join` needs to be enabled",
            )))
        )
    }

    {
        let addr: String = "127.0.0.1".to_string();
        let port = 22222;
        let raft_config = &RaftConfig {
            single: false,
            raft_listen_host: addr.clone(),
            raft_api_port: port,
            join: vec![format!("{}:{}", addr, port)],
            ..Default::default()
        };
        let r = raft_config.check();

        assert_eq!(
            r,
            Err(MetaStartupError::InvalidConfig(String::from(
                "--join must not be set to itself",
            )))
        )
    }

    {
        let raft_config = &RaftConfig {
            single: false,
            join: vec![],
            ..Default::default()
        };
        let r = raft_config.check();

        assert_eq!(
            r,
            Err(MetaStartupError::InvalidConfig(String::from(
                "at least one of `single` and `join` needs to be enabled",
            )))
        )
    }

    {
        // Exactly one of `single` and `join` is what the check asks for.
        for (single, join) in [(true, vec![]), (false, vec!["j1".to_string()])] {
            let raft_config = &RaftConfig {
                single,
                join,
                ..Default::default()
            };

            assert_eq!(raft_config.check(), Ok(()));
        }
    }

    {
        // Leaving a cluster skips topology checks.
        let raft_config = &RaftConfig {
            single: true,
            join: vec!["j1".to_string()],
            leave_via: vec!["l1".to_string()],
            leave_id: Some(1),
            ..Default::default()
        };

        assert_eq!(raft_config.check(), Ok(()));
    }

    Ok(())
}

#[test]
fn test_default_config() {
    assert_eq!(RaftConfig::default(), RaftConfig {
        config_id: "".to_string(),
        raft_listen_host: "127.0.0.1".to_string(),
        raft_advertise_host: get_default_raft_advertise_host(),
        raft_api_port: 28004,
        raft_dir: "./.databend/meta".to_string(),
        log_cache_max_items: 1_000_000,
        log_cache_capacity: 1024 * 1024 * 1024,
        log_wal_chunk_max_records: 100_000,
        log_wal_chunk_max_size: 256 * 1024 * 1024,
        snapshot_logs_since_last: 1024,
        heartbeat_interval: 1000,
        install_snapshot_timeout: 4000,
        max_applied_log_to_keep: 1000,
        snapshot_chunk_size: 4 * 1024 * 1024,
        snapshot_db_debug_check: true,
        snapshot_db_block_keys: 8000,
        snapshot_db_block_cache_size: 1024 * 1024 * 1024,
        compact_immutables_ms: None,
        single: false,
        join: vec![],
        learner: false,
        leave_via: vec![],
        leave_id: None,
        id: 0,
        cluster_name: "foo_cluster".to_string(),
        wait_leader_timeout: 70000,
        raft_grpc_max_message_size: None,
        raft_secret: None,
        raft_accepted_secrets: vec![],
        raft_secret_strict: None,
    });

    assert_ne!(get_default_raft_advertise_host(), "");
}

/// `RaftConfig` is logged in whole with `{:?}` at several places and derives
/// `Serialize`, so neither rendering may carry a secret in plain text.
#[test]
fn test_secret_is_not_rendered_in_plain_text() -> anyhow::Result<()> {
    let c = RaftConfig {
        raft_secret: Some(Secret::new("send-me")),
        raft_accepted_secrets: vec![Secret::new("accept-1"), Secret::new("accept-2")],
        raft_secret_strict: Some(true),
        ..config()
    };

    let debug = format!("{:?}", c);
    let json = serde_json::to_value(&c)?;

    for plain in ["send-me", "accept-1", "accept-2"] {
        assert!(!debug.contains(plain), "Debug leaked {plain}: {debug}");
        assert!(
            !json.to_string().contains(plain),
            "Serialize leaked {plain}: {json}"
        );
    }

    assert!(debug.contains("raft_secret: Some(***)"), "{debug}");
    assert!(
        debug.contains("raft_accepted_secrets: [***, ***]"),
        "{debug}"
    );
    assert_eq!(json["raft_secret"], serde_json::json!("***"));
    assert_eq!(
        json["raft_accepted_secrets"],
        serde_json::json!(["***", "***"])
    );

    // The value stays reachable for the code that has to compare it.
    assert_eq!(c.raft_secret.as_ref().map(Secret::expose), Some("send-me"));

    Ok(())
}

#[test]
fn test_secret_strict() {
    let mut c = config();
    assert!(
        !c.raft_secret_strict(),
        "unspecified resolves to not strict"
    );

    for specified in [true, false] {
        c.raft_secret_strict = Some(specified);
        assert_eq!(c.raft_secret_strict(), specified);
    }
}

#[test]
fn test_check_strict_secret_needs_an_accepted_secret() {
    let c = RaftConfig {
        single: true,
        raft_secret_strict: Some(true),
        ..config()
    };

    assert_eq!(
        c.check(),
        Err(MetaStartupError::InvalidConfig(String::from(
            "`raft_secret_strict` is enabled but `raft_accepted_secrets` is empty: the node would reject every incoming raft RPC",
        )))
    );

    let c = RaftConfig {
        raft_accepted_secrets: vec![Secret::new("accept-1")],
        ..c
    };
    assert_eq!(c.check(), Ok(()));
}

#[test]
fn test_check_rejects_an_empty_accepted_secret() {
    let c = RaftConfig {
        single: true,
        raft_accepted_secrets: vec![Secret::new("")],
        raft_secret_strict: Some(true),
        ..config()
    };

    assert_eq!(
        c.check(),
        Err(MetaStartupError::InvalidConfig(String::from(
            "`raft_accepted_secrets` must not contain an empty secret",
        )))
    );
}

/// A `leave_via` without a `leave_id` used to be treated as leave-only: the
/// topology and secret checks were skipped, leaving is then skipped too for want
/// of an id, and the node starts as an unchecked server.
#[test]
fn test_check_rejects_a_leave_without_an_id() {
    let c = RaftConfig {
        leave_via: vec!["meta-1:9191".to_string()],
        leave_id: None,
        raft_secret_strict: Some(true),
        ..config()
    };

    assert_eq!(
        c.check(),
        Err(MetaStartupError::InvalidConfig(String::from(
            "`leave_via` is set but `leave_id` is not: \
             the node to remove from the cluster is unknown",
        )))
    );
}

#[test]
fn test_check_rejects_an_empty_sending_secret() {
    let c = RaftConfig {
        single: true,
        leave_via: vec!["meta-1:9191".to_string()],
        leave_id: Some(1),
        raft_secret: Some(Secret::new("")),
        ..config()
    };

    assert_eq!(
        c.check(),
        Err(MetaStartupError::InvalidConfig(String::from(
            "`raft_secret` must not be empty",
        )))
    );
}

#[test]
fn test_grpc_message_size() {
    let mut c = config();
    assert_eq!(
        (
            c.raft_grpc_max_message_size(),
            c.raft_grpc_advisory_message_size()
        ),
        (32 * 1024 * 1024, 30_198_988),
        "unset falls back to 32MB, advisory is 90% of it"
    );

    c.raft_grpc_max_message_size = Some(1000);
    assert_eq!(
        (
            c.raft_grpc_max_message_size(),
            c.raft_grpc_advisory_message_size()
        ),
        (1000, 900)
    );
}

#[test]
fn test_to_rotbl_config() {
    let c = RaftConfig {
        snapshot_db_debug_check: false,
        snapshot_db_block_keys: 7,
        snapshot_db_block_cache_size: 11,
        ..config()
    };

    let got = c.to_rotbl_config();

    assert_eq!(got.debug_check, Some(false));
    assert_eq!(got.block_config.max_items, Some(7));
    assert_eq!(got.block_cache.capacity, Some(11));
    assert_eq!(got.root_path, rotbl::v001::Config::default().root_path);
}

#[test]
fn test_to_raft_log_config() {
    let c = RaftConfig {
        raft_dir: "/tmp/meta".to_string(),
        log_wal_chunk_max_records: 5,
        log_wal_chunk_max_size: 6,
        log_cache_max_items: 7,
        log_cache_capacity: 8,
        ..config()
    };

    let got = c.to_raft_log_config();

    assert_eq!(
        got.wal.dir,
        format!("/tmp/meta/df_meta/{}/log", DATA_VERSION)
    );
    assert_eq!(got.wal.read_buffer_size, None);
    assert_eq!(got.wal.chunk_max_records, Some(5));
    assert_eq!(got.wal.chunk_max_size, Some(6));
    assert_eq!(got.wal.truncate_incomplete_record, None);
    assert_eq!(
        got.wal.flush_batch_wait,
        Some(std::time::Duration::from_millis(1))
    );
    assert_eq!(got.wal.flush_batch_max_items, Some(1024));
    assert_eq!(got.log_cache_max_items, Some(7));
    assert_eq!(got.log_cache_capacity, Some(8));
}

#[test]
fn test_raft_api_host_strings_and_endpoints() {
    let c = config();

    assert_eq!(c.raft_api_listen_host_string(), "0.0.0.0:9191");
    assert_eq!(c.raft_api_advertise_host_string(), "meta-1:9191");
    assert_eq!(
        c.raft_api_listen_host_endpoint(),
        Endpoint::new("0.0.0.0", 9191)
    );
    assert_eq!(
        c.raft_api_advertise_host_endpoint(),
        Endpoint::new("meta-1", 9191)
    );
}

/// A literal IP is used as is, without consulting the resolver.
#[tokio::test]
async fn test_raft_api_addr_keeps_a_literal_ip() -> anyhow::Result<()> {
    let c = RaftConfig {
        raft_advertise_host: "1.2.3.4".to_string(),
        ..config()
    };

    assert_eq!(
        c.raft_api_addr::<TokioRuntime>().await?,
        Endpoint::new("1.2.3.4", 9191)
    );

    Ok(())
}

#[test]
fn test_election_timeout() {
    let c = RaftConfig {
        heartbeat_interval: 700,
        ..config()
    };

    assert_eq!(c.election_timeout(), (1400, 2100));
}
