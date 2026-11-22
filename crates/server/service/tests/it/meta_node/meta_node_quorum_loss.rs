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

use std::time::Duration;

use databend_meta_runtime_api::TokioRuntime;
use log::info;
use maplit::btreeset;
use test_harness::test;

use crate::testing::meta_service_test_harness;
use crate::tests::meta_node::start_meta_node_cluster;
use crate::tests::meta_node::timeout;

/// A follower that still reaches a leader which has lost its quorum times out
/// and campaigns, instead of following a leader that can commit nothing.
///
/// The leader keeps leading in its own view: OpenRaft steps a leader down only
/// on a higher vote. Its heartbeats would keep resetting the follower's
/// election timer. `MetaNode::new_raft_config` sets
/// `quorum_loss_probe_interval`, so once its lease lapses the leader sends
/// nothing for periods longer than an election timeout, and the follower's
/// timer fires. Pre-Vote is off, as by default, so a follower whose timer
/// fires raises its term at once.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_a_follower_in_the_leaders_minority_campaigns() -> anyhow::Result<()> {
    let (_log_index, tcs) =
        start_meta_node_cluster(btreeset! {0, 1, 2, 3, 4}, btreeset! {}).await?;

    let follower = tcs[1].meta_node();
    let leader_term = follower
        .raft
        .wait(timeout())
        .current_leader(0, "node-1 follows node-0")
        .await?
        .current_term;

    info!("--- stop nodes 2 to 4: nodes 0 and 1 still reach each other, but are no quorum");
    for tc in &tcs[2..] {
        tc.meta_node().stop().await?;
    }

    follower
        .raft
        .wait(Some(Duration::from_secs(15)))
        .metrics(
            move |m| m.current_term > leader_term,
            "node-1 campaigns in a term after node-0's",
        )
        .await?;

    Ok(())
}
