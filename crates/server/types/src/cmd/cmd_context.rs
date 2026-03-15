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
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use databend_meta_base::MetaSpec;
use display_more::DisplayUnixTimeStampExt;
use state_machine_api::KVMeta;

use crate::Time;
use crate::cmd::io_timing::IoTimer;
use crate::cmd::io_timing::IoTiming;
use crate::raft_types::LogId;
use crate::raft_types::new_log_id;

/// A context used when executing a [`Cmd`], to provide additional environment information.
///
/// [`Cmd`]: crate::Cmd
#[derive(Debug, Clone)]
pub struct CmdContext {
    time: Time,

    /// Current log id to apply
    log_id: LogId,

    /// I/O timing information for tracking read operations
    io_timing: Arc<Mutex<IoTiming>>,
}

impl fmt::Display for CmdContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[time: {}, log_id: {}]",
            self.time.to_duration().display_unix_timestamp_short(),
            self.log_id
        )
    }
}

impl CmdContext {
    pub fn from_millis_and_log_id(millis: u64, log_id: LogId) -> Self {
        Self {
            time: Time::from_millis(millis),
            log_id,
            io_timing: Arc::new(Mutex::new(IoTiming::new())),
        }
    }

    pub fn from_millis(millis: u64) -> Self {
        Self::new(Time::from_millis(millis))
    }

    pub fn new(time: Time) -> Self {
        CmdContext {
            time,
            log_id: new_log_id(0, 0, 0),
            io_timing: Arc::new(Mutex::new(IoTiming::new())),
        }
    }

    /// Returns the time since 1970-01-01 when this log is proposed by the leader.
    pub fn time(&self) -> Time {
        self.time
    }

    /// Record an I/O operation with its timing.
    pub fn record_io(
        &self,
        op_type: impl Into<String>,
        details: impl Into<String>,
        duration: Duration,
    ) {
        self.io_timing
            .lock()
            .unwrap()
            .record(op_type, details, duration);
    }

    /// Get I/O timing total duration.
    pub fn io_total_duration(&self) -> Duration {
        self.io_timing.lock().unwrap().total_duration()
    }

    /// Get I/O timing details (cloned for logging).
    pub fn io_timing(&self) -> IoTiming {
        self.io_timing.lock().unwrap().clone()
    }

    /// Start an RAII-based I/O timer that automatically records timing on drop.
    ///
    /// # Example
    /// ```ignore
    /// let _timer = self.cmd_ctx.start_io_timer("get", key);
    /// let result = self.sm.get_maybe_expired_kv(key).await?;
    /// // Timer automatically records on scope exit
    /// ```
    pub fn start_io_timer(
        &self,
        op_type: impl Into<String>,
        details: impl Into<String>,
    ) -> IoTimer<'_> {
        IoTimer::new(self, op_type, details)
    }

    /// Convert a [`MetaSpec`] into a [`KVMeta`] to be stored in storage.
    ///
    /// If `ttl` is set, it overrides `expire_at`.
    pub fn resolve_meta_spec(&self, meta_spec: &MetaSpec) -> KVMeta {
        let expire_at_ms = if let Some(ttl) = meta_spec.ttl() {
            Some((self.time() + ttl).millis())
        } else {
            // Since 1.2.770 expire_at is in seconds or milliseconds.
            meta_spec.expire_at()
        };

        let proposed_at = Some(self.time().millis());

        KVMeta::new(expire_at_ms, proposed_at)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::Time;

    #[test]
    fn test_resolve_meta_spec() {
        let cmd_ctx = CmdContext::new(Time::from_millis(1_723_102_819_000));

        // ttl
        let meta_spec = MetaSpec::new_ttl(Duration::from_millis(1000));
        let kv_meta = cmd_ctx.resolve_meta_spec(&meta_spec);
        assert_eq!(kv_meta.get_expire_at_ms().unwrap(), 1_723_102_820_000);
        assert_eq!(kv_meta.proposed_at_ms(), Some(1_723_102_819_000));

        // expire_at
        let meta_spec = MetaSpec::new_expire(5);
        let kv_meta = cmd_ctx.resolve_meta_spec(&meta_spec);
        assert_eq!(kv_meta.get_expire_at_ms().unwrap(), 5_000);

        let meta_spec = MetaSpec::new_expire(1_723_102_820);
        let kv_meta = cmd_ctx.resolve_meta_spec(&meta_spec);
        assert_eq!(kv_meta.get_expire_at_ms().unwrap(), 1_723_102_820_000);

        let meta_spec = MetaSpec::new_expire(1_723_102_820_000);
        let kv_meta = cmd_ctx.resolve_meta_spec(&meta_spec);
        assert_eq!(kv_meta.get_expire_at_ms().unwrap(), 1_723_102_820_000);
    }
}
