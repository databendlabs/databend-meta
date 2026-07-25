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

use std::io;
use std::sync::mpsc::SyncSender;
use std::time::Duration;

use databend_meta_types::raft_types;
use log::info;
use log::warn;
use tokio::sync::oneshot;

use crate::IODesc;
use crate::callback_data::CallbackData;

/// The callback to be called when the IO is completed.
///
/// This is used as a wrapper of Openraft callback or used directly internally in RaftLog.
pub struct Callback {
    io_desc: IODesc,
    data: CallbackData,
}

impl Callback {
    pub fn new_io_flushed(io_flushed: raft_types::IOFlushed, mut io_desc: IODesc) -> Self {
        io_desc.set_flush_time();

        Callback {
            io_desc,
            data: CallbackData::IOFlushed(io_flushed),
        }
    }

    pub fn new_oneshot(tx: oneshot::Sender<Result<(), io::Error>>, mut io_desc: IODesc) -> Self {
        io_desc.set_flush_time();

        Callback {
            io_desc,
            data: CallbackData::Oneshot(tx),
        }
    }

    pub fn new_sync_oneshot(tx: SyncSender<Result<(), io::Error>>, mut io_desc: IODesc) -> Self {
        io_desc.set_flush_time();

        Callback {
            io_desc,
            data: CallbackData::SyncOneshot(tx),
        }
    }
}

impl raft_log::Callback for Callback {
    fn send(mut self, res: Result<(), io::Error>) {
        self.io_desc.set_flush_done_time();

        info!("{}: Callback is called with: {:?}", self.io_desc, res);

        if let Some(duration) = self.io_desc.total_duration() {
            if duration > Duration::from_millis(50) {
                warn!("{}: Slow IO operation: {:?}", self.io_desc, res);
            }
        }

        match self.data {
            CallbackData::Oneshot(tx) => {
                let send_res = tx.send(res);
                if send_res.is_err() {
                    warn!(
                        "{}: Callback failed to send Oneshot result back to caller",
                        self.io_desc
                    );
                }
            }
            CallbackData::SyncOneshot(tx) => tx.send(res),
            CallbackData::IOFlushed(io_flushed) => io_flushed.io_completed(res),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc::sync_channel;

    use pretty_assertions::assert_eq;
    use raft_log::Callback as _;

    use super::*;

    /// Every constructor stamps the flush time, so the callback can report the
    /// flush latency when it is called.
    #[test]
    fn test_constructors_stamp_the_flush_time() {
        let (tx, _rx) = oneshot::channel();
        assert!(
            Callback::new_oneshot(tx, IODesc::append("test"))
                .io_desc
                .flush_time
                .is_some()
        );

        let (tx, _rx) = sync_channel(1);
        assert!(
            Callback::new_sync_oneshot(tx, IODesc::append("test"))
                .io_desc
                .flush_time
                .is_some()
        );
    }

    #[tokio::test]
    async fn test_oneshot_callback_delivers_success_and_error() {
        let (tx, rx) = oneshot::channel();
        Callback::new_oneshot(tx, IODesc::append("test")).send(Ok(()));
        assert!(rx.await.unwrap().is_ok());

        let (tx, rx) = oneshot::channel();
        Callback::new_oneshot(tx, IODesc::append("test")).send(Err(io::Error::other("flush fail")));
        assert_eq!(rx.await.unwrap().unwrap_err().to_string(), "flush fail");
    }

    #[test]
    fn test_sync_oneshot_callback_delivers_success_and_error() {
        let (tx, rx) = sync_channel(1);
        Callback::new_sync_oneshot(tx, IODesc::append("test")).send(Ok(()));
        assert!(rx.recv().unwrap().is_ok());

        let (tx, rx) = sync_channel(1);
        Callback::new_sync_oneshot(tx, IODesc::append("test"))
            .send(Err(io::Error::other("flush fail")));
        assert_eq!(rx.recv().unwrap().unwrap_err().to_string(), "flush fail");
    }

    /// A caller that gave up waiting must not bring the IO thread down.
    #[test]
    fn test_a_gone_receiver_is_only_logged() {
        let (tx, rx) = oneshot::channel();
        drop(rx);
        Callback::new_oneshot(tx, IODesc::append("test")).send(Ok(()));
    }
}
