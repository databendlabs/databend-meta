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

//! This mod integrates watcher into meta service

use std::fmt;
use std::future::Future;
use std::io::Error;
use std::ops::Deref;

use databend_meta_types::SeqV;
use databend_meta_types::protobuf::WatchResponse;
use log::debug;
use log::info;
use tokio::sync::watch;
use tonic::Status;
use watcher::dispatch::DispatcherHandle as GenericDispatcherHandle;
use watcher::type_config::KVChange;
use watcher::type_config::KeyOf;
use watcher::type_config::TypeConfig;
use watcher::type_config::ValueOf;

use crate::metrics::server_metrics;

/// Watch Type Config
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WatchTypes;

impl TypeConfig for WatchTypes {
    type Key = String;
    type Value = SeqV;
    type Response = WatchResponse;
    type Error = Status;

    fn new_initialize_response(key: KeyOf<Self>, value: ValueOf<Self>) -> Self::Response {
        WatchResponse::new_initialization_event(key, value)
    }

    fn new_change_response(change: KVChange<Self>) -> Self::Response {
        WatchResponse::new_change_event(change.0, change.1, change.2)
    }

    fn data_error(error: Error) -> Self::Error {
        Status::internal(error.to_string())
    }

    fn update_watcher_metrics(delta: i64) {
        server_metrics::incr_watchers(delta);
    }
}

/// A handle to a watching stream that feeds messages to connected watchers.
///
/// This is a wrapper around the generic dispatcher handle,
/// in order to implement 3rd party traits for it.
#[derive(Debug)]
pub struct DispatcherHandle {
    handle: GenericDispatcherHandle<WatchTypes>,
    name: String,

    /// Incremented by [`Self::close_all_watchers`]; see [`Self::closed`].
    close_generation: watch::Sender<u64>,
}

impl Drop for DispatcherHandle {
    fn drop(&mut self) {
        debug!("{}: drop", self);
    }
}

impl Deref for DispatcherHandle {
    type Target = GenericDispatcherHandle<WatchTypes>;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

impl fmt::Display for DispatcherHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "watcher-DispatcherHandle({})", self.name,)
    }
}

impl DispatcherHandle {
    pub fn new(handle: GenericDispatcherHandle<WatchTypes>, id: impl ToString) -> Self {
        let h = DispatcherHandle {
            handle,
            name: id.to_string(),
            close_generation: watch::channel(0).0,
        };
        debug!("{}: new", h);
        h
    }

    /// Return a future that completes when [`Self::close_all_watchers`] is called after this
    /// call returns, or when this handle is dropped. A watch stream ends when it completes.
    ///
    /// Call it under the state machine writer permit, as the snapshot installation calls
    /// `close_all_watchers`, so that a stream opened on a replaced state machine is closed.
    pub fn closed(&self) -> impl Future<Output = ()> + Send + 'static {
        let mut generation = self.close_generation.subscribe();
        async move {
            // An error means this handle is dropped: the stream receives no more changes.
            let _ = generation.changed().await;
        }
    }

    /// Close every watch stream: each stream ends at once and drops the events still queued for
    /// it, and its client watches again, with a new initial flush.
    ///
    /// The streams end without waiting for the dispatcher, which may be blocked sending to a
    /// watcher whose client does not read. The dispatcher drops their senders later.
    pub fn close_all_watchers(&self, reason: &'static str) {
        self.close_generation
            .send_modify(|generation| *generation += 1);

        let name = self.name.clone();
        self.request(move |dispatcher| {
            let senders = dispatcher
                .watch_senders()
                .into_iter()
                .cloned()
                .collect::<Vec<_>>();
            info!(
                "{}: close {} watch streams: {}",
                name,
                senders.len(),
                reason
            );
            for sender in senders {
                dispatcher.remove_watcher(sender);
            }
        });
    }
}
