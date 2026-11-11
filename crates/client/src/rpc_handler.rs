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

use std::error::Error;
use std::fmt;
use std::fmt::Display;
use std::time::Duration;
use std::time::Instant;

use anyerror::AnyError;
use databend_base::futures::ElapsedFutureExt;
use databend_meta_runtime_api::RuntimeApi;
use databend_meta_types::ConnectionError;
use databend_meta_types::MetaClientError;
use databend_meta_types::MetaNetworkError;
use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;
use futures::stream::BoxStream;
use log::debug;
use log::info;
use log::warn;
use tonic::Code;
use tonic::Response;
use tonic::Status;

use crate::MetaGrpcClient;
use crate::established_client::EstablishedClient;

/// Represents the action to take after processing an RPC response.
///
/// This enum indicates whether the RPC should be retried due to a transient error,
/// or if it succeeded and should return the response.
#[derive(Debug, Clone)]
pub(crate) enum ResponseAction<T> {
    /// The RPC failed with a retryable error and should be attempted again
    ShouldRetry,
    /// The RPC succeeded and contains the response data
    Success(T),
}

/// Handles RPC communication with the meta-service, including connection management,
/// error tracking, and retry logic.
///
/// This handler manages the lifecycle of RPC calls to the meta-service, tracking
/// failed attempts, establishing connections, and determining when operations
/// should be retried vs. when they should fail permanently.
///
/// # Example
/// ```ignore
/// let mut handler = RpcHandler::new(&client, feature_spec);
/// let established = handler.new_established_client().await?;
/// let result = handler.process_response_result(&request, rpc_result)?;
/// ```
pub(crate) struct RpcHandler<'a, RT: RuntimeApi> {
    /// Reference to the gRPC client for meta-service communication
    pub(crate) client: &'a MetaGrpcClient<RT>,

    /// History of failed RPC attempts, storing endpoint description and error details.
    /// Used for error reporting and debugging connection issues.
    pub(crate) rpc_failures: Vec<(String, Status)>,

    /// Currently established connection and its start time.
    /// The client and start time are paired together to ensure consistent timing tracking.
    pub(crate) established_client: Option<(EstablishedClient, Instant)>,

    /// Only reads may additionally retry a locally generated h2 GOAWAY.
    read_only: bool,
}

impl<'a, RT: RuntimeApi> RpcHandler<'a, RT> {
    /// Creates a new RPC handler for the given client and feature requirements.
    pub(crate) fn new(client: &'a MetaGrpcClient<RT>) -> Self {
        Self {
            client,
            rpc_failures: Vec::new(),
            established_client: None,
            read_only: false,
        }
    }

    pub(crate) fn new_read(client: &'a MetaGrpcClient<RT>) -> Self {
        Self {
            read_only: true,
            ..Self::new(client)
        }
    }

    /// Establishes a new connection to the meta-service and validates feature compatibility.
    ///
    /// This method will:
    /// 1. Get an established client from the underlying gRPC client
    /// 2. Validate that the server supports the required features
    /// 3. Store the validated client for future use
    ///
    /// # Returns
    /// A mutable reference to the established client that can be used for RPCs
    ///
    /// # Errors
    /// Returns `MetaClientError` if:
    /// - Connection establishment fails
    /// - Feature validation fails
    /// - Server doesn't support required features
    pub(crate) async fn new_established_client(
        &mut self,
    ) -> Result<&mut EstablishedClient, MetaClientError> {
        if self.read_only && !self.rpc_failures.is_empty() {
            // The caller bounds both attempt count and total elapsed time.
            let attempt = self.rpc_failures.len().min(4) as u32;
            tokio::time::sleep(Duration::from_millis(10 * (1 << (attempt - 1)))).await;
        }

        let client = self
            .client
            .get_established_client()
            .inspect_elapsed_over(
                default_timing_threshold(),
                create_timing_logger("MetaGrpcClient::get_established_client"),
            )
            .await?;

        self.established_client = Some((client, Instant::now()));

        Ok(&mut self.established_client.as_mut().unwrap().0)
    }

    /// Processes an RPC response and determines the appropriate action.
    ///
    /// This method analyzes the RPC result and decides whether to:
    /// - Retry the operation (for transient errors)
    /// - Return success (for successful responses)
    /// - Fail permanently (for non-retryable errors)
    ///
    /// For retryable errors, it will:
    /// - Log the failure
    /// - Record the failure for error reporting
    /// - Switch to the next endpoint for retry
    ///
    /// The established client is consumed by this method to prevent accidental reuse.
    /// For retry attempts, a new client must be established via `new_established_client()`.
    ///
    /// # Returns
    /// - `Ok(ResponseAction::ShouldRetry)` for retryable errors
    /// - `Ok(ResponseAction::Success(response))` for successful responses
    /// - `Err(status)` for non-retryable errors
    pub(crate) fn process_response_result<R, T>(
        &mut self,
        request: &R,
        result: Result<Response<T>, Status>,
    ) -> Result<ResponseAction<Response<T>>, Status>
    where
        R: fmt::Debug,
        T: fmt::Debug,
    {
        let (established_client, start_time) = self
            .established_client
            .take()
            .expect("established client should be set before processing response");

        let elapsed = start_time.elapsed();

        debug!(
            "MetaGrpcClient {}-th try: elapsed: {:?}; with {}; result: {:?}",
            self.rpc_failures.len(),
            elapsed,
            established_client,
            result
        );

        let status = match result {
            Err(e) => e,
            Ok(x) => {
                // Log slow requests even on success
                if elapsed > default_timing_threshold() {
                    warn!(
                        "MetaGrpcClient: done slowly: elapsed: {:?}; with {}; request: {:?}",
                        elapsed, established_client, request
                    );
                }
                return Ok(ResponseAction::Success(x));
            }
        };

        if is_status_retryable(&status) || (self.read_only && is_local_h2_goaway(&status)) {
            established_client.set_error(status.clone());
            let client_display = established_client.to_string();

            warn!(
                "MetaGrpcClient retryable error: elapsed: {:?}; error: {:?}; with {}; request: {:?}",
                elapsed, status, client_display, request
            );

            self.rpc_failures.push((client_display, status));

            established_client.rotate_failing_target();

            Ok(ResponseAction::ShouldRetry)
        } else {
            warn!(
                "MetaGrpcClient non-retryable error: elapsed: {:?}; error: {:?}; with {}; request: {:?}",
                elapsed, status, established_client, request
            );

            Err(status)
        }
    }

    /// Creates a comprehensive network error after all retry attempts have failed.
    ///
    /// This method aggregates all the connection failures that occurred during
    /// retry attempts and creates a detailed error message that includes:
    /// - The number of retry attempts made
    /// - Details of each failure
    /// - The RPC operation that was being attempted
    pub(crate) fn create_network_error(&self) -> MetaNetworkError {
        let conn_err = ConnectionError::new(
            AnyError::error(format_args!(
                "after {} retries: {:?}",
                self.rpc_failures.len(),
                self.rpc_failures
            )),
            "failed to send RPC to meta-service",
        );

        MetaNetworkError::ConnectionError(conn_err)
    }
}

/// Returns the default timing threshold for RPC operations.
fn default_timing_threshold() -> Duration {
    Duration::from_millis(300)
}

/// Creates a closure that logs timing information for RPC operations.
fn create_timing_logger<T>(msg: impl Display) -> impl Fn(&T, Duration, Duration) {
    move |_output, total, busy| {
        info!("{} spent: total: {:?}, busy: {:?}", msg, total, busy);
    }
}

/// Retire failed connections without replaying values already visible to callers.
pub(crate) fn observe_read_stream(
    stream: impl Stream<Item = Result<crate::types::protobuf::StreamItem, Status>> + Send + 'static,
    connection: EstablishedClient,
) -> BoxStream<'static, Result<crate::types::protobuf::StreamItem, Status>> {
    stream
        .inspect_err(move |status| {
            if is_local_h2_goaway(status) {
                connection.set_error(status.clone());
            }
        })
        .boxed()
}

/// Recognize connection failure, not an application's resource-limit status.
/// Do not match error text: only a locally generated h2 GOAWAY qualifies.
pub(crate) fn is_local_h2_goaway(status: &Status) -> bool {
    if status.code() != Code::ResourceExhausted {
        return false;
    }
    let mut source = status.source();
    while let Some(error) = source {
        if let Some(error) = error.downcast_ref::<h2::Error>() {
            return error.is_library()
                && error.is_go_away()
                && error.reason() == Some(h2::Reason::ENHANCE_YOUR_CALM);
        }
        source = error.source();
    }
    false
}

/// Determines if a gRPC status code indicates a retryable error.
fn is_status_retryable(status: &Status) -> bool {
    matches!(
        status.code(),
        Code::Unauthenticated | Code::Unavailable | Code::Internal | Code::Cancelled
    )
}
