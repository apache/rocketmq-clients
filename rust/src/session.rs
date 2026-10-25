/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use mockall::{automock, mock};
use ring::hmac;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::metadata::{AsciiMetadataValue, MetadataMap};
use tonic::transport::{Channel, Endpoint};

use crate::conf::ClientOption;
use crate::error::ErrorKind;
use crate::model::common::Endpoints;
use crate::pb::telemetry_command::Command;
use crate::pb::{
    AckMessageRequest, AckMessageResponse, ChangeInvisibleDurationRequest,
    ChangeInvisibleDurationResponse, EndTransactionRequest, EndTransactionResponse,
    ForwardMessageToDeadLetterQueueRequest, ForwardMessageToDeadLetterQueueResponse,
    HeartbeatRequest, HeartbeatResponse, NotifyClientTerminationRequest,
    NotifyClientTerminationResponse, QueryAssignmentRequest, QueryAssignmentResponse,
    QueryRouteRequest, QueryRouteResponse, RecallMessageRequest, RecallMessageResponse,
    ReceiveMessageRequest, ReceiveMessageResponse, SendMessageRequest, SendMessageResponse,
    TelemetryCommand,
};
use crate::util::{PROTOCOL_VERSION, SDK_LANGUAGE, SDK_VERSION};
use crate::{error::ClientError, pb::messaging_service_client::MessagingServiceClient};
use tracing::{debug, error, info};

const OPERATION_START: &str = "session.start";
const OPERATION_UPDATE_SETTINGS: &str = "session.update_settings";

/// The long-polling timeout, shared between the client layer and every session.
///
/// The server can push a new value at any time through the telemetry stream, so it has to
/// live behind a lock instead of being baked into the immutable [`ClientOption`] clone each
/// session keeps. Both the gRPC deadline of a receive request and the request body itself
/// read from here.
pub(crate) type SharedLongPollingTimeout = Arc<parking_lot::RwLock<std::time::Duration>>;

const OPERATION_QUERY_ROUTE: &str = "rpc.query_route";
const OPERATION_HEARTBEAT: &str = "rpc.heartbeat";
const OPERATION_SEND_MESSAGE: &str = "rpc.send_message";
const OPERATION_RECALL_MESSAGE: &str = "rpc.recall_message";
const OPERATION_RECEIVE_MESSAGE: &str = "rpc.receive_message";
const OPERATION_ACK_MESSAGE: &str = "rpc.ack_message";
const OPERATION_CHANGE_INVISIBLE_DURATION: &str = "rpc.change_invisible_duration";
const OPERATION_END_TRANSACTION: &str = "rpc.end_transaction";
const OPERATION_NOTIFY_CLIENT_TERMINATION: &str = "rpc.notify_client_termination";
const OPERATION_QUERY_ASSIGNMENT: &str = "rpc.query_assignment";
const OPERATION_FORWARD_TO_DEADLETTER_QUEUE: &str = "rpc.forward_to_deadletter_queue";

/// Interval between HTTP/2 keep-alive pings.
///
/// The RocketMQ proxy allows client pings every 30 seconds by default
/// (`ProxyConfig#grpcServerPermitKeepAliveTimeMillis = 30000`; the bare grpc-java default is
/// 5 minutes, but the proxy overrides it), so the interval must stay above that or the
/// server answers with `GOAWAY(ENHANCE_YOUR_CALM)`. 60s doubles the permitted minimum,
/// which detects a dead peer within ~90s instead of the ~330s a 300s interval would take.
const KEEP_ALIVE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

/// How long to wait for the keep-alive ping acknowledgement before considering the
/// connection dead and reconnecting.
/// Kept in line with the Java client (`RpcClientImpl.KEEP_ALIVE_TIMEOUT_MILLIS`).
const KEEP_ALIVE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

#[async_trait]
#[automock]
pub(crate) trait RPCClient {
    async fn query_route(
        &mut self,
        request: QueryRouteRequest,
    ) -> Result<QueryRouteResponse, ClientError>;
    async fn heartbeat(
        &mut self,
        request: HeartbeatRequest,
    ) -> Result<HeartbeatResponse, ClientError>;
    async fn send_message(
        &mut self,
        request: SendMessageRequest,
    ) -> Result<SendMessageResponse, ClientError>;
    async fn recall_message(
        &mut self,
        request: RecallMessageRequest,
    ) -> Result<RecallMessageResponse, ClientError>;
    async fn receive_message(
        &mut self,
        request: ReceiveMessageRequest,
    ) -> Result<Vec<ReceiveMessageResponse>, ClientError>;
    async fn ack_message(
        &mut self,
        request: AckMessageRequest,
    ) -> Result<AckMessageResponse, ClientError>;
    async fn change_invisible_duration(
        &mut self,
        request: ChangeInvisibleDurationRequest,
    ) -> Result<ChangeInvisibleDurationResponse, ClientError>;
    async fn end_transaction(
        &mut self,
        request: EndTransactionRequest,
    ) -> Result<EndTransactionResponse, ClientError>;
    async fn notify_shutdown(
        &mut self,
        request: NotifyClientTerminationRequest,
    ) -> Result<NotifyClientTerminationResponse, ClientError>;
    async fn query_assignment(
        &mut self,
        request: QueryAssignmentRequest,
    ) -> Result<QueryAssignmentResponse, ClientError>;
    async fn forward_to_deadletter_queue(
        &mut self,
        request: ForwardMessageToDeadLetterQueueRequest,
    ) -> Result<ForwardMessageToDeadLetterQueueResponse, ClientError>;
    async fn sync_lite_subscription(
        &mut self,
        request: crate::pb::SyncLiteSubscriptionRequest,
    ) -> Result<crate::pb::SyncLiteSubscriptionResponse, ClientError>;
}

#[derive(Debug)]
pub(crate) struct Session {
    client_id: String,
    option: ClientOption,
    endpoints: Endpoints,
    stub: MessagingServiceClient<Channel>,
    telemetry_tx: Option<mpsc::Sender<TelemetryCommand>>,
    telemetry_command_tx: Option<mpsc::Sender<Command>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    /// Milliseconds elapsed since [`Instant`] `SESSION_ACTIVITY_ORIGIN` at the time of the
    /// last request on this session, shared with every shadow session so that activity is
    /// recorded on the registered session as well.
    last_activity_millis: Arc<std::sync::atomic::AtomicU64>,
    /// Long-polling timeout shared with the client layer, see
    /// [`SharedLongPollingTimeout`].
    long_polling_timeout: SharedLongPollingTimeout,
    /// Notifies the client layer when the telemetry stream ends, so that it can schedule a
    /// proactive reconnect instead of waiting for the next periodic settings sync. Mirrors
    /// `ClientSessionImpl#renewRequestObserver` of the Java client, which renews the stream
    /// as soon as it is observed dead.
    stream_closed_tx: Option<mpsc::Sender<String>>,
}

/// Origin the session activity timestamps are measured from.
static SESSION_ACTIVITY_ORIGIN: once_cell::sync::Lazy<Instant> =
    once_cell::sync::Lazy::new(Instant::now);

fn now_millis() -> u64 {
    SESSION_ACTIVITY_ORIGIN.elapsed().as_millis() as u64
}

impl Session {
    const OPERATION_CREATE: &'static str = "session.create_session";

    const HTTP_SCHEMA: &'static str = "http";
    const HTTPS_SCHEMA: &'static str = "https";

    pub(crate) fn shadow_session(&self) -> Self {
        Session {
            client_id: self.client_id.clone(),
            option: self.option.clone(),
            endpoints: self.endpoints.clone(),
            stub: self.stub.clone(),
            telemetry_tx: self.telemetry_tx.clone(),
            telemetry_command_tx: self.telemetry_command_tx.clone(),
            shutdown_tx: None,
            last_activity_millis: Arc::clone(&self.last_activity_millis),
            long_polling_timeout: Arc::clone(&self.long_polling_timeout),
            stream_closed_tx: self.stream_closed_tx.clone(),
        }
    }

    pub(crate) async fn new(
        endpoints: &Endpoints,
        client_id: String,
        option: &ClientOption,
        long_polling_timeout: SharedLongPollingTimeout,
    ) -> Result<Self, ClientError> {
        let peer = endpoints.endpoint_url().to_owned();

        let mut channel_endpoints = Vec::new();
        for endpoint in endpoints.inner().addresses.clone() {
            channel_endpoints.push(Self::build_endpoint(endpoint.host, endpoint.port, option)?);
        }

        if channel_endpoints.is_empty() {
            return Err(ClientError::new(
                ErrorKind::Connect,
                "no endpoint available",
                Self::OPERATION_CREATE,
            )
            .with_context("peer", peer.clone()));
        }

        let channel = if channel_endpoints.len() == 1 {
            channel_endpoints[0].connect().await.map_err(|e| {
                ClientError::new(
                    ErrorKind::Connect,
                    "failed to connect to peer",
                    Self::OPERATION_CREATE,
                )
                .set_source(e)
                .with_context("peer", peer.clone())
            })?
        } else {
            Channel::balance_list(channel_endpoints.into_iter())
        };

        let stub = MessagingServiceClient::new(channel);

        info!("create session success");

        Ok(Session {
            client_id,
            option: option.clone(),
            endpoints: endpoints.clone(),
            stub,
            telemetry_tx: None,
            telemetry_command_tx: None,
            shutdown_tx: None,
            last_activity_millis: Arc::new(std::sync::atomic::AtomicU64::new(now_millis())),
            long_polling_timeout,
            stream_closed_tx: None,
        })
    }

    fn build_endpoint(
        host: String,
        port: i32,
        option: &ClientOption,
    ) -> Result<Endpoint, ClientError> {
        let url = if option.enable_tls() {
            format!("{}://{}:{}", Self::HTTPS_SCHEMA, host, port)
        } else {
            format!("{}://{}:{}", Self::HTTP_SCHEMA, host, port)
        };
        let endpoint = Endpoint::from_shared(url.clone())
            .map_err(|e| {
                ClientError::new(
                    ErrorKind::Connect,
                    "failed to create channel endpoint",
                    Self::OPERATION_CREATE,
                )
                .set_source(e)
                .with_context("peer", url)
            })?
            // TODO tls config
            // .tls_config(tls)
            // .map_err(|e| {
            //     ClientError::new(
            //         ErrorKind::Connect,
            //         "Failed to configure TLS.".to_string(),
            //         OPERATION,
            //     )
            //     .set_source(e)
            //     .with_context("peer", &peer_addr)
            // })?
            .connect_timeout(option.timeout)
            .tcp_nodelay(true)
            // Keep the long-lived HTTP/2 connection alive so that a rolling deployment of the
            // server does not silently break the telemetry stream, and so that a dead peer is
            // detected within `KEEP_ALIVE_TIMEOUT` instead of leaving a half-open connection.
            //
            // NOTE: `KEEP_ALIVE_INTERVAL` must not fall below the minimum ping interval the
            // server permits. The RocketMQ proxy defaults to 30s
            // (`ProxyConfig#grpcServerPermitKeepAliveTimeMillis`), so 60s keeps a safety
            // margin; lowering it further makes the server answer with
            // GOAWAY(ENHANCE_YOUR_CALM) and drop the connection.
            .http2_keep_alive_interval(KEEP_ALIVE_INTERVAL)
            .keep_alive_timeout(KEEP_ALIVE_TIMEOUT)
            // Ping even while no RPC is in flight: the telemetry stream is long-lived but is
            // idle between settings syncs, so without this the connection is never probed.
            .keep_alive_while_idle(true);
        Ok(endpoint)
    }

    pub(crate) fn peer(&self) -> &str {
        self.endpoints.endpoint_url()
    }

    fn sign<T>(&self, message: T) -> tonic::Request<T> {
        // Any request proves the session is in use, even when it goes through a shadow copy.
        self.touch();
        let mut request = tonic::Request::new(message);
        let metadata = request.metadata_mut();
        self.sign_without_timeout(metadata);
        request.set_timeout(*self.option.timeout());
        request
    }

    fn sign_without_timeout(&self, metadata: &mut MetadataMap) {
        let _ = AsciiMetadataValue::try_from(&self.client_id)
            .map(|v| metadata.insert("x-mq-client-id", v));

        metadata.insert(
            "x-mq-language",
            AsciiMetadataValue::from_static(SDK_LANGUAGE.as_str_name()),
        );
        metadata.insert(
            "x-mq-client-version",
            AsciiMetadataValue::from_static(SDK_VERSION),
        );
        metadata.insert(
            "x-mq-protocol-version",
            AsciiMetadataValue::from_static(PROTOCOL_VERSION),
        );

        // UTC, like the Java client (`Signature#DATE_TIME_FORMAT` ends with 'Z'). Do not use
        // `OffsetDateTime::now_local()`: in a multi-threaded process it reads /proc/self/stat on
        // every call to count the threads and then fails, so it costs ~14µs per request only to
        // fall back to UTC anyway.
        let date_time = OffsetDateTime::now_utc().format(&Rfc3339).unwrap();

        metadata.insert(
            "x-mq-date-time",
            AsciiMetadataValue::try_from(&date_time).unwrap(),
        );

        if let Some((access_key, access_secret)) =
            self.option.access_key().zip(self.option.secret_key())
        {
            let key = hmac::Key::new(
                hmac::HMAC_SHA1_FOR_LEGACY_USE_ONLY,
                access_secret.as_bytes(),
            );
            let signature = hmac::sign(&key, date_time.as_bytes());
            let signature = hex::encode(signature.as_ref());
            let authorization = format!(
                "MQv2-HMAC-SHA1 Credential={access_key}, SignedHeaders=x-mq-date-time, Signature={signature}"
            );
            metadata.insert(
                "authorization",
                AsciiMetadataValue::try_from(authorization).unwrap(),
            );
        }
    }

    pub(crate) async fn start(
        &mut self,
        settings: TelemetryCommand,
        telemetry_command_tx: mpsc::Sender<Command>,
    ) -> Result<(), ClientError> {
        self.telemetry_command_tx = Some(telemetry_command_tx);

        self.establish_telemetry_stream(settings).await?;

        debug!("telemetry_command_tx: {:?}", self.telemetry_command_tx);
        info!("starting client success");
        Ok(())
    }

    async fn establish_telemetry_stream(
        &mut self,
        settings: TelemetryCommand,
    ) -> Result<(), ClientError> {
        if let Some(old_shutdown_tx) = self.shutdown_tx.take() {
            // a `send` error means the receiver is already gone, which is fine.
            let _ = old_shutdown_tx.send(());
            info!("sent shutdown signal to the previous telemetry stream.");
        }

        let (tx, rx) = mpsc::channel(16);
        tx.send(settings).await.map_err(|e| {
            ClientError::new(
                ErrorKind::ChannelSend,
                "failed to send telemetry command",
                OPERATION_START,
            )
            .set_source(e)
        })?;

        let mut request = tonic::Request::new(ReceiverStream::new(rx));
        self.sign_without_timeout(request.metadata_mut());
        let response = self.stub.telemetry(request).await.map_err(|e| {
            ClientError::new(
                ErrorKind::ClientInternal,
                "send rpc telemetry failed",
                OPERATION_START,
            )
            .set_source(e)
        })?;

        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();

        self.shutdown_tx = Some(shutdown_tx);
        self.telemetry_tx = Some(tx);

        let telemetry_command_tx = self.telemetry_command_tx.as_ref().unwrap().clone();
        let stream_closed_tx = self.stream_closed_tx.clone();
        let peer = self.peer().to_string();
        tokio::spawn(async move {
            let mut stream = response.into_inner();
            loop {
                tokio::select! {
                    message = stream.message() => {
                        match message {
                            Ok(Some(item)) => {
                                debug!("receive telemetry command: {:?}", item);
                                if let Some(command) = item.command {
                                    _ = telemetry_command_tx.send(command).await;
                                }
                            }
                            Ok(None) => {
                                info!("telemetry command stream closed by server");
                                if let Some(tx) = stream_closed_tx {
                                    // Tell the client layer so that it re-establishes the
                                    // stream right away instead of waiting for the next
                                    // periodic settings sync.
                                    let _ = tx.send(peer.clone()).await;
                                }
                                break;
                            }
                            Err(e) => {
                                error!("telemetry response error: {:?}", e);
                                if let Some(tx) = stream_closed_tx {
                                    let _ = tx.send(peer.clone()).await;
                                }
                                break;
                            }
                        }
                    }
                    _ = &mut shutdown_rx => {
                        info!("receive shutdown signal, stop dealing with telemetry command");
                        break;
                    }
                }
            }
            // telemetry tx will be dropped here
        });
        debug!("start session success");

        Ok(())
    }

    pub(crate) fn shutdown(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }

    /// Register the channel the stream-end notification is sent through.
    ///
    /// Must be called before [`Self::start`], because the telemetry stream task captures
    /// the sender when it is spawned.
    pub(crate) fn set_stream_closed_tx(&mut self, tx: mpsc::Sender<String>) {
        self.stream_closed_tx = Some(tx);
    }

    /// Unconditionally re-establish the telemetry stream on this session.
    ///
    /// Unlike [`Self::update_settings`], which only rebuilds the stream when it observes a
    /// closed sender, this is called by the client layer when the server closed the stream
    /// (or when a heartbeat error reveals the server no longer knows this client):
    /// re-establishing the stream also re-registers the client with the (possibly new)
    /// server instance.
    pub(crate) async fn reconnect(
        &mut self,
        settings: TelemetryCommand,
    ) -> Result<(), ClientError> {
        info!("re-establishing telemetry stream, peer={}", self.peer());
        self.establish_telemetry_stream(settings).await
    }

    pub(crate) fn is_started(&self) -> bool {
        self.telemetry_tx.is_some() && !self.telemetry_tx.as_ref().unwrap().is_closed()
    }

    /// Whether the session still owns a live telemetry stream.
    ///
    /// A session whose telemetry stream is gone can no longer receive server-push
    /// commands (settings, reconnect, unsubscribe, ...), so it must not be kept in the
    /// session map: keeping it makes every settings sync fail on a dead endpoint and
    /// blocks the long-lived connection from ever being re-established.
    pub(crate) fn is_healthy(&self) -> bool {
        self.is_started()
    }

    /// Record activity on this session so that it is not reaped while still in use.
    pub(crate) fn touch(&self) {
        self.last_activity_millis
            .store(now_millis(), std::sync::atomic::Ordering::Relaxed);
    }

    /// How long this session has been idle.
    pub(crate) fn idle_duration(&self) -> std::time::Duration {
        let last = self
            .last_activity_millis
            .load(std::sync::atomic::Ordering::Relaxed);
        std::time::Duration::from_millis(now_millis().saturating_sub(last))
    }

    /// Deadline applied to a receive request.
    ///
    /// The request itself carries `long_polling_timeout`, so the server may hold the
    /// response for exactly that long before answering. The gRPC deadline therefore has to
    /// leave room for the request tolerance on top of it, which is what
    /// `ClientOption::set_long_polling_timeout` documents and what the Java client does
    /// (`awaitDuration.plus(tolerance)`).
    ///
    /// Using `long_polling_timeout` alone races the server response: the client gives up
    /// right as the answer arrives, so every poll fails once `long_polling_timeout` is short
    /// (a few seconds or less) even though the server behaves correctly.
    pub(crate) fn receive_deadline(&self) -> std::time::Duration {
        *self.long_polling_timeout.read() + *self.option.timeout()
    }

    pub(crate) async fn update_settings(
        &mut self,
        settings: TelemetryCommand,
    ) -> Result<(), ClientError> {
        if self.is_started() {
            debug!("session is already started: {:?}", self.telemetry_tx);
            if let Some(tx) = self.telemetry_tx.as_ref() {
                tx.send(settings).await.map_err(|e| {
                    ClientError::new(
                        ErrorKind::ChannelSend,
                        "failed to send telemetry command",
                        OPERATION_UPDATE_SETTINGS,
                    )
                    .set_source(e)
                })?;
            }
            Ok(())
        } else {
            debug!("session is closed: {:?}", self.telemetry_tx);
            self.establish_telemetry_stream(settings).await?;
            debug!(
                "session is established, closed: {:?}",
                self.telemetry_tx.as_ref().unwrap().is_closed()
            );
            Ok(())
        }
    }
}

#[async_trait]
impl RPCClient for Session {
    async fn query_route(
        &mut self,
        request: QueryRouteRequest,
    ) -> Result<QueryRouteResponse, ClientError> {
        let request = self.sign(request);
        let response = self.stub.query_route(request).await.map_err(|e| {
            ClientError::new(
                ErrorKind::ClientInternal,
                "send rpc query_route failed",
                OPERATION_QUERY_ROUTE,
            )
            .set_source(e)
        })?;
        Ok(response.into_inner())
    }

    async fn heartbeat(
        &mut self,
        request: HeartbeatRequest,
    ) -> Result<HeartbeatResponse, ClientError> {
        let request = self.sign(request);
        let response = self
            .stub
            .heartbeat(request)
            .await
            .map_err(|e| match e.code() {
                tonic::Code::Unavailable => ClientError::new(
                    ErrorKind::ServerUnavailable,
                    "server unavailable",
                    OPERATION_HEARTBEAT,
                )
                .set_source(e),
                _ => ClientError::new(
                    ErrorKind::ClientInternal,
                    "send rpc heartbeat failed",
                    OPERATION_HEARTBEAT,
                )
                .set_source(e),
            })?;
        Ok(response.into_inner())
    }

    async fn send_message(
        &mut self,
        request: SendMessageRequest,
    ) -> Result<SendMessageResponse, ClientError> {
        let request = self.sign(request);
        let response = self.stub.send_message(request).await.map_err(|e| {
            ClientError::new(
                ErrorKind::ClientInternal,
                "send rpc send_message failed",
                OPERATION_SEND_MESSAGE,
            )
            .set_source(e)
        })?;
        Ok(response.into_inner())
    }

    async fn recall_message(
        &mut self,
        request: RecallMessageRequest,
    ) -> Result<RecallMessageResponse, ClientError> {
        let request = self.sign(request);
        let response = self.stub.recall_message(request).await.map_err(|e| {
            ClientError::new(
                ErrorKind::ClientInternal,
                "send rpc recall_message failed",
                OPERATION_RECALL_MESSAGE,
            )
            .set_source(e)
        })?;
        Ok(response.into_inner())
    }

    async fn receive_message(
        &mut self,
        request: ReceiveMessageRequest,
    ) -> Result<Vec<ReceiveMessageResponse>, ClientError> {
        let batch_size = request.batch_size;
        let mut request = self.sign(request);
        request.set_timeout(self.receive_deadline());
        let mut stream = self
            .stub
            .receive_message(request)
            .await
            .map_err(|e| match e.code() {
                tonic::Code::Unavailable => ClientError::new(
                    ErrorKind::ServerUnavailable,
                    "server unavailable",
                    OPERATION_RECEIVE_MESSAGE,
                )
                .set_source(e),
                _ => ClientError::new(
                    ErrorKind::ClientInternal,
                    "send rpc receive_message failed",
                    OPERATION_RECEIVE_MESSAGE,
                )
                .set_source(e),
            })?
            .into_inner();

        let mut responses = Vec::with_capacity(batch_size as usize);
        while let Some(item) = stream.next().await {
            let response = item.map_err(|e| {
                ClientError::new(
                    ErrorKind::ClientInternal,
                    "receive message failed: error in reading stream",
                    OPERATION_RECEIVE_MESSAGE,
                )
                .set_source(e)
            })?;
            responses.push(response);
        }
        Ok(responses)
    }

    async fn ack_message(
        &mut self,
        request: AckMessageRequest,
    ) -> Result<AckMessageResponse, ClientError> {
        let request = self.sign(request);
        let response = self.stub.ack_message(request).await.map_err(|e| {
            ClientError::new(
                ErrorKind::ClientInternal,
                "send rpc ack_message failed",
                OPERATION_ACK_MESSAGE,
            )
            .set_source(e)
        })?;
        Ok(response.into_inner())
    }

    async fn change_invisible_duration(
        &mut self,
        request: ChangeInvisibleDurationRequest,
    ) -> Result<ChangeInvisibleDurationResponse, ClientError> {
        let request = self.sign(request);
        let response = self
            .stub
            .change_invisible_duration(request)
            .await
            .map_err(|e| {
                ClientError::new(
                    ErrorKind::ClientInternal,
                    "send rpc change_invisible_duration failed",
                    OPERATION_CHANGE_INVISIBLE_DURATION,
                )
                .set_source(e)
            })?;
        Ok(response.into_inner())
    }

    async fn end_transaction(
        &mut self,
        request: EndTransactionRequest,
    ) -> Result<EndTransactionResponse, ClientError> {
        let request = self.sign(request);
        let response = self.stub.end_transaction(request).await.map_err(|e| {
            ClientError::new(
                ErrorKind::ClientInternal,
                "send rpc end_transaction failed",
                OPERATION_END_TRANSACTION,
            )
            .set_source(e)
        })?;
        Ok(response.into_inner())
    }

    async fn notify_shutdown(
        &mut self,
        request: NotifyClientTerminationRequest,
    ) -> Result<NotifyClientTerminationResponse, ClientError> {
        let request = self.sign(request);
        let response = self
            .stub
            .notify_client_termination(request)
            .await
            .map_err(|e| {
                ClientError::new(
                    ErrorKind::ClientInternal,
                    "send rpc notify_client_termination failed",
                    OPERATION_NOTIFY_CLIENT_TERMINATION,
                )
                .set_source(e)
            })?;
        Ok(response.into_inner())
    }

    async fn query_assignment(
        &mut self,
        request: QueryAssignmentRequest,
    ) -> Result<QueryAssignmentResponse, ClientError> {
        let request = self.sign(request);
        let response = self.stub.query_assignment(request).await.map_err(|e| {
            ClientError::new(
                ErrorKind::ClientInternal,
                "send rpc query_assignment failed",
                OPERATION_QUERY_ASSIGNMENT,
            )
            .set_source(e)
        })?;
        Ok(response.into_inner())
    }

    async fn forward_to_deadletter_queue(
        &mut self,
        request: ForwardMessageToDeadLetterQueueRequest,
    ) -> Result<ForwardMessageToDeadLetterQueueResponse, ClientError> {
        let request = self.sign(request);
        let response = self
            .stub
            .forward_message_to_dead_letter_queue(request)
            .await
            .map_err(|e| {
                ClientError::new(
                    ErrorKind::ClientInternal,
                    "send rpc forward_to_deadletter_queue failed",
                    OPERATION_FORWARD_TO_DEADLETTER_QUEUE,
                )
                .set_source(e)
            })?;
        Ok(response.into_inner())
    }

    async fn sync_lite_subscription(
        &mut self,
        request: crate::pb::SyncLiteSubscriptionRequest,
    ) -> Result<crate::pb::SyncLiteSubscriptionResponse, ClientError> {
        let request = self.sign(request);
        let response = self
            .stub
            .sync_lite_subscription(request)
            .await
            .map_err(|e| {
                ClientError::new(
                    ErrorKind::ClientInternal,
                    "send rpc sync_lite_subscription failed",
                    "sync_lite_subscription",
                )
                .set_source(e)
            })?;
        Ok(response.into_inner())
    }
}

mock! {
    #[derive(Debug)]
    pub(crate) Session {
        pub(crate) fn shadow_session(&self) -> Self;
        pub(crate) async fn new(
            endpoints: &Endpoints,
            client_id: String,
            option: &ClientOption,
            long_polling_timeout: SharedLongPollingTimeout,
        ) -> Result<Self, ClientError>;
        pub(crate) fn peer(&self) -> &str;
        pub(crate) fn is_started(&self) -> bool;
        pub(crate) fn is_healthy(&self) -> bool;
        pub(crate) fn touch(&self);
        pub(crate) fn idle_duration(&self) -> std::time::Duration;
        pub(crate) fn set_stream_closed_tx(&mut self, tx: mpsc::Sender<String>);
        pub(crate) async fn reconnect(
            &mut self,
            settings: TelemetryCommand,
        ) -> Result<(), ClientError>;
        pub(crate) async fn update_settings(
            &mut self,
            settings: TelemetryCommand,
        ) -> Result<(), ClientError>;
        pub(crate) async fn start(
            &mut self,
            settings: TelemetryCommand,
            telemetry_command_tx: mpsc::Sender<Command>,
        ) -> Result<(), ClientError>;
        pub(crate) fn shutdown(&mut self);
    }

    #[async_trait]
    impl RPCClient for Session {
        async fn query_route(
            &mut self,
            request: QueryRouteRequest,
        ) -> Result<QueryRouteResponse, ClientError>;
        async fn heartbeat(
            &mut self,
            request: HeartbeatRequest,
        ) -> Result<HeartbeatResponse, ClientError>;
        async fn send_message(
            &mut self,
            request: SendMessageRequest,
        ) -> Result<SendMessageResponse, ClientError>;
        async fn receive_message(
            &mut self,
            request: ReceiveMessageRequest,
        ) -> Result<Vec<ReceiveMessageResponse>, ClientError>;
        async fn ack_message(
            &mut self,
            request: AckMessageRequest,
        ) -> Result<AckMessageResponse, ClientError>;
        async fn change_invisible_duration(
            &mut self,
            request: ChangeInvisibleDurationRequest,
        ) -> Result<ChangeInvisibleDurationResponse, ClientError>;
        async fn end_transaction(
            &mut self,
            request: EndTransactionRequest,
        ) -> Result<EndTransactionResponse, ClientError>;
        async fn notify_shutdown(
            &mut self,
            request: NotifyClientTerminationRequest,
        ) -> Result<NotifyClientTerminationResponse, ClientError>;
        async fn query_assignment(
            &mut self,
            request: QueryAssignmentRequest,
        ) -> Result<QueryAssignmentResponse, ClientError>;
        async fn forward_to_deadletter_queue(
            &mut self,
            request: ForwardMessageToDeadLetterQueueRequest,
        ) -> Result<ForwardMessageToDeadLetterQueueResponse, ClientError>;
        async fn recall_message(
            &mut self,
            request: RecallMessageRequest,
        ) -> Result<RecallMessageResponse, ClientError>;
        async fn sync_lite_subscription(
            &mut self,
            request: crate::pb::SyncLiteSubscriptionRequest,
        ) -> Result<crate::pb::SyncLiteSubscriptionResponse, ClientError>;
    }

}

#[cfg(test)]
mod tests {
    use wiremock_grpc::generate;

    use super::*;

    generate!("apache.rocketmq.v2", RocketMQMockServer);

    #[tokio::test]
    async fn session_new() {
        let server = RocketMQMockServer::start_default().await;
        let session = Session::new(
            &Endpoints::from_url(&format!("localhost:{}", server.address().port())).unwrap(),
            "test_client".to_string(),
            &ClientOption::default(),
            Arc::new(parking_lot::RwLock::new(std::time::Duration::from_secs(40))),
        )
        .await;
        debug!("session: {:?}", session);
        assert!(session.is_ok());
    }

    #[tokio::test]
    async fn session_new_multi_addr() {
        let session = Session::new(
            &Endpoints::from_url("127.0.0.1:8080,127.0.0.1:8081").unwrap(),
            "test_client".to_string(),
            &ClientOption::default(),
            Arc::new(parking_lot::RwLock::new(std::time::Duration::from_secs(40))),
        )
        .await;
        debug!("session: {:?}", session);
        assert!(session.is_ok());
    }

    #[tokio::test]
    async fn sign_sets_utc_date_time() {
        let mut option = ClientOption::default();
        option.set_access_key("ak");
        option.set_secret_key("sk");
        let server = RocketMQMockServer::start_default().await;
        let session = Session::new(
            &Endpoints::from_url(&format!("localhost:{}", server.address().port())).unwrap(),
            "test_client".to_string(),
            &option,
            Arc::new(parking_lot::RwLock::new(std::time::Duration::from_secs(40))),
        )
        .await
        .unwrap();
        let today = OffsetDateTime::now_utc().date();
        let request = session.sign(());

        let date_time = request
            .metadata()
            .get("x-mq-date-time")
            .unwrap()
            .to_str()
            .unwrap();
        // RFC 3339 in UTC, like the Java client: "2026-01-02T03:04:05.123456789Z".
        assert!(date_time.ends_with('Z'), "not UTC: {date_time}");
        assert!(
            date_time.starts_with(&format!(
                "{:04}-{:02}-{:02}T",
                today.year(),
                u8::from(today.month()),
                today.day()
            )),
            "{date_time}"
        );
        // The signature covers exactly this header.
        let authorization = request
            .metadata()
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap();
        let key = hmac::Key::new(hmac::HMAC_SHA1_FOR_LEGACY_USE_ONLY, b"sk");
        let signature = hex::encode(hmac::sign(&key, date_time.as_bytes()).as_ref());
        assert!(
            authorization.ends_with(&format!("Signature={signature}")),
            "{authorization}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn session_start() {
        // TODO: add grpc bi-stream test
    }

    #[tokio::test]
    async fn receive_deadline_leaves_room_for_the_server_response() {
        let mut option = ClientOption::default();
        // A short long polling timeout used to make every poll fail, because the gRPC
        // deadline was exactly the time the server is allowed to hold the response.
        option.long_polling_timeout = std::time::Duration::from_secs(3);
        option.timeout = std::time::Duration::from_secs(3);

        let server = RocketMQMockServer::start_default().await;
        // The deadline is driven by the shared handle, which the server can update through
        // the telemetry stream at any time.
        let long_polling_timeout =
            Arc::new(parking_lot::RwLock::new(std::time::Duration::from_secs(3)));
        let session = Session::new(
            &Endpoints::from_url(&format!("localhost:{}", server.address().port())).unwrap(),
            "test_client".to_string(),
            &option,
            Arc::clone(&long_polling_timeout),
        )
        .await
        .unwrap();

        assert_eq!(
            session.receive_deadline(),
            std::time::Duration::from_secs(6),
            "the deadline must be long_polling_timeout + timeout, as documented"
        );
        assert!(
            session.receive_deadline() > *option.long_polling_timeout(),
            "the deadline must be longer than the server side long polling"
        );

        // A server-pushed update must be picked up without recreating the session.
        *long_polling_timeout.write() = std::time::Duration::from_secs(10);
        assert_eq!(
            session.receive_deadline(),
            std::time::Duration::from_secs(13),
            "the deadline must follow the shared long_polling_timeout"
        );
    }
}
