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
use std::clone::Clone;
use std::fmt::Debug;
use std::string::ToString;
use std::{collections::HashMap, sync::atomic::AtomicUsize, sync::Arc};

use mockall::automock;
use mockall_double::double;
use once_cell::sync::Lazy;
use parking_lot::{Mutex, RwLock};
use prost_types::Duration;
use tokio::select;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use tracing::{debug, error, info, warn};

use crate::conf::ClientOption;
use crate::error::{ClientError, ErrorKind};
use crate::model::common::{ClientType, Endpoints, Route, RouteStatus, SendReceipt};
use crate::model::message::AckMessageEntry;
use crate::pb;
use crate::pb::receive_message_response::Content;
use crate::pb::{
    telemetry_command::Command, AckMessageRequest, AckMessageResultEntry,
    ChangeInvisibleDurationRequest, FilterExpression, HeartbeatRequest, HeartbeatResponse, Message,
    MessageQueue, NotifyClientTerminationRequest, QueryRouteRequest, RecallMessageRequest,
    RecallMessageResponse, ReceiveMessageRequest, Resource, SendMessageRequest, Status,
    TelemetryCommand,
};
#[double]
use crate::session::Session;
use crate::session::{RPCClient, SharedLongPollingTimeout};
use crate::util::{
    build_ack_message_entry, handle_receive_message_status, handle_response_status,
    select_message_queue,
};

#[derive(Debug)]
pub(crate) struct Client {
    option: ClientOption,
    session_manager: Arc<SessionManager>,
    route_manager: TopicRouteManager,
    id: String,
    access_endpoints: Endpoints,
    settings: TelemetryCommand,
    telemetry_command_tx: Option<mpsc::Sender<pb::telemetry_command::Command>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    /// Long-polling timeout, shared with every session so that a server-pushed settings
    /// update takes effect on the receive deadline and request body without recreating
    /// anything.
    long_polling_timeout: SharedLongPollingTimeout,
}

#[derive(Debug, Clone)]
pub(crate) struct TopicRouteManager {
    route_table: Arc<Mutex<HashMap<String /* topic */, RouteStatus>>>,
    access_endpoints: Endpoints,
    namespace: String,
}

static CLIENT_ID_SEQUENCE: Lazy<AtomicUsize> = Lazy::new(|| AtomicUsize::new(0));

const OPERATION_CLIENT_NEW: &str = "client.new";
const OPERATION_CLIENT_START: &str = "client.start";
const OPERATION_CLIENT_SHUTDOWN: &str = "client.shutdown";
const OPERATION_GET_SESSION: &str = "client.get_session";
const OPERATION_QUERY_ROUTE: &str = "client.query_route";
const OPERATION_HEARTBEAT: &str = "client.heartbeat";
const OPERATION_SEND_MESSAGE: &str = "client.send_message";
const OPERATION_RECEIVE_MESSAGE: &str = "client.receive_message";
const OPERATION_ACK_MESSAGE: &str = "client.ack_message";

/// Maximum duration a session may stay idle before it is dropped from the session map.
///
/// A session is re-created on demand, so reaping idle ones only costs a re-connect for
/// endpoints that are used again. Mirrors `ClientManagerImpl#RPC_CLIENT_MAX_IDLE_DURATION`
/// of the Java client.
const SESSION_MAX_IDLE_DURATION: std::time::Duration = std::time::Duration::from_secs(30 * 60);

/// Interval of the housekeeping task that drops dead and idle sessions.
///
/// It is deliberately shorter than the settings/heartbeat intervals so that a dead endpoint
/// is removed before the next report cycle tries to use it again.
const SESSION_CLEANUP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

/// Period between heartbeats. Kept in line with the Java client
/// (`ClientImpl#HEART_BEAT_PERIOD`).
const HEART_BEAT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);

/// Number of consecutive heartbeat transport failures before the session is evicted.
/// Kept in line with the Java client (`ClientImpl#HEART_BEAT_FAILURE_THRESHOLD`), which
/// tolerates a transient blip instead of tearing the connection down at the first failure.
const HEART_BEAT_FAILURE_THRESHOLD: u32 = 2;

/// Initial delay before the first reconnect attempt after the telemetry stream was
/// observed dead. Kept in line with the Java client
/// (`ClientSessionImpl#REQUEST_OBSERVER_RENEW_BACKOFF_DELAY`).
const RECONNECT_BACKOFF_DELAY: std::time::Duration = std::time::Duration::from_secs(1);

/// Upper bound of the delay between reconnect attempts. The delay doubles on every
/// failure, so a permanently offline endpoint is probed at most every 30s instead of being
/// hammered once a second, while a peer that comes back is still picked up quickly.
const RECONNECT_RETRY_MAX_DELAY: std::time::Duration = std::time::Duration::from_secs(30);

/// Number of reconnect attempts after a stream-close notification before giving up on the
/// peer (about two minutes in total with the doubling back-off). After that the endpoint is
/// considered gone for now: recovery is left to the periodic settings sync, the heartbeat
/// and the next business traffic, so a permanently offline endpoint is not probed forever.
const RECONNECT_MAX_ATTEMPTS: u32 = 8;

/// Interval of the periodic topic-route sync.
///
/// Deliberately shorter than the 30s the Java client uses: when a broker endpoint drifts
/// (failover, migration), the client keeps sending to the stale, dead endpoints until the
/// route data is refreshed, and this timer — not the telemetry reconnect — decides how long
/// that window lasts. 10s halves-to-thirds the measured 13-29s interruption of the 30s
/// variant.
const ROUTE_SYNC_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);

#[automock]
impl Client {
    pub(crate) fn new(
        option: ClientOption,
        settings: TelemetryCommand,
    ) -> Result<Self, ClientError> {
        let id = Self::generate_client_id();
        let endpoints = Endpoints::from_url(option.access_url())
            .map_err(|e| e.with_operation(OPERATION_CLIENT_NEW))?;
        let long_polling_timeout = Arc::new(RwLock::new(*option.long_polling_timeout()));
        let session_manager =
            SessionManager::new(id.clone(), &option, Arc::clone(&long_polling_timeout));
        let route_manager =
            TopicRouteManager::new(option.get_namespace().to_string(), endpoints.clone());
        Ok(Client {
            option,
            session_manager: Arc::new(session_manager),
            route_manager,
            id,
            access_endpoints: endpoints,
            settings,
            telemetry_command_tx: None,
            shutdown_tx: None,
            long_polling_timeout,
        })
    }

    pub(crate) fn get_endpoints(&self) -> Endpoints {
        self.access_endpoints.clone()
    }

    pub(crate) fn is_started(&self) -> bool {
        self.shutdown_tx.is_some()
    }

    pub(crate) fn get_route_manager(&self) -> TopicRouteManager {
        self.route_manager.clone()
    }

    /// Clone client for LitePushConsumer (shares SessionManager for true Lite mode)
    ///
    /// This method creates a lightweight clone of the client specifically for LitePushConsumer
    /// and LiteSubscriptionManager. The key feature is that it shares the same SessionManager
    /// with the original client, enabling true Lite mode where both clients use the same
    /// telemetry session.
    ///
    /// Key design decisions:
    /// - **SessionManager Sharing**: Uses Arc::clone to share the same SessionManager instance,
    ///   ensuring both the original client and the cloned Lite client use the same telemetry
    ///   session. This is the core of true Lite mode.
    /// - **Client Type**: Changed to LitePushConsumer to identify this as a lite consumer.
    /// - **FIFO Mode**: Explicitly enabled for LitePushConsumer to ensure ordered message consumption.
    /// - **No shutdown_tx Copy**: Sets shutdown_tx to None to avoid duplicate shutdown signals.
    ///   Only the original client should trigger shutdown to prevent race conditions.
    /// - **Lightweight Clone**: Only clones necessary fields, avoiding unnecessary deep copies.
    ///
    /// Usage:
    /// ```ignore
    /// let lite_client = original_client.clone_for_lite_consumer();
    /// // Use lite_client for LiteSubscriptionManager
    /// // Both clients share the same SessionManager and telemetry session
    /// ```
    pub(crate) fn clone_for_lite_consumer(&self) -> Self {
        // FIFO is enabled for LitePushConsumer to keep the message order of a lite topic.
        self.clone_for_lite(
            ClientType::LitePushConsumer,
            pb::ClientType::LitePushConsumer,
            true,
        )
    }

    /// Same as [`Client::clone_for_lite_consumer`] but for `LiteSimpleConsumer`.
    pub(crate) fn clone_for_lite_simple_consumer(&self) -> Self {
        self.clone_for_lite(
            ClientType::LiteSimpleConsumer,
            pb::ClientType::LiteSimpleConsumer,
            false,
        )
    }

    fn clone_for_lite(
        &self,
        client_type: ClientType,
        pb_client_type: pb::ClientType,
        fifo: bool,
    ) -> Self {
        let mut new_option = self.option.clone();
        new_option.client_type = client_type;

        let mut new_settings = self.settings.clone();
        if let Some(Command::Settings(ref mut settings)) = &mut new_settings.command {
            settings.client_type = Some(pb_client_type as i32);
            if let Some(pb::settings::PubSub::Subscription(ref mut sub)) = settings.pub_sub {
                sub.fifo = Some(fifo);
            }
        }

        // Share the same SessionManager via Arc::clone so that both clients reuse the same
        // telemetry session.
        let session_manager = Arc::clone(&self.session_manager);

        Self {
            option: new_option,
            session_manager,
            route_manager: self.route_manager.clone(),
            id: self.id.clone(),
            access_endpoints: self.access_endpoints.clone(),
            settings: new_settings,
            telemetry_command_tx: None, // Will be set during start()
            shutdown_tx: None,          // Intentionally None to avoid duplicate shutdown
            long_polling_timeout: Arc::clone(&self.long_polling_timeout),
        }
    }

    pub(crate) async fn start(
        &mut self,
        telemetry_command_tx: mpsc::Sender<pb::telemetry_command::Command>,
    ) -> Result<(), ClientError> {
        let session_manager = self.session_manager.clone();

        let group = self.option.group.clone();
        let namespace = self.option.namespace.to_string();
        let client_type = self.option.client_type.clone();

        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        self.shutdown_tx = Some(shutdown_tx);

        self.telemetry_command_tx = Some(telemetry_command_tx.clone());

        // Channel the sessions notify through when the server closes a telemetry stream.
        // Registered before the first session is created, so that the stream task captures
        // the sender while it is spawned.
        let (stream_closed_tx, mut stream_closed_rx) = mpsc::channel::<String>(16);
        session_manager.set_stream_closed_channel(stream_closed_tx);
        // Also captured so that reconnect_session can rebuild a session that was evicted
        // or reaped meanwhile.
        session_manager.set_telemetry_command_channel(telemetry_command_tx);

        let mut rpc_client = match self.get_session().await {
            Ok(rpc_client) => rpc_client,
            Err(error) => {
                // Roll back, so that a client whose startup failed is not considered started.
                // Reference Java: the client service ends up in state FAILED.
                self.shutdown_tx = None;
                self.telemetry_command_tx = None;
                return Err(error.with_operation(OPERATION_CLIENT_START));
            }
        };

        let route_manager = self.route_manager.clone();

        let settings = self.settings.clone();
        tokio::spawn(async move {
            rpc_client.is_started();
            let mut heartbeat_interval = tokio::time::interval(HEART_BEAT_INTERVAL);
            let seconds_30 = std::time::Duration::from_secs(30);
            let mut sync_settings_interval =
                tokio::time::interval_at(Instant::now() + seconds_30, seconds_30);
            let mut sync_route_timer = tokio::time::interval(ROUTE_SYNC_INTERVAL);
            let mut session_cleanup_timer = tokio::time::interval_at(
                Instant::now() + SESSION_CLEANUP_INTERVAL,
                SESSION_CLEANUP_INTERVAL,
            );
            // Consecutive heartbeat transport failures per peer; a session is only evicted
            // after the failure threshold is reached, so a transient blip does not tear a
            // working connection down.
            let mut heartbeat_failures: HashMap<String, u32> = HashMap::new();
            loop {
                select! {
                    _ = heartbeat_interval.tick() => {
                        let sessions = session_manager.get_all_sessions().await;
                        if let Err(e) = sessions {
                            error!(
                                "send heartbeat failed: failed to get sessions: {}",
                                e
                            );
                            continue;
                        }

                        for session in sessions.unwrap() {
                            let peer = session.peer().to_string();
                            let response = Self::heart_beat_inner(session, &group, &namespace, &client_type).await;
                            if let Err(e) = response {
                                error!(
                                    "send heartbeat failed: failed to send heartbeat rpc: {}",
                                    e
                                );
                                // The endpoint did not answer at all. Evict it only after
                                // the failure threshold is reached so that the next access
                                // rebuilds it against a fresh connection, instead of
                                // keeping a dead endpoint around forever.
                                let failures = heartbeat_failures.entry(peer.clone()).or_insert(0);
                                *failures += 1;
                                if *failures >= HEART_BEAT_FAILURE_THRESHOLD {
                                    session_manager.evict_session(&peer).await;
                                    heartbeat_failures.remove(&peer);
                                }
                                continue;
                            }
                            let result =
                                handle_response_status(response.unwrap().status, OPERATION_HEARTBEAT);
                            if let Err(e) = result {
                                error!(
                                    "send heartbeat failed: server return error: {}",
                                    e
                                );
                                heartbeat_failures.remove(&peer);
                                // A business error such as `cannot find client` means the
                                // server no longer knows this client, which happens when a
                                // failover moved the traffic to another instance. Rebuild
                                // the telemetry stream: the telemetry RPC itself
                                // re-registers the client with the new instance.
                                match session_manager.reconnect_session(&peer, &settings).await {
                                    Ok(_) => {}
                                    Err(re) => {
                                        error!(
                                            "reconnect session after heartbeat error failed, peer={}, error={}",
                                            peer, re
                                        );
                                    }
                                }
                                continue;
                            }
                            heartbeat_failures.remove(&peer);
                            debug!("send heartbeat to server success, peer={}",peer);
                        }
                    },
                    _ = sync_settings_interval.tick() => {
                        // Sync on the registered sessions, not on shadow copies: rebuilding a
                        // stream on a throw-away copy loses it immediately, which is what left
                        // the client with a new one-off stream on every cycle.
                        session_manager.sync_settings(&settings).await;
                    },
                    _ = session_cleanup_timer.tick() => {
                        // Keep the session map from growing without bound: drop sessions whose
                        // long-lived connection is gone, or that nobody used for a long time.
                        session_manager.clear_dead_and_idle_sessions().await;
                    },
                    peer = stream_closed_rx.recv() => {
                        match peer {
                            Some(peer) => {
                                warn!(
                                    "telemetry stream for peer {} was closed, scheduling reconnect",
                                    peer
                                );
                                // Retry in the background: the first attempt follows the
                                // initial back-off delay, then the delay doubles up to a
                                // cap, so a permanently offline endpoint is not hammered
                                // once a second. After the attempt budget is exhausted the
                                // endpoint is left to the periodic settings sync, the
                                // heartbeat and the next business traffic.
                                let session_manager = session_manager.clone();
                                let settings = settings.clone();
                                tokio::spawn(async move {
                                    let mut delay = RECONNECT_BACKOFF_DELAY;
                                    for attempt in 1..=RECONNECT_MAX_ATTEMPTS {
                                        tokio::time::sleep(delay).await;
                                        match session_manager
                                            .reconnect_session(&peer, &settings)
                                            .await
                                        {
                                            Ok(true) => {
                                                info!(
                                                    "telemetry stream for peer {} re-established after {} attempt(s)",
                                                    peer, attempt
                                                );
                                                return;
                                            }
                                            Ok(false) => {
                                                // Nothing could be driven (no access
                                                // endpoints configured); retrying without
                                                // a session to rebuild on is pointless.
                                                warn!(
                                                    "session for peer {} could not be rebuilt, it will be recreated on the next access",
                                                    peer
                                                );
                                                return;
                                            }
                                            Err(e) => {
                                                error!(
                                                    "reconnect to peer {} failed (attempt {}/{}): {}, next attempt in {:?}",
                                                    peer, attempt, RECONNECT_MAX_ATTEMPTS, e, delay
                                                );
                                                delay = (delay * 2).min(RECONNECT_RETRY_MAX_DELAY);
                                            }
                                        }
                                    }
                                    error!(
                                        "gave up reconnecting to peer {} after {} attempts; it will be rebuilt by the periodic settings sync or the next traffic",
                                        peer, RECONNECT_MAX_ATTEMPTS
                                    );
                                });
                            }
                            None => {
                                // Unreachable while the session manager is alive: it holds
                                // a sender of this channel.
                                error!("stream closed channel was closed unexpectedly");
                            }
                        }
                    },
                    _ = sync_route_timer.tick() => {
                        let result = route_manager.sync_route_data(&mut rpc_client).await;
                        if let Err(e) = result {
                            error!("sync route failed: {}", e);
                        }
                    },
                    _ = &mut shutdown_rx => {
                        info!("receive shutdown signal, stop heartbeat and telemetry tasks.");
                        break;
                    }
                }
            }
            info!("heartbeat and telemetry task were stopped");
        });
        Ok(())
    }

    pub(crate) fn check_started(&self, operation: &'static str) -> Result<(), ClientError> {
        if !self.is_started() {
            return Err(ClientError::new(
                ErrorKind::ClientIsNotRunning,
                "client is not started",
                operation,
            )
            .with_context("client_id", self.id.clone()));
        }
        Ok(())
    }

    pub(crate) async fn shutdown(mut self) -> Result<(), ClientError> {
        self.check_started(OPERATION_CLIENT_SHUTDOWN)?;
        let mut rpc_client = self.get_session().await?;
        self.telemetry_command_tx = None;
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        let group = self.option.group.as_ref().map(|group| Resource {
            name: group.to_string(),
            resource_namespace: self.option.namespace.to_string(),
        });
        let response = rpc_client.notify_shutdown(NotifyClientTerminationRequest { group });
        handle_response_status(response.await?.status, OPERATION_CLIENT_SHUTDOWN)?;
        self.session_manager.shutdown().await;
        Ok(())
    }

    /// Shutdown without consuming self (for internal use)
    pub(crate) async fn shutdown_ref(&mut self) -> Result<(), ClientError> {
        self.check_started(OPERATION_CLIENT_SHUTDOWN)?;
        let mut rpc_client = self.get_session().await?;
        self.telemetry_command_tx = None;
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        let group = self.option.group.as_ref().map(|group| Resource {
            name: group.to_string(),
            resource_namespace: self.option.namespace.to_string(),
        });
        let response = rpc_client.notify_shutdown(NotifyClientTerminationRequest { group });
        handle_response_status(response.await?.status, OPERATION_CLIENT_SHUTDOWN)?;
        self.session_manager.shutdown().await;
        Ok(())
    }

    pub(crate) fn client_id(&self) -> &str {
        &self.id
    }

    /// Handle of the shared long-polling timeout, so that consumers can apply a
    /// server-pushed settings update to it from their telemetry loop.
    pub(crate) fn long_polling_timeout_handle(&self) -> SharedLongPollingTimeout {
        Arc::clone(&self.long_polling_timeout)
    }

    /// Get the client type
    #[allow(dead_code)]
    pub(crate) fn get_client_type(&self) -> ClientType {
        self.option.client_type.clone()
    }

    /// Check if this is a lite consumer (LitePushConsumer or LiteSimpleConsumer)
    #[allow(dead_code)]
    pub(crate) fn is_lite_consumer(&self) -> bool {
        matches!(self.option.client_type, ClientType::LitePushConsumer)
    }

    /// Verify that this client shares the same SessionManager with another client
    /// This is useful for verifying Lite mode session sharing
    #[allow(dead_code)]
    pub(crate) fn shares_session_manager_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.session_manager, &other.session_manager)
    }

    /// Get session for LitePushConsumer without checking if client is started
    ///
    /// This method is specifically designed for LitePushConsumer which uses a cloned client.
    /// The cloned client doesn't have shutdown_tx set (to avoid duplicate shutdown), so
    /// is_started() would return false. However, it shares the same SessionManager as the
    /// original client, so it can still get sessions.
    ///
    /// Key features:
    /// - Skips the check_started validation (cloned clients don't have shutdown_tx)
    /// - Creates a virtual telemetry channel if missing (for cloned clients)
    /// - Shares the same SessionManager, ensuring the same underlying connection
    ///
    /// This enables true Lite mode where both the original and cloned clients
    /// use the same telemetry session through shared SessionManager.
    pub(crate) async fn get_session_for_lite_consumer(&self) -> Result<Session, ClientError> {
        // For LitePushConsumer, we skip the check_started check because:
        // 1. The cloned client intentionally has shutdown_tx = None to avoid duplicate shutdown
        // 2. But it shares the same SessionManager, so sessions are still valid
        // 3. The original client manages the lifecycle, not the clone

        // Handle missing telemetry_command_tx by creating a virtual channel
        // This is safe because:
        // - The cloned client shares SessionManager with the original
        // - The original client's telemetry channel is already established
        // - The virtual channel won't be used for actual communication
        let telemetry_tx = self.telemetry_command_tx.clone().unwrap_or_else(|| {
            // Create a virtual/dummy channel for cloned clients
            // This channel won't receive actual messages, but satisfies the API requirement
            let (tx, _rx) = mpsc::channel(1);
            tx
        });

        let session = self
            .session_manager
            .get_or_create_session(&self.access_endpoints, self.settings.clone(), telemetry_tx)
            .await?;
        Ok(session)
    }

    fn generate_client_id() -> String {
        let host = match hostname::get() {
            Ok(name) => name,
            Err(_) => "localhost".into(),
        };

        let host = match host.into_string() {
            Ok(host) => host,
            Err(_) => String::from("localhost"),
        };

        format!(
            "{}@{}#{}",
            host,
            std::process::id(),
            CLIENT_ID_SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        )
    }

    pub(crate) async fn get_session(&self) -> Result<Session, ClientError> {
        self.check_started(OPERATION_GET_SESSION)?;
        let session = self
            .session_manager
            .get_or_create_session(
                &self.access_endpoints,
                self.settings.clone(),
                self.telemetry_command_tx.clone().unwrap(),
            )
            .await?;
        Ok(session)
    }

    async fn get_session_with_endpoints(
        &self,
        endpoints: &Endpoints,
    ) -> Result<Session, ClientError> {
        let session = self
            .session_manager
            .get_or_create_session(
                endpoints,
                self.settings.clone(),
                self.telemetry_command_tx.clone().unwrap(),
            )
            .await?;
        Ok(session)
    }

    pub(crate) fn topic_route_from_cache(&self, topic: &str) -> Option<Arc<Route>> {
        self.route_manager.find_topic_route(topic)
    }

    pub(crate) async fn topic_route(
        &self,
        topic: &str,
        lookup_cache: bool,
    ) -> Result<Arc<Route>, ClientError> {
        if lookup_cache {
            if let Some(route) = self.topic_route_from_cache(topic) {
                return Ok(route);
            }
        }
        let mut rpc_client = self.get_session().await?;
        self.route_manager
            .topic_route_inner(&mut rpc_client, topic)
            .await
    }

    async fn heart_beat_inner<T: RPCClient + 'static>(
        mut rpc_client: T,
        group: &Option<String>,
        namespace: &str,
        client_type: &ClientType,
    ) -> Result<HeartbeatResponse, ClientError> {
        let group = group.as_ref().map(|group| Resource {
            name: group.to_string(),
            resource_namespace: namespace.to_string(),
        });
        let request = HeartbeatRequest {
            group,
            client_type: client_type.clone() as i32,
        };
        let response = rpc_client.heartbeat(request).await?;
        Ok(response)
    }

    pub(crate) async fn send_message(
        &self,
        endpoints: &Endpoints,
        messages: Vec<Message>,
    ) -> Result<Vec<SendReceipt>, ClientError> {
        let rpc_client = self.get_session_with_endpoints(endpoints).await?;
        self.send_message_inner(rpc_client, messages).await
    }

    pub(crate) async fn recall_message(
        &self,
        endpoints: &Endpoints,
        request: RecallMessageRequest,
    ) -> Result<RecallMessageResponse, ClientError> {
        let rpc_client = self.get_session_with_endpoints(endpoints).await?;
        self.recall_message_inner(rpc_client, request).await
    }

    pub(crate) async fn send_message_inner<T: RPCClient + 'static>(
        &self,
        mut rpc_client: T,
        messages: Vec<Message>,
    ) -> Result<Vec<SendReceipt>, ClientError> {
        let request = SendMessageRequest { messages };
        let response = rpc_client.send_message(request).await?;
        handle_response_status(response.status, OPERATION_SEND_MESSAGE)?;

        Ok(response
            .entries
            .iter()
            .map(SendReceipt::from_pb_send_result)
            .collect())
    }

    pub(crate) async fn recall_message_inner<T: RPCClient + 'static>(
        &self,
        mut rpc_client: T,
        request: RecallMessageRequest,
    ) -> Result<RecallMessageResponse, ClientError> {
        let response = rpc_client.recall_message(request).await?;
        Ok(response)
    }

    pub(crate) async fn receive_message(
        &self,
        endpoints: &Endpoints,
        message_queue: MessageQueue,
        expression: FilterExpression,
        batch_size: i32,
        invisible_duration: Duration,
    ) -> Result<Vec<Message>, ClientError> {
        let rpc_client = self.get_session_with_endpoints(endpoints).await?;
        self.receive_message_inner(
            rpc_client,
            message_queue,
            expression,
            batch_size,
            invisible_duration,
        )
        .await
    }

    pub(crate) async fn receive_message_inner<T: RPCClient + 'static>(
        &self,
        mut rpc_client: T,
        message_queue: MessageQueue,
        expression: FilterExpression,
        batch_size: i32,
        invisible_duration: Duration,
    ) -> Result<Vec<Message>, ClientError> {
        let request = ReceiveMessageRequest {
            group: Some(Resource {
                name: self.option.group.as_ref().unwrap().to_string(),
                resource_namespace: self.option.namespace.to_string(),
            }),
            message_queue: Some(message_queue),
            filter_expression: Some(expression),
            batch_size,
            invisible_duration: Some(invisible_duration),
            auto_renew: false,
            // Read from the shared handle: the server may have pushed a new value through
            // the telemetry stream since this client was created.
            long_polling_timeout: Some(
                Duration::try_from(*self.long_polling_timeout.read()).unwrap(),
            ),
            attempt_id: None,
        };
        let responses = rpc_client.receive_message(request).await?;

        let mut messages = Vec::with_capacity(batch_size as usize);
        let mut status: Option<Status> = None;
        let mut delivery_timestamp: Option<prost_types::Timestamp> = None;

        for response in responses {
            match response.content.unwrap() {
                Content::Status(response_status) => {
                    status = Some(response_status);
                }
                Content::Message(message) => {
                    messages.push(message);
                }
                Content::DeliveryTimestamp(timestamp) => {
                    delivery_timestamp = Some(timestamp);
                }
            }
        }

        if let Some(status) = status {
            handle_receive_message_status(&status, OPERATION_RECEIVE_MESSAGE)?;
        }
        if let Some(ref delivery_timestamp) = delivery_timestamp {
            for message in &mut messages {
                if let Some(system_properties) = message.system_properties.as_mut() {
                    system_properties.delivery_timestamp = Some(delivery_timestamp.clone());
                }
            }
        }

        Ok(messages)
    }

    /// Ack one message.
    ///
    /// Only the status of the response is checked, like Java `SimpleConsumerImpl#ack0`: the
    /// result entries are not needed, and a response without any must not fail the ack.
    pub(crate) async fn ack_message<T: AckMessageEntry + 'static>(
        &self,
        ack_entry: &T,
    ) -> Result<(), ClientError> {
        let rpc_client = self
            .get_session_with_endpoints(ack_entry.endpoints())
            .await?;
        self.ack_message_inner(
            rpc_client,
            ack_entry.topic(),
            vec![build_ack_message_entry(ack_entry)],
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn ack_message_inner<T: RPCClient + 'static>(
        &self,
        mut rpc_client: T,
        topic: String,
        entries: Vec<pb::AckMessageEntry>,
    ) -> Result<Vec<AckMessageResultEntry>, ClientError> {
        let request = AckMessageRequest {
            group: Some(Resource {
                name: self.option.group.as_ref().unwrap().to_string(),
                resource_namespace: self.option.namespace.to_string(),
            }),
            topic: Some(Resource {
                name: topic,
                resource_namespace: self.option.namespace.to_string(),
            }),
            entries,
        };
        let response = rpc_client.ack_message(request).await?;
        handle_response_status(response.status, OPERATION_ACK_MESSAGE)?;
        Ok(response.entries)
    }

    pub(crate) async fn change_invisible_duration<T: AckMessageEntry + 'static>(
        &self,
        ack_entry: &T,
        invisible_duration: Duration,
    ) -> Result<String, ClientError> {
        let rpc_client = self
            .get_session_with_endpoints(ack_entry.endpoints())
            .await?;
        let result = self
            .change_invisible_duration_inner(
                rpc_client,
                ack_entry.topic(),
                ack_entry.receipt_handle(),
                invisible_duration,
                ack_entry.message_id(),
                ack_entry.lite_topic().map(|topic| topic.to_string()),
            )
            .await?;
        Ok(result)
    }

    pub(crate) async fn change_invisible_duration_inner<T: RPCClient + 'static>(
        &self,
        mut rpc_client: T,
        topic: String,
        receipt_handle: String,
        invisible_duration: Duration,
        message_id: String,
        lite_topic: Option<String>,
    ) -> Result<String, ClientError> {
        let request = ChangeInvisibleDurationRequest {
            group: Some(Resource {
                name: self.option.group.as_ref().unwrap().to_string(),
                resource_namespace: self.option.namespace.to_string(),
            }),
            topic: Some(Resource {
                name: topic,
                resource_namespace: self.option.namespace.to_string(),
            }),
            receipt_handle,
            invisible_duration: Some(invisible_duration),
            message_id,
            // Reference Java: ConsumerImpl#wrapChangeInvisibleDuration. A lite simple consumer that
            // changes the invisible duration of a lite message asks the broker to suspend the lite
            // queue until then, instead of only rescheduling this message.
            suspend: (lite_topic.is_some()
                && matches!(self.option.client_type, ClientType::LiteSimpleConsumer))
            .then_some(true),
            lite_topic,
        };
        let response = rpc_client.change_invisible_duration(request).await?;
        handle_response_status(response.status, OPERATION_ACK_MESSAGE)?;
        Ok(response.receipt_handle)
    }
}

impl TopicRouteManager {
    pub(crate) fn new(namespace: String, access_endpoints: Endpoints) -> Self {
        Self {
            namespace,
            access_endpoints,
            route_table: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(crate) async fn sync_route_data<T: RPCClient + 'static>(
        &self,
        rpc_client: &mut T,
    ) -> Result<(), ClientError> {
        let topics: Vec<String>;
        {
            topics = self.route_table.lock().keys().cloned().collect();
        }
        debug!("sync topic route of topics {:?}", topics);
        for topic in topics {
            self.topic_route_inner(rpc_client, &topic).await?;
        }
        Ok(())
    }

    pub(crate) async fn sync_topic_routes<T: RPCClient + 'static>(
        &self,
        rpc_client: &mut T,
        topics: Vec<String>,
    ) -> Result<(), ClientError> {
        debug!("sync topic route of topics {:?}.", topics);
        for topic in topics {
            self.topic_route_inner(rpc_client, topic.as_str()).await?;
        }
        Ok(())
    }

    pub(crate) async fn topic_route_inner<T: RPCClient + 'static>(
        &self,
        rpc_client: &mut T,
        topic: &str,
    ) -> Result<Arc<Route>, ClientError> {
        debug!("query route for topic={}", topic);
        let rx = match self
            .route_table
            .lock()
            .entry(topic.to_owned())
            .or_insert_with(|| RouteStatus::Querying(None))
        {
            RouteStatus::Found(_route) => None,
            RouteStatus::Querying(ref mut option) => {
                match option {
                    Some(vec) => {
                        // add self to waiting list
                        let (tx, rx) = oneshot::channel();
                        vec.push(tx);
                        Some(rx)
                    }
                    None => {
                        // there is no ongoing request, so we need to send a new request
                        let _ = option.insert(Vec::new());
                        None
                    }
                }
            }
        };

        // wait for inflight request
        if let Some(rx) = rx {
            return match rx.await {
                Ok(route) => route,
                Err(_e) => Err(ClientError::new(
                    ErrorKind::ChannelReceive,
                    "wait for inflight query topic route request failed",
                    OPERATION_QUERY_ROUTE,
                )),
            };
        }

        let result = Self::query_topic_route(
            rpc_client,
            self.namespace.clone(),
            self.access_endpoints.clone(),
            topic,
        )
        .await;

        // send result to all waiters
        if let Ok(route) = result {
            debug!("query route for topic={} success: route={:?}", topic, route);
            let route = Arc::new(route);
            let mut route_table_lock = self.route_table.lock();

            // if message queues in previous and new route are the same, just keep the previous.
            if let Some(RouteStatus::Found(prev)) = route_table_lock.get(topic) {
                if prev.queue == route.queue {
                    return Ok(Arc::clone(prev));
                }
            }

            let prev =
                route_table_lock.insert(topic.to_owned(), RouteStatus::Found(Arc::clone(&route)));
            info!("update route for topic={}", topic);

            if let Some(RouteStatus::Querying(Some(mut v))) = prev {
                for item in v.drain(..) {
                    let _ = item.send(Ok(Arc::clone(&route)));
                }
            };
            Ok(route)
        } else {
            let err = result.unwrap_err();
            warn!("query route for topic={} failed: error={}", topic, err);
            let mut route_table_lock = self.route_table.lock();
            // keep the existing route if error occurs.
            if let Some(RouteStatus::Found(prev)) = route_table_lock.get(topic) {
                return Ok(Arc::clone(prev));
            }
            let prev = route_table_lock.remove(topic);
            if let Some(RouteStatus::Querying(Some(mut v))) = prev {
                for item in v.drain(..) {
                    let _ = item.send(Err(ClientError::new(
                        ErrorKind::Server,
                        "query topic route failed",
                        OPERATION_QUERY_ROUTE,
                    )));
                }
            };
            Err(err)
        }
    }

    async fn query_topic_route<T: RPCClient + 'static>(
        rpc_client: &mut T,
        namespace: String,
        access_endpoints: Endpoints,
        topic: &str,
    ) -> Result<Route, ClientError> {
        let request = QueryRouteRequest {
            topic: Some(Resource {
                name: topic.to_owned(),
                resource_namespace: namespace,
            }),
            endpoints: Some(access_endpoints.into_inner()),
        };

        let response = rpc_client.query_route(request).await?;
        handle_response_status(response.status, OPERATION_QUERY_ROUTE)?;

        // Start round-robin at a random queue, as Java SubscriptionLoadBalancer /
        // PublishingLoadBalancer do, so that clients do not all start with the same queue.
        let route = Route {
            index: AtomicUsize::new(random_index()),
            queue: response.message_queues,
        };
        Ok(route)
    }

    pub(crate) fn find_topic_route(&self, topic: &str) -> Option<Arc<Route>> {
        self.route_table.lock().get(topic).and_then(|route_status| {
            if let RouteStatus::Found(route) = route_status {
                // debug!(self.logger, "get route for topic={} from cache", topic);
                Some(Arc::clone(route))
            } else {
                None
            }
        })
    }

    pub(crate) fn pick_endpoints(&self, topic: &str) -> Option<Endpoints> {
        self.route_table.lock().get(topic).and_then(|route_status| {
            if let RouteStatus::Found(route) = route_status {
                if let Some(broker) = select_message_queue(Arc::clone(route)).broker {
                    if let Some(endpoints) = broker.endpoints {
                        return Some(Endpoints::from_pb_endpoints(endpoints));
                    }
                }
            }
            None
        })
    }
}

/// A random start index in `[0, i32::MAX)`, like Java `RandomUtils.nextInt(0, Integer.MAX_VALUE)`.
fn random_index() -> usize {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    (RandomState::new().build_hasher().finish() % i32::MAX as u64) as usize
}

#[derive(Debug)]
pub(crate) struct SessionManager {
    client_id: String,
    option: ClientOption,
    session_map: tokio::sync::Mutex<HashMap<String, Session>>,
    /// Long-polling timeout shared with every session, see [`SharedLongPollingTimeout`].
    long_polling_timeout: SharedLongPollingTimeout,
    /// Sender of the channel stream-end notifications are sent through; handed to every
    /// session when it is created.
    stream_closed_tx: Mutex<Option<mpsc::Sender<String>>>,
    /// Sender of the client's telemetry command channel, captured on start so that
    /// `reconnect_session` can rebuild a missing session from scratch.
    telemetry_command_tx: Mutex<Option<mpsc::Sender<pb::telemetry_command::Command>>>,
}

#[automock]
impl SessionManager {
    pub(crate) fn new(
        client_id: String,
        option: &ClientOption,
        long_polling_timeout: SharedLongPollingTimeout,
    ) -> Self {
        let session_map = tokio::sync::Mutex::new(HashMap::new());
        SessionManager {
            client_id,
            option: option.clone(),
            session_map,
            long_polling_timeout,
            stream_closed_tx: Mutex::new(None),
            telemetry_command_tx: Mutex::new(None),
        }
    }

    /// Register the channel telemetry stream-end notifications are sent through.
    ///
    /// Must be called before the first session is created.
    pub(crate) fn set_stream_closed_channel(&self, tx: mpsc::Sender<String>) {
        *self.stream_closed_tx.lock() = Some(tx);
    }

    /// Capture the client's telemetry command channel so that `reconnect_session` can
    /// rebuild a missing session from scratch. Called by `Client::start`.
    pub(crate) fn set_telemetry_command_channel(
        &self,
        tx: mpsc::Sender<pb::telemetry_command::Command>,
    ) {
        *self.telemetry_command_tx.lock() = Some(tx);
    }

    pub(crate) async fn get_or_create_session(
        &self,
        endpoints: &Endpoints,
        settings: TelemetryCommand,
        telemetry_command_tx: mpsc::Sender<pb::telemetry_command::Command>,
    ) -> Result<Session, ClientError> {
        let mut session_map = self.session_map.lock().await;
        let endpoint_url = endpoints.endpoint_url().to_string();
        if session_map.contains_key(&endpoint_url) {
            Ok(session_map.get(&endpoint_url).unwrap().shadow_session())
        } else {
            let mut session = Session::new(
                endpoints,
                self.client_id.clone(),
                &self.option,
                Arc::clone(&self.long_polling_timeout),
            )
            .await?;
            if let Some(tx) = self.stream_closed_tx.lock().clone() {
                session.set_stream_closed_tx(tx);
            }
            session.start(settings, telemetry_command_tx).await?;
            let shadow_session = session.shadow_session();
            session_map.insert(endpoint_url.clone(), session);
            Ok(shadow_session)
        }
    }
    pub(crate) async fn get_all_sessions(&self) -> Result<Vec<Session>, ClientError> {
        let session_map = self.session_map.lock().await;
        let mut sessions = Vec::new();
        for session in session_map.values() {
            sessions.push(session.shadow_session());
        }
        Ok(sessions)
    }

    /// Drop the session bound to `endpoint_url`, if it is still the one registered under
    /// that key.
    ///
    /// A session removed here is torn down and a later call to
    /// [`Self::get_or_create_session`] rebuilds it from scratch, which is the only way to
    /// recover from a server that closed the connection for good.
    ///
    /// Mirrors `ClientImpl#removeClientSession` of the Java client.
    pub(crate) async fn evict_session(&self, endpoint_url: &str) -> bool {
        let mut session_map = self.session_map.lock().await;
        match session_map.remove(endpoint_url) {
            Some(mut session) => {
                session.shutdown();
                info!("evicted session for endpoint {}", endpoint_url);
                true
            }
            None => false,
        }
    }

    /// Re-establish the telemetry stream of the session bound to `endpoint_url`.
    ///
    /// Rebuilding the stream also re-registers the client with the server instance that
    /// answers, which is what recovers from a failover or rolling deployment where the
    /// server no longer knows this client (`cannot find client`, `settings is null`).
    ///
    /// The session is taken out of the map for the duration of the RPC so that other
    /// callers are not blocked on the network round trip. On failure it is put back so
    /// that the reconnect retry loop keeps driving recovery on the same session even when
    /// the client is idle.
    ///
    /// Returns `Ok(true)` when the stream of `endpoint_url` was re-established. A session
    /// that is missing from the map (evicted or reaped meanwhile) is rebuilt against that
    /// same endpoint, so an idle client keeps a driver for recovery even when no traffic
    /// refers to the endpoint anymore. `Ok(false)` means nothing could be driven (no
    /// telemetry command channel captured yet).
    pub(crate) async fn reconnect_session(
        &self,
        endpoint_url: &str,
        settings: &TelemetryCommand,
    ) -> Result<bool, ClientError> {
        // Take the session out of the map and release the lock before any await: the
        // rebuild path below calls get_or_create_session, which locks the same map again
        // (tokio::sync::Mutex is not reentrant).
        let session_opt = {
            let mut session_map = self.session_map.lock().await;
            session_map.remove(endpoint_url)
        };
        let Some(mut session) = session_opt else {
            // The session was evicted or reaped meanwhile. Rebuild it against the very
            // endpoint that was asked for: rebuilding the access session instead would
            // leave this peer without a stream while still reporting `Ok(true)`, which
            // makes the reconnect retry loop log "re-established" and stop. Every key of
            // `session_map` comes from `Endpoints::endpoint_url`, so it round-trips back
            // into a valid endpoint here.
            let telemetry_command_tx = self.telemetry_command_tx.lock().clone();
            let Some(telemetry_command_tx) = telemetry_command_tx else {
                return Ok(false);
            };
            let endpoints = Endpoints::from_url(endpoint_url)?;
            return self
                .get_or_create_session(&endpoints, settings.clone(), telemetry_command_tx)
                .await
                .map(|_| true);
        };
        match session.reconnect(settings.clone()).await {
            Ok(()) => {
                self.session_map
                    .lock()
                    .await
                    .insert(endpoint_url.to_string(), session);
                Ok(true)
            }
            Err(e) => {
                // Keep the session registered so that the reconnect retry loop keeps
                // retrying on it; dropping it here would leave an idle client without any
                // driver to re-establish the stream until the next traffic arrives.
                self.session_map
                    .lock()
                    .await
                    .insert(endpoint_url.to_string(), session);
                Err(e)
            }
        }
    }

    /// Push the client settings to every session that currently holds a telemetry stream.
    ///
    /// This deliberately operates on the *registered* sessions rather than on throw-away
    /// shadow copies: a shadow that rebuilds its telemetry stream would drop that stream
    /// again as soon as it goes out of scope, which used to leave the registered session
    /// permanently without a stream (one short-lived stream per sync interval, and a dead
    /// endpoint that fails on every cycle). Mirrors `ClientImpl#syncSettings` of the Java
    /// client, where the same `ClientSession` instance is reused.
    ///
    /// Sessions whose stream cannot be restored are evicted so that `session_map` shrinks
    /// instead of accumulating dead endpoints.
    pub(crate) async fn sync_settings(&self, settings: &TelemetryCommand) {
        let mut session_map = self.session_map.lock().await;
        let mut broken: Vec<String> = Vec::new();
        for (endpoint_url, session) in session_map.iter_mut() {
            match session.update_settings(settings.clone()).await {
                Ok(()) => {
                    debug!("sync settings success, peer = {}", endpoint_url);
                }
                Err(e) => {
                    error!(
                        "sync settings failed, peer = {}, error = {}",
                        endpoint_url, e
                    );
                    broken.push(endpoint_url.clone());
                }
            }
        }
        for endpoint_url in broken {
            if let Some(mut session) = session_map.remove(&endpoint_url) {
                session.shutdown();
                warn!(
                    "evicted session after settings sync failure, peer = {}",
                    endpoint_url
                );
            }
        }
    }

    /// Forget the sessions that are no longer usable.
    ///
    /// Two kinds of session are dropped, both of which used to accumulate forever because
    /// `session_map` only ever grew:
    ///
    /// * sessions whose telemetry stream is gone, i.e. the long-lived connection was lost
    ///   (for example after a rolling deployment of the server);
    /// * sessions that have been idle for longer than [`SESSION_MAX_IDLE_DURATION`],
    ///   mirroring `ClientManagerImpl#clearIdleRpcClients` of the Java client.
    ///
    /// Every dropped session is torn down so that the next access rebuilds it.
    pub(crate) async fn clear_dead_and_idle_sessions(&self) {
        let mut session_map = self.session_map.lock().await;
        let stale: Vec<String> = session_map
            .iter()
            .filter(|(_, session)| {
                !session.is_healthy() || session.idle_duration() > SESSION_MAX_IDLE_DURATION
            })
            .map(|(url, _)| url.clone())
            .collect();
        for url in stale {
            if let Some(mut session) = session_map.remove(&url) {
                session.shutdown();
                info!("evicted stale session for endpoint {}", url);
            }
        }
    }

    pub(crate) async fn shutdown(&self) {
        let mut session_map = self.session_map.lock().await;
        for session in session_map.values_mut() {
            session.shutdown();
        }
        session_map.clear();
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread::sleep;
    use std::time::Duration;

    use once_cell::sync::Lazy;

    use crate::client::Client;
    use crate::conf::ClientOption;
    use crate::error::{ClientError, ErrorKind};
    use crate::model::common::{ClientType, Route};
    use crate::pb::{
        AckMessageEntry, AckMessageResponse, ChangeInvisibleDurationResponse, Code,
        FilterExpression, HeartbeatResponse, Message, MessageQueue, QueryRouteResponse,
        ReceiveMessageResponse, Resource, SendMessageResponse, Status, TelemetryCommand,
    };
    use crate::session;
    use crate::session::MockSession;

    use super::*;

    // The lock is used to prevent the mocking static function at same time during parallel testing.
    pub(crate) static MTX: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

    fn new_route_manager_for_test() -> TopicRouteManager {
        TopicRouteManager {
            route_table: Arc::new(Mutex::new(HashMap::new())),
            access_endpoints: Endpoints::from_url("http://localhost:8081").unwrap(),
            namespace: "".to_string(),
        }
    }

    fn new_session_manager() -> SessionManager {
        SessionManager::new(
            Client::generate_client_id(),
            &ClientOption {
                group: Some("group".to_string()),
                ..Default::default()
            },
            Arc::new(RwLock::new(std::time::Duration::from_secs(40))),
        )
    }

    fn mock_session(started: bool) -> Session {
        let mut session = MockSession::default();
        session.expect_is_started().return_const(started);
        session.expect_is_healthy().return_const(started);
        session
            .expect_idle_duration()
            .return_const(std::time::Duration::ZERO);
        session.expect_shutdown().returning(|| ());
        session
    }

    fn new_client_for_test() -> Client {
        Client {
            option: ClientOption {
                group: Some("group".to_string()),
                ..Default::default()
            },
            session_manager: Arc::new(new_session_manager()),
            route_manager: new_route_manager_for_test(),
            id: Client::generate_client_id(),
            access_endpoints: Endpoints::from_url("http://localhost:8081").unwrap(),
            settings: TelemetryCommand::default(),
            telemetry_command_tx: None,
            shutdown_tx: None,
            long_polling_timeout: Arc::new(RwLock::new(std::time::Duration::from_secs(40))),
        }
    }

    fn new_client_with_session_manager(session_manager: SessionManager) -> Client {
        let (tx, _) = mpsc::channel(16);
        Client {
            option: ClientOption::default(),
            session_manager: Arc::new(session_manager),
            route_manager: new_route_manager_for_test(),
            id: Client::generate_client_id(),
            access_endpoints: Endpoints::from_url("http://localhost:8081").unwrap(),
            settings: TelemetryCommand::default(),
            telemetry_command_tx: Some(tx),
            shutdown_tx: None,
            long_polling_timeout: Arc::new(RwLock::new(std::time::Duration::from_secs(40))),
        }
    }

    #[test]
    fn client_id_sequence() {
        let v1 = CLIENT_ID_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let v2 = CLIENT_ID_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        assert!(v2 > v1, "Client ID sequence should be increasing");
    }

    #[test]
    fn client_new() -> Result<(), ClientError> {
        Client::new(ClientOption::default(), TelemetryCommand::default())?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn client_start() -> Result<(), ClientError> {
        // TODO: add grpc bi-stream test
        Ok(())
    }

    #[tokio::test]
    async fn client_get_session() {
        let _m = MTX.lock();
        let context = MockSession::new_context();
        context.expect().returning(|_, _, _, _| {
            let mut session = MockSession::default();
            session.expect_start().returning(|_, _| Ok(()));
            session.expect_set_stream_closed_tx().returning(|_| ());
            session
                .expect_shadow_session()
                .returning(|| MockSession::default());
            Ok(session)
        });
        let session_manager = new_session_manager();
        let mut client = new_client_with_session_manager(session_manager);
        let (tx, _rx) = mpsc::channel(16);
        let _ = client.start(tx).await;
        let result = client.get_session().await;
        assert!(result.is_ok());
        let result = client
            .get_session_with_endpoints(&Endpoints::from_url("localhost:8081").unwrap())
            .await;
        assert!(result.is_ok());
    }

    // Reference Java: a client whose startup failed is not running (Guava `Service` state FAILED).
    #[tokio::test]
    async fn client_start_failure_leaves_client_not_started() {
        let _m = MTX.lock();
        let context = MockSession::new_context();
        context.expect().returning(|_, _, _, _| {
            let mut session = MockSession::default();
            session.expect_set_stream_closed_tx().returning(|_| ());
            session.expect_start().returning(|_, _| {
                Err(ClientError::new(
                    ErrorKind::ClientInternal,
                    "send rpc telemetry failed",
                    "session.start",
                ))
            });
            Ok(session)
        });
        let mut client = new_client_with_session_manager(new_session_manager());
        let (tx, _rx) = mpsc::channel(16);
        let result = client.start(tx).await;
        assert!(result.is_err());
        assert!(!client.is_started());
        assert!(client.telemetry_command_tx.is_none());
        let error = client.get_session().await.unwrap_err();
        assert_eq!(error.kind, ErrorKind::ClientIsNotRunning);
    }

    pub(crate) fn new_topic_route_response() -> Result<QueryRouteResponse, ClientError> {
        Ok(QueryRouteResponse {
            status: Some(Status {
                code: Code::Ok as i32,
                message: "Success".to_string(),
            }),
            message_queues: vec![MessageQueue {
                topic: Some(Resource {
                    name: "DefaultCluster".to_string(),
                    resource_namespace: "default".to_string(),
                }),
                id: 0,
                permission: 0,
                broker: None,
                accept_message_types: vec![],
            }],
        })
    }

    #[tokio::test]
    async fn client_query_route_from_cache() {
        let client = new_client_for_test();
        client.route_manager.route_table.lock().insert(
            "DefaultCluster".to_string(),
            RouteStatus::Found(Arc::new(Route {
                index: AtomicUsize::new(0),
                queue: vec![],
            })),
        );
        let option = client.topic_route_from_cache("DefaultCluster");
        assert!(option.is_some());
    }

    #[tokio::test]
    async fn client_query_route() {
        let client = new_client_for_test();

        let mut mock = session::MockRPCClient::new();
        mock.expect_query_route()
            .return_once(|_| Box::pin(futures::future::ready(new_topic_route_response())));

        let result = client
            .route_manager
            .topic_route_inner(&mut mock, "DefaultCluster")
            .await;
        assert!(result.is_ok());

        let route = result.unwrap();
        assert!(!route.queue.is_empty());

        let topic = &route.queue[0].topic;
        assert!(topic.is_some());

        let topic = topic.clone().unwrap();
        assert_eq!(topic.name, "DefaultCluster");
        assert_eq!(topic.resource_namespace, "default");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn client_query_route_with_inflight_request() {
        let client = new_client_for_test();
        let client = Arc::new(client);

        let client_clone = client.clone();
        tokio::spawn(async move {
            let mut mock = session::MockRPCClient::new();
            mock.expect_query_route().return_once(|_| {
                sleep(Duration::from_millis(200));
                Box::pin(futures::future::ready(new_topic_route_response()))
            });

            let result = client_clone
                .route_manager
                .topic_route_inner(&mut mock, "DefaultCluster")
                .await;
            assert!(result.is_ok());
        });

        let handle = tokio::spawn(async move {
            sleep(Duration::from_millis(100));
            let mut mock = session::MockRPCClient::new();
            let result = client
                .route_manager
                .topic_route_inner(&mut mock, "DefaultCluster")
                .await;
            assert!(result.is_ok());
        });

        awaitility::at_most(Duration::from_secs(1)).until(|| handle.is_finished());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn client_query_route_with_failed_request() {
        let client = new_client_for_test();
        let client = Arc::new(client);

        let client_clone = client.clone();
        tokio::spawn(async move {
            let mut mock = session::MockRPCClient::new();
            mock.expect_query_route().return_once(|_| {
                sleep(Duration::from_millis(200));
                Box::pin(futures::future::ready(Err(ClientError::new(
                    ErrorKind::Server,
                    "server error",
                    "test",
                ))))
            });

            let result = client_clone
                .route_manager
                .topic_route_inner(&mut mock, "DefaultCluster")
                .await;
            assert!(result.is_err());
        });

        let handle = tokio::spawn(async move {
            sleep(Duration::from_millis(100));
            let mut mock = session::MockRPCClient::new();
            let result = client
                .route_manager
                .topic_route_inner(&mut mock, "DefaultCluster")
                .await;
            assert!(result.is_err());
        });

        awaitility::at_most(Duration::from_secs(1)).until(|| handle.is_finished());
    }

    #[tokio::test]
    async fn client_query_existing_route_with_failed_request() {
        let client = new_client_for_test();
        let message_queues = if let Ok(QueryRouteResponse {
            status: _,
            message_queues,
        }) = new_topic_route_response()
        {
            message_queues
        } else {
            vec![]
        };
        client.route_manager.route_table.lock().insert(
            "DefaultCluster".to_string(),
            RouteStatus::Found(Arc::new(Route {
                index: AtomicUsize::new(0),
                queue: message_queues,
            })),
        );

        let mut mock = session::MockRPCClient::new();
        mock.expect_query_route().return_once(|_| {
            sleep(Duration::from_millis(200));
            Box::pin(futures::future::ready(Err(ClientError::new(
                ErrorKind::Server,
                "server error",
                "test",
            ))))
        });

        let result = client
            .route_manager
            .topic_route_inner(&mut mock, "DefaultCluster")
            .await;
        assert!(result.is_ok());
    }

    // Reference Java: SubscriptionLoadBalancer / PublishingLoadBalancer start round-robin at a
    // random index, so that clients do not all start with the same queue.
    #[tokio::test]
    async fn client_query_route_starts_at_random_index() {
        let client = new_client_for_test();
        let mut indexes = std::collections::HashSet::new();
        for _ in 0..8 {
            let mut mock = session::MockRPCClient::new();
            mock.expect_query_route()
                .return_once(|_| Box::pin(futures::future::ready(new_topic_route_response())));
            let route = TopicRouteManager::query_topic_route(
                &mut mock,
                client.route_manager.namespace.clone(),
                client.route_manager.access_endpoints.clone(),
                "DefaultCluster",
            )
            .await
            .unwrap();
            indexes.insert(route.index.load(Ordering::Relaxed));
        }
        assert!(indexes.len() > 1, "start indexes: {indexes:?}");
    }

    #[tokio::test]
    async fn client_update_same_route() {
        let client = new_client_for_test();

        let mut mock = session::MockRPCClient::new();
        mock.expect_query_route()
            .return_once(|_| Box::pin(futures::future::ready(new_topic_route_response())));

        let result = client
            .route_manager
            .topic_route_inner(&mut mock, "DefaultCluster")
            .await;
        assert!(result.is_ok());

        let route = result.unwrap();
        assert!(!route.queue.is_empty());
        let index = route.index.fetch_add(1, Ordering::Relaxed) + 1;

        let topic = &route.queue[0].topic;
        assert!(topic.is_some());

        let topic = topic.clone().unwrap();
        assert_eq!(topic.name, "DefaultCluster");
        assert_eq!(topic.resource_namespace, "default");

        mock = session::MockRPCClient::new();
        mock.expect_query_route()
            .return_once(|_| Box::pin(futures::future::ready(new_topic_route_response())));

        let result2 = client
            .route_manager
            .topic_route_inner(&mut mock, "DefaultCluster")
            .await;
        assert!(result2.is_ok());

        let route2 = result2.unwrap();
        assert_eq!(index, route2.index.load(Ordering::Relaxed));
    }
    #[tokio::test]
    async fn client_heartbeat() {
        let response = Ok(HeartbeatResponse {
            status: Some(Status {
                code: Code::Ok as i32,
                message: "Success".to_string(),
            }),
        });
        let mut mock = session::MockRPCClient::new();
        mock.expect_heartbeat()
            .return_once(|_| Box::pin(futures::future::ready(response)));

        let send_result =
            Client::heart_beat_inner(mock, &Some("group".to_string()), "", &ClientType::Producer)
                .await;
        assert!(send_result.is_ok());
    }

    #[tokio::test]
    async fn client_send_message() {
        let response = Ok(SendMessageResponse {
            status: Some(Status {
                code: Code::Ok as i32,
                message: "Success".to_string(),
            }),
            entries: vec![],
        });
        let mut mock = session::MockRPCClient::new();
        mock.expect_send_message()
            .return_once(|_| Box::pin(futures::future::ready(response)));

        let client = new_client_for_test();
        let send_result = client.send_message_inner(mock, vec![]).await;
        assert!(send_result.is_ok());

        let send_results = send_result.unwrap();
        assert_eq!(send_results.len(), 0);
    }

    #[tokio::test]
    async fn client_receive_message() {
        let response = Ok(vec![ReceiveMessageResponse {
            content: Some(Content::Message(Message::default())),
        }]);
        let mut mock = session::MockRPCClient::new();
        mock.expect_receive_message()
            .return_once(|_| Box::pin(futures::future::ready(response)));

        let client = new_client_for_test();
        let receive_result = client
            .receive_message_inner(
                mock,
                MessageQueue::default(),
                FilterExpression::default(),
                32,
                prost_types::Duration::default(),
            )
            .await;
        assert!(receive_result.is_ok());

        let messages = receive_result.unwrap();
        assert_eq!(messages.len(), 1);
    }

    #[tokio::test]
    async fn client_receive_message_failed() {
        let response = Ok(vec![ReceiveMessageResponse {
            content: Some(Content::Status(Status {
                code: Code::BadRequest as i32,
                message: "Unknown error".to_string(),
            })),
        }]);
        let mut mock = session::MockRPCClient::new();
        mock.expect_receive_message()
            .return_once(|_| Box::pin(futures::future::ready(response)));

        let client = new_client_for_test();
        let receive_result = client
            .receive_message_inner(
                mock,
                MessageQueue::default(),
                FilterExpression::default(),
                32,
                prost_types::Duration::default(),
            )
            .await;
        assert!(receive_result.is_err());

        let error = receive_result.unwrap_err();
        assert_eq!(error.kind, ErrorKind::Config);
        assert_eq!(error.message, "bad request");
        assert_eq!(error.operation, "client.receive_message");
    }

    #[tokio::test]
    async fn client_ack_message() {
        let response = Ok(AckMessageResponse {
            status: Some(Status {
                code: Code::Ok as i32,
                message: "Success".to_string(),
            }),
            entries: vec![],
        });
        let mut mock = session::MockRPCClient::new();
        mock.expect_ack_message()
            .return_once(|_| Box::pin(futures::future::ready(response)));

        let client = new_client_for_test();
        let ack_entries: Vec<AckMessageEntry> = vec![];
        let ack_result = client
            .ack_message_inner(mock, "test_topic".to_string(), ack_entries)
            .await;
        assert!(ack_result.is_ok());
        assert_eq!(ack_result.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn client_change_invisible_duration() {
        let response = Ok(ChangeInvisibleDurationResponse {
            status: Some(Status {
                code: Code::Ok as i32,
                message: "Success".to_string(),
            }),
            receipt_handle: "receipt_handle".to_string(),
        });
        let mut mock = session::MockRPCClient::new();
        mock.expect_change_invisible_duration()
            .return_once(|_| Box::pin(futures::future::ready(response)));

        let client = new_client_for_test();
        let change_invisible_duration_result = client
            .change_invisible_duration_inner(
                mock,
                "test_topic".to_string(),
                "receipt_handle".to_string(),
                prost_types::Duration::default(),
                "message_id".to_string(),
                None,
            )
            .await;
        assert!(change_invisible_duration_result.is_ok());
        assert_eq!(change_invisible_duration_result.unwrap(), "receipt_handle");
    }

    async fn change_invisible_duration_request(
        client_type: ClientType,
        lite_topic: Option<String>,
    ) -> ChangeInvisibleDurationRequest {
        let captured = Arc::new(Mutex::new(None));
        let captured_in_mock = Arc::clone(&captured);
        let mut mock = session::MockRPCClient::new();
        mock.expect_change_invisible_duration()
            .return_once(move |request| {
                captured_in_mock.lock().replace(request);
                Box::pin(futures::future::ready(Ok(
                    ChangeInvisibleDurationResponse {
                        status: Some(Status {
                            code: Code::Ok as i32,
                            message: "Success".to_string(),
                        }),
                        receipt_handle: "receipt_handle".to_string(),
                    },
                )))
            });
        let mut client = new_client_for_test();
        client.option.client_type = client_type;
        client
            .change_invisible_duration_inner(
                mock,
                "test_topic".to_string(),
                "receipt_handle".to_string(),
                prost_types::Duration::default(),
                "message_id".to_string(),
                lite_topic,
            )
            .await
            .unwrap();
        let request = captured.lock().take();
        request.unwrap()
    }

    // Reference Java: ConsumerImpl#wrapChangeInvisibleDuration sets suspend=true when a lite
    // consumer changes the invisible duration of a lite message (LiteSimpleConsumer).
    #[tokio::test]
    async fn client_change_invisible_duration_of_lite_message_suspends() {
        let request = change_invisible_duration_request(
            ClientType::LiteSimpleConsumer,
            Some("lite-a".to_string()),
        )
        .await;
        assert_eq!(request.lite_topic.as_deref(), Some("lite-a"));
        assert_eq!(request.suspend, Some(true));
    }

    #[tokio::test]
    async fn client_change_invisible_duration_of_normal_message_does_not_suspend() {
        let request = change_invisible_duration_request(ClientType::SimpleConsumer, None).await;
        assert_eq!(request.lite_topic, None);
        assert_eq!(request.suspend, None);
        let request = change_invisible_duration_request(ClientType::LiteSimpleConsumer, None).await;
        assert_eq!(request.suspend, None);
    }

    #[tokio::test]
    async fn client_ack_message_failed() {
        let response = Ok(AckMessageResponse {
            status: Some(Status {
                code: Code::BadRequest as i32,
                message: "Success".to_string(),
            }),
            entries: vec![],
        });
        let mut mock = session::MockRPCClient::new();
        mock.expect_ack_message()
            .return_once(|_| Box::pin(futures::future::ready(response)));

        let client = new_client_for_test();
        let ack_entries: Vec<AckMessageEntry> = vec![];
        let ack_result = client
            .ack_message_inner(mock, "test_topic".to_string(), ack_entries)
            .await;

        let error = ack_result.unwrap_err();
        assert_eq!(error.kind, ErrorKind::Server);
        assert_eq!(error.message, "server return an error");
        assert_eq!(error.operation, "client.ack_message");
    }

    #[test]
    fn test_clone_for_lite_consumer() {
        let _m = MTX.lock();

        // Create original client
        let original_client = new_client_for_test();

        // Clone for lite consumer
        let lite_client = original_client.clone_for_lite_consumer();

        // Verify client type is changed
        assert!(matches!(
            lite_client.option.client_type,
            ClientType::LitePushConsumer
        ));

        // Verify settings are updated
        if let Some(Command::Settings(ref settings)) = lite_client.settings.command {
            assert_eq!(
                settings.client_type,
                Some(pb::ClientType::LitePushConsumer as i32)
            );

            // Verify FIFO is enabled
            if let Some(pb::settings::PubSub::Subscription(ref sub)) = settings.pub_sub {
                assert_eq!(sub.fifo, Some(true));
            }
        }

        // Verify SessionManager is shared
        assert!(original_client.shares_session_manager_with(&lite_client));

        // Verify other fields are cloned correctly
        assert_eq!(original_client.id, lite_client.id);
        assert_eq!(
            original_client.access_endpoints.endpoint_url(),
            lite_client.access_endpoints.endpoint_url()
        );

        // Verify telemetry_command_tx and shutdown_tx are reset
        assert!(lite_client.telemetry_command_tx.is_none());
        assert!(lite_client.shutdown_tx.is_none());
    }

    #[test]
    fn test_is_lite_consumer() {
        let _m = MTX.lock();

        // Create original client (not lite)
        let original_client = new_client_for_test();
        assert!(!original_client.is_lite_consumer());

        // Clone for lite consumer
        let lite_client = original_client.clone_for_lite_consumer();
        assert!(lite_client.is_lite_consumer());
    }

    #[tokio::test]
    async fn test_lite_consumer_session_sharing() {
        let _m = MTX.lock();

        let context = MockSession::new_context();
        context.expect().returning(|_, _, _, _| {
            let mut session = MockSession::default();
            session.expect_start().returning(|_, _| Ok(()));
            session.expect_set_stream_closed_tx().returning(|_| ());
            session
                .expect_shadow_session()
                .returning(|| MockSession::default());
            Ok(session)
        });

        // Create original client
        let session_manager = new_session_manager();
        let mut original_client = new_client_with_session_manager(session_manager);
        let (tx1, _rx1) = mpsc::channel(16);
        let _ = original_client.start(tx1).await;

        // Clone for lite consumer
        let mut lite_client = original_client.clone_for_lite_consumer();
        let (tx2, _rx2) = mpsc::channel(16);
        let _ = lite_client.start(tx2).await;

        // Verify they share the same SessionManager
        assert!(original_client.shares_session_manager_with(&lite_client));

        // Both clients should be able to get sessions
        let result1 = original_client.get_session().await;
        assert!(result1.is_ok());

        let result2 = lite_client.get_session().await;
        assert!(result2.is_ok());
    }

    #[tokio::test]
    async fn test_get_session_for_lite_consumer() {
        let _m = MTX.lock();

        let context = MockSession::new_context();
        context.expect().returning(|_, _, _, _| {
            let mut session = MockSession::default();
            session.expect_start().returning(|_, _| Ok(()));
            session.expect_set_stream_closed_tx().returning(|_| ());
            session
                .expect_shadow_session()
                .returning(|| MockSession::default());
            Ok(session)
        });

        // Create original client and start it
        let session_manager = new_session_manager();
        let mut original_client = new_client_with_session_manager(session_manager);
        let (tx1, _rx1) = mpsc::channel(16);
        let _ = original_client.start(tx1).await;

        // Clone for lite consumer (this clone has shutdown_tx = None)
        let lite_client = original_client.clone_for_lite_consumer();

        // Verify the lite client is not "started" (shutdown_tx is None)
        assert!(!lite_client.is_started());

        // But it should still be able to get a session using get_session_for_lite_consumer
        // This method skips check_started and creates a virtual telemetry channel
        let result = lite_client.get_session_for_lite_consumer().await;
        assert!(result.is_ok());

        // Verify they share the same SessionManager
        assert!(original_client.shares_session_manager_with(&lite_client));
    }

    #[test]
    fn test_is_lite_consumer_with_clone() {
        let _m = MTX.lock();

        // Create original client with LitePushConsumer type
        let mut option = ClientOption::default();
        option.client_type = ClientType::LitePushConsumer;
        option.group = Some("group".to_string());

        let original_client = Client::new(option, TelemetryCommand::default()).unwrap();
        assert!(original_client.is_lite_consumer());

        // Clone should also be lite consumer
        let cloned_client = original_client.clone_for_lite_consumer();
        assert!(cloned_client.is_lite_consumer());
    }

    #[tokio::test]
    async fn test_evict_session() {
        let session_manager = new_session_manager();
        session_manager
            .session_map
            .lock()
            .await
            .insert("endpoint_a".to_string(), mock_session(true));

        assert!(session_manager.evict_session("endpoint_a").await);
        assert!(session_manager.session_map.lock().await.is_empty());

        // Evicting an unknown endpoint is a no-op.
        assert!(!session_manager.evict_session("endpoint_a").await);
    }

    #[tokio::test]
    async fn test_clear_dead_and_idle_sessions() {
        let session_manager = new_session_manager();
        {
            let mut session_map = session_manager.session_map.lock().await;
            // Healthy session: kept.
            session_map.insert("healthy".to_string(), mock_session(true));
            // Lost its telemetry stream: dropped.
            session_map.insert("dead".to_string(), mock_session(false));
        }

        session_manager.clear_dead_and_idle_sessions().await;

        let session_map = session_manager.session_map.lock().await;
        assert_eq!(session_map.len(), 1);
        assert!(session_map.contains_key("healthy"));
    }

    #[tokio::test]
    async fn test_clear_idle_sessions() {
        let session_manager = new_session_manager();
        {
            let mut session_map = session_manager.session_map.lock().await;
            let mut session = MockSession::default();
            session.expect_is_started().return_const(true);
            session.expect_is_healthy().return_const(true);
            session
                .expect_idle_duration()
                .return_const(SESSION_MAX_IDLE_DURATION + std::time::Duration::from_secs(1));
            session.expect_shutdown().returning(|| ());
            session_map.insert("idle".to_string(), session);
        }

        session_manager.clear_dead_and_idle_sessions().await;

        assert!(session_manager.session_map.lock().await.is_empty());
    }

    #[tokio::test]
    async fn test_sync_settings_rebuilds_stream_on_registered_session() {
        let session_manager = new_session_manager();
        let rebuilt = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        {
            let mut session_map = session_manager.session_map.lock().await;
            let mut session = MockSession::default();
            // The stream is gone, so the settings sync must re-establish it...
            session.expect_is_started().return_const(false);
            session.expect_is_healthy().return_const(false);
            let counter = Arc::clone(&rebuilt);
            session.expect_update_settings().returning(move |_| {
                counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            });
            session.expect_shutdown().returning(|| ());
            session_map.insert("endpoint_a".to_string(), session);
        }

        session_manager
            .sync_settings(&TelemetryCommand::default())
            .await;

        // ... and it must have been rebuilt exactly once, on the registered session rather
        // than on a throw-away shadow copy.
        assert_eq!(
            rebuilt.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "settings sync should rebuild the stream of the registered session once"
        );
        // A session whose stream was restored successfully stays registered.
        assert_eq!(session_manager.session_map.lock().await.len(), 1);
    }

    #[tokio::test]
    async fn test_sync_settings_evicts_unrecoverable_session() {
        let session_manager = new_session_manager();
        {
            let mut session_map = session_manager.session_map.lock().await;
            let mut session = MockSession::default();
            session.expect_is_started().return_const(false);
            session.expect_is_healthy().return_const(false);
            session.expect_update_settings().returning(|_| {
                Err(ClientError::new(
                    ErrorKind::ServerUnavailable,
                    "server unavailable",
                    "test.sync_settings",
                ))
            });
            session.expect_shutdown().returning(|| ());
            session_map.insert("endpoint_a".to_string(), session);
        }

        session_manager
            .sync_settings(&TelemetryCommand::default())
            .await;

        // A dead endpoint must not be kept around: it would fail again on every cycle.
        assert!(session_manager.session_map.lock().await.is_empty());
    }

    #[tokio::test]
    async fn test_reconnect_session_rebuilds_stream_on_registered_session() {
        let session_manager = new_session_manager();
        let reconnected = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        {
            let mut session_map = session_manager.session_map.lock().await;
            let mut session = MockSession::default();
            let counter = Arc::clone(&reconnected);
            session.expect_reconnect().returning(move |_| {
                counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            });
            session_map.insert("endpoint_a".to_string(), session);
        }

        session_manager
            .reconnect_session("endpoint_a", &TelemetryCommand::default())
            .await
            .unwrap();

        // The stream was rebuilt on the registered session, which stayed in the map.
        assert_eq!(
            reconnected.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "reconnect should re-establish the stream of the registered session"
        );
        assert_eq!(session_manager.session_map.lock().await.len(), 1);
        assert!(
            session_manager
                .reconnect_session("endpoint_a", &TelemetryCommand::default())
                .await
                .unwrap(),
            "a re-established stream must be reported as restored"
        );
    }

    #[tokio::test]
    async fn test_reconnect_session_keeps_session_that_cannot_be_restored() {
        let session_manager = new_session_manager();
        {
            let mut session_map = session_manager.session_map.lock().await;
            let mut session = MockSession::default();
            session.expect_reconnect().returning(|_| {
                Err(ClientError::new(
                    ErrorKind::ServerUnavailable,
                    "server unavailable",
                    "test.reconnect_session",
                ))
            });
            session_map.insert("endpoint_a".to_string(), session);
        }

        assert!(session_manager
            .reconnect_session("endpoint_a", &TelemetryCommand::default())
            .await
            .is_err());

        // The session must stay registered: the reconnect retry loop keeps driving
        // recovery on it, which is the only driver an idle client has while the server is
        // unavailable.
        assert_eq!(session_manager.session_map.lock().await.len(), 1);
    }

    #[tokio::test]
    async fn test_reconnect_session_rebuilds_missing_session() {
        let _m = MTX.lock();

        let session_manager = new_session_manager();
        let (tx, _rx) = mpsc::channel(16);
        session_manager.set_telemetry_command_channel(tx);
        let created: Arc<parking_lot::Mutex<Vec<String>>> =
            Arc::new(parking_lot::Mutex::new(Vec::new()));
        let context = MockSession::new_context();
        let recorded = Arc::clone(&created);
        context.expect().returning(move |endpoints, _, _, _| {
            recorded.lock().push(endpoints.endpoint_url().to_string());
            let mut session = MockSession::default();
            session.expect_start().returning(|_, _| Ok(()));
            session.expect_set_stream_closed_tx().returning(|_| ());
            session
                .expect_shadow_session()
                .returning(|| MockSession::default());
            Ok(session)
        });

        // A session that was evicted or reaped meanwhile must be rebuilt against the peer
        // that is being reconnected, not against the access endpoint. Rebuilding the access
        // session used to report Ok(true) for a peer that had no stream left, so the
        // reconnect retry loop stopped as if it had restored the connection. The peer here
        // deliberately differs from the default access_url (localhost:8081).
        let peer = "127.0.0.1:9999";
        let restored = session_manager
            .reconnect_session(peer, &TelemetryCommand::default())
            .await
            .unwrap();
        assert!(
            restored,
            "the rebuilt peer stream must be reported as restored"
        );
        assert_eq!(created.lock().as_slice(), &[peer.to_string()]);
        let keys: Vec<String> = session_manager
            .session_map
            .lock()
            .await
            .keys()
            .cloned()
            .collect();
        assert_eq!(
            keys,
            vec![peer.to_string()],
            "the session must be registered under the reconnected peer"
        );
    }

    // The proxy returns one result entry per ack entry, but `entries` is a repeated field: a
    // server that omits it must not panic the caller.
    #[tokio::test]
    async fn client_ack_message_without_result_entry() {
        let _m = MTX.lock();
        let context = MockSession::new_context();
        context.expect().returning(|_, _, _, _| {
            let mut session = MockSession::default();
            session.expect_start().returning(|_, _| Ok(()));
            session.expect_set_stream_closed_tx().returning(|_| ());
            session.expect_shadow_session().returning(|| {
                let mut shadow = MockSession::default();
                shadow.expect_ack_message().returning(|_| {
                    Ok(AckMessageResponse {
                        status: Some(Status {
                            code: Code::Ok as i32,
                            message: "Success".to_string(),
                        }),
                        entries: vec![],
                    })
                });
                shadow
            });
            Ok(session)
        });
        let mut client = new_client_with_session_manager(new_session_manager());
        client.option.group = Some("group".to_string());
        let message = crate::model::message::MessageView::from_pb_message(
            Message {
                topic: Some(Resource {
                    name: "test_topic".to_string(),
                    resource_namespace: "".to_string(),
                }),
                system_properties: Some(pb::SystemProperties {
                    receipt_handle: Some("receipt_handle".to_string()),
                    ..pb::SystemProperties::default()
                }),
                ..Message::default()
            },
            Endpoints::from_url("http://localhost:8081").unwrap(),
        )
        .unwrap();

        let result = client.ack_message(&message).await;
        assert!(result.is_ok(), "{:?}", result);
    }
}
