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

use mockall::automock;
use mockall_double::double;
use parking_lot::{Mutex, RwLock};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

#[double]
use crate::client::Client;
use crate::conf::{BackOffRetryPolicy, ClientOption, PushConsumerOption};
use crate::error::{ClientError, ErrorKind};
use crate::model::common::{ClientType, ConsumeResult, FilterExpression, MessageQueue};
use crate::model::message::{AckMessageEntry, MessageView};
use crate::pb;
use crate::pb::receive_message_response::Content;
use crate::pb::{
    AckMessageRequest, Assignment, ChangeInvisibleDurationRequest,
    ForwardMessageToDeadLetterQueueRequest, QueryAssignmentRequest, ReceiveMessageRequest,
    Resource, Status,
};
#[double]
use crate::session::Session;
use crate::session::{RPCClient, SharedLongPollingTimeout};
use crate::util::{
    build_endpoints_by_message_queue, build_push_consumer_settings, handle_receive_message_status,
    handle_response_status,
};
use tracing::{debug, error, info, warn};

const OPERATION_NEW_PUSH_CONSUMER: &str = "push_consumer.new";
const OPERATION_RECEIVE_MESSAGE: &str = "push_consumer.receive_message";
const OPERATION_ACK_MESSAGE: &str = "push_consumer.ack_message";
const OPERATION_START_PUSH_CONSUMER: &str = "push_consumer.start";
const OPERATION_CHANGE_INVISIBLE_DURATION: &str = "push_consumer.change_invisible_duration";
const OPERATION_FORWARD_TO_DEADLETTER_QUEUE: &str = "push_consumer.forward_to_deadletter_queue";

// Reference Java: ProcessQueueImpl.RECEIVING_FLOW_CONTROL_BACKOFF_DELAY / RECEIVING_FAILURE_BACKOFF_DELAY
const RECEIVING_FLOW_CONTROL_BACKOFF_DELAY: Duration = Duration::from_millis(20);
const RECEIVING_FAILURE_BACKOFF_DELAY: Duration = Duration::from_secs(1);

/// Delay before the next receive attempt after a failure.
fn receive_failure_backoff_delay(error: &ClientError) -> Duration {
    let too_many_requests = (pb::Code::TooManyRequests as i32).to_string();
    let flow_controlled = error
        .context()
        .iter()
        .any(|(key, value)| *key == "code" && *value == too_many_requests);
    if flow_controlled {
        RECEIVING_FLOW_CONTROL_BACKOFF_DELAY
    } else {
        RECEIVING_FAILURE_BACKOFF_DELAY
    }
}

pub type MessageListener = Box<dyn Fn(&MessageView) -> ConsumeResult + Send + Sync>;

/// Invoke the message listener, treating a panic as [`ConsumeResult::FAILURE`].
///
/// Reference Java: `ConsumeTask#call` catches any `Throwable` raised by the listener, logs it
/// and marks the consumption as failure, so the message goes through the normal retry or
/// dead-letter path and the consumer keeps running. Without this, a panic would unwind
/// through the consumer worker task and stop it for good.
///
/// Panics cannot be caught when the binary is built with `panic = "abort"`.
fn consume_message(message_listener: &MessageListener, message: &MessageView) -> ConsumeResult {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| message_listener(message))) {
        Ok(consume_result) => consume_result,
        Err(panic) => {
            let reason = panic
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| panic.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "unknown panic payload".to_string());
            error!(
                "message listener panicked while consuming message, treat it as failure, topic={}, message_id={}, reason={}",
                message.topic(),
                message.message_id(),
                reason
            );
            ConsumeResult::FAILURE
        }
    }
}

pub struct PushConsumer {
    client: Client,
    message_listener: Arc<MessageListener>,
    option: Arc<RwLock<PushConsumerOption>>,
    shutdown_token: Option<CancellationToken>,
    task_tracker: Option<TaskTracker>,
}

impl PushConsumer {
    pub fn new(
        client_option: ClientOption,
        option: PushConsumerOption,
        message_listener: MessageListener,
    ) -> Result<Self, ClientError> {
        if option.consumer_group().is_empty() {
            return Err(ClientError::new(
                ErrorKind::Config,
                "consumer group is required.",
                OPERATION_NEW_PUSH_CONSUMER,
            ));
        }
        if option.subscription_expressions().is_empty() {
            return Err(ClientError::new(
                ErrorKind::Config,
                "subscription expressions is required.",
                OPERATION_NEW_PUSH_CONSUMER,
            ));
        }
        let client_option = ClientOption {
            client_type: ClientType::PushConsumer,
            group: Some(option.consumer_group().to_string()),
            ..client_option
        };
        let client = Client::new(client_option, build_push_consumer_settings(&option))?;
        Ok(Self {
            client,
            message_listener: Arc::new(message_listener),
            option: Arc::new(RwLock::new(option)),
            shutdown_token: None,
            task_tracker: None,
        })
    }

    /// Create a new PushConsumer with an existing client (for LitePushConsumer)
    /// The client should already be configured with the correct ClientType
    pub(crate) fn new_with_client(
        client: Client,
        option: PushConsumerOption,
        message_listener: MessageListener,
    ) -> Result<Self, ClientError> {
        if option.consumer_group().is_empty() {
            return Err(ClientError::new(
                ErrorKind::Config,
                "consumer group is required.",
                OPERATION_NEW_PUSH_CONSUMER,
            ));
        }
        Ok(Self {
            client,
            message_listener: Arc::new(message_listener),
            option: Arc::new(RwLock::new(option)),
            shutdown_token: None,
            task_tracker: None,
        })
    }

    /// Check if the PushConsumer is started
    pub(crate) fn check_started(&self, operation: &'static str) -> Result<(), ClientError> {
        self.client.check_started(operation)
    }

    pub async fn start(&mut self) -> Result<(), ClientError> {
        let (telemetry_command_tx, mut telemetry_command_rx) = mpsc::channel(16);
        self.client.start(telemetry_command_tx).await?;
        let option = Arc::clone(&self.option);
        let long_polling_timeout = self.client.long_polling_timeout_handle();
        let mut rpc_client = self.client.get_session().await?;
        let route_manager = self.client.get_route_manager();
        let topics;
        {
            topics = option
                .read()
                .subscription_expressions()
                .keys()
                .cloned()
                .collect();
        }
        route_manager
            .sync_topic_routes(&mut rpc_client, topics)
            .await?;
        let mut actor_table: HashMap<MessageQueue, MessageQueueActor> = HashMap::new();

        let message_listener = Arc::clone(&self.message_listener);
        let retry_policy: Arc<Mutex<Option<BackOffRetryPolicy>>> = Arc::new(Mutex::new(None));

        let shutdown_token = CancellationToken::new();
        self.shutdown_token = Some(shutdown_token.clone());
        let task_tracker = TaskTracker::new();
        self.task_tracker = Some(task_tracker.clone());

        // Removed queue actors that are still shutting down; the consumer task waits for them
        // before it completes, so that `shutdown` still waits for every in-flight request.
        let dropped_actors = TaskTracker::new();
        task_tracker.spawn(async move {
            let mut scan_assignments_timer =
                tokio::time::interval(std::time::Duration::from_secs(30));
            loop {
                select! {
                    command = telemetry_command_rx.recv() => {
                        if let Some(command) = command {
                            if let pb::telemetry_command::Command::Settings(ref settings) = command {
                                // Apply what the server pushed, otherwise these settings are
                                // silently dropped and the client keeps polling with the
                                // configured values.
                                Self::apply_settings(settings, &option, &long_polling_timeout);
                            }
                            let remote_backoff_policy = BackOffRetryPolicy::try_from(command);
                            if let Ok(remote_backoff_policy) = remote_backoff_policy {
                                retry_policy.lock().replace(remote_backoff_policy);
                            }
                        }
                    }
                    _ = scan_assignments_timer.tick() => {
                        let option_retry_policy;
                        {
                            option_retry_policy = retry_policy.lock().clone();
                        }
                        if option_retry_policy.is_none() {
                            warn!("retry policy is not set. skip scanning.");
                            continue;
                        }
                        let retry_policy_inner = option_retry_policy.unwrap();
                        let consumer_option;
                        {
                            consumer_option = option.read().clone();
                        }
                        let subscription_table = consumer_option.subscription_expressions();
                        // query endpoints from topic route
                        for (topic, filter_expression) in subscription_table.iter() {
                            if let Some(endpoints) = route_manager.pick_endpoints(topic.as_str()) {
                                let request = QueryAssignmentRequest {
                                    topic: Some(Resource {
                                        name: topic.to_string(),
                                        resource_namespace: consumer_option.namespace().to_string(),
                                    }),
                                    group: Some(Resource {
                                        name: consumer_option.consumer_group().to_string(),
                                        resource_namespace: consumer_option.namespace().to_string(),
                                    }),
                                    endpoints: Some(endpoints.into_inner()),
                                };
                                let result = rpc_client.query_assignment(request).await;
                                if let Ok(response) = result {
                                    if handle_response_status(response.status, OPERATION_START_PUSH_CONSUMER).is_ok() {
                                            let result = Self::process_assignments(
                                                &rpc_client,
                                                &consumer_option,
                                                filter_expression,
                                                Arc::clone(&message_listener),
                                                &mut actor_table,
                                                response.assignments,
                                                retry_policy_inner.clone(),
                                            ).await;
                                            match result {
                                                // Shut the removed queues down in the background, so that
                                                // neither the newly assigned queues nor the telemetry loop wait
                                                // for their long polling. Reference Java:
                                                // `PushConsumerImpl#dropProcessQueue` only marks the process
                                                // queue as dropped.
                                                Ok(removed) if !removed.is_empty() => {
                                                    dropped_actors.spawn(Self::shutdown_actors(removed));
                                                }
                                                Ok(_) => {}
                                                Err(e) => error!("process assignments failed: {:?}", e),
                                            }
                                    } else {
                                        error!("query assignment failed, no status in response.");
                                    }
                                } else {
                                    error!("query assignment failed: {:?}", result.unwrap_err());
                                }
                            }
                        }
                    }
                    _ = shutdown_token.cancelled() => {
                        info!("shutdown {:?} actors", actor_table.len());
                        Self::shutdown_actors(actor_table.drain().map(|(_, actor)| actor)).await;
                        dropped_actors.close();
                        dropped_actors.wait().await;
                        break;
                    }
                }
            }
        });
        Ok(())
    }

    /// Start with external telemetry channel (for LitePushConsumer)
    ///
    /// Every telemetry command received from the server is handled by this push consumer and
    /// then forwarded to `telemetry_command_tx`, so that the caller can handle the commands it
    /// is interested in (reference Java: `LitePushConsumerImpl#onSettingsCommand` calls
    /// `super.onSettingsCommand` before syncing the lite settings).
    pub async fn start_with_telemetry(
        &mut self,
        telemetry_command_tx: mpsc::Sender<pb::TelemetryCommand>,
    ) -> Result<(), ClientError> {
        // Extract the command channel from TelemetryCommand
        let (command_tx, mut command_rx) = mpsc::channel(16);
        self.client.start(command_tx).await?;
        let option = Arc::clone(&self.option);
        let long_polling_timeout = self.client.long_polling_timeout_handle();
        let mut rpc_client = self.client.get_session().await?;
        let route_manager = self.client.get_route_manager();
        let topics;
        {
            topics = option
                .read()
                .subscription_expressions()
                .keys()
                .cloned()
                .collect();
        }
        route_manager
            .sync_topic_routes(&mut rpc_client, topics)
            .await?;

        let mut actor_table: HashMap<MessageQueue, MessageQueueActor> = HashMap::new();
        let message_listener = Arc::clone(&self.message_listener);
        let retry_policy: Arc<Mutex<Option<BackOffRetryPolicy>>> = Arc::new(Mutex::new(None));

        let shutdown_token = CancellationToken::new();
        self.shutdown_token = Some(shutdown_token.clone());
        let task_tracker = TaskTracker::new();
        self.task_tracker = Some(task_tracker.clone());

        // Removed queue actors that are still shutting down; the consumer task waits for them
        // before it completes, so that `shutdown` still waits for every in-flight request.
        let dropped_actors = TaskTracker::new();
        task_tracker.spawn(async move {
            let mut scan_assignments_timer =
                tokio::time::interval(std::time::Duration::from_secs(30));
            loop {
                select! {
                    command = command_rx.recv() => {
                        if let Some(command) = command {
                            if let pb::telemetry_command::Command::Settings(ref settings) = command {
                                Self::apply_settings(settings, &option, &long_polling_timeout);
                            }
                            let remote_backoff_policy = BackOffRetryPolicy::try_from(command.clone());
                            if let Ok(remote_backoff_policy) = remote_backoff_policy {
                                retry_policy.lock().replace(remote_backoff_policy);
                            }
                            let forwarded = pb::TelemetryCommand {
                                command: Some(command),
                                ..pb::TelemetryCommand::default()
                            };
                            if let Err(e) = telemetry_command_tx.send(forwarded).await {
                                warn!(
                                    "failed to forward telemetry command, receiver dropped: {:?}",
                                    e.0.command
                                );
                            }
                        }
                    }
                    _ = scan_assignments_timer.tick() => {
                        let option_retry_policy;
                        {
                            option_retry_policy = retry_policy.lock().clone();
                        }
                        if option_retry_policy.is_none() {
                            warn!("retry policy is not set. skip scanning.");
                            continue;
                        }
                        let retry_policy_inner = option_retry_policy.unwrap();
                        let consumer_option;
                        {
                            consumer_option = option.read().clone();
                        }
                        let subscription_table = consumer_option.subscription_expressions();
                        // query endpoints from topic route
                        for (topic, filter_expression) in subscription_table.iter() {
                            if let Some(endpoints) = route_manager.pick_endpoints(topic.as_str()) {
                                let request = QueryAssignmentRequest {
                                    topic: Some(Resource {
                                        name: topic.to_string(),
                                        resource_namespace: consumer_option.namespace().to_string(),
                                    }),
                                    group: Some(Resource {
                                        name: consumer_option.consumer_group().to_string(),
                                        resource_namespace: consumer_option.namespace().to_string(),
                                    }),
                                    endpoints: Some(endpoints.into_inner()),
                                };
                                let result = rpc_client.query_assignment(request).await;
                                if let Ok(response) = result {
                                    if handle_response_status(response.status, OPERATION_START_PUSH_CONSUMER).is_ok() {
                                            let result = Self::process_assignments(
                                                &rpc_client,
                                                &consumer_option,
                                                filter_expression,
                                                Arc::clone(&message_listener),
                                                &mut actor_table,
                                                response.assignments,
                                                retry_policy_inner.clone(),
                                            ).await;
                                            match result {
                                                // Shut the removed queues down in the background, so that
                                                // neither the newly assigned queues nor the telemetry loop wait
                                                // for their long polling. Reference Java:
                                                // `PushConsumerImpl#dropProcessQueue` only marks the process
                                                // queue as dropped.
                                                Ok(removed) if !removed.is_empty() => {
                                                    dropped_actors.spawn(Self::shutdown_actors(removed));
                                                }
                                                Ok(_) => {}
                                                Err(e) => error!("process assignments failed: {:?}", e),
                                            }
                                    } else {
                                        error!("query assignment failed, no status in response.");
                                    }
                                } else {
                                    error!("query assignment failed: {:?}", result.unwrap_err());
                                }
                            }
                        }
                    }
                    _ = shutdown_token.cancelled() => {
                        info!("shutdown {:?} actors", actor_table.len());
                        Self::shutdown_actors(actor_table.drain().map(|(_, actor)| actor)).await;
                        dropped_actors.close();
                        dropped_actors.wait().await;
                        break;
                    }
                }
            }
        });

        info!("PushConsumer started with external telemetry");
        Ok(())
    }

    /// Apply the settings pushed by the server.
    ///
    /// Only subscription settings are applied; the back-off policy is handled separately by
    /// [`BackOffRetryPolicy::try_from`]. Mirrors `PushSubscriptionSettings#sync` of the Java
    /// client, where the same values are also taken from the server-pushed settings.
    ///
    /// The long-polling timeout is also written to the client's shared value, which sessions use
    /// to compute the receive deadline. Otherwise the request would carry the server value while
    /// the client still waits for the configured one, as `ConsumerImpl#receiveMessage` of the Java
    /// client uses the same value for both.
    fn apply_settings(
        settings: &pb::Settings,
        option: &Arc<RwLock<PushConsumerOption>>,
        shared_long_polling_timeout: &SharedLongPollingTimeout,
    ) {
        let Some(pb::settings::PubSub::Subscription(ref subscription)) = settings.pub_sub else {
            warn!(
                "received settings that do not match the client type, pub_sub={:?}",
                settings.pub_sub
            );
            return;
        };

        let mut option = option.write();
        if subscription.receive_batch_size.unwrap_or_default() > 0 {
            option.set_batch_size(subscription.receive_batch_size.unwrap());
        }
        if let Some(ref long_polling_timeout) = subscription.long_polling_timeout {
            let seconds = long_polling_timeout.seconds;
            let nanos = long_polling_timeout.nanos;
            if seconds > 0 || nanos > 0 {
                let long_polling_timeout = Duration::new(seconds as u64, nanos as u32);
                option.set_long_polling_timeout(long_polling_timeout);
                *shared_long_polling_timeout.write() = long_polling_timeout;
            }
        }
        if let Some(fifo) = subscription.fifo {
            option.set_fifo(fifo);
        }
    }

    async fn process_assignments(
        rpc_client: &Session,
        option: &PushConsumerOption,
        filter_expression: &FilterExpression,
        message_listener: Arc<MessageListener>,
        actor_table: &mut HashMap<MessageQueue, MessageQueueActor>,
        mut assignments: Vec<Assignment>,
        retry_policy: BackOffRetryPolicy,
    ) -> Result<Vec<MessageQueueActor>, ClientError> {
        let message_queues: Vec<MessageQueue> = assignments
            .iter_mut()
            .filter_map(|assignment| {
                if let Some(message_queue) = assignment.message_queue.take() {
                    return MessageQueue::from_pb_message_queue(message_queue).ok();
                }
                None
            })
            .collect();
        // remove existing actors from map, the remaining ones will be shutdown.
        let mut actors: Vec<MessageQueueActor> = Vec::with_capacity(actor_table.len());
        message_queues.iter().for_each(|message_queue| {
            let entry = actor_table.remove(message_queue);
            if let Some(actor) = entry {
                actors.push(actor);
            }
        });
        // The remaining actors are no longer assigned. They are handed back to the caller instead
        // of being shut down here: shutting an actor down waits for its in-flight long polling
        // request, which must not delay the newly assigned queues.
        let removed: Vec<MessageQueueActor> = actor_table.drain().map(|(_, actor)| actor).collect();

        for mut actor in actors {
            let option = option.clone();
            actor.set_option(option);
            actor.set_retry_policy(retry_policy.clone());
            actor_table.insert(actor.message_queue.clone(), actor);
        }

        // start new actors
        for message_queue in message_queues {
            if actor_table.contains_key(&message_queue) {
                continue;
            }
            let option = option.clone();
            let mut actor = MessageQueueActor::new(
                rpc_client.shadow_session(),
                message_queue.clone(),
                filter_expression.clone(),
                option,
                Arc::clone(&message_listener),
                retry_policy.clone(),
            );
            let result = actor.start().await;
            if result.is_ok() {
                actor_table.insert(message_queue, actor);
            }
        }
        Ok(removed)
    }

    /// Shut the actors down concurrently.
    ///
    /// Each actor waits for its in-flight receive requests to finish, which can take up to a long
    /// polling timeout, so shutting them down one by one takes that time once per actor.
    async fn shutdown_actors(actors: impl IntoIterator<Item = MessageQueueActor>) {
        let task_tracker = TaskTracker::new();
        for actor in actors {
            task_tracker.spawn(async move {
                let message_queue = actor.message_queue.clone();
                if let Err(e) = actor.shutdown().await {
                    error!(
                        "failed to shutdown queue actor {:?}: {:?}",
                        message_queue, e
                    );
                }
            });
        }
        task_tracker.close();
        task_tracker.wait().await;
    }

    pub async fn shutdown(mut self) -> Result<(), ClientError> {
        if let Some(shutdown_token) = self.shutdown_token.take() {
            shutdown_token.cancel();
        }
        if let Some(task_tracker) = self.task_tracker.take() {
            task_tracker.close();
            task_tracker.wait().await;
        }
        self.client.shutdown().await?;
        Ok(())
    }

    /// Shutdown without consuming self (for LitePushConsumer)
    pub async fn shutdown_ref(&mut self) -> Result<(), ClientError> {
        if let Some(shutdown_token) = self.shutdown_token.take() {
            shutdown_token.cancel();
        }
        if let Some(task_tracker) = self.task_tracker.take() {
            task_tracker.close();
            task_tracker.wait().await;
        }
        self.client.shutdown_ref().await?;
        Ok(())
    }

    async fn receive_messages<T: RPCClient + 'static>(
        rpc_client: &mut T,
        message_queue: &MessageQueue,
        filter_expression: &FilterExpression,
        option: &PushConsumerOption,
    ) -> Result<Vec<MessageView>, ClientError> {
        let request = ReceiveMessageRequest {
            group: Some(option.get_consumer_group_resource()),
            message_queue: Some(message_queue.to_pb_message_queue()),
            filter_expression: Some(pb::FilterExpression {
                expression: filter_expression.expression().to_string(),
                r#type: filter_expression.filter_type() as i32,
            }),
            batch_size: option.batch_size(),
            auto_renew: true,
            invisible_duration: None,
            long_polling_timeout: Some(
                prost_types::Duration::try_from(*option.long_polling_timeout()).unwrap(),
            ),
            attempt_id: None,
        };
        let responses = rpc_client.receive_message(request).await?;
        let mut messages: Vec<MessageView> = Vec::with_capacity(option.batch_size() as usize);
        let endpoints = build_endpoints_by_message_queue(
            &message_queue.to_pb_message_queue(),
            OPERATION_RECEIVE_MESSAGE,
        )?;
        let mut status: Option<Status> = None;
        let mut _delivery_timestamp: Option<prost_types::Timestamp> = None;

        for response in responses {
            if let Some(content) = response.content {
                match content {
                    Content::Status(response_status) => {
                        // Store the status for later processing
                        status = Some(response_status);
                    }
                    Content::DeliveryTimestamp(timestamp) => {
                        // Store the delivery timestamp for later use
                        _delivery_timestamp = Some(timestamp);
                    }
                    Content::Message(message) => {
                        messages.push(
                            MessageView::from_pb_message(message, endpoints.clone()).ok_or(
                                ClientError::new(
                                    ErrorKind::InvalidMessage,
                                    "error parsing from pb",
                                    OPERATION_RECEIVE_MESSAGE,
                                ),
                            )?,
                        );
                    }
                }
            }
        }

        // Handle the status message properly
        if let Some(status) = status {
            handle_receive_message_status(&status, OPERATION_RECEIVE_MESSAGE)?;
        }
        Ok(messages)
    }
}

#[derive(Debug)]
enum AckEntryItem {
    Ack(AckEntry),
    Nack(NackEntry),
    Dlq(DlqEntry),
}

impl AckEntryItem {
    fn inc_attempt(&mut self) {
        match self {
            Self::Ack(ack_entry) => {
                ack_entry.attempt += 1;
            }
            Self::Nack(nack_entry) => {
                nack_entry.attempt += 1;
            }
            Self::Dlq(entry) => {
                entry.attempt += 1;
            }
        }
    }
}

#[derive(Debug)]
struct AckEntry {
    message: MessageView,
    attempt: usize,
}

impl AckEntry {
    fn new(message: MessageView) -> Self {
        Self {
            message,
            attempt: 0,
        }
    }
}

#[derive(Debug)]
struct NackEntry {
    message: MessageView,
    attempt: usize,
    invisible_duration: Duration,
}

impl NackEntry {
    fn new(message: MessageView, invisible_duration: Duration) -> Self {
        Self {
            message,
            invisible_duration,
            attempt: 0,
        }
    }
}

#[derive(Debug)]
struct DlqEntry {
    message: MessageView,
    attempt: usize,
    delivery_attempt: i32,
    max_delivery_attempt: i32,
}

impl DlqEntry {
    fn new(message: MessageView, delivery_attempt: i32, max_delivery_attempt: i32) -> Self {
        Self {
            message,
            delivery_attempt,
            max_delivery_attempt,
            attempt: 0,
        }
    }
}

struct MessageQueueActor {
    rpc_client: Session,
    message_queue: MessageQueue,
    // Taken from the subscription table when the queue is assigned (same as Java's
    // ProcessQueueImpl). Never look it up by the assigned queue's topic name, because
    // servers may return a decorated name such as "<instance>%<topic>".
    filter_expression: FilterExpression,
    option: PushConsumerOption,
    message_listener: Arc<MessageListener>,
    retry_policy: BackOffRetryPolicy,
    shutdown_token: Option<CancellationToken>,
    task_tracker: Option<TaskTracker>,
}

#[automock]
impl MessageQueueActor {
    pub(crate) fn new(
        rpc_client: Session,
        message_queue: MessageQueue,
        filter_expression: FilterExpression,
        option: PushConsumerOption,
        message_listener: Arc<MessageListener>,
        retry_policy: BackOffRetryPolicy,
    ) -> Self {
        Self {
            rpc_client,
            message_queue,
            filter_expression,
            option,
            message_listener: Arc::clone(&message_listener),
            retry_policy,
            shutdown_token: None,
            task_tracker: None,
        }
    }

    // The following two methods won't affect the running actor in fact.
    pub(crate) fn set_option(&mut self, option: PushConsumerOption) {
        self.option = option;
    }

    pub(crate) fn set_retry_policy(&mut self, retry_policy: BackOffRetryPolicy) {
        self.retry_policy = retry_policy;
    }

    pub(crate) async fn start(&mut self) -> Result<(), ClientError> {
        debug!("start a new queue actor {:?}", self.message_queue);
        let shutdown_token = CancellationToken::new();
        self.shutdown_token = Some(shutdown_token.clone());
        let task_tracker = TaskTracker::new();
        self.task_tracker = Some(task_tracker.clone());

        let mut consumer_worker_count = self.option.consumer_worker_count_each_queue();
        if self.option.fifo() {
            consumer_worker_count = 1;
        }
        info!("start consumer worker count: {}", consumer_worker_count);

        for _ in 0..consumer_worker_count {
            // create consumer worker
            let mut consumer_worker: ConsumerWorker;
            if self.option.fifo() {
                consumer_worker = ConsumerWorker::Fifo(FifoConsumerWorker::new(
                    self.rpc_client.shadow_session(),
                    Arc::clone(&self.message_listener),
                ));
            } else {
                consumer_worker = ConsumerWorker::Standard(StandardConsumerWorker::new(
                    self.rpc_client.shadow_session(),
                    Arc::clone(&self.message_listener),
                ));
            }
            let mut ticker = tokio::time::interval(Duration::from_millis(1));
            let message_queue = self.message_queue.clone();
            let filter_expression = self.filter_expression.clone();
            let option = self.option.clone();
            let retry_policy = self.retry_policy.clone();
            // TODO: Pass is_lite_consumer from Client when LitePushConsumer is fully integrated
            let mut ack_processor = AckEntryProcessor::new(
                self.rpc_client.shadow_session(),
                self.option.get_consumer_group_resource(),
                self.message_queue.topic.to_owned(),
                false, // Default to false for now, will be set by LitePushConsumer
            );
            ack_processor.start().await?;
            let shutdown_token = shutdown_token.clone();
            task_tracker.spawn(async move {
                loop {
                    select! {
                        // Check shutdown first. The ticker is always ready again once a long
                        // polling request returns, so an unbiased select would keep issuing new
                        // receive requests after shutdown started, like Java's
                        // ProcessQueueImpl checking consumer.isRunning() before receiving.
                        biased;
                        _ = shutdown_token.cancelled() => {
                            ack_processor.shutdown().await;
                            break;
                        }
                        _ = ticker.tick() => {
                            let result = consumer_worker.receive_messages(&message_queue, &filter_expression, &option, &mut ack_processor, &retry_policy).await;
                            if let Err(e) = result {
                                // Reference Java: ProcessQueueImpl.onReceiveMessageException()
                                let delay = receive_failure_backoff_delay(&e);
                                error!("receive messages error, retry after {:?}, mq={:?}: {:?}", delay, message_queue, e);
                                select! {
                                    _ = tokio::time::sleep(delay) => {}
                                    _ = shutdown_token.cancelled() => {}
                                }
                            }
                        }
                    }
                }
            });
        }
        Ok(())
    }

    pub(crate) async fn shutdown(mut self) -> Result<(), ClientError> {
        debug!("shutdown queue actor {:?}", self.message_queue);
        if let Some(shutdown_token) = self.shutdown_token.take() {
            shutdown_token.cancel();
        }
        if let Some(task_tracker) = self.task_tracker.take() {
            task_tracker.close();
            task_tracker.wait().await;
        }
        Ok(())
    }
}

enum ConsumerWorker {
    Standard(StandardConsumerWorker),
    Fifo(FifoConsumerWorker),
}

impl ConsumerWorker {
    async fn receive_messages(
        &mut self,
        message_queue: &MessageQueue,
        filter_expression: &FilterExpression,
        option: &PushConsumerOption,
        ack_processor: &mut AckEntryProcessor,
        retry_policy: &BackOffRetryPolicy,
    ) -> Result<(), ClientError> {
        match self {
            ConsumerWorker::Standard(consumer_worker) => {
                consumer_worker
                    .receive_messages(
                        message_queue,
                        filter_expression,
                        option,
                        ack_processor,
                        retry_policy,
                    )
                    .await
            }
            ConsumerWorker::Fifo(consumer_worker) => {
                consumer_worker
                    .receive_messages(
                        message_queue,
                        filter_expression,
                        option,
                        ack_processor,
                        retry_policy,
                    )
                    .await
            }
        }
    }
}

struct StandardConsumerWorker {
    rpc_client: Session,
    message_listener: Arc<MessageListener>,
}

impl StandardConsumerWorker {
    fn new(rpc_client: Session, message_listener: Arc<MessageListener>) -> Self {
        Self {
            rpc_client,
            message_listener,
        }
    }

    async fn receive_messages(
        &mut self,
        message_queue: &MessageQueue,
        filter_expression: &FilterExpression,
        option: &PushConsumerOption,
        ack_processor: &mut AckEntryProcessor,
        retry_policy: &BackOffRetryPolicy,
    ) -> Result<(), ClientError> {
        let messages = PushConsumer::receive_messages(
            &mut self.rpc_client,
            message_queue,
            filter_expression,
            option,
        )
        .await?;
        for message in messages {
            // Reference Java: `StandardConsumeService#consume` nacks a corrupted message
            // instead of passing it to the listener.
            if message.is_corrupted() {
                error!(
                    "Message is corrupted for standard consumption, prepare to discard it, topic={}, message_id={}",
                    message.topic(),
                    message.message_id()
                );
                let delay = retry_policy.get_next_attempt_delay(message.delivery_attempt());
                ack_processor
                    .change_invisible_duration(message, delay)
                    .await;
                continue;
            }
            let consume_result = consume_message(&self.message_listener, &message);
            match consume_result {
                ConsumeResult::SUCCESS => {
                    ack_processor.ack_message(message).await;
                }
                ConsumeResult::FAILURE => {
                    let delay = retry_policy.get_next_attempt_delay(message.delivery_attempt());
                    ack_processor
                        .change_invisible_duration(message, delay)
                        .await;
                }
            }
        }

        Ok(())
    }
}

struct FifoConsumerWorker {
    rpc_client: Session,
    message_listener: Arc<MessageListener>,
}

impl FifoConsumerWorker {
    fn new(rpc_client: Session, message_listener: Arc<MessageListener>) -> Self {
        Self {
            rpc_client,
            message_listener,
        }
    }

    async fn receive_messages(
        &mut self,
        message_queue: &MessageQueue,
        filter_expression: &FilterExpression,
        option: &PushConsumerOption,
        ack_processor: &mut AckEntryProcessor,
        retry_policy: &BackOffRetryPolicy,
    ) -> Result<(), ClientError> {
        let messages = PushConsumer::receive_messages(
            &mut self.rpc_client,
            message_queue,
            filter_expression,
            option,
        )
        .await?;

        // If FIFO consume accelerator is enabled, consume messages in parallel by messageGroup
        if option.enable_fifo_consume_accelerator() && !messages.is_empty() {
            self.consume_with_accelerator(messages, ack_processor, retry_policy)
                .await;
        } else {
            // Use sequential consumption for standard FIFO behavior
            for message in messages {
                self.consume_message_sequentially(message, ack_processor, retry_policy)
                    .await;
            }
        }

        Ok(())
    }

    /// Consume messages with accelerator: messages with the same messageGroup are consumed sequentially,
    /// but messages with different messageGroups are consumed in parallel.
    async fn consume_with_accelerator(
        &self,
        messages: Vec<MessageView>,
        ack_processor: &mut AckEntryProcessor,
        retry_policy: &BackOffRetryPolicy,
    ) {
        let message_count = messages.len();

        // Group messages by message_group
        let mut grouped_messages: HashMap<Option<String>, Vec<MessageView>> = HashMap::new();
        for message in messages {
            let group = message.message_group().map(|s| s.to_string());
            grouped_messages.entry(group).or_default().push(message);
        }

        info!(
            "FIFO consume accelerator enabled, message_count={}, group_count={}",
            message_count,
            grouped_messages.len()
        );

        // Spawn a task for each message group to consume messages in parallel
        let mut handles = vec![];
        for (_group, group_messages) in grouped_messages {
            let message_listener = Arc::clone(&self.message_listener);
            let mut ack_processor_shadow = ack_processor.shadow_self();
            let retry_policy = retry_policy.clone();

            let handle = tokio::spawn(async move {
                // Within the same group, consume messages sequentially
                for message in group_messages {
                    Self::consume_message_sequentially_static(
                        message,
                        &message_listener,
                        &mut ack_processor_shadow,
                        &retry_policy,
                    )
                    .await;
                }
            });
            handles.push(handle);
        }

        // Wait for all group tasks to complete
        for handle in handles {
            let _ = handle.await;
        }
    }

    /// Consume a single message sequentially with retries (instance method)
    async fn consume_message_sequentially(
        &self,
        message: MessageView,
        ack_processor: &mut AckEntryProcessor,
        retry_policy: &BackOffRetryPolicy,
    ) {
        Self::consume_message_sequentially_static(
            message,
            &self.message_listener,
            ack_processor,
            retry_policy,
        )
        .await;
    }

    /// Consume a single message sequentially with retries (static method for use in spawned tasks)
    ///
    /// Reference Java: `ProcessQueueImpl#eraseFifoMessage`. While the delivery attempt is below
    /// the max attempts, a failed message is consumed again locally after the delay of the
    /// current attempt, with its delivery attempt increased, so the listener sees 1, 2, ...,
    /// max attempts. Then the message is forwarded to the DLQ with its current delivery attempt.
    async fn consume_message_sequentially_static(
        mut message: MessageView,
        message_listener: &Arc<MessageListener>,
        ack_processor: &mut AckEntryProcessor,
        retry_policy: &BackOffRetryPolicy,
    ) {
        let max_delivery_attempts = retry_policy.get_max_attempts();
        // Reference Java: `FifoConsumeService#getNextValidMessage` forwards a corrupted message
        // to the dead letter queue instead of passing it to the listener.
        if message.is_corrupted() {
            error!(
                "Message is corrupted for FIFO consumption, prepare to discard it, topic={}, message_id={}",
                message.topic(),
                message.message_id()
            );
            let delivery_attempt = message.delivery_attempt();
            ack_processor
                .forward_to_deadletter_queue(message, delivery_attempt, max_delivery_attempts)
                .await;
            return;
        }
        loop {
            let consume_result = consume_message(message_listener, &message);
            match consume_result {
                ConsumeResult::SUCCESS => {
                    ack_processor.ack_message(message).await;
                    break;
                }
                ConsumeResult::FAILURE => {
                    let delivery_attempt = message.delivery_attempt();
                    if delivery_attempt < max_delivery_attempts {
                        let delay = retry_policy.get_next_attempt_delay(delivery_attempt);
                        message.increment_and_get_delivery_attempt();
                        tokio::time::sleep(delay).await;
                    } else {
                        ack_processor
                            .forward_to_deadletter_queue(
                                message,
                                delivery_attempt,
                                max_delivery_attempts,
                            )
                            .await;
                        break;
                    }
                }
            }
        }
    }
}

struct AckEntryProcessor {
    rpc_client: Session,
    consumer_group: Resource,
    topic: Resource,
    is_lite_consumer: bool, // Flag to indicate if this is a lite consumer
    ack_entry_sender: Option<mpsc::Sender<AckEntryItem>>,
    shutdown_token: Option<CancellationToken>,
    task_tracker: Option<TaskTracker>,
}

impl AckEntryProcessor {
    fn new(
        rpc_client: Session,
        consumer_group: Resource,
        topic: Resource,
        is_lite_consumer: bool,
    ) -> Self {
        Self {
            rpc_client,
            consumer_group,
            topic,
            is_lite_consumer,
            ack_entry_sender: None,
            shutdown_token: None,
            task_tracker: None,
        }
    }

    fn shadow_self(&self) -> Self {
        Self {
            rpc_client: self.rpc_client.shadow_session(),
            consumer_group: self.consumer_group.clone(),
            topic: self.topic.clone(),
            is_lite_consumer: self.is_lite_consumer,
            ack_entry_sender: None,
            shutdown_token: None,
            task_tracker: None,
        }
    }

    async fn ack_message(&mut self, message: MessageView) {
        let result = self.ack_message_inner(&message).await;
        if result.is_err() {
            if let Some(ack_entry_sender) = self.ack_entry_sender.as_ref() {
                let send_result = ack_entry_sender
                    .send(AckEntryItem::Ack(AckEntry::new(message)))
                    .await;
                if send_result.is_err() {
                    error!("put ack entry to queue error {:?}", send_result.err());
                }
            } else {
                error!("The ack entry sender is not set. Drop the ack message.");
            }
        }
    }

    async fn change_invisible_duration(
        &mut self,
        message: MessageView,
        invisible_duration: Duration,
    ) {
        let result = self
            .change_invisible_duration_inner(&message, invisible_duration)
            .await;
        if result.is_err() {
            if let Some(ack_entry_sender) = self.ack_entry_sender.as_ref() {
                let send_result = ack_entry_sender
                    .send(AckEntryItem::Nack(NackEntry::new(
                        message,
                        invisible_duration,
                    )))
                    .await;
                if send_result.is_err() {
                    error!("put nack entry to queue error {:?}", send_result.err());
                }
            } else {
                error!("Drop the nack message due to ack entry sender is not set.")
            }
        }
    }

    async fn forward_to_deadletter_queue(
        &mut self,
        message: MessageView,
        delivery_attempt: i32,
        max_delivery_attempts: i32,
    ) {
        let result = self
            .forward_to_deadletter_queue_inner(&message, delivery_attempt, max_delivery_attempts)
            .await;
        if result.is_err() {
            if let Some(ack_entry_sender) = self.ack_entry_sender.as_ref() {
                let send_result = ack_entry_sender
                    .send(AckEntryItem::Dlq(DlqEntry::new(
                        message,
                        delivery_attempt,
                        max_delivery_attempts,
                    )))
                    .await;
                if send_result.is_err() {
                    error!("put dlq entry to queue error {:?}", send_result.err());
                }
            } else {
                error!("Drop the dlq message due to ack edntry sender is not set.");
            }
        }
    }

    async fn forward_to_deadletter_queue_inner(
        &mut self,
        message: &MessageView,
        delivery_attempt: i32,
        max_delivery_attempts: i32,
    ) -> Result<(), ClientError> {
        // Directly get lite_topic from message if it exists
        // This solves the duplicate consumption problem by ensuring ForwardToDeadLetterQueue requests
        // always include lite_topic when the message has it, regardless of client type flag
        let lite_topic = message.lite_topic().map(|s| s.to_string());

        let request = ForwardMessageToDeadLetterQueueRequest {
            group: Some(self.consumer_group.clone()),
            topic: Some(self.topic.clone()),
            receipt_handle: message.receipt_handle(),
            message_id: message.message_id().to_string(),
            delivery_attempt,
            max_delivery_attempts,
            lite_topic,
        };
        let response = self.rpc_client.forward_to_deadletter_queue(request).await?;
        handle_response_status(response.status, OPERATION_FORWARD_TO_DEADLETTER_QUEUE)
    }

    async fn change_invisible_duration_inner(
        &mut self,
        ack_entry: &MessageView,
        invisible_duration: Duration,
    ) -> Result<(), ClientError> {
        // Directly get lite_topic from message if it exists
        // This solves the duplicate consumption problem by ensuring ChangeInvisibleDuration requests
        // always include lite_topic when the message has it, regardless of client type flag
        let lite_topic = ack_entry.lite_topic().map(|s| s.to_string());

        let request = ChangeInvisibleDurationRequest {
            group: Some(self.consumer_group.clone()),
            topic: Some(self.topic.clone()),
            receipt_handle: ack_entry.receipt_handle().to_string(),
            message_id: ack_entry.message_id().to_string(),
            invisible_duration: Some(prost_types::Duration::try_from(invisible_duration).map_err(
                |_| {
                    ClientError::new(
                        ErrorKind::InvalidMessage,
                        "invalid invisbile duration",
                        OPERATION_CHANGE_INVISIBLE_DURATION,
                    )
                },
            )?),
            lite_topic,
            suspend: None,
        };
        let response = self.rpc_client.change_invisible_duration(request).await?;
        handle_response_status(response.status, OPERATION_CHANGE_INVISIBLE_DURATION)
    }

    async fn start(&mut self) -> Result<(), ClientError> {
        let (ack_entry_sender, mut ack_entry_receiver) = mpsc::channel(1024);
        self.ack_entry_sender = Some(ack_entry_sender);
        let mut ack_entry_queue: VecDeque<AckEntryItem> = VecDeque::new();
        let mut ack_ticker = tokio::time::interval(Duration::from_millis(100));
        let mut processor = self.shadow_self();
        let shutdown_token = CancellationToken::new();
        self.shutdown_token = Some(shutdown_token.clone());
        let task_tracker = TaskTracker::new();
        self.task_tracker = Some(task_tracker.clone());
        task_tracker.spawn(async move {
            loop {
                select! {
                    _ = ack_ticker.tick() => {
                        let result = processor.process_ack_entry_queue(&mut ack_entry_queue).await;
                        if result.is_err() {
                            error!("process ack entry queue failed: {:?}", result);
                        }
                    }
                    Some(ack_entry) = ack_entry_receiver.recv() => {
                        ack_entry_queue.push_back(ack_entry);
                        debug!("ack entry queue size: {}", ack_entry_queue.len());
                    }
                    _ = shutdown_token.cancelled() => {
                        info!("need to process remaining {} entries on shutdown.", ack_entry_queue.len());
                        processor.flush_ack_entry_queue(&mut ack_entry_queue).await;
                        break;
                    }
                }
            }
        });
        Ok(())
    }

    async fn process_ack_entry_queue(
        &mut self,
        ack_entry_queue: &mut VecDeque<AckEntryItem>,
    ) -> Result<(), ClientError> {
        if let Some(ack_entry_item) = ack_entry_queue.front_mut() {
            let result = match ack_entry_item {
                AckEntryItem::Ack(ack_entry) => self.ack_message_inner(&ack_entry.message).await,
                AckEntryItem::Nack(nack_entry) => {
                    self.change_invisible_duration_inner(
                        &nack_entry.message,
                        nack_entry.invisible_duration,
                    )
                    .await
                }
                AckEntryItem::Dlq(entry) => {
                    self.forward_to_deadletter_queue_inner(
                        &entry.message,
                        entry.delivery_attempt,
                        entry.max_delivery_attempt,
                    )
                    .await
                }
            };
            if result.is_ok() {
                ack_entry_queue.pop_front();
            } else {
                error!("ack message failed: {:?}, will deliver later.", result);
                ack_entry_item.inc_attempt();
            }
        }
        Ok(())
    }

    async fn flush_ack_entry_queue(&mut self, ack_entry_queue: &mut VecDeque<AckEntryItem>) {
        for ack_entry_item in ack_entry_queue {
            match ack_entry_item {
                AckEntryItem::Ack(entry) => {
                    let _ = self.ack_message_inner(&entry.message).await;
                }
                AckEntryItem::Nack(entry) => {
                    let _ = self
                        .change_invisible_duration_inner(&entry.message, entry.invisible_duration)
                        .await;
                }
                AckEntryItem::Dlq(entry) => {
                    let _ = self
                        .forward_to_deadletter_queue_inner(
                            &entry.message,
                            entry.delivery_attempt,
                            entry.max_delivery_attempt,
                        )
                        .await;
                }
            }
        }
    }

    async fn ack_message_inner(&mut self, ack_entry: &MessageView) -> Result<(), ClientError> {
        // Directly get lite_topic from message if it exists
        // This solves the duplicate consumption problem by ensuring ACK requests
        // always include lite_topic when the message has it, regardless of client type flag
        let lite_topic = ack_entry.lite_topic().map(|s| s.to_string());

        let request = AckMessageRequest {
            group: Some(self.consumer_group.clone()),
            topic: Some(self.topic.clone()),
            entries: vec![pb::AckMessageEntry {
                message_id: ack_entry.message_id().to_string(),
                receipt_handle: ack_entry.receipt_handle().to_string(),
                lite_topic,
            }],
        };
        let response = self.rpc_client.ack_message(request).await?;
        handle_response_status(response.status, OPERATION_ACK_MESSAGE)
    }

    async fn shutdown(mut self) {
        if let Some(shutdown_token) = self.shutdown_token.take() {
            shutdown_token.cancel();
        }
        if let Some(task_tracker) = self.task_tracker.take() {
            info!("waiting for task to complete");
            task_tracker.close();
            task_tracker.wait().await;
            info!("task completed");
        }
        info!("The ack entry processor shuts down completely.");
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::{str::FromStr, vec};

    use pb::{
        AckMessageResponse, Address, Broker, ChangeInvisibleDurationResponse, Code,
        ForwardMessageToDeadLetterQueueResponse, QueryRouteResponse, ReceiveMessageResponse,
        Status, SystemProperties,
    };

    use crate::client::TopicRouteManager;
    use crate::model::common::{Endpoints, FilterType};
    use crate::{
        conf::{CustomizedBackOffRetryPolicy, ExponentialBackOffRetryPolicy},
        model::common::FilterExpression,
    };

    use super::*;

    #[test]
    fn test_new_push_consumer() {
        let _m = crate::client::tests::MTX.lock();
        let result = PushConsumer::new(
            ClientOption::default(),
            PushConsumerOption::default(),
            Box::new(|_| ConsumeResult::SUCCESS),
        );
        assert!(result.is_err());

        let mut option = PushConsumerOption::default();
        option.set_consumer_group("test");
        let result2 = PushConsumer::new(
            ClientOption::default(),
            option,
            Box::new(|_| ConsumeResult::SUCCESS),
        );
        assert!(result2.is_err());

        let mut option2 = PushConsumerOption::default();
        option2.set_consumer_group("test");
        option2.subscribe("test_topic", FilterExpression::new(FilterType::Tag, "*"));
        let context = Client::new_context();
        context.expect().returning(|_, _| Ok(Client::default()));
        let result3 = PushConsumer::new(
            ClientOption::default(),
            option2,
            Box::new(|_| ConsumeResult::SUCCESS),
        );
        assert!(result3.is_ok());
    }

    #[tokio::test]
    async fn test_push_consumer_start_shutdown() -> Result<(), ClientError> {
        let _m = crate::client::tests::MTX.lock();
        let context = Client::new_context();
        let mut client = Client::default();
        client.expect_start().return_once(|_| Ok(()));
        client.expect_shutdown().return_once(|| Ok(()));
        client
            .expect_long_polling_timeout_handle()
            .returning(|| Arc::new(RwLock::new(Duration::from_secs(40))));
        client.expect_get_session().returning(|| {
            let mut session = Session::default();
            session.expect_query_route().returning(|_| {
                Ok(QueryRouteResponse {
                    status: Some(Status {
                        code: Code::Ok as i32,
                        message: "Success".to_string(),
                    }),
                    message_queues: vec![pb::MessageQueue {
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
            });
            Ok(session)
        });
        client.expect_get_route_manager().returning(|| {
            TopicRouteManager::new(
                "".to_string(),
                Endpoints::from_url("http://localhost:8081").unwrap(),
            )
        });
        context.expect().return_once(|_, _| Ok(client));
        let mut push_consumer_option = PushConsumerOption::default();
        push_consumer_option.set_consumer_group("test_group");
        push_consumer_option.subscribe("test_topic", FilterExpression::new(FilterType::Tag, "*"));
        let mut push_consumer = PushConsumer::new(
            ClientOption::default(),
            push_consumer_option,
            Box::new(|_| ConsumeResult::SUCCESS),
        )?;
        push_consumer.start().await?;
        push_consumer.shutdown().await?;

        Ok(())
    }

    /// Starts a push consumer on a mocked client, pushes a `Settings` command with a 20s
    /// long-polling timeout, and returns the shared long-polling timeout of the client, which
    /// sessions use to compute the receive deadline.
    async fn apply_server_long_polling_timeout(with_telemetry: bool) -> Duration {
        let telemetry_tx = Arc::new(Mutex::new(None));
        let shared_long_polling_timeout = Arc::new(RwLock::new(Duration::from_secs(40)));
        // The mocked `Client::new` is only needed while constructing the consumer, so the lock is
        // released before the first await.
        let mut push_consumer = {
            let _m = crate::client::tests::MTX.lock();
            let context = Client::new_context();
            let mut client = Client::default();
            let telemetry_tx_clone = Arc::clone(&telemetry_tx);
            client.expect_start().return_once(move |tx| {
                telemetry_tx_clone.lock().replace(tx);
                Ok(())
            });
            client.expect_shutdown().return_once(|| Ok(()));
            client.expect_shutdown_ref().return_once(|| Ok(()));
            client.expect_get_session().returning(|| {
                let mut session = Session::default();
                session.expect_query_route().returning(|_| {
                    Ok(QueryRouteResponse {
                        status: Some(Status {
                            code: Code::Ok as i32,
                            message: "Success".to_string(),
                        }),
                        message_queues: vec![],
                    })
                });
                Ok(session)
            });
            client.expect_get_route_manager().returning(|| {
                TopicRouteManager::new(
                    "".to_string(),
                    Endpoints::from_url("http://localhost:8081").unwrap(),
                )
            });
            let handle = Arc::clone(&shared_long_polling_timeout);
            client
                .expect_long_polling_timeout_handle()
                .returning(move || Arc::clone(&handle));
            context.expect().return_once(|_, _| Ok(client));

            let mut option = PushConsumerOption::default();
            option.set_consumer_group("test_group");
            option.subscribe("test_topic", FilterExpression::new(FilterType::Tag, "*"));
            PushConsumer::new(
                ClientOption::default(),
                option,
                Box::new(|_| ConsumeResult::SUCCESS),
            )
            .unwrap()
        };
        if with_telemetry {
            let (tx, _rx) = mpsc::channel(16);
            push_consumer.start_with_telemetry(tx).await.unwrap();
        } else {
            push_consumer.start().await.unwrap();
        }

        let tx = telemetry_tx.lock().take().unwrap();
        tx.send(pb::telemetry_command::Command::Settings(pb::Settings {
            pub_sub: Some(pb::settings::PubSub::Subscription(pb::Subscription {
                long_polling_timeout: Some(prost_types::Duration {
                    seconds: 20,
                    nanos: 0,
                }),
                ..Default::default()
            })),
            ..Default::default()
        }))
        .await
        .unwrap();
        // Wait until the telemetry loop has handled the command.
        let _ = tokio::time::timeout(Duration::from_secs(1), async {
            while push_consumer.option.read().long_polling_timeout() != &Duration::from_secs(20) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        assert_eq!(
            push_consumer.option.read().long_polling_timeout(),
            &Duration::from_secs(20)
        );
        let shared = *shared_long_polling_timeout.read();
        if with_telemetry {
            push_consumer.shutdown_ref().await.unwrap();
        } else {
            push_consumer.shutdown().await.unwrap();
        }
        shared
    }

    #[tokio::test]
    async fn test_server_long_polling_timeout_updates_receive_deadline() {
        // The request carries the server value, so the deadline must follow it too.
        // Reference Java: ConsumerImpl#receiveMessage uses the same awaitDuration for both.
        assert_eq!(
            apply_server_long_polling_timeout(false).await,
            Duration::from_secs(20)
        );
    }

    #[tokio::test]
    async fn test_server_long_polling_timeout_updates_receive_deadline_with_telemetry() {
        assert_eq!(
            apply_server_long_polling_timeout(true).await,
            Duration::from_secs(20)
        );
    }

    // LitePushConsumer relies on start_with_telemetry() forwarding the telemetry commands
    // (Settings, NotifyUnsubscribeLiteCommand) to the channel it passes in. Reference Java:
    // LitePushConsumerImpl#onSettingsCommand / #onNotifyUnsubscribeLiteCommand.
    #[tokio::test]
    async fn test_start_with_telemetry_forwards_commands() -> Result<(), ClientError> {
        let _m = crate::client::tests::MTX.lock();
        let context = Client::new_context();
        let command_tx_slot: Arc<Mutex<Option<mpsc::Sender<pb::telemetry_command::Command>>>> =
            Arc::new(Mutex::new(None));
        let slot = Arc::clone(&command_tx_slot);
        let mut client = Client::default();
        client.expect_start().return_once(move |tx| {
            slot.lock().replace(tx);
            Ok(())
        });
        client.expect_shutdown_ref().return_once(|| Ok(()));
        client.expect_get_session().returning(|| {
            let mut session = Session::default();
            session.expect_query_route().returning(|_| {
                Ok(QueryRouteResponse {
                    status: Some(Status {
                        code: Code::Ok as i32,
                        message: "Success".to_string(),
                    }),
                    message_queues: vec![],
                })
            });
            Ok(session)
        });
        client.expect_get_route_manager().returning(|| {
            TopicRouteManager::new(
                "".to_string(),
                Endpoints::from_url("http://localhost:8081").unwrap(),
            )
        });
        client
            .expect_long_polling_timeout_handle()
            .returning(|| Arc::new(RwLock::new(Duration::from_secs(40))));
        context.expect().return_once(|_, _| Ok(client));

        let mut option = PushConsumerOption::default();
        option.set_consumer_group("test_group");
        option.subscribe("test_topic", FilterExpression::new(FilterType::Tag, "*"));
        let mut push_consumer = PushConsumer::new(
            ClientOption::default(),
            option,
            Box::new(|_| ConsumeResult::SUCCESS),
        )?;

        let (external_tx, mut external_rx) = mpsc::channel(16);
        push_consumer.start_with_telemetry(external_tx).await?;

        let command_tx = command_tx_slot
            .lock()
            .take()
            .expect("client must be started with a command channel");
        let settings = pb::Settings {
            pub_sub: Some(pb::settings::PubSub::Subscription(pb::Subscription {
                lite_subscription_quota: Some(2000),
                max_lite_topic_size: Some(64),
                ..pb::Subscription::default()
            })),
            ..pb::Settings::default()
        };
        command_tx
            .send(pb::telemetry_command::Command::Settings(settings))
            .await
            .unwrap();
        command_tx
            .send(
                pb::telemetry_command::Command::NotifyUnsubscribeLiteCommand(
                    pb::NotifyUnsubscribeLiteCommand {
                        lite_topic: "lite_a".to_string(),
                    },
                ),
            )
            .await
            .unwrap();

        let first = tokio::time::timeout(Duration::from_secs(1), external_rx.recv())
            .await
            .expect("Settings must be forwarded to the external channel")
            .unwrap();
        match first.command {
            Some(pb::telemetry_command::Command::Settings(s)) => match s.pub_sub {
                Some(pb::settings::PubSub::Subscription(sub)) => {
                    assert_eq!(Some(2000), sub.lite_subscription_quota);
                    assert_eq!(Some(64), sub.max_lite_topic_size);
                }
                other => panic!("unexpected pub_sub: {:?}", other),
            },
            other => panic!("unexpected command: {:?}", other),
        }
        let second = tokio::time::timeout(Duration::from_secs(1), external_rx.recv())
            .await
            .expect("NotifyUnsubscribeLiteCommand must be forwarded to the external channel")
            .unwrap();
        match second.command {
            Some(pb::telemetry_command::Command::NotifyUnsubscribeLiteCommand(c)) => {
                assert_eq!("lite_a", c.lite_topic);
            }
            other => panic!("unexpected command: {:?}", other),
        }

        push_consumer.shutdown_ref().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_process_assignments_invalid_assignment() -> Result<(), ClientError> {
        let rpc_client = Session::default();
        let option = &PushConsumerOption::default();
        let message_listener: Arc<MessageListener> = Arc::new(Box::new(|_| ConsumeResult::SUCCESS));
        let mut actor_table: HashMap<MessageQueue, MessageQueueActor> = HashMap::new();
        let retry_policy =
            BackOffRetryPolicy::Exponential(ExponentialBackOffRetryPolicy::default());
        let assignments = vec![Assignment {
            message_queue: Some(pb::MessageQueue {
                topic: None,
                id: 0,
                permission: 0,
                broker: None,
                accept_message_types: vec![],
            }),
        }];
        let context = MockMessageQueueActor::new_context();
        context.expect().returning(|_, _, _, _, _, _| {
            let mut actor = MockMessageQueueActor::default();
            actor.expect_start().returning(|| Ok(()));
            return actor;
        });
        PushConsumer::process_assignments(
            &rpc_client,
            option,
            &FilterExpression::sub_all(),
            message_listener,
            &mut actor_table,
            assignments,
            retry_policy,
        )
        .await?;
        assert!(actor_table.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_process_assignments() -> Result<(), ClientError> {
        let option = &PushConsumerOption::default();
        let message_listener: Arc<MessageListener> = Arc::new(Box::new(|_| ConsumeResult::SUCCESS));

        let mut rpc_client = Session::default();

        rpc_client.expect_shadow_session().returning(|| {
            let mut mock = Session::default();
            mock.expect_shadow_session().returning(|| {
                let mut mock = Session::default();
                mock.expect_shadow_session()
                    .returning(|| Session::default());
                mock
            });
            mock
        });
        let mut actor_table: HashMap<MessageQueue, MessageQueueActor> = HashMap::new();
        let retry_policy =
            BackOffRetryPolicy::Exponential(ExponentialBackOffRetryPolicy::default());
        let assignments = vec![Assignment {
            message_queue: Some(pb::MessageQueue {
                topic: Some(Resource {
                    name: "test_topic".to_string(),
                    resource_namespace: "".to_string(),
                }),
                id: 0,
                permission: 0,
                broker: Some(Broker {
                    name: "".to_string(),
                    id: 0,
                    endpoints: Some(pb::Endpoints {
                        scheme: 0,
                        addresses: vec![Address {
                            host: "localhost".to_string(),
                            port: 8081,
                        }],
                    }),
                }),
                accept_message_types: vec![],
            }),
        }];
        PushConsumer::process_assignments(
            &rpc_client,
            option,
            &FilterExpression::sub_all(),
            message_listener,
            &mut actor_table,
            assignments,
            retry_policy,
        )
        .await?;
        assert_eq!(1, actor_table.len());
        Ok(())
    }

    #[tokio::test]
    async fn test_process_assignments_two_queues() -> Result<(), ClientError> {
        let option = &PushConsumerOption::default();
        let message_listener: Arc<MessageListener> = Arc::new(Box::new(|_| ConsumeResult::SUCCESS));
        let mut actor_table: HashMap<MessageQueue, MessageQueueActor> = HashMap::new();
        let retry_policy =
            BackOffRetryPolicy::Exponential(ExponentialBackOffRetryPolicy::default());
        let assignments = vec![
            Assignment {
                message_queue: Some(pb::MessageQueue {
                    topic: Some(Resource {
                        name: "test_topic".to_string(),
                        resource_namespace: "".to_string(),
                    }),
                    id: 0,
                    permission: 0,
                    broker: Some(Broker {
                        name: "".to_string(),
                        id: 0,
                        endpoints: Some(pb::Endpoints {
                            scheme: 0,
                            addresses: vec![Address {
                                host: "localhost".to_string(),
                                port: 8081,
                            }],
                        }),
                    }),
                    accept_message_types: vec![],
                }),
            },
            Assignment {
                message_queue: Some(pb::MessageQueue {
                    topic: Some(Resource {
                        name: "test_topic".to_string(),
                        resource_namespace: "".to_string(),
                    }),
                    id: 1,
                    permission: 0,
                    broker: Some(Broker {
                        name: "".to_string(),
                        id: 0,
                        endpoints: Some(pb::Endpoints {
                            scheme: 0,
                            addresses: vec![Address {
                                host: "localhost".to_string(),
                                port: 8081,
                            }],
                        }),
                    }),
                    accept_message_types: vec![],
                }),
            },
        ];
        let mut session = Session::default();
        session.expect_shadow_session().returning(|| {
            let mut mock = Session::default();
            mock.expect_shadow_session().returning(|| {
                let mut mock = Session::default();
                mock.expect_shadow_session()
                    .returning(|| Session::default());
                mock
            });
            mock
        });
        PushConsumer::process_assignments(
            &session,
            option,
            &FilterExpression::sub_all(),
            message_listener,
            &mut actor_table,
            assignments.clone(),
            retry_policy.clone(),
        )
        .await?;
        let assignments2 = vec![Assignment {
            message_queue: Some(pb::MessageQueue {
                topic: Some(Resource {
                    name: "test_topic".to_string(),
                    resource_namespace: "".to_string(),
                }),
                id: 0,
                permission: 0,
                broker: Some(Broker {
                    name: "".to_string(),
                    id: 0,
                    endpoints: Some(pb::Endpoints {
                        scheme: 0,
                        addresses: vec![Address {
                            host: "localhost".to_string(),
                            port: 8081,
                        }],
                    }),
                }),
                accept_message_types: vec![],
            }),
        }];
        let removed = PushConsumer::process_assignments(
            &session,
            option,
            &FilterExpression::sub_all(),
            Arc::new(Box::new(|_| ConsumeResult::SUCCESS)),
            &mut actor_table,
            assignments2,
            retry_policy,
        )
        .await?;
        // The actor of queue 1 is handed back to be shut down by the caller.
        assert_eq!(1, removed.len());
        assert_eq!(1, removed[0].message_queue.id);
        PushConsumer::shutdown_actors(removed).await;
        assert_eq!(1, actor_table.len());
        assert!(actor_table.contains_key(&MessageQueue {
            id: 0,
            permission: 0,
            topic: Resource {
                name: "test_topic".to_string(),
                resource_namespace: "".to_string(),
            },
            broker: Broker {
                name: "".to_string(),
                id: 0,
                endpoints: Some(pb::Endpoints {
                    scheme: 0,
                    addresses: vec![Address {
                        host: "localhost".to_string(),
                        port: 8081,
                    }],
                }),
            },
            accept_message_types: vec![],
        }));
        Ok(())
    }

    // Reference Java: PushConsumerImpl#dropProcessQueue only marks the process queue as dropped,
    // and a new process queue fetches messages immediately; neither waits for the in-flight long
    // polling request of the dropped queue.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_process_assignments_does_not_wait_for_removed_queue_long_polling() {
        let poll = Duration::from_millis(800);
        let old_calls = Arc::new(AtomicUsize::new(0));
        let shutting_down = Arc::new(AtomicBool::new(false));
        let late_calls = Arc::new(AtomicUsize::new(0));
        let mut option = PushConsumerOption::default();
        option.subscribe("test_topic", FilterExpression::new(FilterType::Tag, "*"));
        option.set_consumer_worker_count_each_queue(1);
        let retry_policy =
            BackOffRetryPolicy::Exponential(ExponentialBackOffRetryPolicy::default());
        let message_listener: Arc<MessageListener> = Arc::new(Box::new(|_| ConsumeResult::SUCCESS));

        // Queue 0 is assigned and its worker is inside a long polling request.
        let mut actor_table: HashMap<MessageQueue, MessageQueueActor> = HashMap::new();
        let old_session = new_session_with_long_polling(
            poll,
            Arc::clone(&old_calls),
            Arc::clone(&shutting_down),
            Arc::clone(&late_calls),
        );
        let mut old_actor = new_actor_with_long_polling(0, 1, old_session);
        old_actor.start().await.unwrap();
        actor_table.insert(old_actor.message_queue.clone(), old_actor);
        wait_for_calls(&old_calls, 1).await;

        // The server now assigns queue 1 instead of queue 0.
        let new_calls = Arc::new(AtomicUsize::new(0));
        let mut rpc_client = Session::default();
        let counter = Arc::clone(&new_calls);
        rpc_client.expect_shadow_session().returning(move || {
            new_session_with_long_polling(
                Duration::from_millis(10),
                Arc::clone(&counter),
                Arc::new(AtomicBool::new(false)),
                Arc::new(AtomicUsize::new(0)),
            )
        });
        let mut new_queue = new_message_queue();
        new_queue.id = 1;
        shutting_down.store(true, Ordering::SeqCst);
        let started = std::time::Instant::now();
        let removed = PushConsumer::process_assignments(
            &rpc_client,
            &option,
            &FilterExpression::sub_all(),
            message_listener,
            &mut actor_table,
            vec![Assignment {
                message_queue: Some(new_queue.to_pb_message_queue()),
            }],
            retry_policy,
        )
        .await
        .unwrap();
        let elapsed = started.elapsed();
        let dropped_actors = TaskTracker::new();
        dropped_actors.spawn(PushConsumer::shutdown_actors(removed));

        assert!(
            elapsed < poll / 4,
            "rebalancing took {:?}, it must not wait for the {:?} long polling of the removed queue",
            elapsed,
            poll
        );
        assert_eq!(1, actor_table.len());
        assert!(actor_table.contains_key(&new_queue));
        wait_for_calls(&new_calls, 1).await;

        // The removed queue is shut down in the background, and issues no further receive.
        dropped_actors.close();
        tokio::time::timeout(poll * 3, dropped_actors.wait())
            .await
            .expect("the removed actor must finish shutting down");
        assert_eq!(0, late_calls.load(Ordering::SeqCst));
        PushConsumer::shutdown_actors(actor_table.into_values()).await;
    }

    fn new_message_queue() -> MessageQueue {
        MessageQueue {
            id: 0,
            topic: Resource {
                name: "test_topic".to_string(),
                resource_namespace: "".to_string(),
            },
            broker: Broker {
                name: "broker-0".to_string(),
                id: 0,
                endpoints: Some(pb::Endpoints {
                    scheme: 0,
                    addresses: vec![Address {
                        host: "localhost".to_string(),
                        port: 8081,
                    }],
                }),
            },
            accept_message_types: vec![],
            permission: 0,
        }
    }

    fn new_message() -> pb::Message {
        pb::Message {
            topic: Some(Resource {
                name: "test_topic".to_string(),
                resource_namespace: "".to_string(),
            }),
            user_properties: HashMap::new(),
            system_properties: Some(SystemProperties {
                receipt_handle: Some("receipt".to_string()),
                ..SystemProperties::default()
            }),
            body: vec![],
        }
    }

    #[tokio::test]
    async fn test_actor_standard_start() {
        let message_queue = new_message_queue();
        let mut option = PushConsumerOption::default();
        option.subscribe("test_topic", FilterExpression::new(FilterType::Tag, "*"));
        option.set_consumer_worker_count_each_queue(3);
        let retry_policy = BackOffRetryPolicy::Customized(CustomizedBackOffRetryPolicy::new(
            pb::CustomizedBackoff {
                next: vec![prost_types::Duration::from_str("1s").unwrap()],
            },
            1,
        ));
        let mut session = Session::default();
        session.expect_shadow_session().times(6).returning(|| {
            let mut mock = Session::default();
            mock.expect_shadow_session()
                .returning(|| Session::default());
            mock
        });
        let mut actor = MessageQueueActor::new(
            session,
            message_queue,
            FilterExpression::sub_all(),
            option,
            Arc::new(Box::new(|_| ConsumeResult::SUCCESS)),
            retry_policy,
        );
        let result = actor.start().await;
        assert!(result.is_ok());
        let result2 = actor.shutdown().await;
        assert!(result2.is_ok());
    }

    /// A session whose `receive_message` blocks for `poll` like a long polling request held by
    /// the server, then returns no message. The mocked call is synchronous, so the test runtime
    /// needs one worker thread per in-flight request. `calls` counts receive requests, and
    /// `late_calls` those issued after `shutting_down` was set.
    fn new_session_with_long_polling(
        poll: Duration,
        calls: Arc<AtomicUsize>,
        shutting_down: Arc<AtomicBool>,
        late_calls: Arc<AtomicUsize>,
    ) -> Session {
        let mut session = Session::default();
        session.expect_shadow_session().returning(move || {
            let calls = Arc::clone(&calls);
            let shutting_down = Arc::clone(&shutting_down);
            let late_calls = Arc::clone(&late_calls);
            let mut mock = Session::default();
            mock.expect_receive_message().returning(move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                if shutting_down.load(Ordering::SeqCst) {
                    late_calls.fetch_add(1, Ordering::SeqCst);
                }
                std::thread::sleep(poll);
                Ok(vec![])
            });
            mock.expect_shadow_session().returning(Session::default);
            mock
        });
        session
    }

    fn new_actor_with_long_polling(
        queue_id: i32,
        worker_count: usize,
        session: Session,
    ) -> MessageQueueActor {
        let mut option = PushConsumerOption::default();
        option.subscribe("test_topic", FilterExpression::new(FilterType::Tag, "*"));
        option.set_consumer_worker_count_each_queue(worker_count);
        let mut message_queue = new_message_queue();
        message_queue.id = queue_id;
        MessageQueueActor::new(
            session,
            message_queue,
            FilterExpression::sub_all(),
            option,
            Arc::new(Box::new(|_| ConsumeResult::SUCCESS)),
            BackOffRetryPolicy::Exponential(ExponentialBackOffRetryPolicy::default()),
        )
    }

    async fn wait_for_calls(calls: &AtomicUsize, expected: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while calls.load(Ordering::SeqCst) < expected {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("workers should be polling");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn test_actor_stops_receiving_once_shutdown_starts() {
        let calls = Arc::new(AtomicUsize::new(0));
        let shutting_down = Arc::new(AtomicBool::new(false));
        let late_calls = Arc::new(AtomicUsize::new(0));
        let session = new_session_with_long_polling(
            Duration::from_millis(100),
            Arc::clone(&calls),
            Arc::clone(&shutting_down),
            Arc::clone(&late_calls),
        );
        let mut actor = new_actor_with_long_polling(0, 8, session);
        actor.start().await.unwrap();
        wait_for_calls(&calls, 8).await;

        shutting_down.store(true, Ordering::SeqCst);
        actor.shutdown().await.unwrap();
        // Reference Java: ProcessQueueImpl#receiveMessageImmediately checks
        // consumer.isRunning() before every receive request.
        assert_eq!(
            late_calls.load(Ordering::SeqCst),
            0,
            "no receive request may be issued after shutdown started"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_shutdown_actors_waits_for_long_polling_concurrently() {
        let poll = Duration::from_millis(200);
        let calls = Arc::new(AtomicUsize::new(0));
        let shutting_down = Arc::new(AtomicBool::new(false));
        let late_calls = Arc::new(AtomicUsize::new(0));
        let mut actors = Vec::new();
        for queue_id in 0..6 {
            let session = new_session_with_long_polling(
                poll,
                Arc::clone(&calls),
                Arc::clone(&shutting_down),
                Arc::clone(&late_calls),
            );
            let mut actor = new_actor_with_long_polling(queue_id, 1, session);
            actor.start().await.unwrap();
            actors.push(actor);
        }
        wait_for_calls(&calls, 6).await;

        // Each in-flight request still has to finish, like Java's
        // PushConsumerImpl#waitingReceiveRequestFinished, but all actors wait at the same time:
        // about one poll in total instead of one poll per actor.
        let started = std::time::Instant::now();
        PushConsumer::shutdown_actors(actors).await;
        let elapsed = started.elapsed();
        assert!(
            elapsed < poll * 3,
            "shutting down 6 actors took {:?}, expected about one poll of {:?}",
            elapsed,
            poll
        );
    }

    #[tokio::test]
    async fn test_actor_fifo_start() {
        let message_queue = new_message_queue();
        let mut option = PushConsumerOption::default();
        option.set_fifo(true);
        option.subscribe("test_topic", FilterExpression::new(FilterType::Tag, "*"));
        option.set_consumer_worker_count_each_queue(3);
        let retry_policy = BackOffRetryPolicy::Customized(CustomizedBackOffRetryPolicy::new(
            pb::CustomizedBackoff {
                next: vec![prost_types::Duration::from_str("1s").unwrap()],
            },
            1,
        ));
        let mut session = Session::default();
        session.expect_shadow_session().times(2).returning(|| {
            let mut mock = Session::default();
            mock.expect_shadow_session()
                .returning(|| Session::default());
            mock
        });
        let mut actor = MessageQueueActor::new(
            session,
            message_queue,
            FilterExpression::sub_all(),
            option,
            Arc::new(Box::new(|_| ConsumeResult::SUCCESS)),
            retry_policy,
        );
        let result = actor.start().await;
        assert!(result.is_ok());
        let result2 = actor.shutdown().await;
        assert!(result2.is_ok());
    }

    #[tokio::test]
    async fn test_ack_messages_success() {
        let mut rpc_client = Session::default();
        rpc_client.expect_shadow_session().returning(|| {
            let mut mock = Session::default();
            mock.expect_ack_message().never();
            mock
        });
        rpc_client.expect_ack_message().times(1).returning(|_| {
            Ok(AckMessageResponse {
                status: Some(Status {
                    code: Code::Ok as i32,
                    message: "".to_string(),
                }),
                entries: vec![],
            })
        });
        let consumer_group = Resource {
            name: "test_group".to_string(),
            resource_namespace: "".to_string(),
        };
        let topic = Resource {
            name: "test_topic".to_string(),
            resource_namespace: "".to_string(),
        };
        let mut ack_processor = AckEntryProcessor::new(rpc_client, consumer_group, topic, false);
        let result = ack_processor.start().await;
        assert!(result.is_ok());

        ack_processor
            .ack_message(
                MessageView::from_pb_message(
                    new_message(),
                    Endpoints::from_url("http://127.0.0.1:8080").unwrap(),
                )
                .unwrap(),
            )
            .await;

        tokio::time::sleep(Duration::from_millis(500)).await;
        ack_processor.shutdown().await;
    }

    #[tokio::test]
    async fn test_ack_messages_failure_once() {
        let mut rpc_client = Session::default();
        let mut mock = Session::default();
        mock.expect_ack_message().times(1).returning(|_| {
            Ok(AckMessageResponse {
                status: Some(Status {
                    code: Code::Ok as i32,
                    message: "".to_string(),
                }),
                entries: vec![],
            })
        });
        rpc_client.expect_shadow_session().return_once(|| mock);
        rpc_client.expect_ack_message().times(1).returning(|_| {
            Ok(AckMessageResponse {
                status: Some(Status {
                    code: Code::BadRequest as i32,
                    message: "".to_string(),
                }),
                entries: vec![],
            })
        });
        let consumer_group = Resource {
            name: "test_group".to_string(),
            resource_namespace: "".to_string(),
        };
        let topic = Resource {
            name: "test_topic".to_string(),
            resource_namespace: "".to_string(),
        };
        let mut ack_processor = AckEntryProcessor::new(rpc_client, consumer_group, topic, false);
        let result = ack_processor.start().await;
        assert!(result.is_ok());

        ack_processor
            .ack_message(
                MessageView::from_pb_message(
                    new_message(),
                    Endpoints::from_url("http://127.0.0.1:8080").unwrap(),
                )
                .unwrap(),
            )
            .await;

        tokio::time::sleep(Duration::from_millis(500)).await;
        ack_processor.shutdown().await;
    }

    #[tokio::test]
    async fn test_nack_messages_success() {
        let mut rpc_client = Session::default();
        rpc_client.expect_shadow_session().returning(|| {
            let mut mock = Session::default();
            mock.expect_change_invisible_duration().never();
            mock
        });
        rpc_client
            .expect_change_invisible_duration()
            .times(1)
            .returning(|_| {
                Ok(ChangeInvisibleDurationResponse {
                    status: Some(Status {
                        code: Code::Ok as i32,
                        message: "".to_string(),
                    }),
                    receipt_handle: "".to_string(),
                })
            });
        let consumer_group = Resource {
            name: "test_group".to_string(),
            resource_namespace: "".to_string(),
        };
        let topic = Resource {
            name: "test_topic".to_string(),
            resource_namespace: "".to_string(),
        };
        let mut ack_processor = AckEntryProcessor::new(rpc_client, consumer_group, topic, false);
        let result = ack_processor.start().await;
        assert!(result.is_ok());

        ack_processor
            .change_invisible_duration(
                MessageView::from_pb_message(
                    new_message(),
                    Endpoints::from_url("http://127.0.0.1:8080").unwrap(),
                )
                .unwrap(),
                Duration::from_secs(3),
            )
            .await;

        tokio::time::sleep(Duration::from_millis(500)).await;
        ack_processor.shutdown().await;
    }

    #[tokio::test]
    async fn test_nack_messages_failure_once() {
        let mut rpc_client = Session::default();
        let mut mock = Session::default();
        mock.expect_change_invisible_duration()
            .times(1)
            .returning(|_| {
                Ok(ChangeInvisibleDurationResponse {
                    status: Some(Status {
                        code: Code::Ok as i32,
                        message: "".to_string(),
                    }),
                    receipt_handle: "".to_string(),
                })
            });
        rpc_client.expect_shadow_session().return_once(|| mock);
        rpc_client
            .expect_change_invisible_duration()
            .times(1)
            .returning(|_| {
                Ok(ChangeInvisibleDurationResponse {
                    status: Some(Status {
                        code: Code::BadRequest as i32,
                        message: "".to_string(),
                    }),
                    receipt_handle: "".to_string(),
                })
            });
        let consumer_group = Resource {
            name: "test_group".to_string(),
            resource_namespace: "".to_string(),
        };
        let topic = Resource {
            name: "test_topic".to_string(),
            resource_namespace: "".to_string(),
        };
        let mut ack_processor = AckEntryProcessor::new(rpc_client, consumer_group, topic, false);
        let result = ack_processor.start().await;
        assert!(result.is_ok());

        ack_processor
            .change_invisible_duration(
                MessageView::from_pb_message(
                    new_message(),
                    Endpoints::from_url("http://127.0.0.1:8080").unwrap(),
                )
                .unwrap(),
                Duration::from_secs(3),
            )
            .await;

        tokio::time::sleep(Duration::from_millis(500)).await;
        ack_processor.shutdown().await;
    }

    #[tokio::test]
    async fn test_send_dlq_messages_success() {
        let mut rpc_client = Session::default();
        rpc_client.expect_shadow_session().returning(|| {
            let mut mock = Session::default();
            mock.expect_forward_to_deadletter_queue().never();
            mock
        });
        rpc_client
            .expect_forward_to_deadletter_queue()
            .times(1)
            .returning(|_| {
                Ok(ForwardMessageToDeadLetterQueueResponse {
                    status: Some(Status {
                        code: Code::Ok as i32,
                        message: "".to_string(),
                    }),
                })
            });
        let consumer_group = Resource {
            name: "test_group".to_string(),
            resource_namespace: "".to_string(),
        };
        let topic = Resource {
            name: "test_topic".to_string(),
            resource_namespace: "".to_string(),
        };
        let mut ack_processor = AckEntryProcessor::new(rpc_client, consumer_group, topic, false);
        let result = ack_processor.start().await;
        assert!(result.is_ok());

        ack_processor
            .forward_to_deadletter_queue(
                MessageView::from_pb_message(
                    new_message(),
                    Endpoints::from_url("http://127.0.0.1:8080").unwrap(),
                )
                .unwrap(),
                1,
                2,
            )
            .await;

        tokio::time::sleep(Duration::from_millis(500)).await;
        ack_processor.shutdown().await;
    }

    #[tokio::test]
    async fn test_send_dlq_messages_failure_once() {
        let mut rpc_client = Session::default();
        let mut mock = Session::default();
        mock.expect_forward_to_deadletter_queue()
            .times(1)
            .returning(|_| {
                Ok(ForwardMessageToDeadLetterQueueResponse {
                    status: Some(Status {
                        code: Code::Ok as i32,
                        message: "".to_string(),
                    }),
                })
            });
        rpc_client.expect_shadow_session().return_once(|| mock);
        rpc_client
            .expect_forward_to_deadletter_queue()
            .times(1)
            .returning(|_| {
                Ok(ForwardMessageToDeadLetterQueueResponse {
                    status: Some(Status {
                        code: Code::BadRequest as i32,
                        message: "".to_string(),
                    }),
                })
            });
        let consumer_group = Resource {
            name: "test_group".to_string(),
            resource_namespace: "".to_string(),
        };
        let topic = Resource {
            name: "test_topic".to_string(),
            resource_namespace: "".to_string(),
        };
        let mut ack_processor = AckEntryProcessor::new(rpc_client, consumer_group, topic, false);
        let result = ack_processor.start().await;
        assert!(result.is_ok());

        ack_processor
            .forward_to_deadletter_queue(
                MessageView::from_pb_message(
                    new_message(),
                    Endpoints::from_url("http://127.0.0.1:8080").unwrap(),
                )
                .unwrap(),
                1,
                2,
            )
            .await;

        tokio::time::sleep(Duration::from_millis(500)).await;
        ack_processor.shutdown().await;
    }

    #[tokio::test]
    async fn test_standard_consume_messages_success() {
        let mut rpc_client = Session::default();
        rpc_client.expect_receive_message().returning(|_| {
            Ok(vec![ReceiveMessageResponse {
                content: Some(Content::Message(new_message())),
            }])
        });
        let consumer_worker =
            StandardConsumerWorker::new(rpc_client, Arc::new(Box::new(|_| ConsumeResult::SUCCESS)));
        let consumer_group = Resource {
            name: "test_group".to_string(),
            resource_namespace: "".to_string(),
        };
        let topic = Resource {
            name: "test_topic".to_string(),
            resource_namespace: "".to_string(),
        };
        let retry_policy = &BackOffRetryPolicy::Exponential(ExponentialBackOffRetryPolicy::new(
            pb::ExponentialBackoff {
                initial: Some(prost_types::Duration::from_str("1s").unwrap()),
                max: Some(prost_types::Duration::from_str("60s").unwrap()),
                multiplier: 2.0,
            },
            3,
        ));
        let mut option = PushConsumerOption::default();
        option.subscribe("test_topic", FilterExpression::new(FilterType::Tag, "*"));
        let mut session = Session::default();
        session
            .expect_shadow_session()
            .returning(|| Session::default());
        session.expect_ack_message().times(1).returning(|_| {
            Ok(AckMessageResponse {
                status: Some(Status {
                    code: Code::Ok as i32,
                    message: "".to_string(),
                }),
                entries: vec![],
            })
        });
        let mut ack_processor = AckEntryProcessor::new(session, consumer_group, topic, false);
        let is_start_ok = ack_processor.start().await;
        assert!(is_start_ok.is_ok());
        let result = ConsumerWorker::Standard(consumer_worker)
            .receive_messages(
                &new_message_queue(),
                &FilterExpression::sub_all(),
                &option,
                &mut ack_processor,
                retry_policy,
            )
            .await;
        assert!(result.is_ok(), "{:?}", result);
    }

    #[test]
    fn test_receive_failure_backoff_delay() {
        let flow_control = ClientError::new(ErrorKind::Server, "too many requests", "test")
            .with_context("code", format!("{}", Code::TooManyRequests as i32));
        assert_eq!(
            Duration::from_millis(20),
            receive_failure_backoff_delay(&flow_control)
        );

        let other_code = ClientError::new(ErrorKind::Server, "internal error", "test")
            .with_context("code", format!("{}", Code::InternalError as i32));
        assert_eq!(
            Duration::from_secs(1),
            receive_failure_backoff_delay(&other_code)
        );

        let transport = ClientError::new(ErrorKind::ClientInternal, "rpc failed", "test");
        assert_eq!(
            Duration::from_secs(1),
            receive_failure_backoff_delay(&transport)
        );
    }

    // Some servers (e.g. TDMQ) return assignment topics with an instance prefix such as
    // "rmq-xxx%test_topic", which differs from the subscription key "test_topic".
    // The filter expression must come from the subscription, not be looked up by the
    // topic name of the assigned queue.
    #[tokio::test]
    async fn test_receive_messages_with_prefixed_topic_name() {
        let mut rpc_client = Session::default();
        rpc_client
            .expect_receive_message()
            .times(1)
            .returning(|request| {
                let topic = request.message_queue.unwrap().topic.unwrap();
                assert_eq!("rmq-xxx%test_topic", topic.name);
                let filter = request.filter_expression.unwrap();
                assert_eq!("tagA", filter.expression);
                assert_eq!(FilterType::Tag as i32, filter.r#type);
                Ok(vec![ReceiveMessageResponse {
                    content: Some(Content::Message(new_message())),
                }])
            });
        let consumer_worker =
            StandardConsumerWorker::new(rpc_client, Arc::new(Box::new(|_| ConsumeResult::SUCCESS)));
        let mut message_queue = new_message_queue();
        message_queue.topic.name = "rmq-xxx%test_topic".to_string();
        let mut option = PushConsumerOption::default();
        option.subscribe("test_topic", FilterExpression::new(FilterType::Tag, "tagA"));
        let retry_policy =
            &BackOffRetryPolicy::Exponential(ExponentialBackOffRetryPolicy::default());
        let mut session = Session::default();
        session.expect_shadow_session().returning(Session::default);
        session.expect_ack_message().times(1).returning(|_| {
            Ok(AckMessageResponse {
                status: Some(Status {
                    code: Code::Ok as i32,
                    message: "".to_string(),
                }),
                entries: vec![],
            })
        });
        let mut ack_processor = AckEntryProcessor::new(
            session,
            option.get_consumer_group_resource(),
            message_queue.topic.clone(),
            false,
        );
        assert!(ack_processor.start().await.is_ok());
        let result = ConsumerWorker::Standard(consumer_worker)
            .receive_messages(
                &message_queue,
                option.get_filter_expression("test_topic").unwrap(),
                &option,
                &mut ack_processor,
                retry_policy,
            )
            .await;
        assert!(result.is_ok(), "{:?}", result);
        ack_processor.shutdown().await;
    }

    #[tokio::test]
    async fn test_standard_consume_messages_failure() {
        let mut rpc_client = Session::default();
        rpc_client.expect_receive_message().returning(|_| {
            Ok(vec![ReceiveMessageResponse {
                content: Some(Content::Message(new_message())),
            }])
        });
        let consumer_worker =
            StandardConsumerWorker::new(rpc_client, Arc::new(Box::new(|_| ConsumeResult::FAILURE)));
        let consumer_group = Resource {
            name: "test_group".to_string(),
            resource_namespace: "".to_string(),
        };
        let topic = Resource {
            name: "test_topic".to_string(),
            resource_namespace: "".to_string(),
        };
        let retry_policy = &BackOffRetryPolicy::Exponential(ExponentialBackOffRetryPolicy::new(
            pb::ExponentialBackoff {
                initial: Some(prost_types::Duration::from_str("1s").unwrap()),
                max: Some(prost_types::Duration::from_str("60s").unwrap()),
                multiplier: 2.0,
            },
            3,
        ));
        let mut option = PushConsumerOption::default();
        option.subscribe("test_topic", FilterExpression::new(FilterType::Tag, "*"));
        let mut session = Session::default();
        session
            .expect_shadow_session()
            .returning(|| Session::default());
        session.expect_ack_message().never();
        session
            .expect_change_invisible_duration()
            .times(1)
            .returning(|_| {
                Ok(ChangeInvisibleDurationResponse {
                    status: Some(Status {
                        code: Code::Ok as i32,
                        message: "".to_string(),
                    }),
                    receipt_handle: "".to_string(),
                })
            });
        let mut ack_processor = AckEntryProcessor::new(session, consumer_group, topic, false);
        let is_start_ok = ack_processor.start().await;
        assert!(is_start_ok.is_ok());
        let result = ConsumerWorker::Standard(consumer_worker)
            .receive_messages(
                &new_message_queue(),
                &FilterExpression::sub_all(),
                &option,
                &mut ack_processor,
                retry_policy,
            )
            .await;
        assert!(result.is_ok(), "{:?}", result);
    }

    #[tokio::test]
    async fn test_fifo_consume_message_success() {
        let mut rpc_client = Session::default();
        rpc_client.expect_receive_message().returning(|_| {
            Ok(vec![ReceiveMessageResponse {
                content: Some(Content::Message(new_message())),
            }])
        });
        let consumer_worker =
            FifoConsumerWorker::new(rpc_client, Arc::new(Box::new(|_| ConsumeResult::SUCCESS)));
        let consumer_group = Resource {
            name: "test_group".to_string(),
            resource_namespace: "".to_string(),
        };
        let topic = Resource {
            name: "test_topic".to_string(),
            resource_namespace: "".to_string(),
        };
        let retry_policy = &BackOffRetryPolicy::Customized(CustomizedBackOffRetryPolicy::new(
            pb::CustomizedBackoff {
                next: vec![prost_types::Duration::from_str("0.01s").unwrap()],
            },
            1,
        ));
        let mut option = PushConsumerOption::default();
        option.subscribe("test_topic", FilterExpression::new(FilterType::Tag, "*"));
        let mut session = Session::default();
        session
            .expect_shadow_session()
            .returning(|| Session::default());
        session.expect_ack_message().times(1).returning(|_| {
            Ok(AckMessageResponse {
                status: Some(Status {
                    code: Code::Ok as i32,
                    message: "".to_string(),
                }),
                entries: vec![],
            })
        });
        let mut ack_processor = AckEntryProcessor::new(session, consumer_group, topic, false);
        let result = ConsumerWorker::Fifo(consumer_worker)
            .receive_messages(
                &new_message_queue(),
                &FilterExpression::sub_all(),
                &option,
                &mut ack_processor,
                retry_policy,
            )
            .await;
        assert!(result.is_ok(), "{:?}", result);
    }

    #[tokio::test]
    async fn test_fifo_consume_message_failure() {
        let mut rpc_client = Session::default();
        rpc_client.expect_receive_message().returning(|_| {
            Ok(vec![ReceiveMessageResponse {
                content: Some(Content::Message(new_message())),
            }])
        });
        let receive_call_count = Arc::new(AtomicUsize::new(0));
        let outer_call_counter = Arc::clone(&receive_call_count);
        let consumer_worker = FifoConsumerWorker::new(
            rpc_client,
            Arc::new(Box::new(move |_| {
                receive_call_count.fetch_add(1, Ordering::Relaxed);
                ConsumeResult::FAILURE
            })),
        );
        let consumer_group = Resource {
            name: "test_group".to_string(),
            resource_namespace: "".to_string(),
        };
        let topic = Resource {
            name: "test_topic".to_string(),
            resource_namespace: "".to_string(),
        };
        let retry_policy = &BackOffRetryPolicy::Customized(CustomizedBackOffRetryPolicy::new(
            pb::CustomizedBackoff {
                next: vec![prost_types::Duration::from_str("0.01s").unwrap()],
            },
            1,
        ));
        let mut option = PushConsumerOption::default();
        option.subscribe("test_topic", FilterExpression::new(FilterType::Tag, "*"));
        let mut session = Session::default();
        session
            .expect_shadow_session()
            .returning(|| Session::default());
        session.expect_ack_message().never();
        session
            .expect_forward_to_deadletter_queue()
            .times(1)
            .returning(|_| {
                Ok(ForwardMessageToDeadLetterQueueResponse {
                    status: Some(Status {
                        code: Code::Ok as i32,
                        message: "".to_string(),
                    }),
                })
            });
        let mut ack_processor = AckEntryProcessor::new(session, consumer_group, topic, false);
        let result = ConsumerWorker::Fifo(consumer_worker)
            .receive_messages(
                &new_message_queue(),
                &FilterExpression::sub_all(),
                &option,
                &mut ack_processor,
                retry_policy,
            )
            .await;
        assert!(result.is_ok(), "{:?}", result);
        assert_eq!(2, outer_call_counter.load(Ordering::Relaxed));
    }

    // Reference Java: ConsumeTask#call catches any Throwable raised by the listener and treats
    // the consumption as FAILURE, so the message is redelivered and the worker keeps running.
    #[tokio::test]
    async fn test_standard_consume_messages_listener_panic() {
        let mut rpc_client = Session::default();
        rpc_client.expect_receive_message().returning(|_| {
            Ok(vec![ReceiveMessageResponse {
                content: Some(Content::Message(new_message())),
            }])
        });
        let consumer_worker = StandardConsumerWorker::new(
            rpc_client,
            Arc::new(Box::new(|_| panic!("listener panic in test"))),
        );
        let consumer_group = Resource {
            name: "test_group".to_string(),
            resource_namespace: "".to_string(),
        };
        let topic = Resource {
            name: "test_topic".to_string(),
            resource_namespace: "".to_string(),
        };
        let retry_policy = &BackOffRetryPolicy::Exponential(ExponentialBackOffRetryPolicy::new(
            pb::ExponentialBackoff {
                initial: Some(prost_types::Duration::from_str("1s").unwrap()),
                max: Some(prost_types::Duration::from_str("60s").unwrap()),
                multiplier: 2.0,
            },
            3,
        ));
        let mut option = PushConsumerOption::default();
        option.subscribe("test_topic", FilterExpression::new(FilterType::Tag, "*"));
        let mut session = Session::default();
        session
            .expect_shadow_session()
            .returning(|| Session::default());
        session.expect_ack_message().never();
        session
            .expect_change_invisible_duration()
            .times(1)
            .returning(|_| {
                Ok(ChangeInvisibleDurationResponse {
                    status: Some(Status {
                        code: Code::Ok as i32,
                        message: "".to_string(),
                    }),
                    receipt_handle: "".to_string(),
                })
            });
        let mut ack_processor = AckEntryProcessor::new(session, consumer_group, topic, false);
        let is_start_ok = ack_processor.start().await;
        assert!(is_start_ok.is_ok());
        let result = ConsumerWorker::Standard(consumer_worker)
            .receive_messages(
                &new_message_queue(),
                &FilterExpression::sub_all(),
                &option,
                &mut ack_processor,
                retry_policy,
            )
            .await;
        assert!(result.is_ok(), "{:?}", result);
    }

    #[tokio::test]
    async fn test_fifo_consume_message_listener_panic_then_success() {
        let mut rpc_client = Session::default();
        rpc_client.expect_receive_message().returning(|_| {
            Ok(vec![ReceiveMessageResponse {
                content: Some(Content::Message(new_message())),
            }])
        });
        let call_count = Arc::new(AtomicUsize::new(0));
        let outer_call_counter = Arc::clone(&call_count);
        let consumer_worker = FifoConsumerWorker::new(
            rpc_client,
            Arc::new(Box::new(move |_| {
                if call_count.fetch_add(1, Ordering::Relaxed) == 0 {
                    panic!("listener panic in test");
                }
                ConsumeResult::SUCCESS
            })),
        );
        let consumer_group = Resource {
            name: "test_group".to_string(),
            resource_namespace: "".to_string(),
        };
        let topic = Resource {
            name: "test_topic".to_string(),
            resource_namespace: "".to_string(),
        };
        let retry_policy = &BackOffRetryPolicy::Customized(CustomizedBackOffRetryPolicy::new(
            pb::CustomizedBackoff {
                next: vec![prost_types::Duration::from_str("0.01s").unwrap()],
            },
            2,
        ));
        let mut option = PushConsumerOption::default();
        option.subscribe("test_topic", FilterExpression::new(FilterType::Tag, "*"));
        let mut session = Session::default();
        session
            .expect_shadow_session()
            .returning(|| Session::default());
        session.expect_forward_to_deadletter_queue().never();
        session.expect_ack_message().times(1).returning(|_| {
            Ok(AckMessageResponse {
                status: Some(Status {
                    code: Code::Ok as i32,
                    message: "".to_string(),
                }),
                entries: vec![],
            })
        });
        let mut ack_processor = AckEntryProcessor::new(session, consumer_group, topic, false);
        let result = ConsumerWorker::Fifo(consumer_worker)
            .receive_messages(
                &new_message_queue(),
                &FilterExpression::sub_all(),
                &option,
                &mut ack_processor,
                retry_policy,
            )
            .await;
        assert!(result.is_ok(), "{:?}", result);
        // The panic counts as one failed attempt; the retry succeeds and is acked.
        assert_eq!(2, outer_call_counter.load(Ordering::Relaxed));
    }

    fn new_message_with_delivery_attempt(delivery_attempt: i32) -> pb::Message {
        let mut message = new_message();
        message.system_properties.as_mut().unwrap().delivery_attempt = Some(delivery_attempt);
        message
    }

    fn new_fifo_worker_recording_attempts(
        rpc_client: Session,
        failures: usize,
    ) -> (FifoConsumerWorker, Arc<Mutex<Vec<i32>>>) {
        let attempts = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&attempts);
        let worker = FifoConsumerWorker::new(
            rpc_client,
            Arc::new(Box::new(move |message: &MessageView| {
                let mut recorded = recorded.lock();
                recorded.push(message.delivery_attempt());
                if recorded.len() <= failures {
                    ConsumeResult::FAILURE
                } else {
                    ConsumeResult::SUCCESS
                }
            })),
        );
        (worker, attempts)
    }

    fn new_test_resources() -> (Resource, Resource) {
        (
            Resource {
                name: "test_group".to_string(),
                resource_namespace: "".to_string(),
            },
            Resource {
                name: "test_topic".to_string(),
                resource_namespace: "".to_string(),
            },
        )
    }

    // Reference Java: ProcessQueueImpl#eraseFifoMessage. While attempt < maxAttempts, a failed
    // FIFO message is consumed again locally after messageView.incrementAndGetDeliveryAttempt(),
    // so the listener sees 1, 2, ..., maxAttempts. Once attempt reaches maxAttempts the message
    // is forwarded to the DLQ with the current delivery attempt.
    #[tokio::test]
    async fn test_fifo_local_retry_increments_delivery_attempt_until_dlq() {
        let mut rpc_client = Session::default();
        rpc_client.expect_receive_message().returning(|_| {
            Ok(vec![ReceiveMessageResponse {
                content: Some(Content::Message(new_message_with_delivery_attempt(1))),
            }])
        });
        let (consumer_worker, attempts) =
            new_fifo_worker_recording_attempts(rpc_client, usize::MAX);
        let (consumer_group, topic) = new_test_resources();
        let retry_policy = &BackOffRetryPolicy::Customized(CustomizedBackOffRetryPolicy::new(
            pb::CustomizedBackoff {
                next: vec![prost_types::Duration::from_str("0.01s").unwrap()],
            },
            4,
        ));
        let mut option = PushConsumerOption::default();
        option.subscribe("test_topic", FilterExpression::new(FilterType::Tag, "*"));
        let mut session = Session::default();
        session
            .expect_shadow_session()
            .returning(|| Session::default());
        session.expect_ack_message().never();
        session
            .expect_forward_to_deadletter_queue()
            .withf(|request| request.delivery_attempt == 4 && request.max_delivery_attempts == 4)
            .times(1)
            .returning(|_| {
                Ok(ForwardMessageToDeadLetterQueueResponse {
                    status: Some(Status {
                        code: Code::Ok as i32,
                        message: "".to_string(),
                    }),
                })
            });
        let mut ack_processor = AckEntryProcessor::new(session, consumer_group, topic, false);
        let result = ConsumerWorker::Fifo(consumer_worker)
            .receive_messages(
                &new_message_queue(),
                &FilterExpression::sub_all(),
                &option,
                &mut ack_processor,
                retry_policy,
            )
            .await;
        assert!(result.is_ok(), "{:?}", result);
        assert_eq!(vec![1, 2, 3, 4], *attempts.lock());
    }

    #[tokio::test]
    async fn test_fifo_local_retry_succeeds_with_incremented_delivery_attempt() {
        let mut rpc_client = Session::default();
        rpc_client.expect_receive_message().returning(|_| {
            Ok(vec![ReceiveMessageResponse {
                content: Some(Content::Message(new_message_with_delivery_attempt(1))),
            }])
        });
        let (consumer_worker, attempts) = new_fifo_worker_recording_attempts(rpc_client, 2);
        let (consumer_group, topic) = new_test_resources();
        // The first retry must wait next[0] (the delay for attempt 1), as in Java
        // (retryPolicy.getNextAttemptDelay(attempt) is called before the increment).
        let retry_policy = &BackOffRetryPolicy::Customized(CustomizedBackOffRetryPolicy::new(
            pb::CustomizedBackoff {
                next: vec![
                    prost_types::Duration::from_str("0.01s").unwrap(),
                    prost_types::Duration::from_str("0.01s").unwrap(),
                    prost_types::Duration::from_str("5s").unwrap(),
                ],
            },
            17,
        ));
        let mut option = PushConsumerOption::default();
        option.subscribe("test_topic", FilterExpression::new(FilterType::Tag, "*"));
        let mut session = Session::default();
        session
            .expect_shadow_session()
            .returning(|| Session::default());
        session.expect_forward_to_deadletter_queue().never();
        session.expect_ack_message().times(1).returning(|_| {
            Ok(AckMessageResponse {
                status: Some(Status {
                    code: Code::Ok as i32,
                    message: "".to_string(),
                }),
                entries: vec![],
            })
        });
        let mut ack_processor = AckEntryProcessor::new(session, consumer_group, topic, false);
        let started = std::time::Instant::now();
        let result = ConsumerWorker::Fifo(consumer_worker)
            .receive_messages(
                &new_message_queue(),
                &FilterExpression::sub_all(),
                &option,
                &mut ack_processor,
                retry_policy,
            )
            .await;
        assert!(result.is_ok(), "{:?}", result);
        assert_eq!(vec![1, 2, 3], *attempts.lock());
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "retries must use the delays of attempts 1 and 2, took {:?}",
            started.elapsed()
        );
    }

    // The standard (non-FIFO) path redelivers through the server and must keep reporting the
    // server-provided delivery attempt, with the delay for that attempt.
    #[tokio::test]
    async fn test_standard_failure_keeps_server_delivery_attempt() {
        let mut rpc_client = Session::default();
        rpc_client.expect_receive_message().returning(|_| {
            Ok(vec![ReceiveMessageResponse {
                content: Some(Content::Message(new_message_with_delivery_attempt(2))),
            }])
        });
        let attempts = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&attempts);
        let consumer_worker = StandardConsumerWorker::new(
            rpc_client,
            Arc::new(Box::new(move |message: &MessageView| {
                recorded.lock().push(message.delivery_attempt());
                ConsumeResult::FAILURE
            })),
        );
        let (consumer_group, topic) = new_test_resources();
        let retry_policy = &BackOffRetryPolicy::Customized(CustomizedBackOffRetryPolicy::new(
            pb::CustomizedBackoff {
                next: vec![
                    prost_types::Duration::from_str("1s").unwrap(),
                    prost_types::Duration::from_str("7s").unwrap(),
                ],
            },
            17,
        ));
        let mut option = PushConsumerOption::default();
        option.subscribe("test_topic", FilterExpression::new(FilterType::Tag, "*"));
        let mut session = Session::default();
        session
            .expect_shadow_session()
            .returning(|| Session::default());
        session.expect_ack_message().never();
        session.expect_forward_to_deadletter_queue().never();
        session
            .expect_change_invisible_duration()
            .withf(|request| {
                request.invisible_duration
                    == Some(prost_types::Duration {
                        seconds: 7,
                        nanos: 0,
                    })
            })
            .times(1)
            .returning(|_| {
                Ok(ChangeInvisibleDurationResponse {
                    status: Some(Status {
                        code: Code::Ok as i32,
                        message: "".to_string(),
                    }),
                    receipt_handle: "".to_string(),
                })
            });
        let mut ack_processor = AckEntryProcessor::new(session, consumer_group, topic, false);
        let is_start_ok = ack_processor.start().await;
        assert!(is_start_ok.is_ok());
        let result = ConsumerWorker::Standard(consumer_worker)
            .receive_messages(
                &new_message_queue(),
                &FilterExpression::sub_all(),
                &option,
                &mut ack_processor,
                retry_policy,
            )
            .await;
        assert!(result.is_ok(), "{:?}", result);
        assert_eq!(vec![2], *attempts.lock());
    }

    /// A message whose GZIP body can not be decompressed.
    fn new_corrupted_message() -> pb::Message {
        let mut message = new_message();
        let system_properties = message.system_properties.as_mut().unwrap();
        system_properties.body_encoding = pb::Encoding::Gzip as i32;
        message.body = b"not compressed".to_vec();
        message
    }

    fn new_listener_counter() -> (Arc<AtomicUsize>, Arc<MessageListener>) {
        let count = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&count);
        let listener: Arc<MessageListener> = Arc::new(Box::new(move |_| {
            counter.fetch_add(1, Ordering::Relaxed);
            ConsumeResult::SUCCESS
        }));
        (count, listener)
    }

    #[tokio::test]
    async fn test_standard_consume_discards_corrupted_message() {
        let mut rpc_client = Session::default();
        rpc_client.expect_receive_message().returning(|_| {
            Ok(vec![ReceiveMessageResponse {
                content: Some(Content::Message(new_corrupted_message())),
            }])
        });
        let (listener_calls, listener) = new_listener_counter();
        let consumer_worker = StandardConsumerWorker::new(rpc_client, listener);
        let retry_policy = &BackOffRetryPolicy::Exponential(ExponentialBackOffRetryPolicy::new(
            pb::ExponentialBackoff {
                initial: Some(prost_types::Duration::from_str("1s").unwrap()),
                max: Some(prost_types::Duration::from_str("60s").unwrap()),
                multiplier: 2.0,
            },
            3,
        ));
        let mut option = PushConsumerOption::default();
        option.subscribe("test_topic", FilterExpression::new(FilterType::Tag, "*"));
        let mut session = Session::default();
        session.expect_shadow_session().returning(Session::default);
        session.expect_ack_message().never();
        session
            .expect_change_invisible_duration()
            .times(1)
            .returning(|_| {
                Ok(ChangeInvisibleDurationResponse {
                    status: Some(Status {
                        code: Code::Ok as i32,
                        message: "".to_string(),
                    }),
                    receipt_handle: "".to_string(),
                })
            });
        let mut ack_processor = AckEntryProcessor::new(
            session,
            Resource {
                name: "test_group".to_string(),
                resource_namespace: "".to_string(),
            },
            Resource {
                name: "test_topic".to_string(),
                resource_namespace: "".to_string(),
            },
            false,
        );
        let result = ConsumerWorker::Standard(consumer_worker)
            .receive_messages(
                &new_message_queue(),
                &FilterExpression::sub_all(),
                &option,
                &mut ack_processor,
                retry_policy,
            )
            .await;
        assert!(result.is_ok(), "{:?}", result);
        assert_eq!(0, listener_calls.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn test_fifo_consume_forwards_corrupted_message_to_dlq() {
        let mut rpc_client = Session::default();
        rpc_client.expect_receive_message().returning(|_| {
            Ok(vec![ReceiveMessageResponse {
                content: Some(Content::Message(new_corrupted_message())),
            }])
        });
        let (listener_calls, listener) = new_listener_counter();
        let consumer_worker = FifoConsumerWorker::new(rpc_client, listener);
        let retry_policy = &BackOffRetryPolicy::Customized(CustomizedBackOffRetryPolicy::new(
            pb::CustomizedBackoff {
                next: vec![prost_types::Duration::from_str("0.01s").unwrap()],
            },
            1,
        ));
        let mut option = PushConsumerOption::default();
        option.subscribe("test_topic", FilterExpression::new(FilterType::Tag, "*"));
        let mut session = Session::default();
        session.expect_shadow_session().returning(Session::default);
        session.expect_ack_message().never();
        session
            .expect_forward_to_deadletter_queue()
            .times(1)
            .returning(|_| {
                Ok(ForwardMessageToDeadLetterQueueResponse {
                    status: Some(Status {
                        code: Code::Ok as i32,
                        message: "".to_string(),
                    }),
                })
            });
        let mut ack_processor = AckEntryProcessor::new(
            session,
            Resource {
                name: "test_group".to_string(),
                resource_namespace: "".to_string(),
            },
            Resource {
                name: "test_topic".to_string(),
                resource_namespace: "".to_string(),
            },
            false,
        );
        let result = ConsumerWorker::Fifo(consumer_worker)
            .receive_messages(
                &new_message_queue(),
                &FilterExpression::sub_all(),
                &option,
                &mut ack_processor,
                retry_policy,
            )
            .await;
        assert!(result.is_ok(), "{:?}", result);
        assert_eq!(0, listener_calls.load(Ordering::Relaxed));
    }
}
