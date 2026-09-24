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

//! LiteSimpleConsumer - the lite topic variant of `SimpleConsumer`.
//!
//! Reference Java: `LiteSimpleConsumerImpl extends SimpleConsumerImpl`
//!
//! A `LiteSimpleConsumer` is bound to one parent topic (the *bind topic*) and dynamically
//! (un)subscribes lite topics of that parent topic through the `SyncLiteSubscription` RPC.
//! Messages are pulled explicitly with `receive()` and committed with `ack()`, exactly like a
//! regular `SimpleConsumer`.
//!
//! Key design points (aligned with Java):
//! 1. The consumer is a thin wrapper around `SimpleConsumer` (which owns the `Client`), plus a
//!    lightweight cloned client used by `LiteSubscriptionManager` for the subscription RPCs.
//! 2. Telemetry commands related to lite subscriptions (`NotifyUnsubscribeLiteCommand`,
//!    `Settings`) are forwarded to `LiteSubscriptionManager`.
//! 3. `ack()` / `change_invisible_duration()` carry the lite topic of the message, otherwise the
//!    proxy cannot resolve the LMQ receipt handle and answers `INTERNAL_SERVER_ERROR`.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use mockall_double::double;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, warn};

#[double]
use crate::client::Client;
use crate::conf::{ClientOption, SimpleConsumerOption};
use crate::error::{ClientError, ErrorKind};
use crate::lite_subscription_manager::LiteSubscriptionManager;
use crate::model::common::{ClientType, FilterExpression};
use crate::model::message::MessageView;
use crate::model::offset_option::OffsetOption;
use crate::pb;
use crate::simple_consumer::SimpleConsumer;
use crate::util::build_lite_simple_consumer_settings;

const OPERATION_NEW_LITE_SIMPLE_CONSUMER: &str = "lite_simple_consumer.new";
const OPERATION_LITE_SIMPLE_CONSUMER_RECEIVE: &str = "lite_simple_consumer.receive";
const OPERATION_SUBSCRIBE_LITE: &str = "lite_simple_consumer.subscribe_lite";
const OPERATION_UNSUBSCRIBE_LITE: &str = "lite_simple_consumer.unsubscribe_lite";

/// LiteSimpleConsumer trait defining the interface for lite simple consumers
#[async_trait]
pub trait LiteSimpleConsumerTrait {
    /// Subscribe to a lite topic
    async fn subscribe_lite(&self, lite_topic: String) -> Result<(), ClientError>;

    /// Subscribe to a lite topic with offset option
    async fn subscribe_lite_with_offset(
        &self,
        lite_topic: String,
        offset_option: OffsetOption,
    ) -> Result<(), ClientError>;

    /// Unsubscribe from a lite topic
    async fn unsubscribe_lite(&self, lite_topic: String) -> Result<(), ClientError>;

    /// Get the set of subscribed lite topics
    fn get_lite_topic_set(&self) -> HashSet<String>;

    /// Get the consumer group name
    fn get_consumer_group(&self) -> String;

    /// Get the parent (bind) topic name
    fn get_bind_topic(&self) -> String;

    /// Shutdown the consumer
    async fn shutdown(&mut self) -> Result<(), ClientError>;
}

/// LiteSimpleConsumer implementation.
///
/// Reference Java: `LiteSimpleConsumerImpl extends SimpleConsumerImpl`
pub struct LiteSimpleConsumer {
    inner: SimpleConsumer,
    lite_client: Arc<Client>,
    lite_subscription_manager: Arc<LiteSubscriptionManager>,
    bind_topic: String,
    shutdown_token: Option<CancellationToken>,
    task_tracker: Option<TaskTracker>,
}

impl LiteSimpleConsumer {
    /// Create a new LiteSimpleConsumer
    pub fn new(
        client_option: ClientOption,
        option: SimpleConsumerOption,
        bind_topic: String,
    ) -> Result<Self, ClientError> {
        if option.consumer_group().is_empty() {
            return Err(ClientError::new(
                ErrorKind::Config,
                "consumer group is required.",
                OPERATION_NEW_LITE_SIMPLE_CONSUMER,
            ));
        }

        if bind_topic.is_empty() {
            return Err(ClientError::new(
                ErrorKind::Config,
                "bind topic is required.",
                OPERATION_NEW_LITE_SIMPLE_CONSUMER,
            ));
        }

        // Reference Java: LiteSimpleConsumerBuilderImpl sets {bindTopic: SUB_ALL} as
        // subscriptionExpressions and prefetches the route of the bind topic.
        let mut option = option;
        option.set_topics(vec![bind_topic.clone()]);

        let settings = build_lite_simple_consumer_settings(&option, &bind_topic);
        let namespace = option.namespace().to_string();
        let consumer_group = option.consumer_group().to_string();

        let client_option = ClientOption {
            client_type: ClientType::LiteSimpleConsumer,
            group: Some(consumer_group.clone()),
            namespace: namespace.clone(),
            ..client_option
        };
        let client = Client::new(client_option, settings)?;

        // Reference Java: new LiteSubscriptionManager(this, new Resource(bindTopic), groupResource)
        let lite_client = Arc::new(client.clone_for_lite_simple_consumer());
        let lite_subscription_manager = Arc::new(LiteSubscriptionManager::new(
            Arc::clone(&lite_client),
            bind_topic.clone(),
            namespace,
            consumer_group,
        ));

        let inner = SimpleConsumer::new_with_client(client, option)?;

        Ok(Self {
            inner,
            lite_client,
            lite_subscription_manager,
            bind_topic,
            shutdown_token: None,
            task_tracker: None,
        })
    }

    /// Start the LiteSimpleConsumer
    ///
    /// Reference Java: `LiteSimpleConsumerImpl#startUp()`
    pub async fn start(&mut self) -> Result<(), ClientError> {
        let client_id = self.lite_client.client_id().to_string();
        info!(
            "Begin to start the LiteSimpleConsumer, bindTopic={}, clientId={}",
            self.bind_topic, client_id
        );

        // Step 1: pre-fetch the route of the bind topic.
        match self.lite_client.topic_route(&self.bind_topic, false).await {
            Ok(route) => {
                info!(
                    "Pre-fetched route for bindTopic={}, message_queues={}",
                    self.bind_topic,
                    route.queue.len()
                );
            }
            Err(e) => {
                warn!(
                    "Failed to pre-fetch route for bindTopic={}, error={:?}. Will retry later.",
                    self.bind_topic, e
                );
            }
        }

        // Step 2: start the underlying simple consumer (reference Java: super.startUp())
        let (telemetry_command_tx, mut telemetry_command_rx) = mpsc::channel(16);
        if let Err(e) = self.inner.start_with_telemetry(telemetry_command_tx).await {
            error!(
                "Failed to start LiteSimpleConsumer inner, bindTopic={}, clientId={}, error={:?}",
                self.bind_topic, client_id, e
            );
            if let Err(shutdown_err) = self.shutdown().await {
                error!(
                    "Failed to shutdown after start failure, clientId={}, error={:?}",
                    client_id, shutdown_err
                );
            }
            return Err(ClientError::new(
                ErrorKind::ClientInternal,
                &format!("startUp err={:?}", e),
                "lite_simple_consumer.start",
            ));
        }

        // Step 3: start the lite subscription manager (reference Java: liteSubscriptionManager.startUp())
        if let Err(e) = self.lite_subscription_manager.start().await {
            error!(
                "Failed to start LiteSubscriptionManager, bindTopic={}, clientId={}, error={:?}",
                self.bind_topic, client_id, e
            );
            if let Err(shutdown_err) = self.shutdown().await {
                error!(
                    "Failed to shutdown after start failure, clientId={}, error={:?}",
                    client_id, shutdown_err
                );
            }
            return Err(ClientError::new(
                ErrorKind::ClientInternal,
                &format!("startUp err={:?}", e),
                "lite_simple_consumer.start",
            ));
        }

        // Step 4: handle lite specific telemetry commands.
        // Reference Java: onSettingsCommand() -> liteSubscriptionManager.sync(settings)
        //                 onNotifyUnsubscribeLiteCommand() -> liteSubscriptionManager.onNotifyUnsubscribeLiteCommand()
        let shutdown_token = CancellationToken::new();
        self.shutdown_token = Some(shutdown_token.clone());
        let task_tracker = TaskTracker::new();
        self.task_tracker = Some(task_tracker.clone());

        let manager = Arc::clone(&self.lite_subscription_manager);
        let client_id_clone = client_id.clone();
        task_tracker.spawn(async move {
            loop {
                tokio::select! {
                    command = telemetry_command_rx.recv() => {
                        match command {
                            Some(pb::telemetry_command::Command::NotifyUnsubscribeLiteCommand(ref cmd)) => {
                                info!(
                                    "Received unsubscribe notification for lite topic: {}, clientId={}",
                                    cmd.lite_topic, client_id_clone
                                );
                                manager.on_notify_unsubscribe_lite_command(cmd.lite_topic.clone());
                            }
                            Some(pb::telemetry_command::Command::Settings(ref settings_cmd)) => {
                                debug!(
                                    "Received settings update from server, clientId={}",
                                    client_id_clone
                                );
                                manager.sync_settings(settings_cmd);
                            }
                            other => {
                                debug!(
                                    "Command {:?} cannot be handled in lite simple consumer, clientId={}",
                                    other.is_some(),
                                    client_id_clone
                                );
                            }
                        }
                    }
                    _ = shutdown_token.cancelled() => {
                        break;
                    }
                }
            }
            info!(
                "LiteSimpleConsumer telemetry handler stopped, clientId={}",
                client_id_clone
            );
        });

        info!(
            "The LiteSimpleConsumer starts successfully, bindTopic={}, consumerGroup={}, clientId={}",
            self.bind_topic,
            self.get_consumer_group(),
            client_id
        );
        Ok(())
    }

    /// Receive messages of the subscribed lite topics
    ///
    /// # Arguments
    ///
    /// * `max_message_num` - max message num of server returned
    /// * `invisible_duration` - set the invisible duration of messages returned by the server
    pub async fn receive(
        &self,
        max_message_num: i32,
        invisible_duration: Duration,
    ) -> Result<Vec<MessageView>, ClientError> {
        self.inner
            .check_started(OPERATION_LITE_SIMPLE_CONSUMER_RECEIVE)?;
        self.inner
            .receive_lite(
                &self.bind_topic,
                &FilterExpression::sub_all(),
                max_message_num,
                invisible_duration,
            )
            .await
    }

    /// Ack the specified message
    ///
    /// It is important to acknowledge every consumed message, otherwise, they will be received
    /// again after the invisible duration.
    pub async fn ack(&self, message: &MessageView) -> Result<(), ClientError> {
        self.inner.ack(message).await
    }

    /// Change the invisible duration of the specified message
    pub async fn change_invisible_duration(
        &self,
        message: &MessageView,
        invisible_duration: Duration,
    ) -> Result<String, ClientError> {
        self.inner
            .change_invisible_duration(message, invisible_duration)
            .await
    }
}

#[async_trait]
impl LiteSimpleConsumerTrait for LiteSimpleConsumer {
    /// Subscribe to a lite topic
    ///
    /// Reference Java: `LiteSimpleConsumerImpl#subscribeLite(String)`
    async fn subscribe_lite(&self, lite_topic: String) -> Result<(), ClientError> {
        self.inner.check_started(OPERATION_SUBSCRIBE_LITE)?;
        self.lite_subscription_manager
            .subscribe_lite(lite_topic, None)
            .await
    }

    /// Subscribe to a lite topic with offset option
    ///
    /// Reference Java: `LiteSimpleConsumerImpl#subscribeLite(String, OffsetOption)`
    async fn subscribe_lite_with_offset(
        &self,
        lite_topic: String,
        offset_option: OffsetOption,
    ) -> Result<(), ClientError> {
        self.inner.check_started(OPERATION_SUBSCRIBE_LITE)?;
        self.lite_subscription_manager
            .subscribe_lite(lite_topic, Some(offset_option))
            .await
    }

    /// Unsubscribe from a lite topic
    ///
    /// Reference Java: `LiteSimpleConsumerImpl#unsubscribeLite(String)`
    async fn unsubscribe_lite(&self, lite_topic: String) -> Result<(), ClientError> {
        self.inner.check_started(OPERATION_UNSUBSCRIBE_LITE)?;
        self.lite_subscription_manager
            .unsubscribe_lite(lite_topic)
            .await
    }

    /// Get the set of subscribed lite topics
    ///
    /// Reference Java: `LiteSimpleConsumerImpl#getLiteTopicSet()`
    fn get_lite_topic_set(&self) -> HashSet<String> {
        self.lite_subscription_manager.get_lite_topic_set()
    }

    /// Get the consumer group name
    ///
    /// Reference Java: `LiteSimpleConsumerImpl#getConsumerGroup()`
    fn get_consumer_group(&self) -> String {
        self.lite_subscription_manager
            .get_consumer_group_name()
            .to_string()
    }

    /// Get the parent (bind) topic name
    fn get_bind_topic(&self) -> String {
        self.bind_topic.clone()
    }

    /// Shutdown the consumer
    async fn shutdown(&mut self) -> Result<(), ClientError> {
        info!("Shutting down LiteSimpleConsumer...");

        if let Some(token) = self.shutdown_token.take() {
            token.cancel();
        }

        if let Some(tracker) = self.task_tracker.take() {
            tracker.close();
            tracker.wait().await;
        }

        self.inner.shutdown_ref().await?;

        info!("LiteSimpleConsumer shutdown successfully");
        Ok(())
    }
}
