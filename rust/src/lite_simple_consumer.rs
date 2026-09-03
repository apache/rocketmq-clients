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

//! LiteSimpleConsumer - A specialized simple consumer for lite topics with reduced metadata and storage overhead.
//!
//! Reference Java: LiteSimpleConsumerImpl extends SimpleConsumerImpl
//!
//! LiteSimpleConsumer is a pull-style consumer variant for lite topics. It shares the same
//! consumption infrastructure as SimpleConsumer (receive/ack/change invisible duration)
//! but adds lite topic subscription management via LiteSubscriptionManager.
//!
//! Key design points (aligned with Java):
//! 1. LiteSimpleConsumer wraps a SimpleConsumer for message consumption logic
//! 2. LiteSubscriptionManager handles lite topic lifecycle (subscribe/unsubscribe/sync)
//! 3. The lite_client is a lightweight clone of the main client for LiteSubscriptionManager RPC calls
//! 4. Telemetry commands related to lite subscriptions (NotifyUnsubscribeLiteCommand, Settings)
//!    are forwarded to LiteSubscriptionManager for processing
//! 5. receive() fetches messages from the bind topic (aggregation topic); each returned
//!    message carries a `__LITE_TOPIC` property identifying its actual lite topic

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info};

use crate::client::Client;
use crate::conf::{ClientOption, SimpleConsumerOption};
use crate::error::{ClientError, ErrorKind};
use crate::lite_subscription_manager::LiteSubscriptionManager;
use crate::model::common::{ClientType, FilterExpression};
use crate::model::message::{AckMessageEntry, MessageView};
use crate::model::offset_option::OffsetOption;
use crate::pb;
use crate::simple_consumer::SimpleConsumer;
use crate::util::build_simple_consumer_settings;

const OPERATION_NEW_LITE_SIMPLE_CONSUMER: &str = "lite_simple_consumer.new";

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
}

/// LiteSimpleConsumer implementation.
///
/// Reference Java: LiteSimpleConsumerImpl extends SimpleConsumerImpl
///
/// Implementation notes:
/// - Java's LiteSimpleConsumerImpl extends SimpleConsumerImpl, which extends ConsumerImpl extends ClientImpl.
///   All classes share the same underlying client infrastructure.
/// - In Rust, since Client doesn't implement Clone for shared ownership, we use clone_for_lite_simple_consumer()
///   to create a lightweight clone for LiteSubscriptionManager. This ensures both the inner SimpleConsumer
///   and LiteSubscriptionManager have their own Client instances sharing the same SessionManager.
/// - receive() delegates to the inner SimpleConsumer, which selects a readable master queue
///   (select_first_readable_queue) instead of round-robin when the client is a lite consumer.
pub struct LiteSimpleConsumer {
    inner: SimpleConsumer,
    bind_topic: String,
    lite_client: Arc<Client>,
    lite_subscription_manager: Arc<LiteSubscriptionManager>,
    shutdown_token: Option<CancellationToken>,
    task_tracker: Option<TaskTracker>,
}

impl LiteSimpleConsumer {
    /// Create a new LiteSimpleConsumer
    ///
    /// Reference Java:
    /// 1. LiteSimpleConsumerBuilderImpl sets {bindTopic: SUB_ALL} as the single subscription topic
    /// 2. LiteSimpleConsumerImpl constructor calls super(...) which creates SimpleConsumerImpl
    /// 3. LiteSubscriptionManager is created with (thisConsumerImpl, new Resource(bindTopic), groupResource)
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

        // Reference Java: LiteSimpleConsumerBuilderImpl sets the bind topic as the only
        // topic so that the inner SimpleConsumer prefetches its route on startup.
        let mut option_with_bind_topic = option.clone();
        option_with_bind_topic.set_topics(vec![bind_topic.clone()]);

        // Reference Java: SubscriptionSettings.toProtobuf() with clientType = LiteSimpleConsumer
        let mut settings = build_simple_consumer_settings(&option_with_bind_topic);
        if let Some(pb::telemetry_command::Command::Settings(ref mut s)) = settings.command {
            s.client_type = Some(pb::ClientType::LiteSimpleConsumer as i32);
        }

        let namespace = option.namespace().to_string();
        let consumer_group = option.consumer_group().to_string();

        // Reference Java: ConsumerImpl constructor
        // Create the main client for consumption (inner SimpleConsumer)
        let client_option = ClientOption {
            client_type: ClientType::LiteSimpleConsumer,
            group: Some(consumer_group.clone()),
            namespace: namespace.clone(),
            ..client_option
        };
        let client = Client::new(client_option, settings)?;

        // Reference Java: LiteSubscriptionManager(consumerImpl, new Resource(bindTopic), groupResource)
        // clone_for_lite_simple_consumer creates a lightweight Client clone sharing the same SessionManager.
        let lite_client = Arc::new(client.clone_for_lite_simple_consumer());
        let lite_subscription_manager = Arc::new(LiteSubscriptionManager::new(
            Arc::clone(&lite_client),
            bind_topic.clone(),
            namespace,
            consumer_group,
        ));

        // Create inner SimpleConsumer with option_with_bind_topic
        let inner = SimpleConsumer::new_with_client(client, option_with_bind_topic)?;

        Ok(Self {
            inner,
            bind_topic,
            lite_client,
            lite_subscription_manager,
            shutdown_token: None,
            task_tracker: None,
        })
    }

    /// Start the LiteSimpleConsumer
    ///
    /// Reference Java: LiteSimpleConsumerImpl.startUp()
    ///
    /// Java flow:
    /// 1. super.startUp() -> SimpleConsumerImpl.startUp() -> ClientImpl.startUp()
    ///    Starts the client, establishes telemetry, fetches topic routes.
    /// 2. liteSubscriptionManager.startUp() -> syncAllLiteSubscription() + schedule periodic sync
    pub async fn start(&mut self) -> Result<(), ClientError> {
        let bind_topic = self
            .lite_subscription_manager
            .get_bind_topic_name()
            .to_string();
        let consumer_group = self
            .lite_subscription_manager
            .get_consumer_group_name()
            .to_string();
        let client_id = self.lite_client.client_id().to_string();

        info!(
            "Begin to start the LiteSimpleConsumer, bindTopic={}, consumerGroup={}, clientId={}",
            bind_topic, consumer_group, client_id
        );

        // Step 1: Start inner SimpleConsumer (reference Java: super.startUp())
        // This starts the client, establishes telemetry, and prefetches the bind topic route.
        let (telemetry_command_tx, mut telemetry_command_rx) = mpsc::channel(16);
        if let Err(e) = self.inner.start_with_telemetry(telemetry_command_tx).await {
            error!(
                "Failed to start LiteSimpleConsumer inner, bindTopic={}, clientId={}, error={:?}",
                bind_topic, client_id, e
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

        // Step 2: Start lite subscription manager (reference Java: liteSubscriptionManager.startUp())
        if let Err(e) = self.lite_subscription_manager.start().await {
            error!(
                "Failed to start LiteSubscriptionManager, bindTopic={}, clientId={}, error={:?}",
                bind_topic, client_id, e
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

        // Step 3: Setup telemetry command handler for lite-specific commands
        // Reference Java: LiteSimpleConsumerImpl handles:
        //   - onSettingsCommand() -> super.onSettingsCommand() + liteSubscriptionManager.sync(settings)
        //   - onNotifyUnsubscribeLiteCommand() -> liteSubscriptionManager.onNotifyUnsubscribeLiteCommand()
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
                        if let Some(command) = command {
                            match command {
                                // Reference Java: onNotifyUnsubscribeLiteCommand()
                                pb::telemetry_command::Command::NotifyUnsubscribeLiteCommand(cmd) => {
                                    let lite_topic = cmd.lite_topic;
                                    info!(
                                        "Received unsubscribe notification for lite topic: {}, clientId={}",
                                        lite_topic, client_id_clone
                                    );
                                    manager.on_notify_unsubscribe_lite_command(lite_topic);
                                }
                                // Reference Java: onSettingsCommand() -> liteSubscriptionManager.sync(settings)
                                pb::telemetry_command::Command::Settings(settings_cmd) => {
                                    debug!(
                                        "Received settings update from server, clientId={}",
                                        client_id_clone
                                    );
                                    manager.sync_settings(&settings_cmd);
                                }
                                _ => {}
                            }
                        } else {
                            // Channel closed, exit loop
                            break;
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
            bind_topic, consumer_group, client_id
        );
        Ok(())
    }

    /// Fetch messages from the bind topic synchronously.
    ///
    /// Reference Java: LiteSimpleConsumerImpl.receive(maxMessageNum, invisibleDuration)
    /// -> SimpleConsumerImpl.receive(getTopic(), SUB_ALL, maxMessageNum, invisibleDuration)
    ///
    /// In lite mode, the bind topic is the aggregation topic; each returned message
    /// carries a `__LITE_TOPIC` property identifying its actual lite topic.
    pub async fn receive(
        &self,
        batch_size: i32,
        invisible_duration: Duration,
    ) -> Result<Vec<MessageView>, ClientError> {
        self.inner
            .receive_with(
                &self.bind_topic,
                &FilterExpression::sub_all(),
                batch_size,
                invisible_duration,
            )
            .await
    }

    /// Ack the specified message.
    pub async fn ack(
        &self,
        ack_entry: &(impl AckMessageEntry + 'static),
    ) -> Result<(), ClientError> {
        self.inner.ack(ack_entry).await
    }

    /// Change the invisible duration of a specified message.
    pub async fn change_invisible_duration(
        &self,
        ack_entry: &(impl AckMessageEntry + 'static),
        invisible_duration: Duration,
    ) -> Result<String, ClientError> {
        self.inner
            .change_invisible_duration(ack_entry, invisible_duration)
            .await
    }

    /// Shutdown the consumer.
    ///
    /// Reference Java: SimpleConsumerImpl.close() -> this.stopAsync().awaitTerminated()
    pub async fn shutdown(&mut self) -> Result<(), ClientError> {
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

#[async_trait]
impl LiteSimpleConsumerTrait for LiteSimpleConsumer {
    /// Subscribe to a lite topic
    ///
    /// Reference Java: LiteSimpleConsumerImpl.subscribeLite(String)
    async fn subscribe_lite(&self, lite_topic: String) -> Result<(), ClientError> {
        // Check if client is started (public API validation)
        self.inner
            .check_started("lite_simple_consumer.subscribe_lite")?;

        self.lite_subscription_manager
            .subscribe_lite(lite_topic, None)
            .await
    }

    /// Subscribe to a lite topic with offset option
    ///
    /// Reference Java: LiteSimpleConsumerImpl.subscribeLite(String, OffsetOption)
    async fn subscribe_lite_with_offset(
        &self,
        lite_topic: String,
        offset_option: OffsetOption,
    ) -> Result<(), ClientError> {
        // Check if client is started (public API validation)
        self.inner
            .check_started("lite_simple_consumer.subscribe_lite_with_offset")?;

        self.lite_subscription_manager
            .subscribe_lite(lite_topic, Some(offset_option))
            .await
    }

    /// Unsubscribe from a lite topic
    ///
    /// Reference Java: LiteSimpleConsumerImpl.unsubscribeLite(String)
    async fn unsubscribe_lite(&self, lite_topic: String) -> Result<(), ClientError> {
        // Check if client is started (public API validation)
        self.inner
            .check_started("lite_simple_consumer.unsubscribe_lite")?;

        self.lite_subscription_manager
            .unsubscribe_lite(lite_topic)
            .await
    }

    /// Get the set of subscribed lite topics
    ///
    /// Reference Java: LiteSimpleConsumerImpl.getLiteTopicSet()
    fn get_lite_topic_set(&self) -> HashSet<String> {
        self.lite_subscription_manager.get_lite_topic_set()
    }

    /// Get the consumer group name
    ///
    /// Reference Java: LiteSimpleConsumerImpl.getConsumerGroup()
    fn get_consumer_group(&self) -> String {
        self.lite_subscription_manager
            .get_consumer_group_name()
            .to_string()
    }
}
