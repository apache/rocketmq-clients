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

//! LitePushConsumer - A specialized consumer for lite topics with reduced metadata and storage overhead.
//!
//! Reference Java: LitePushConsumerImpl extends PushConsumerImpl
//!
//! LitePushConsumer is a push consumer variant for lite topics. It shares the same
//! consumption infrastructure as PushConsumer (assignment scanning, message receiving, processing)
//! but adds lite topic subscription management via LiteSubscriptionManager.
//!
//! Key design points (aligned with Java):
//! 1. LitePushConsumer wraps a PushConsumer for message consumption logic
//! 2. LiteSubscriptionManager handles lite topic lifecycle (subscribe/unsubscribe/sync)
//! 3. The lite_client is a lightweight clone of the main client for LiteSubscriptionManager RPC calls
//! 4. Telemetry commands related to lite subscriptions (NotifyUnsubscribeLiteCommand, Settings)
//!    are forwarded to LiteSubscriptionManager for processing

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, warn};

use crate::client::Client;
use crate::conf::{ClientOption, PushConsumerOption};
use crate::error::{ClientError, ErrorKind};
use crate::lite_subscription_manager::LiteSubscriptionManager;
use crate::model::common::ClientType;
use crate::model::offset_option::OffsetOption;
use crate::pb;
use crate::push_consumer::{MessageListener, PushConsumer};
use crate::util::build_push_consumer_settings;

const OPERATION_NEW_LITE_PUSH_CONSUMER: &str = "lite_push_consumer.new";

/// LitePushConsumer trait defining the interface for lite push consumers
#[async_trait]
pub trait LitePushConsumerTrait {
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

    /// Shutdown the consumer
    async fn shutdown(&mut self) -> Result<(), ClientError>;
}

/// LitePushConsumer implementation.
///
/// Reference Java: LitePushConsumerImpl extends PushConsumerImpl
///
/// Implementation notes:
/// - Java's LitePushConsumerImpl extends PushConsumerImpl, which extends ConsumerImpl extends ClientImpl.
///   All classes share the same underlying client infrastructure.
/// - In Rust, since Client doesn't implement Clone for shared ownership, we use clone_for_lite_consumer()
///   to create a lightweight clone for LiteSubscriptionManager. This ensures both the inner PushConsumer
///   and LiteSubscriptionManager have their own Client instances sharing the same SessionManager.
/// - LitePushSubscriptionSettings is intentionally NOT included here because LiteSubscriptionManager
///   already handles settings sync (sync(), subscribeLite(), unsubscribeLite(), quota management).
///   Adding a separate settings wrapper would duplicate the logic from LiteSubscriptionManager.
pub struct LitePushConsumer {
    inner: PushConsumer,
    lite_client: Arc<Client>,
    lite_subscription_manager: Arc<LiteSubscriptionManager>,
    shutdown_token: Option<CancellationToken>,
    task_tracker: Option<TaskTracker>,
}

impl LitePushConsumer {
    /// Create a new LitePushConsumer
    ///
    /// Reference Java:
    /// 1. LitePushConsumerBuilderImpl sets {bindTopic: SUB_ALL} as subscriptionExpressions
    /// 2. LitePushConsumerImpl constructor calls super(...) which creates PushConsumerImpl
    /// 3. LiteSubscriptionManager is created with (thisConsumerImpl, new Resource(bindTopic), groupResource)
    pub fn new(
        client_option: ClientOption,
        option: PushConsumerOption,
        bind_topic: String,
        message_listener: MessageListener,
    ) -> Result<Self, ClientError> {
        if option.consumer_group().is_empty() {
            return Err(ClientError::new(
                ErrorKind::Config,
                "consumer group is required.",
                OPERATION_NEW_LITE_PUSH_CONSUMER,
            ));
        }

        if bind_topic.is_empty() {
            return Err(ClientError::new(
                ErrorKind::Config,
                "bind topic is required.",
                OPERATION_NEW_LITE_PUSH_CONSUMER,
            ));
        }

        // Reference Java: ImmutableMap.of(bindTopic, FilterExpression.SUB_ALL)
        // This ensures the subscription_table contains bind_topic so that scan_assignments
        // can query assignments and start messageQueueActor for the bind topic
        let mut option_with_default_subscription = option.clone();
        option_with_default_subscription.set_subscription_expressions(
            std::collections::HashMap::from([(bind_topic.clone(), crate::model::common::FilterExpression::sub_all())]),
        );

        // Reference Java: PushSubscriptionSettings.toProtobuf() with clientType = LitePushConsumer
        let mut settings = build_push_consumer_settings(&option_with_default_subscription);
        if let Some(pb::telemetry_command::Command::Settings(ref mut s)) = settings.command {
            s.client_type = Some(pb::ClientType::LitePushConsumer as i32);
        }

        let namespace = option.namespace().to_string();
        let consumer_group = option.consumer_group().to_string();

        // Reference Java: ConsumerImpl constructor
        // Create the main client for consumption (inner PushConsumer)
        let client_option = ClientOption {
            client_type: ClientType::LitePushConsumer,
            group: Some(option.consumer_group().to_string()),
            namespace: namespace.clone(),
            ..client_option
        };
        let client = Client::new(client_option, settings)?;

        // Reference Java: LiteSubscriptionManager(consumerImpl, new Resource(bindTopic), groupResource)
        // clone_for_lite_consumer creates a lightweight Client clone sharing the same SessionManager.
        // This simulates Java's approach where LiteSubscriptionManager uses consumerImpl directly.
        // 
        // Dual Client Architecture:
        // - inner client: Used by PushConsumer for message consumption (owns the client)
        // - lite_client: Used by LiteSubscriptionManager for subscription management (cloned, shares SessionManager)
        // Both clients share the same SessionManager via Arc::clone, enabling true Lite mode
        // where they use the same underlying telemetry session and connection.
        let lite_client = Arc::new(client.clone_for_lite_consumer());
        let lite_subscription_manager = Arc::new(LiteSubscriptionManager::new(
            Arc::clone(&lite_client),
            bind_topic.clone(),
            namespace,
            consumer_group,
        ));

        // Create inner PushConsumer with option_with_default_subscription
        // This is CRITICAL: The inner PushConsumer must use option_with_default_subscription
        // to ensure subscription_table contains bind_topic. This allows scan_assignments to
        // query assignments for the bind topic and start messageQueueActor.
        // 
        // Reference Java: LitePushConsumerImpl extends PushConsumerImpl, which uses the
        // subscriptionExpressions set in the builder (ImmutableMap.of(bindTopic, SUB_ALL))
        let inner = PushConsumer::new_with_client(client, option_with_default_subscription, message_listener)?;

        Ok(Self {
            inner,
            lite_client,
            lite_subscription_manager,
            shutdown_token: None,
            task_tracker: None,
        })
    }

    /// Start the LitePushConsumer
    ///
    /// Reference Java: LitePushConsumerImpl.startUp()
    ///
    /// Java flow:
    /// 1. super.startUp() -> PushConsumerImpl.startUp() -> ConsumerImpl.startUp() -> ClientImpl.startUp()
    ///    Starts the client, establishes telemetry, fetches topic routes, starts assignment scanning.
    /// 2. liteSubscriptionManager.startUp() -> syncAllLiteSubscription() + schedule periodic sync
    pub async fn start(&mut self) -> Result<(), ClientError> {
        let bind_topic = self.lite_subscription_manager.get_bind_topic_name().to_string();
        let consumer_group = self.lite_subscription_manager.get_consumer_group_name().to_string();
        let client_id = self.lite_client.client_id().to_string();

        info!(
            "Begin to start the LitePushConsumer, bindTopic={}, consumerGroup={}, clientId={}",
            bind_topic, consumer_group, client_id
        );

        // Step 1: Pre-fetch bindTopic route (reference Java: fetchTopicRoute during ClientImpl.startUp())
        // This is done early to validate the bind topic exists before starting consumption
        match self.lite_client.topic_route(&bind_topic, false).await {
            Ok(route) => {
                info!(
                    "Pre-fetched route for bindTopic={}, message_queues={}",
                    bind_topic,
                    route.queue.len()
                );
            }
            Err(e) => {
                warn!(
                    "Failed to pre-fetch route for bindTopic={}, error={:?}. Will retry later.",
                    bind_topic, e
                );
            }
        }

        // Step 2: Start inner PushConsumer (reference Java: super.startUp())
        // This starts the client, establishes telemetry, fetches topic routes, and sets up assignment scanning
        let (telemetry_command_tx, mut telemetry_command_rx) = mpsc::channel(16);
        if let Err(e) = self.inner.start_with_telemetry(telemetry_command_tx).await {
            error!(
                "Failed to start LitePushConsumer inner, bindTopic={}, clientId={}, error={:?}",
                bind_topic, client_id, e
            );
            // Reference Java: startUp() calls shutDown() on failure
            if let Err(shutdown_err) = self.shutdown().await {
                error!(
                    "Failed to shutdown after start failure, clientId={}, error={:?}",
                    client_id, shutdown_err
                );
            }
            return Err(ClientError::new(
                ErrorKind::ClientInternal,
                &format!("startUp err={:?}", e),
                "lite_push_consumer.start",
            ));
        }

        // Step 3: Start lite subscription manager (reference Java: liteSubscriptionManager.startUp())
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
                "lite_push_consumer.start",
            ));
        }

        // Step 4: Setup telemetry command handler for lite-specific commands
        // Reference Java: LitePushConsumerImpl handles:
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
                            // Reference Java: onNotifyUnsubscribeLiteCommand()
                            if let Some(pb::telemetry_command::Command::NotifyUnsubscribeLiteCommand(ref cmd)) =
                                command.command
                            {
                                info!(
                                    "Received unsubscribe notification for lite topic: {}, clientId={}",
                                    cmd.lite_topic, client_id_clone
                                );
                                manager.on_notify_unsubscribe_lite_command(cmd.lite_topic.clone());
                            }

                            // Reference Java: onSettingsCommand() -> liteSubscriptionManager.sync(settings)
                            // super.onSettingsCommand() is handled by inner PushConsumer's telemetry loop
                            if let Some(pb::telemetry_command::Command::Settings(ref settings_cmd)) =
                                command.command
                            {
                                debug!(
                                    "Received settings update from server, clientId={}",
                                    client_id_clone
                                );
                                // LiteSubscriptionManager.sync_settings() handles quota and max_topic_size
                                // Reference Java: liteSubscriptionManager.sync(settings)
                                manager.sync_settings(settings_cmd);
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
                "LitePushConsumer telemetry handler stopped, clientId={}",
                client_id_clone
            );
        });

        info!(
            "The LitePushConsumer starts successfully, bindTopic={}, consumerGroup={}, clientId={}",
            bind_topic, consumer_group, client_id
        );
        Ok(())
    }
}

#[async_trait]
impl LitePushConsumerTrait for LitePushConsumer {
    /// Subscribe to a lite topic
    ///
    /// Reference Java: LitePushConsumerImpl.subscribeLite(String)
    async fn subscribe_lite(&self, lite_topic: String) -> Result<(), ClientError> {
        // Check if client is started (public API validation)
        // Use inner's client because lite_client is a clone without shutdown_tx
        self.inner.check_started("lite_push_consumer.subscribe_lite")?;
        
        self.lite_subscription_manager
            .subscribe_lite(lite_topic, None)
            .await
    }

    /// Subscribe to a lite topic with offset option
    ///
    /// Reference Java: LitePushConsumerImpl.subscribeLite(String, OffsetOption)
    async fn subscribe_lite_with_offset(
        &self,
        lite_topic: String,
        offset_option: OffsetOption,
    ) -> Result<(), ClientError> {
        // Check if client is started (public API validation)
        // Use inner's client because lite_client is a clone without shutdown_tx
        self.inner.check_started("lite_push_consumer.subscribe_lite_with_offset")?;
        
        self.lite_subscription_manager
            .subscribe_lite(lite_topic, Some(offset_option))
            .await
    }

    /// Unsubscribe from a lite topic
    ///
    /// Reference Java: LitePushConsumerImpl.unsubscribeLite(String)
    async fn unsubscribe_lite(&self, lite_topic: String) -> Result<(), ClientError> {
        // Check if client is started (public API validation)
        // Use inner's client because lite_client is a clone without shutdown_tx
        self.inner.check_started("lite_push_consumer.unsubscribe_lite")?;
        
        self.lite_subscription_manager
            .unsubscribe_lite(lite_topic)
            .await
    }

    /// Get the set of subscribed lite topics
    ///
    /// Reference Java: LitePushConsumerImpl.getLiteTopicSet()
    fn get_lite_topic_set(&self) -> HashSet<String> {
        self.lite_subscription_manager.get_lite_topic_set()
    }

    /// Get the consumer group name
    ///
    /// Reference Java: LitePushConsumerImpl.getConsumerGroup()
    fn get_consumer_group(&self) -> String {
        self.lite_subscription_manager
            .get_consumer_group_name()
            .to_string()
    }

    /// Shutdown the consumer
    ///
    /// Reference Java: PushConsumerImpl.close() -> this.stopAsync().awaitTerminated()
    async fn shutdown(&mut self) -> Result<(), ClientError> {
        info!("Shutting down LitePushConsumer...");

        if let Some(token) = self.shutdown_token.take() {
            token.cancel();
        }

        if let Some(tracker) = self.task_tracker.take() {
            tracker.close();
            tracker.wait().await;
        }

        self.inner.shutdown_ref().await?;

        info!("LitePushConsumer shutdown successfully");
        Ok(())
    }
}
