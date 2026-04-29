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

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::info;

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

/// LitePushConsumer implementation
pub struct LitePushConsumer {
    inner: PushConsumer,
    lite_subscription_manager: Arc<LiteSubscriptionManager>,
    shutdown_token: Option<CancellationToken>,
    task_tracker: Option<TaskTracker>,
}

impl LitePushConsumer {
    /// Create a new LitePushConsumer
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

        // Create base client with LitePushConsumer type
        let client_option = ClientOption {
            client_type: ClientType::LitePushConsumer,
            group: Some(option.consumer_group().to_string()),
            ..client_option
        };

        // Build settings with lite subscription configuration
        let mut settings = build_push_consumer_settings(&option);
        if let Some(pb::telemetry_command::Command::Settings(ref mut s)) = settings.command {
            s.client_type = Some(pb::ClientType::LitePushConsumer as i32);
        }
        // Create the main client for PushConsumer
        let client = Client::new(client_option, settings)?;

        // Clone client for LiteSubscriptionManager using clone_for_lite_consumer
        // This ensures both clients have the correct LitePushConsumer type
        let namespace = option.namespace().to_string();
        let consumer_group = option.consumer_group().to_string();
        let lite_subscription_manager = Arc::new(LiteSubscriptionManager::new(
            Arc::new(client.clone_for_lite_consumer()),
            bind_topic.clone(),
            namespace,
            consumer_group,
        ));

        // Create inner PushConsumer with the main client
        let inner = PushConsumer::new_with_client(client, option, message_listener)?;

        Ok(Self {
            inner,
            lite_subscription_manager,
            shutdown_token: None,
            task_tracker: None,
        })
    }

    /// Start the LitePushConsumer
    pub async fn start(&mut self) -> Result<(), ClientError> {
        info!("Starting LitePushConsumer...");

        // Start the inner push consumer
        let (telemetry_command_tx, mut telemetry_command_rx) = mpsc::channel(16);
        self.inner
            .start_with_telemetry(telemetry_command_tx)
            .await?;

        // Start lite subscription manager
        self.lite_subscription_manager.start().await?;

        // Setup telemetry command handling
        let shutdown_token = CancellationToken::new();
        self.shutdown_token = Some(shutdown_token.clone());
        let task_tracker = TaskTracker::new();
        self.task_tracker = Some(task_tracker.clone());

        let manager = Arc::clone(&self.lite_subscription_manager);
        task_tracker.spawn(async move {
            while let Some(command) = telemetry_command_rx.recv().await {
                // Handle NotifyUnsubscribeLiteCommand
                if let Some(pb::telemetry_command::Command::NotifyUnsubscribeLiteCommand(ref cmd)) =
                    command.command
                {
                    manager.on_notify_unsubscribe_lite_command(cmd.lite_topic.clone());
                }

                // Handle settings updates
                if let Some(pb::telemetry_command::Command::Settings(ref settings)) =
                    command.command
                {
                    manager.sync_settings(settings);
                }
            }
        });

        info!("LitePushConsumer started successfully");
        Ok(())
    }
}

#[async_trait]
impl LitePushConsumerTrait for LitePushConsumer {
    async fn subscribe_lite(&self, lite_topic: String) -> Result<(), ClientError> {
        self.lite_subscription_manager
            .subscribe_lite(lite_topic, None)
            .await
    }

    async fn subscribe_lite_with_offset(
        &self,
        lite_topic: String,
        offset_option: OffsetOption,
    ) -> Result<(), ClientError> {
        self.lite_subscription_manager
            .subscribe_lite(lite_topic, Some(offset_option))
            .await
    }

    async fn unsubscribe_lite(&self, lite_topic: String) -> Result<(), ClientError> {
        self.lite_subscription_manager
            .unsubscribe_lite(lite_topic)
            .await
    }

    fn get_lite_topic_set(&self) -> HashSet<String> {
        self.lite_subscription_manager.get_lite_topic_set()
    }

    fn get_consumer_group(&self) -> String {
        self.lite_subscription_manager
            .get_consumer_group_name()
            .to_string()
    }

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
