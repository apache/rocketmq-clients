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

//! Lite subscription manager for managing lite topic subscriptions lifecycle.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tracing::{error, info};

use crate::client::Client;
use crate::error::{ClientError, ErrorKind};
use crate::model::offset_option::OffsetOption;
use crate::pb;
use crate::pb::{LiteSubscriptionAction, Resource, SyncLiteSubscriptionRequest};
use crate::session::RPCClient;
use crate::util::handle_response_status;

const OPERATION_SYNC_LITE_SUBSCRIPTION: &str = "lite_subscription.sync";

/// Manages lite topic subscriptions for LitePushConsumer
pub struct LiteSubscriptionManager {
    client: Arc<Client>,
    bind_topic: Resource,
    group: Resource,
    lite_topic_set: Arc<Mutex<HashSet<String>>>,
    lite_subscription_quota: Arc<Mutex<i32>>,
    max_lite_topic_size: Arc<Mutex<i32>>,
}

impl LiteSubscriptionManager {
    /// Create a new LiteSubscriptionManager
    pub fn new(
        client: Arc<Client>,
        bind_topic_name: String,
        namespace: String,
        consumer_group: String,
    ) -> Self {
        Self {
            client,
            bind_topic: Resource {
                resource_namespace: namespace.clone(),
                name: bind_topic_name,
            },
            group: Resource {
                resource_namespace: namespace,
                name: consumer_group,
            },
            lite_topic_set: Arc::new(Mutex::new(HashSet::new())),
            lite_subscription_quota: Arc::new(Mutex::new(0)),
            max_lite_topic_size: Arc::new(Mutex::new(64)), // default value
        }
    }

    /// Start the subscription manager - sync all subscriptions after startup
    pub async fn start(&self) -> Result<(), ClientError> {
        self.sync_all_lite_subscription().await?;

        // Schedule periodic sync every 30 seconds
        let manager = self.clone_for_scheduler();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            loop {
                interval.tick().await;
                if let Err(e) = manager.sync_all_lite_subscription().await {
                    error!("Schedule syncAllLiteSubscription error: {:?}", e);
                }
            }
        });

        Ok(())
    }

    /// Clone necessary fields for scheduler task
    fn clone_for_scheduler(&self) -> Arc<Self> {
        Arc::new(Self {
            client: Arc::clone(&self.client),
            bind_topic: self.bind_topic.clone(),
            group: self.group.clone(),
            lite_topic_set: Arc::clone(&self.lite_topic_set),
            lite_subscription_quota: Arc::clone(&self.lite_subscription_quota),
            max_lite_topic_size: Arc::clone(&self.max_lite_topic_size),
        })
    }

    /// Get bind topic name
    pub fn get_bind_topic_name(&self) -> &str {
        &self.bind_topic.name
    }

    /// Get consumer group name
    pub fn get_consumer_group_name(&self) -> &str {
        &self.group.name
    }

    /// Get the set of subscribed lite topics
    pub fn get_lite_topic_set(&self) -> HashSet<String> {
        self.lite_topic_set.lock().clone()
    }

    /// Sync settings from server (reference Java: check hasSubscription first)
    pub fn sync_settings(&self, settings: &pb::Settings) {
        // Check if settings has subscription (similar to Java's settings.hasSubscription())
        let has_subscription = matches!(
            &settings.pub_sub,
            Some(pb::settings::PubSub::Subscription(_))
        );

        if !has_subscription {
            return;
        }

        // Now we know it's a Subscription variant, extract and sync
        if let Some(pb::settings::PubSub::Subscription(subscription)) = &settings.pub_sub {
            if let Some(quota) = subscription.lite_subscription_quota {
                *self.lite_subscription_quota.lock() = quota;
                info!("Updated lite subscription quota to {}", quota);
            }
            if let Some(max_size) = subscription.max_lite_topic_size {
                *self.max_lite_topic_size.lock() = max_size;
                info!("Updated max lite topic size to {}", max_size);
            }
        }
    }

    /// Subscribe to a lite topic (reference Java: checkRunning first)
    pub async fn subscribe_lite(
        &self,
        lite_topic: String,
        offset_option: Option<OffsetOption>,
    ) -> Result<(), ClientError> {
        // Check if client is running (reference Java: consumerImpl.checkRunning())
        self.client
            .check_started(OPERATION_SYNC_LITE_SUBSCRIPTION)?;

        // Check if already subscribed
        if self.lite_topic_set.lock().contains(&lite_topic) {
            return Ok(());
        }

        // Validate lite topic
        let max_size = *self.max_lite_topic_size.lock();
        self.validate_lite_topic(&lite_topic, max_size)?;

        // Check quota
        self.check_lite_subscription_quota(1)?;

        // Sync subscription to server
        self.sync_lite_subscription(
            LiteSubscriptionAction::PartialAdd,
            vec![lite_topic.clone()],
            offset_option,
        )
        .await?;

        // Add to local set
        self.lite_topic_set.lock().insert(lite_topic.clone());

        info!(
            "SubscribeLite {}, topic={}, group={}, clientId={}",
            lite_topic,
            self.get_bind_topic_name(),
            self.get_consumer_group_name(),
            self.client.client_id()
        );

        Ok(())
    }

    /// Unsubscribe from a lite topic (reference Java: checkRunning first)
    pub async fn unsubscribe_lite(&self, lite_topic: String) -> Result<(), ClientError> {
        // Check if client is running (reference Java: consumerImpl.checkRunning() line 119)
        self.client
            .check_started(OPERATION_SYNC_LITE_SUBSCRIPTION)?;

        // Check if subscribed
        if !self.lite_topic_set.lock().contains(&lite_topic) {
            return Ok(());
        }

        // Sync unsubscription to server
        self.sync_lite_subscription(
            LiteSubscriptionAction::PartialRemove,
            vec![lite_topic.clone()],
            None,
        )
        .await?;

        // Remove from local set
        self.lite_topic_set.lock().remove(&lite_topic);

        info!(
            "UnsubscribeLite {}, topic={}, group={}, clientId={}",
            lite_topic,
            self.get_bind_topic_name(),
            self.get_consumer_group_name(),
            self.client.client_id()
        );

        Ok(())
    }

    /// Sync all lite subscriptions periodically
    async fn sync_all_lite_subscription(&self) -> Result<(), ClientError> {
        // Check quota
        self.check_lite_subscription_quota(0)?;

        let topics: Vec<String> = self.lite_topic_set.lock().iter().cloned().collect();
        if topics.is_empty() {
            return Ok(());
        }

        match self
            .sync_lite_subscription(LiteSubscriptionAction::CompleteAdd, topics, None)
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                error!("Failed to sync all lite subscriptions: {:?}", e);
                Err(e)
            }
        }
    }

    /// Sync lite subscription to server
    async fn sync_lite_subscription(
        &self,
        action: LiteSubscriptionAction,
        lite_topics: Vec<String>,
        offset_option: Option<OffsetOption>,
    ) -> Result<(), ClientError> {
        let request = SyncLiteSubscriptionRequest {
            action: action as i32,
            topic: Some(self.bind_topic.clone()),
            group: Some(self.group.clone()),
            lite_topic_set: lite_topics,
            version: None,
            offset_option: offset_option.map(|opt| opt.to_protobuf()),
        };

        let mut rpc_client = self.client.get_session().await?;

        let response = rpc_client.sync_lite_subscription(request).await?;

        handle_response_status(response.status, OPERATION_SYNC_LITE_SUBSCRIPTION)?;

        Ok(())
    }

    /// Handle NotifyUnsubscribeLiteCommand from server
    pub fn on_notify_unsubscribe_lite_command(&self, lite_topic: String) {
        info!(
            "Notify unsubscribe lite liteTopic={} group={} bindTopic={}",
            lite_topic,
            self.get_consumer_group_name(),
            self.get_bind_topic_name()
        );

        if !lite_topic.is_empty() {
            self.lite_topic_set.lock().remove(&lite_topic);
        }
    }

    /// Validate lite topic format and length
    fn validate_lite_topic(&self, lite_topic: &str, max_length: i32) -> Result<(), ClientError> {
        if lite_topic.trim().is_empty() {
            return Err(ClientError::new(
                ErrorKind::Config,
                "liteTopic is blank",
                OPERATION_SYNC_LITE_SUBSCRIPTION,
            ));
        }

        if lite_topic.len() > max_length as usize {
            return Err(ClientError::new(
                ErrorKind::Config,
                &format!(
                    "liteTopic length exceeded max length {}, liteTopic: {}",
                    max_length, lite_topic
                ),
                OPERATION_SYNC_LITE_SUBSCRIPTION,
            ));
        }

        Ok(())
    }

    /// Check if adding delta subscriptions would exceed quota
    fn check_lite_subscription_quota(&self, delta: i32) -> Result<(), ClientError> {
        let current_size = self.lite_topic_set.lock().len() as i32;
        let quota = *self.lite_subscription_quota.lock();

        if current_size + delta > quota {
            return Err(ClientError::new(
                ErrorKind::Server,
                &format!("Lite subscription quota exceeded {}", quota),
                OPERATION_SYNC_LITE_SUBSCRIPTION,
            )
            .with_context(
                "code",
                format!("{}", pb::Code::LiteSubscriptionQuotaExceeded as i32),
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_client() -> Arc<Client> {
        // Create a minimal client for testing
        let option = crate::conf::ClientOption::default();
        let settings = pb::TelemetryCommand {
            command: None,
            status: None,
        };
        Arc::new(Client::new(option, settings).unwrap())
    }

    #[test]
    fn test_validate_lite_topic() {
        let client = create_test_client();
        let manager = LiteSubscriptionManager::new(
            client,
            "bind_topic".to_string(),
            "namespace".to_string(),
            "group".to_string(),
        );

        // Valid topic
        assert!(manager.validate_lite_topic("valid-topic", 64).is_ok());

        // Blank topic
        assert!(manager.validate_lite_topic("", 64).is_err());
        assert!(manager.validate_lite_topic("   ", 64).is_err());

        // Too long topic
        let long_topic = "a".repeat(65);
        assert!(manager.validate_lite_topic(&long_topic, 64).is_err());
    }

    #[test]
    fn test_check_quota() {
        let client = create_test_client();
        let manager = LiteSubscriptionManager::new(
            client,
            "bind_topic".to_string(),
            "namespace".to_string(),
            "group".to_string(),
        );

        // Set quota to 5
        *manager.lite_subscription_quota.lock() = 5;

        // Should pass when under quota
        assert!(manager.check_lite_subscription_quota(3).is_ok());

        // Should fail when exceeding quota
        assert!(manager.check_lite_subscription_quota(6).is_err());

        // Add some topics
        manager.lite_topic_set.lock().insert("topic1".to_string());
        manager.lite_topic_set.lock().insert("topic2".to_string());

        // Now only 3 more allowed
        assert!(manager.check_lite_subscription_quota(3).is_ok());
        assert!(manager.check_lite_subscription_quota(4).is_err());
    }
}
