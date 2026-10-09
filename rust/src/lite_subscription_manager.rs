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

use mockall_double::double;
use parking_lot::Mutex;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

#[double]
use crate::client::Client;
use crate::error::{ClientError, ErrorKind};
use crate::model::offset_option::OffsetOption;
use crate::pb;
use crate::pb::{LiteSubscriptionAction, Resource, SyncLiteSubscriptionRequest};
use crate::session::RPCClient;
use crate::util::handle_response_status;

const OPERATION_SYNC_LITE_SUBSCRIPTION: &str = "lite_subscription.sync";

/// Default interval between two periodic `SyncLiteSubscription` rounds.
const DEFAULT_PERIODIC_SYNC_INTERVAL: Duration = Duration::from_secs(30);

/// Manages lite topic subscriptions for LitePushConsumer
pub struct LiteSubscriptionManager {
    client: Arc<Client>,
    bind_topic: Resource,
    group: Resource,
    lite_topic_set: Arc<Mutex<HashSet<String>>>,
    lite_subscription_quota: Arc<Mutex<i32>>,
    max_lite_topic_size: Arc<Mutex<i32>>,
    /// Interval between two periodic sync rounds. Defaults to 30 seconds; shortened by tests.
    periodic_sync_interval: Duration,
    /// Cancellation token of the periodic-sync scheduler.
    scheduler_token: Mutex<Option<CancellationToken>>,
    /// Join handle of the periodic-sync scheduler, so `stop_scheduler` can await its exit.
    scheduler_handle: Mutex<Option<JoinHandle<()>>>,
    /// Flipped to `true` once the telemetry handler applied the server's `Settings`.
    settings_ready_tx: Arc<watch::Sender<bool>>,
}

impl LiteSubscriptionManager {
    /// Create a new LiteSubscriptionManager
    pub fn new(
        client: Arc<Client>,
        bind_topic_name: String,
        namespace: String,
        consumer_group: String,
    ) -> Self {
        let (settings_ready_tx, _) = watch::channel(false);
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
            periodic_sync_interval: DEFAULT_PERIODIC_SYNC_INTERVAL,
            scheduler_token: Mutex::new(None),
            scheduler_handle: Mutex::new(None),
            settings_ready_tx: Arc::new(settings_ready_tx),
        }
    }

    /// Start the subscription manager - sync all subscriptions after startup
    pub async fn start(&self) -> Result<(), ClientError> {
        self.sync_all_lite_subscription().await?;

        // Replace any scheduler left over from a previous start.
        self.stop_scheduler().await;

        // Schedule periodic sync every `periodic_sync_interval`. The task holds an `Arc<Self>`,
        // hence it must be cancellable: otherwise it would keep the client alive and could
        // re-create sessions after `Client::shutdown` cleared the session map.
        let token = CancellationToken::new();
        let task_token = token.clone();
        let manager = self.clone_for_scheduler();
        let interval = self.periodic_sync_interval;
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(interval);
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(e) = manager.sync_all_lite_subscription().await {
                            error!("Schedule syncAllLiteSubscription error: {:?}", e);
                        }
                    }
                    _ = task_token.cancelled() => {
                        break;
                    }
                }
            }
            info!("LiteSubscriptionManager periodic sync scheduler stopped");
        });

        *self.scheduler_token.lock() = Some(token);
        *self.scheduler_handle.lock() = Some(handle);

        Ok(())
    }

    /// Stop the periodic-sync scheduler, waiting until it exited.
    ///
    /// Must be called before closing the client sessions: the scheduler retains an `Arc<Client>`
    /// and the subscription set, and its next tick would call
    /// `Client::get_session_for_lite_consumer()` — which bypasses the running-state check and
    /// could re-create a session and send another `CompleteAdd` after shutdown.
    pub async fn stop_scheduler(&self) {
        if let Some(token) = self.scheduler_token.lock().take() {
            token.cancel();
        }
        let handle = self.scheduler_handle.lock().take();
        if let Some(handle) = handle {
            if let Err(e) = handle.await {
                warn!("Failed to join the periodic-sync scheduler: {:?}", e);
            }
        }
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
            periodic_sync_interval: self.periodic_sync_interval,
            scheduler_token: Mutex::new(None),
            scheduler_handle: Mutex::new(None),
            settings_ready_tx: Arc::clone(&self.settings_ready_tx),
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
        // The initial `Settings` command has arrived: whoever waits for it may proceed.
        // `send_replace` (not `send`) so the value is stored even when nobody is waiting.
        self.settings_ready_tx.send_replace(true);

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

    /// Wait until the telemetry handler applied the server's initial `Settings`.
    ///
    /// Returns `true` when the initial settings have been applied, `false` when `timeout`
    /// elapsed first (e.g. the server did not push its settings in time). Callers should log a
    /// warning in the latter case and continue with the local defaults.
    pub async fn wait_for_initial_settings(&self, timeout: Duration) -> bool {
        let mut rx = self.settings_ready_tx.subscribe();
        let ready = async { rx.wait_for(|ready| *ready).await.is_ok() };
        tokio::time::timeout(timeout, ready).await.is_ok()
    }

    /// Subscribe to a lite topic (reference Java: checkRunning first)
    pub async fn subscribe_lite(
        &self,
        lite_topic: String,
        offset_option: Option<OffsetOption>,
    ) -> Result<(), ClientError> {
        // For LitePushConsumer, we skip the check_started check because:
        // 1. The cloned client intentionally has shutdown_tx = None to avoid duplicate shutdown
        // 2. But it shares the same SessionManager, so sessions are still valid
        // 3. The original client manages the lifecycle, not the clone
        // Note: The public API (LitePushConsumerTrait::subscribe_lite) should validate state

        // Check if already subscribed
        if self.lite_topic_set.lock().contains(&lite_topic) {
            return Ok(());
        }

        // Validate lite topic format and length
        let max_size = *self.max_lite_topic_size.lock();
        self.validate_lite_topic(&lite_topic, max_size)?;

        // Check quota before adding new subscription
        self.check_lite_subscription_quota(1)?;

        // Sync subscription to server using PartialAdd action
        // This adds the new lite topic to the existing set on the server
        self.sync_lite_subscription(
            LiteSubscriptionAction::PartialAdd,
            vec![lite_topic.clone()],
            offset_option,
        )
        .await?;

        // Add to local set after successful sync
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
        // The running state is checked by the public API (LitePushConsumer / LiteSimpleConsumer) on
        // the owning client: the client held here is a lightweight clone whose shutdown_tx is
        // intentionally `None`, so `check_started` would always report "not started".
        // See `Client::clone_for_lite_consumer`.

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
        // For LitePushConsumer, we skip the check_started check because:
        // 1. The cloned client intentionally has shutdown_tx = None to avoid duplicate shutdown
        // 2. But it shares the same SessionManager, so sessions are still valid
        // 3. The original client manages the lifecycle, not the clone
        // Note: subscribe_lite and unsubscribe_lite still check started status via the public API

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

        // Use get_session_for_lite_consumer for LitePushConsumer
        // This method skips check_started and handles missing telemetry channel
        let mut rpc_client = self.client.get_session_for_lite_consumer().await?;

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
    pub(crate) fn check_lite_subscription_quota(&self, delta: i32) -> Result<(), ClientError> {
        let current_size = self.lite_topic_set.lock().len() as i32;
        let quota = *self.lite_subscription_quota.lock();

        // If quota is 0, it means the server hasn't returned the quota yet.
        // In this case, we should allow subscriptions to proceed.
        // Once the server returns the quota, it will be updated via sync_settings().
        if quota == 0 {
            return Ok(());
        }

        if current_size + delta > quota {
            return Err(ClientError::new(
                ErrorKind::Server,
                &format!(
                    "Lite subscription quota exceeded: current={}, delta={}, quota={}",
                    current_size, delta, quota
                ),
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
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    /// In test builds `Client` is doubled (`mockall_double`), so a fresh `MockClient` is used
    /// instead of the real one. Callers must set up the expectations they need.
    fn create_test_client() -> Arc<Client> {
        Arc::new(Client::default())
    }

    fn test_settings(max_lite_topic_size: i32) -> pb::Settings {
        pb::Settings {
            pub_sub: Some(pb::settings::PubSub::Subscription(pb::Subscription {
                lite_subscription_quota: Some(16),
                max_lite_topic_size: Some(max_lite_topic_size),
                ..Default::default()
            })),
            ..Default::default()
        }
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

        // When quota is 0 (not set by server yet), should allow subscriptions
        assert!(manager.check_lite_subscription_quota(1).is_ok());
        assert!(manager.check_lite_subscription_quota(100).is_ok());

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

    /// Regression test: the periodic-sync scheduler must stop when requested, otherwise it
    /// keeps issuing `SyncLiteSubscription` RPCs (and creating sessions) forever after the
    /// consumer has been shut down.
    #[tokio::test]
    async fn test_stop_scheduler_cancels_periodic_sync() {
        let sync_calls = Arc::new(AtomicUsize::new(0));

        // The scheduler only issues the RPC when the subscription set is not empty; every
        // attempt goes through `get_session_for_lite_consumer`, so counting that call is
        // enough. It returns an error to avoid mocking the whole session layer.
        let mut client = Client::default();
        {
            let sync_calls = Arc::clone(&sync_calls);
            client
                .expect_get_session_for_lite_consumer()
                .times(1..)
                .returning(move || {
                    sync_calls.fetch_add(1, Ordering::SeqCst);
                    Err(ClientError::new(
                        ErrorKind::ClientInternal,
                        "no session in test",
                        OPERATION_SYNC_LITE_SUBSCRIPTION,
                    ))
                });
        }
        let client = Arc::new(client);

        let mut manager = LiteSubscriptionManager::new(
            client,
            "bind_topic".to_string(),
            "namespace".to_string(),
            "group".to_string(),
        );
        manager.periodic_sync_interval = Duration::from_millis(50);

        // Start with an empty subscription set so the initial sync inside `start()` is a no-op
        // and succeeds without any server interaction.
        manager.start().await.expect("manager should start");

        // Subscribe from "outside": the scheduler now has a topic to sync and should start
        // issuing RPCs.
        manager.lite_topic_set.lock().insert("topic1".to_string());

        // Give the scheduler a chance to tick, so the test proves it was running.
        tokio::time::sleep(Duration::from_millis(120)).await;
        let calls_before_stop = sync_calls.load(Ordering::SeqCst);
        assert!(
            calls_before_stop >= 1,
            "the scheduler should keep issuing syncs while running"
        );

        manager.stop_scheduler().await;

        // Keep the runtime alive for much longer than one sync interval: no further RPC may
        // happen now that the scheduler has been cancelled.
        tokio::time::sleep(Duration::from_millis(250)).await;
        let calls_after_stop = sync_calls.load(Ordering::SeqCst);
        assert_eq!(
            calls_before_stop, calls_after_stop,
            "no sync RPC may be issued after the scheduler was stopped"
        );
    }

    /// Regression test: the server-provided `max_lite_topic_size` must be visible before the
    /// first `subscribe_lite`, otherwise a topic that is valid on the server is rejected
    /// locally against the built-in default of 64.
    #[tokio::test]
    async fn test_initial_settings_are_applied_before_first_subscribe() {
        let mut client = Client::default();
        {
            // Validation passes, quota passes, then the RPC itself fails in this offline test.
            client
                .expect_get_session_for_lite_consumer()
                .times(1..)
                .returning(|| {
                    Err(ClientError::new(
                        ErrorKind::ClientInternal,
                        "no session in test",
                        OPERATION_SYNC_LITE_SUBSCRIPTION,
                    ))
                });
        }
        let client = Arc::new(client);

        let manager = LiteSubscriptionManager::new(
            client,
            "bind_topic".to_string(),
            "namespace".to_string(),
            "group".to_string(),
        );

        // 80 ASCII bytes: longer than the built-in default (64), shorter than the server
        // limit used below (128).
        let lite_topic = "a".repeat(80);

        // Before any settings arrived, `wait_for_initial_settings` must time out.
        assert!(
            !manager
                .wait_for_initial_settings(Duration::from_millis(50))
                .await,
            "initial settings must not be reported as ready before they arrived"
        );

        // And the topic is rejected locally against the default max size.
        let err = manager
            .subscribe_lite(lite_topic.clone(), None)
            .await
            .err()
            .expect("subscribe must fail while the default max size is in effect");
        assert_eq!(*err.kind(), ErrorKind::Config);
        assert!(err.message().contains("max length"));

        // The server reports its settings (limit 128 > 80).
        manager.sync_settings(&test_settings(128));

        assert!(
            manager
                .wait_for_initial_settings(Duration::from_millis(50))
                .await,
            "initial settings must be reported as ready right after sync_settings"
        );

        // Now validation passes; the failure (if any) must come from the RPC layer, not from
        // the local length check.
        match manager.subscribe_lite(lite_topic, None).await {
            Ok(()) => {}
            Err(err) => {
                assert_ne!(
                    *err.kind(),
                    ErrorKind::Config,
                    "the topic must not be rejected by local validation anymore, got: {}",
                    err.message()
                );
            }
        }
    }
}
