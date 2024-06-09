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
use prost_types::Duration;
use slog::Logger;
use slog::{debug, error, info, warn};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::select;
use tokio::sync::{mpsc, oneshot};

#[double]
use crate::client::Client;
use crate::conf::{
    BackOffRetryPolicy, ClientOption, CustomizedBackOffRetryPolicy, ExponentialBackOffRetryPolicy,
    PushConsumerOption,
};
use crate::error::{ClientError, ErrorKind};
use crate::model::common::{ClientType, ConsumeResult, FilterExpression, MessageQueue};
use crate::model::message::{AckMessageEntry, MessageView};
use crate::pb::receive_message_response::Content;
use crate::pb::settings::PubSub;
use crate::pb::telemetry_command::Command;
use crate::pb::{
    AckMessageRequest, Assignment, ChangeInvisibleDurationRequest, QueryAssignmentRequest,
    ReceiveMessageRequest, Resource,
};
use crate::session::{RPCClient, Session};
use crate::util::{build_endpoints_by_message_queue, build_push_consumer_settings};
use crate::{log, pb};

const OPERATION_NEW_PUSH_CONSUMER: &str = "push_consumer.new";
const OPERATION_RECEIVE_MESSAGE: &str = "push_consumer.receive_message";
const OPERATION_ACK_MESSAGE: &str = "push_consumer.ack_message";
const OPERATION_START_PUSH_CONSUMER: &str = "push_consumer.start";
const OPERATION_CHANGE_INVISIBLE_DURATION: &str = "push_consumer.change_invisible_duration";

pub type MessageListener = dyn Fn(&MessageView) -> ConsumeResult + Send + Sync;

pub struct PushConsumer {
    logger: Logger,
    client: Client,
    message_listener: Arc<Box<MessageListener>>,
    option: Arc<RwLock<PushConsumerOption>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

/*
 * An actor is required for each message queue.
 * It is responsible for polling messages from the message queue. It communicates with PushConsumer by channels.
 */
struct MessageQueueActor {
    logger: Logger,
    rpc_client: Session,
    message_queue: MessageQueue,
    shutdown_tx: Option<oneshot::Sender<()>>,
    option: PushConsumerOption,
    message_listener: Arc<Box<MessageListener>>,
    retry_policy: BackOffRetryPolicy,
}

impl PushConsumer {
    pub fn new(
        client_option: ClientOption,
        option: PushConsumerOption,
        message_listener: Box<MessageListener>,
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
        let logger = log::logger(option.logging_format());
        let client = Client::new(
            &logger,
            client_option,
            build_push_consumer_settings(&option),
        )?;
        Ok(Self {
            logger,
            client,
            message_listener: Arc::new(message_listener),
            option: Arc::new(RwLock::new(option)),
            shutdown_tx: None,
        })
    }

    fn get_backoff_policy(command: Command) -> Option<BackOffRetryPolicy> {
        if let pb::telemetry_command::Command::Settings(settings) = command {
            let pubsub = settings.pub_sub.or(None)?;
            if let PubSub::Subscription(_) = pubsub {
                let retry_policy = settings.backoff_policy.or(None)?;
                let strategy = retry_policy.strategy.or(None)?;
                return match strategy {
                    pb::retry_policy::Strategy::ExponentialBackoff(strategy) => {
                        Some(BackOffRetryPolicy::Exponential(
                            ExponentialBackOffRetryPolicy::new(strategy),
                        ))
                    }
                    pb::retry_policy::Strategy::CustomizedBackoff(strategy) => Some(
                        BackOffRetryPolicy::Customized(CustomizedBackOffRetryPolicy::new(strategy)),
                    ),
                };
            }
        }
        None
    }

    pub async fn start(&mut self) -> Result<(), ClientError> {
        let (telemetry_command_tx, mut telemetry_command_rx) = mpsc::channel(16);
        self.client.start(telemetry_command_tx).await?;
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        self.shutdown_tx = Some(shutdown_tx);
        let option = Arc::clone(&self.option);
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
        let logger = self.logger.clone();
        let mut actor_table: HashMap<MessageQueue, MessageQueueActor> = HashMap::new();

        let message_listener = Arc::clone(&self.message_listener);
        // must retrieve settings from server first.
        let command = telemetry_command_rx.recv().await.ok_or(ClientError::new(
            ErrorKind::Connect,
            "telemetry command channel closed.",
            OPERATION_START_PUSH_CONSUMER,
        ))?;
        let backoff_policy = Arc::new(Mutex::new(Self::get_backoff_policy(command).ok_or(
            ClientError::new(
                ErrorKind::Connect,
                "backoff policy is required.",
                OPERATION_START_PUSH_CONSUMER,
            ),
        )?));

        tokio::spawn(async move {
            let mut scan_assignments_timer =
                tokio::time::interval(std::time::Duration::from_secs(30));
            loop {
                select! {
                    command = telemetry_command_rx.recv() => {
                        if let Some(command) = command {
                            let remote_backoff_policy = Self::get_backoff_policy(command);
                            if let Some(remote_backoff_policy) = remote_backoff_policy {
                                let mut guard = backoff_policy.lock();
                                *guard = remote_backoff_policy;
                            }
                        }
                    }
                    _ = scan_assignments_timer.tick() => {
                        let consumer_option;
                        {
                            consumer_option = option.read().clone();
                        }
                        let subscription_table = consumer_option.subscription_expressions();
                        // query endpoints from topic route
                        let retry_policy = Arc::clone(&backoff_policy);
                        let mut client = rpc_client.shadow_session();
                        for topic in subscription_table.keys() {
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
                                let retry_policy_inner;
                                {
                                    retry_policy_inner = retry_policy.lock().clone();
                                }
                                let result = client.query_assignment(request).await;
                                if let Ok(response) = result {
                                    if Client::handle_response_status(response.status, OPERATION_START_PUSH_CONSUMER).is_ok() {
                                            let _ = Self::process_assignments(logger.clone(),
                                                rpc_client.shadow_session(),
                                                &consumer_option,
                                                Arc::clone(&message_listener),
                                                &mut actor_table,
                                                response.assignments,
                                                retry_policy_inner,
                                            ).await;
                                    } else {
                                        error!(logger, "query assignment failed, no status in response.");
                                    }
                                } else {
                                    error!(logger, "query assignment failed: {:?}", result.unwrap_err());
                                }
                            }
                        }
                    }
                    _ = &mut shutdown_rx => {
                        let entries = actor_table.drain();
                        info!(logger, "shutdown {:?} actors", entries.len());
                        for (_, actor) in entries {
                            let _ = actor.shutdown().await;
                        }
                        break;
                    }
                }
            }
        });
        Ok(())
    }

    async fn process_assignments(
        logger: Logger,
        rpc_client: Session,
        option: &PushConsumerOption,
        message_listener: Arc<Box<MessageListener>>,
        actor_table: &mut HashMap<MessageQueue, MessageQueueActor>,
        mut assignments: Vec<Assignment>,
        retry_policy: BackOffRetryPolicy,
    ) -> Result<(), ClientError> {
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
        // the remaining actors will be shutdown.
        let shutdown_entries = actor_table.drain();
        for (_, actor) in shutdown_entries {
            let _ = actor.shutdown().await;
        }
        let mut max_cache_messages_per_queue = option.max_cache_message_count();
        if !message_queues.is_empty() {
            max_cache_messages_per_queue = option.max_cache_message_count() / message_queues.len();
        }
        for mut actor in actors {
            let mut option = option.clone();
            option.set_max_cache_message_count(max_cache_messages_per_queue);
            actor.set_option(option);
            actor.set_retry_policy(retry_policy.clone());
            actor_table.insert(actor.message_queue.clone(), actor);
        }

        // start new actors
        for message_queue in message_queues {
            if actor_table.contains_key(&message_queue) {
                continue;
            }
            let mut option = option.clone();
            option.set_max_cache_message_count(max_cache_messages_per_queue);
            let mut actor = MessageQueueActor::new(
                logger.clone(),
                rpc_client.shadow_session(),
                message_queue.clone(),
                option,
                Arc::clone(&message_listener),
                retry_policy.clone(),
            );
            let result = actor.start().await;
            if result.is_ok() {
                actor_table.insert(message_queue, actor);
            }
        }
        Ok(())
    }

    pub async fn shutdown(mut self) -> Result<(), ClientError> {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
        self.client.shutdown().await?;
        Ok(())
    }
}

struct MessageHandler {
    message: MessageView,
    status: ConsumeResult,
    attempt: usize,
}

#[automock]
impl MessageQueueActor {
    pub(crate) fn new(
        logger: Logger,
        rpc_client: Session,
        message_queue: MessageQueue,
        option: PushConsumerOption,
        message_listener: Arc<Box<MessageListener>>,
        retry_policy: BackOffRetryPolicy,
    ) -> Self {
        Self {
            logger,
            rpc_client,
            message_queue,
            shutdown_tx: None,
            option,
            message_listener: Arc::clone(&message_listener),
            retry_policy,
        }
    }

    // The following two methods won't affect the running actor in fact.
    pub(crate) fn set_option(&mut self, option: PushConsumerOption) {
        self.option = option;
    }

    pub(crate) fn set_retry_policy(&mut self, retry_policy: BackOffRetryPolicy) {
        self.retry_policy = retry_policy;
    }

    fn fork_self(&self) -> Self {
        Self {
            logger: self.logger.clone(),
            rpc_client: self.rpc_client.shadow_session(),
            message_queue: self.message_queue.clone(),
            shutdown_tx: None,
            option: self.option.clone(),
            message_listener: Arc::clone(&self.message_listener),
            retry_policy: self.retry_policy.clone(),
        }
    }

    pub(crate) fn get_consumer_group(&self) -> Resource {
        Resource {
            name: self.option.consumer_group().to_string(),
            resource_namespace: self.option.namespace().to_string(),
        }
    }

    #[allow(clippy::needless_lifetimes)]
    pub(crate) fn get_filter_expression<'a>(&'a self) -> Option<&'a FilterExpression> {
        self.option
            .subscription_expressions()
            .get(self.message_queue.topic.name.as_str())
    }

    pub(crate) async fn start(&mut self) -> Result<(), ClientError> {
        debug!(
            self.logger,
            "start a new queue actor {:?}", self.message_queue
        );
        let (tx, mut shutdown_rx) = oneshot::channel();
        self.shutdown_tx = Some(tx);
        let (tx, mut poll_rx) = mpsc::channel(8);
        let poll_tx: mpsc::Sender<()> = tx;
        let logger = self.logger.clone();
        let mut actor = self.fork_self();
        let mut message_handler_queue: VecDeque<MessageHandler> = VecDeque::new();
        let (tx, mut ack_rx) = mpsc::channel(8);
        let ack_tx: mpsc::Sender<()> = tx;
        let mut queue_check_ticker = tokio::time::interval(std::time::Duration::from_secs(1));
        tokio::spawn(async move {
            loop {
                select! {
                    _ = poll_rx.recv() => {
                        actor.receive_messages(&mut actor.rpc_client.shadow_session(), &mut message_handler_queue, poll_tx.clone()).await;
                    }
                    _ = ack_rx.recv() => {
                        actor.ack_message_in_waiting_queue(&mut actor.rpc_client.shadow_session(), &mut message_handler_queue, ack_tx.clone()).await;
                    }
                    _ = queue_check_ticker.tick() => {
                        if !message_handler_queue.is_empty() {
                            let _ = ack_tx.try_send(());
                        }
                        if message_handler_queue.len() < actor.option.max_cache_message_count() {
                            let _ = poll_tx.try_send(());
                        }
                    }
                    _ = &mut shutdown_rx => {
                        info!(logger, "message queue actor shutdown");
                        break;
                    }
                }
            }
        });
        Ok(())
    }

    async fn receive_messages<T: RPCClient + 'static>(
        &mut self,
        rpc_client: &mut T,
        message_handler_queue: &mut VecDeque<MessageHandler>,
        poll_tx: mpsc::Sender<()>,
    ) {
        let poll_result = self.poll_messages(rpc_client).await;
        if let Ok(messages) = poll_result {
            for message in messages {
                let consume_result = (self.message_listener)(&message);
                match consume_result {
                    ConsumeResult::SUCCESS => {
                        let result = self.ack_message(rpc_client, &message).await;
                        if result.is_err() {
                            info!(
                                self.logger,
                                "Ack message failed, put it back to queue, result: {:?}",
                                result.unwrap_err()
                            );
                            // put it back to queue
                            message_handler_queue.push_back(MessageHandler {
                                message,
                                status: ConsumeResult::SUCCESS,
                                attempt: 0,
                            });
                        }
                    }
                    ConsumeResult::FAILURE => {
                        let result = self.change_invisible_duration(rpc_client, &message).await;
                        if result.is_err() {
                            info!(self.logger, "Change invisible duration failed, put it back to queue, result: {:?}", result.unwrap_err());
                            message_handler_queue.push_back(MessageHandler {
                                message,
                                status: ConsumeResult::FAILURE,
                                attempt: 0,
                            });
                        }
                    }
                }
            }
        }
        if message_handler_queue.len() < self.option.max_cache_message_count() {
            let _ = poll_tx.try_send(());
        } else {
            error!(
                self.logger,
                "message queue has reached its limit {:?}, stop for a while.",
                self.option.max_cache_message_count()
            );
        }
    }

    async fn poll_messages<T: RPCClient + 'static>(
        &mut self,
        rpc_client: &mut T,
    ) -> Result<Vec<MessageView>, ClientError> {
        let filter_expression = self.get_filter_expression().ok_or(ClientError::new(
            ErrorKind::Unknown,
            "no filter expression presents",
            OPERATION_RECEIVE_MESSAGE,
        ))?;
        let request = ReceiveMessageRequest {
            group: Some(self.get_consumer_group()),
            message_queue: Some(self.message_queue.to_pb_message_queue()),
            filter_expression: Some(pb::FilterExpression {
                expression: filter_expression.expression().to_string(),
                r#type: filter_expression.filter_type() as i32,
            }),
            batch_size: self.option.batch_size(),
            auto_renew: true,
            invisible_duration: None,
            long_polling_timeout: Some(
                Duration::try_from(*self.option.long_polling_timeout()).unwrap(),
            ),
        };
        let responses = rpc_client.receive_message(request).await?;
        let mut messages: Vec<MessageView> = Vec::with_capacity(self.option.batch_size() as usize);
        let endpoints = build_endpoints_by_message_queue(
            &self.message_queue.to_pb_message_queue(),
            OPERATION_RECEIVE_MESSAGE,
        )?;
        for response in responses {
            if response.content.is_some() {
                let content = response.content.unwrap();
                match content {
                    Content::Status(status) => {
                        warn!(self.logger, "unhandled status message {:?}", status);
                    }
                    Content::DeliveryTimestamp(_) => {
                        warn!(self.logger, "unhandled delivery timestamp message");
                    }
                    Content::Message(message) => {
                        messages.push(MessageView::from_pb_message(message, endpoints.clone())?);
                    }
                }
            }
        }
        Ok(messages)
    }

    async fn ack_message_in_waiting_queue<T: RPCClient + 'static>(
        &mut self,
        rpc_client: &mut T,
        message_handler_queue: &mut VecDeque<MessageHandler>,
        ack_tx: mpsc::Sender<()>,
    ) {
        if let Some(mut handler) = message_handler_queue.pop_front() {
            match handler.status {
                ConsumeResult::SUCCESS => {
                    let result = self.ack_message(rpc_client, &handler.message).await;
                    if result.is_err() {
                        handler.attempt += 1;
                        warn!(
                            self.logger,
                            "{:?} attempt to ack message failed, result: {:?}",
                            handler.attempt,
                            result.unwrap_err()
                        );
                        message_handler_queue.push_front(handler);
                    } else if !message_handler_queue.is_empty() {
                        let _ = ack_tx.try_send(());
                    }
                }
                ConsumeResult::FAILURE => {
                    let result = self
                        .change_invisible_duration(rpc_client, &handler.message)
                        .await;
                    if result.is_err() {
                        handler.attempt += 1;
                        warn!(
                            self.logger,
                            "{:?} attempt to change invisible duration failed, result: {:?}",
                            handler.attempt,
                            result.unwrap_err()
                        );
                        message_handler_queue.push_front(handler);
                    } else if !message_handler_queue.is_empty() {
                        let _ = ack_tx.try_send(());
                    }
                }
            }
        }
    }

    async fn ack_message<T: RPCClient + 'static>(
        &mut self,
        rpc_client: &mut T,
        ack_entry: &MessageView,
    ) -> Result<(), ClientError> {
        let request = AckMessageRequest {
            group: Some(self.get_consumer_group()),
            topic: Some(self.message_queue.topic.clone()),
            entries: vec![pb::AckMessageEntry {
                message_id: ack_entry.message_id().to_string(),
                receipt_handle: ack_entry.receipt_handle().to_string(),
            }],
        };
        let response = rpc_client.ack_message(request).await?;
        Client::handle_response_status(response.status, OPERATION_ACK_MESSAGE)
    }

    async fn change_invisible_duration<T: RPCClient + 'static>(
        &mut self,
        rpc_client: &mut T,
        ack_entry: &MessageView,
    ) -> Result<(), ClientError> {
        let invisible_duration = match &self.retry_policy {
            BackOffRetryPolicy::Customized(policy) => {
                policy.get_next_attempt_delay(ack_entry.delivery_attempt())
            }
            BackOffRetryPolicy::Exponential(policy) => {
                policy.get_next_attempt_delay(ack_entry.delivery_attempt())
            }
        };
        let request = ChangeInvisibleDurationRequest {
            group: Some(self.get_consumer_group()),
            topic: Some(self.message_queue.topic.clone()),
            receipt_handle: ack_entry.receipt_handle().to_string(),
            message_id: ack_entry.message_id().to_string(),
            invisible_duration: Some(prost_types::Duration {
                seconds: invisible_duration.as_secs() as i64,
                nanos: invisible_duration.subsec_nanos() as i32,
            }),
        };
        let response = rpc_client.change_invisible_duration(request).await?;
        Client::handle_response_status(response.status, OPERATION_CHANGE_INVISIBLE_DURATION)
    }

    pub(crate) async fn shutdown(mut self) -> Result<(), ClientError> {
        debug!(self.logger, "shutdown queue actor {:?}", self.message_queue);
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        str::FromStr,
        sync::atomic::{AtomicUsize, Ordering},
        vec,
    };

    use log::terminal_logger;
    use pb::{
        AckMessageResponse, Address, Broker, ChangeInvisibleDurationResponse, Code,
        CustomizedBackoff, Endpoints, ExponentialBackoff, ReceiveMessageResponse, RetryPolicy,
        Settings, Status, Subscription, SystemProperties, VerifyMessageCommand,
    };

    use crate::{client::MockClient, model::common};
    use crate::{model::common::FilterType, session::MockRPCClient};

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
        context.expect().returning(|_, _, _| Ok(Client::default()));
        let result3 = PushConsumer::new(
            ClientOption::default(),
            option2,
            Box::new(|_| ConsumeResult::SUCCESS),
        );
        assert!(result3.is_ok());
    }

    #[test]
    fn test_get_back_policy() -> Result<(), String> {
        let command =
            pb::telemetry_command::Command::VerifyMessageCommand(VerifyMessageCommand::default());
        let result = PushConsumer::get_backoff_policy(command);
        assert!(result.is_none());

        let command2 = pb::telemetry_command::Command::Settings(Settings::default());
        let result2 = PushConsumer::get_backoff_policy(command2);
        assert!(result2.is_none());

        let exponential_backoff_settings = Settings {
            pub_sub: Some(PubSub::Subscription(Subscription::default())),
            client_type: Some(ClientType::PushConsumer as i32),
            backoff_policy: Some(RetryPolicy {
                max_attempts: 0,
                strategy: Some(pb::retry_policy::Strategy::ExponentialBackoff(
                    ExponentialBackoff {
                        initial: Some(prost_types::Duration::from_str("1s").unwrap()),
                        max: Some(prost_types::Duration::from_str("60s").unwrap()),
                        multiplier: 2.0,
                    },
                )),
            }),
            ..Settings::default()
        };
        let command3 = pb::telemetry_command::Command::Settings(exponential_backoff_settings);
        let result3 = PushConsumer::get_backoff_policy(command3);
        if let Some(BackOffRetryPolicy::Exponential(_)) = result3 {
        } else {
            return Err("get_backoff_policy failed, expected ExponentialBackoff.".to_string());
        }

        let customized_backoff_settings = Settings {
            pub_sub: Some(PubSub::Subscription(Subscription::default())),
            client_type: Some(ClientType::PushConsumer as i32),
            backoff_policy: Some(RetryPolicy {
                max_attempts: 0,
                strategy: Some(pb::retry_policy::Strategy::CustomizedBackoff(
                    CustomizedBackoff {
                        next: vec![prost_types::Duration::from_str("1s").unwrap()],
                    },
                )),
            }),
            ..Settings::default()
        };
        let command4 = pb::telemetry_command::Command::Settings(customized_backoff_settings);
        let result4 = PushConsumer::get_backoff_policy(command4);
        if let Some(BackOffRetryPolicy::Customized(_)) = result4 {
        } else {
            return Err("get_backoff_policy failed, expected CustomizedBackoff.".to_string());
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_process_assignments_invalid_assignment() -> Result<(), ClientError> {
        let rpc_client = Session::mock();
        let logger = terminal_logger();
        let option = &PushConsumerOption::default();
        let message_listener: Arc<Box<MessageListener>> =
            Arc::new(Box::new(|_| ConsumeResult::SUCCESS));
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
            logger,
            rpc_client,
            option,
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
        let rpc_client = Session::mock();
        let logger = terminal_logger();
        let option = &PushConsumerOption::default();
        let message_listener: Arc<Box<MessageListener>> =
            Arc::new(Box::new(|_| ConsumeResult::SUCCESS));
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
        let context = MockMessageQueueActor::new_context();
        context.expect().returning(|_, _, _, _, _, _| {
            let mut actor = MockMessageQueueActor::default();
            actor.expect_start().returning(|| Ok(()));
            return actor;
        });
        PushConsumer::process_assignments(
            logger,
            rpc_client,
            option,
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
        let logger = terminal_logger();
        let option = &PushConsumerOption::default();
        let message_listener: Arc<Box<MessageListener>> =
            Arc::new(Box::new(|_| ConsumeResult::SUCCESS));
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
        let context = MockMessageQueueActor::new_context();
        context.expect().returning(|_, _, _, _, _, _| {
            let mut actor = MockMessageQueueActor::default();
            actor.expect_start().returning(|| Ok(()));
            return actor;
        });
        PushConsumer::process_assignments(
            logger.clone(),
            Session::mock(),
            option,
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
        PushConsumer::process_assignments(
            logger,
            Session::mock(),
            option,
            Arc::new(Box::new(|_| ConsumeResult::SUCCESS)),
            &mut actor_table,
            assignments2,
            retry_policy,
        )
        .await?;
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
                endpoints: Some(Endpoints {
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

    fn new_actor_for_test(message_listener: Arc<Box<MessageListener>>) -> MessageQueueActor {
        let message_queue = new_message_queue();
        let mut option = PushConsumerOption::default();
        option.subscribe("test_topic", FilterExpression::new(FilterType::Tag, "*"));
        let retry_policy = BackOffRetryPolicy::Customized(CustomizedBackOffRetryPolicy::new(
            pb::CustomizedBackoff {
                next: vec![prost_types::Duration::from_str("1s").unwrap()],
            },
        ));
        let session = Session::mock();
        MessageQueueActor::new(
            terminal_logger(),
            session,
            message_queue,
            option,
            message_listener,
            retry_policy,
        )
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
    async fn test_actor_consume_messages_success() {
        let _m = crate::client::tests::MTX.lock();
        let receive_count = Arc::new(AtomicUsize::new(0));
        let receive_count_2 = Arc::clone(&receive_count);
        let message_listener: Arc<Box<MessageListener>> = Arc::new(Box::new(move |_| {
            receive_count.fetch_add(1, Ordering::Relaxed);
            ConsumeResult::SUCCESS
        }));

        let response = Ok(vec![ReceiveMessageResponse {
            content: Some(Content::Message(new_message())),
        }]);
        let mut mock = MockRPCClient::new();
        mock.expect_receive_message()
            .return_once(|_| Box::pin(futures::future::ready(response)));
        let ack_response = Ok(AckMessageResponse {
            status: Some(Status {
                code: Code::Ok as i32,
                message: "ok".to_string(),
            }),
            entries: vec![],
        });
        mock.expect_ack_message()
            .return_once(|_| Box::pin(futures::future::ready(ack_response)));

        let context = MockClient::handle_response_status_context();
        context.expect().returning(|_, _| Ok(()));

        let mut actor = new_actor_for_test(message_listener);
        let mut message_handler_queue = VecDeque::new();
        let (tx, mut rx) = mpsc::channel(16);
        let poll_tx: mpsc::Sender<()> = tx;
        actor
            .receive_messages(&mut mock, &mut message_handler_queue, poll_tx)
            .await;
        let signal = rx.recv().await;
        assert!(signal.is_some());
        assert_eq!(1, receive_count_2.load(Ordering::Relaxed));
        assert!(message_handler_queue.is_empty());
    }

    #[tokio::test]
    async fn test_actor_consume_messages_failure() {
        let _m = crate::client::tests::MTX.lock();
        let receive_count = Arc::new(AtomicUsize::new(0));
        let receive_count_2 = Arc::clone(&receive_count);
        let message_listener: Arc<Box<MessageListener>> = Arc::new(Box::new(move |_| {
            receive_count.fetch_add(1, Ordering::Relaxed);
            ConsumeResult::FAILURE
        }));

        let response = Ok(vec![ReceiveMessageResponse {
            content: Some(Content::Message(new_message())),
        }]);
        let mut mock = MockRPCClient::new();
        mock.expect_receive_message()
            .return_once(|_| Box::pin(futures::future::ready(response)));
        let ack_response = Ok(ChangeInvisibleDurationResponse {
            status: Some(Status {
                code: Code::Ok as i32,
                message: "ok".to_string(),
            }),
            receipt_handle: "test".to_string(),
        });
        mock.expect_change_invisible_duration()
            .return_once(|_| Box::pin(futures::future::ready(ack_response)));

        let context = MockClient::handle_response_status_context();
        context.expect().returning(|_, _| Ok(()));

        let mut actor = new_actor_for_test(message_listener);
        let mut message_handler_queue = VecDeque::new();
        let (tx, mut rx) = mpsc::channel(16);
        let poll_tx: mpsc::Sender<()> = tx;
        actor
            .receive_messages(&mut mock, &mut message_handler_queue, poll_tx)
            .await;
        let signal = rx.recv().await;
        assert!(signal.is_some());
        assert_eq!(1, receive_count_2.load(Ordering::Relaxed));
        assert!(message_handler_queue.is_empty());
    }

    #[tokio::test]
    async fn test_actor_consume_messages_ack_failure() {
        let _m = crate::client::tests::MTX.lock();
        let receive_count = Arc::new(AtomicUsize::new(0));
        let receive_count_2 = Arc::clone(&receive_count);
        let message_listener: Arc<Box<MessageListener>> = Arc::new(Box::new(move |_| {
            receive_count.fetch_add(1, Ordering::Relaxed);
            ConsumeResult::SUCCESS
        }));

        let response = Ok(vec![ReceiveMessageResponse {
            content: Some(Content::Message(new_message())),
        }]);
        let mut mock = MockRPCClient::new();
        mock.expect_receive_message()
            .return_once(|_| Box::pin(futures::future::ready(response)));
        let ack_response = Ok(AckMessageResponse {
            status: Some(Status {
                code: Code::BadRequest as i32,
                message: "ok".to_string(),
            }),
            entries: vec![],
        });
        mock.expect_ack_message()
            .return_once(|_| Box::pin(futures::future::ready(ack_response)));

        let context = MockClient::handle_response_status_context();
        context
            .expect()
            .returning(|_, _| Err(ClientError::new(ErrorKind::Server, "test", "test")));

        let mut actor = new_actor_for_test(message_listener);
        let mut message_handler_queue = VecDeque::new();
        let (tx, mut rx) = mpsc::channel(16);
        let poll_tx: mpsc::Sender<()> = tx;
        actor
            .receive_messages(&mut mock, &mut message_handler_queue, poll_tx)
            .await;
        let signal = rx.recv().await;
        assert!(signal.is_some());
        assert_eq!(1, receive_count_2.load(Ordering::Relaxed));
        let handler = message_handler_queue.pop_front().unwrap();
        assert!(matches!(handler.status, ConsumeResult::SUCCESS));
    }

    #[tokio::test]
    async fn test_actor_consume_messages_nack_failure() {
        let _m = crate::client::tests::MTX.lock();
        let receive_count = Arc::new(AtomicUsize::new(0));
        let receive_count_2 = Arc::clone(&receive_count);
        let message_listener: Arc<Box<MessageListener>> = Arc::new(Box::new(move |_| {
            receive_count.fetch_add(1, Ordering::Relaxed);
            ConsumeResult::FAILURE
        }));

        let response = Ok(vec![ReceiveMessageResponse {
            content: Some(Content::Message(new_message())),
        }]);
        let mut mock = MockRPCClient::new();
        mock.expect_receive_message()
            .return_once(|_| Box::pin(futures::future::ready(response)));
        let ack_response = Ok(ChangeInvisibleDurationResponse {
            status: Some(Status {
                code: Code::BadRequest as i32,
                message: "ok".to_string(),
            }),
            receipt_handle: "test".to_string(),
        });
        mock.expect_change_invisible_duration()
            .return_once(|_| Box::pin(futures::future::ready(ack_response)));

        let context = MockClient::handle_response_status_context();
        context
            .expect()
            .returning(|_, _| Err(ClientError::new(ErrorKind::Server, "test", "test")));

        let mut actor = new_actor_for_test(message_listener);
        let mut message_handler_queue = VecDeque::new();
        let (tx, mut rx) = mpsc::channel(16);
        let poll_tx: mpsc::Sender<()> = tx;
        actor
            .receive_messages(&mut mock, &mut message_handler_queue, poll_tx)
            .await;
        let signal = rx.recv().await;
        assert!(signal.is_some());
        assert_eq!(1, receive_count_2.load(Ordering::Relaxed));
        let handler = message_handler_queue.pop_front().unwrap();
        assert!(matches!(handler.status, ConsumeResult::FAILURE));
    }

    #[tokio::test]
    async fn test_actor_ack_message_in_waiting_queue_success() {
        let _m = crate::client::tests::MTX.lock();
        let message_listener: Arc<Box<MessageListener>> =
            Arc::new(Box::new(|_| ConsumeResult::SUCCESS));
        let mut actor = new_actor_for_test(message_listener);
        let mut mock = MockRPCClient::new();
        let ack_response = Ok(AckMessageResponse {
            status: Some(Status {
                code: Code::Ok as i32,
                message: "ok".to_string(),
            }),
            entries: vec![],
        });
        mock.expect_ack_message()
            .return_once(|_| Box::pin(futures::future::ready(ack_response)));

        let context = MockClient::handle_response_status_context();
        context.expect().returning(|_, _| Ok(()));
        let mut message_handler_queue: VecDeque<MessageHandler> = VecDeque::new();
        let handle = MessageHandler {
            message: MessageView::from_pb_message(
                new_message(),
                common::Endpoints::from_url("localhost:8081").unwrap(),
            )
            .unwrap(),
            status: ConsumeResult::SUCCESS,
            attempt: 0,
        };
        message_handler_queue.push_back(handle);
        let (tx, _) = mpsc::channel(16);
        let ack_tx = tx;
        actor
            .ack_message_in_waiting_queue(&mut mock, &mut message_handler_queue, ack_tx)
            .await;
        assert!(message_handler_queue.is_empty());
    }

    #[tokio::test]
    async fn test_actor_ack_message_in_waiting_queue_failure() {
        let _m = crate::client::tests::MTX.lock();
        let message_listener: Arc<Box<MessageListener>> =
            Arc::new(Box::new(|_| ConsumeResult::SUCCESS));
        let mut actor = new_actor_for_test(message_listener);
        let mut mock = MockRPCClient::new();
        let ack_response = Ok(AckMessageResponse {
            status: Some(Status {
                code: Code::BadRequest as i32,
                message: "ok".to_string(),
            }),
            entries: vec![],
        });
        mock.expect_ack_message()
            .return_once(|_| Box::pin(futures::future::ready(ack_response)));

        let context = MockClient::handle_response_status_context();
        context
            .expect()
            .returning(|_, _| Err(ClientError::new(ErrorKind::Server, "test", "test")));
        let mut message_handler_queue: VecDeque<MessageHandler> = VecDeque::new();
        let handle = MessageHandler {
            message: MessageView::from_pb_message(
                new_message(),
                common::Endpoints::from_url("localhost:8081").unwrap(),
            )
            .unwrap(),
            status: ConsumeResult::SUCCESS,
            attempt: 0,
        };
        message_handler_queue.push_back(handle);
        let (tx, _) = mpsc::channel(16);
        let ack_tx = tx;
        actor
            .ack_message_in_waiting_queue(&mut mock, &mut message_handler_queue, ack_tx)
            .await;
        let handler2 = message_handler_queue.pop_front().unwrap();
        assert_eq!(1, handler2.attempt);
    }

    #[tokio::test]
    async fn test_actor_nack_message_in_waiting_queue_success() {
        let _m = crate::client::tests::MTX.lock();
        let message_listener: Arc<Box<MessageListener>> =
            Arc::new(Box::new(|_| ConsumeResult::SUCCESS));
        let mut actor = new_actor_for_test(message_listener);
        let mut mock = MockRPCClient::new();
        let ack_response = Ok(ChangeInvisibleDurationResponse {
            status: Some(Status {
                code: Code::Ok as i32,
                message: "ok".to_string(),
            }),
            receipt_handle: "test".to_string(),
        });
        mock.expect_change_invisible_duration()
            .return_once(|_| Box::pin(futures::future::ready(ack_response)));

        let context = MockClient::handle_response_status_context();
        context.expect().returning(|_, _| Ok(()));
        let mut message_handler_queue: VecDeque<MessageHandler> = VecDeque::new();
        let handle = MessageHandler {
            message: MessageView::from_pb_message(
                new_message(),
                common::Endpoints::from_url("localhost:8081").unwrap(),
            )
            .unwrap(),
            status: ConsumeResult::FAILURE,
            attempt: 0,
        };
        message_handler_queue.push_back(handle);
        let (tx, _) = mpsc::channel(16);
        let ack_tx = tx;
        actor
            .ack_message_in_waiting_queue(&mut mock, &mut message_handler_queue, ack_tx)
            .await;
        assert!(message_handler_queue.is_empty());
    }

    #[tokio::test]
    async fn test_actor_nack_message_in_waiting_queue_failure() {
        let _m = crate::client::tests::MTX.lock();
        let message_listener: Arc<Box<MessageListener>> =
            Arc::new(Box::new(|_| ConsumeResult::SUCCESS));
        let mut actor = new_actor_for_test(message_listener);
        let mut mock = MockRPCClient::new();
        let ack_response = Ok(ChangeInvisibleDurationResponse {
            status: Some(Status {
                code: Code::BadRequest as i32,
                message: "ok".to_string(),
            }),
            receipt_handle: "test".to_string(),
        });
        mock.expect_change_invisible_duration()
            .return_once(|_| Box::pin(futures::future::ready(ack_response)));

        let context = MockClient::handle_response_status_context();
        context
            .expect()
            .returning(|_, _| Err(ClientError::new(ErrorKind::Server, "test", "test")));
        let mut message_handler_queue: VecDeque<MessageHandler> = VecDeque::new();
        let handle = MessageHandler {
            message: MessageView::from_pb_message(
                new_message(),
                common::Endpoints::from_url("localhost:8081").unwrap(),
            )
            .unwrap(),
            status: ConsumeResult::FAILURE,
            attempt: 0,
        };
        message_handler_queue.push_back(handle);
        let (tx, _) = mpsc::channel(16);
        let ack_tx = tx;
        actor
            .ack_message_in_waiting_queue(&mut mock, &mut message_handler_queue, ack_tx)
            .await;
        let handler2 = message_handler_queue.pop_front().unwrap();
        assert_eq!(1, handler2.attempt);
    }
}
