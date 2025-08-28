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
use crate::model::common::{ClientType, ConsumeResult, MessageQueue};
use crate::model::message::{AckMessageEntry, MessageView};
use crate::pb;
use crate::pb::receive_message_response::Content;
use crate::pb::{
    AckMessageRequest, Assignment, ChangeInvisibleDurationRequest,
    ForwardMessageToDeadLetterQueueRequest, QueryAssignmentRequest, ReceiveMessageRequest,
    Resource, Status,
};
use crate::session::RPCClient;
#[double]
use crate::session::Session;
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

pub type MessageListener = Box<dyn Fn(&MessageView) -> ConsumeResult + Send + Sync>;

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

    pub async fn start(&mut self) -> Result<(), ClientError> {
        let (telemetry_command_tx, mut telemetry_command_rx) = mpsc::channel(16);
        self.client.start(telemetry_command_tx).await?;
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
        let mut actor_table: HashMap<MessageQueue, MessageQueueActor> = HashMap::new();

        let message_listener = Arc::clone(&self.message_listener);
        let retry_policy: Arc<Mutex<Option<BackOffRetryPolicy>>> = Arc::new(Mutex::new(None));

        let shutdown_token = CancellationToken::new();
        self.shutdown_token = Some(shutdown_token.clone());
        let task_tracker = TaskTracker::new();
        self.task_tracker = Some(task_tracker.clone());

        task_tracker.spawn(async move {
            let mut scan_assignments_timer =
                tokio::time::interval(std::time::Duration::from_secs(30));
            loop {
                select! {
                    command = telemetry_command_rx.recv() => {
                        if let Some(command) = command {
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
                                let result = rpc_client.query_assignment(request).await;
                                if let Ok(response) = result {
                                    if handle_response_status(response.status, OPERATION_START_PUSH_CONSUMER).is_ok() {
                                            let result = Self::process_assignments(
                                                &rpc_client,
                                                &consumer_option,
                                                Arc::clone(&message_listener),
                                                &mut actor_table,
                                                response.assignments,
                                                retry_policy_inner.clone(),
                                            ).await;
                                            if result.is_err() {
                                                error!("process assignments failed: {:?}", result.unwrap_err());
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
                        let entries = actor_table.drain();
                        info!("shutdown {:?} actors", entries.len());
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
        rpc_client: &Session,
        option: &PushConsumerOption,
        message_listener: Arc<MessageListener>,
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

    async fn receive_messages<T: RPCClient + 'static>(
        rpc_client: &mut T,
        message_queue: &MessageQueue,
        option: &PushConsumerOption,
    ) -> Result<Vec<MessageView>, ClientError> {
        let filter_expression = option
            .get_filter_expression(&message_queue.topic.name)
            .ok_or(ClientError::new(
                ErrorKind::Unknown,
                "no filter expression presents",
                OPERATION_RECEIVE_MESSAGE,
            ))?;
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
            if response.content.is_some() {
                let content = response.content.unwrap();
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
        option: PushConsumerOption,
        message_listener: Arc<MessageListener>,
        retry_policy: BackOffRetryPolicy,
    ) -> Self {
        Self {
            rpc_client,
            message_queue,
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
            let option = self.option.clone();
            let retry_policy = self.retry_policy.clone();
            let mut ack_processor = AckEntryProcessor::new(
                self.rpc_client.shadow_session(),
                self.option.get_consumer_group_resource(),
                self.message_queue.topic.to_owned(),
            );
            ack_processor.start().await?;
            let shutdown_token = shutdown_token.clone();
            task_tracker.spawn(async move {
                loop {
                    select! {
                        _ = ticker.tick() => {
                            let result = consumer_worker.receive_messages(&message_queue, &option, &mut ack_processor, &retry_policy).await;
                            if result.is_err() {
                                error!("receive messages error: {:?}", result.err());
                            }
                        }
                        _ = shutdown_token.cancelled() => {
                            ack_processor.shutdown().await;
                            break;
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
        option: &PushConsumerOption,
        ack_processor: &mut AckEntryProcessor,
        retry_policy: &BackOffRetryPolicy,
    ) -> Result<(), ClientError> {
        match self {
            ConsumerWorker::Standard(consumer_worker) => {
                consumer_worker
                    .receive_messages(message_queue, option, ack_processor, retry_policy)
                    .await
            }
            ConsumerWorker::Fifo(consumer_worker) => {
                consumer_worker
                    .receive_messages(message_queue, option, ack_processor, retry_policy)
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
        option: &PushConsumerOption,
        ack_processor: &mut AckEntryProcessor,
        retry_policy: &BackOffRetryPolicy,
    ) -> Result<(), ClientError> {
        let messages =
            PushConsumer::receive_messages(&mut self.rpc_client, message_queue, option).await?;
        for message in messages {
            let consume_result = (self.message_listener)(&message);
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
        option: &PushConsumerOption,
        ack_processor: &mut AckEntryProcessor,
        retry_policy: &BackOffRetryPolicy,
    ) -> Result<(), ClientError> {
        let messages =
            PushConsumer::receive_messages(&mut self.rpc_client, message_queue, option).await?;
        for message in messages {
            let mut delivery_attempt = message.delivery_attempt();
            let max_delivery_attempts = retry_policy.get_max_attempts();
            loop {
                let consume_result = (self.message_listener)(&message);
                match consume_result {
                    ConsumeResult::SUCCESS => {
                        ack_processor.ack_message(message).await;
                        break;
                    }
                    ConsumeResult::FAILURE => {
                        delivery_attempt += 1;
                        if delivery_attempt > max_delivery_attempts {
                            ack_processor
                                .forward_to_deadletter_queue(
                                    message,
                                    delivery_attempt,
                                    max_delivery_attempts,
                                )
                                .await;
                            break;
                        } else {
                            tokio::time::sleep(
                                retry_policy.get_next_attempt_delay(delivery_attempt),
                            )
                            .await;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

struct AckEntryProcessor {
    rpc_client: Session,
    consumer_group: Resource,
    topic: Resource,
    ack_entry_sender: Option<mpsc::Sender<AckEntryItem>>,
    shutdown_token: Option<CancellationToken>,
    task_tracker: Option<TaskTracker>,
}

impl AckEntryProcessor {
    fn new(rpc_client: Session, consumer_group: Resource, topic: Resource) -> Self {
        Self {
            rpc_client,
            consumer_group,
            topic,
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
        let request = ForwardMessageToDeadLetterQueueRequest {
            group: Some(self.consumer_group.clone()),
            topic: Some(self.topic.clone()),
            receipt_handle: message.receipt_handle(),
            message_id: message.message_id().to_string(),
            delivery_attempt,
            max_delivery_attempts,
        };
        let response = self.rpc_client.forward_to_deadletter_queue(request).await?;
        handle_response_status(response.status, OPERATION_FORWARD_TO_DEADLETTER_QUEUE)
    }

    async fn change_invisible_duration_inner(
        &mut self,
        ack_entry: &MessageView,
        invisible_duration: Duration,
    ) -> Result<(), ClientError> {
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
        let request = AckMessageRequest {
            group: Some(self.consumer_group.clone()),
            topic: Some(self.topic.clone()),
            entries: vec![pb::AckMessageEntry {
                message_id: ack_entry.message_id().to_string(),
                receipt_handle: ack_entry.receipt_handle().to_string(),
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
    use std::sync::atomic::{AtomicUsize, Ordering};
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
        context.expect().returning(|_, _, _, _, _| {
            let mut actor = MockMessageQueueActor::default();
            actor.expect_start().returning(|| Ok(()));
            return actor;
        });
        PushConsumer::process_assignments(
            &rpc_client,
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
            &session,
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
        let mut ack_processor = AckEntryProcessor::new(rpc_client, consumer_group, topic);
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
        let mut ack_processor = AckEntryProcessor::new(rpc_client, consumer_group, topic);
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
        let mut ack_processor = AckEntryProcessor::new(rpc_client, consumer_group, topic);
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
        let mut ack_processor = AckEntryProcessor::new(rpc_client, consumer_group, topic);
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
        let mut ack_processor = AckEntryProcessor::new(rpc_client, consumer_group, topic);
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
        let mut ack_processor = AckEntryProcessor::new(rpc_client, consumer_group, topic);
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
        let mut ack_processor = AckEntryProcessor::new(session, consumer_group, topic);
        let is_start_ok = ack_processor.start().await;
        assert!(is_start_ok.is_ok());
        let result = ConsumerWorker::Standard(consumer_worker)
            .receive_messages(
                &new_message_queue(),
                &option,
                &mut ack_processor,
                retry_policy,
            )
            .await;
        assert!(result.is_ok(), "{:?}", result);
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
        let mut ack_processor = AckEntryProcessor::new(session, consumer_group, topic);
        let is_start_ok = ack_processor.start().await;
        assert!(is_start_ok.is_ok());
        let result = ConsumerWorker::Standard(consumer_worker)
            .receive_messages(
                &new_message_queue(),
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
        let mut ack_processor = AckEntryProcessor::new(session, consumer_group, topic);
        let result = ConsumerWorker::Fifo(consumer_worker)
            .receive_messages(
                &new_message_queue(),
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
        let mut ack_processor = AckEntryProcessor::new(session, consumer_group, topic);
        let result = ConsumerWorker::Fifo(consumer_worker)
            .receive_messages(
                &new_message_queue(),
                &option,
                &mut ack_processor,
                retry_policy,
            )
            .await;
        assert!(result.is_ok(), "{:?}", result);
        assert_eq!(2, outer_call_counter.load(Ordering::Relaxed));
    }
}
