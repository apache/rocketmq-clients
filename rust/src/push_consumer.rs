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
    AckMessageRequest, Assignment, ChangeInvisibleDurationRequest, Code, QueryAssignmentRequest,
    ReceiveMessageRequest, Resource,
};
use crate::session::{RPCClient, Session};
use crate::util::build_endpoints_by_message_queue;
use crate::{log, pb};
use parking_lot::{Mutex, RwLock};
use prost_types::Duration;
use slog::Logger;
use slog::{debug, error, info, warn};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::select;
use tokio::sync::{mpsc, oneshot};

const OPERATION_NEW_PUSH_CONSUMER: &str = "push_consumer.new";
const OPERATION_RECEIVE_MESSAGE: &str = "push_consumer.receive_message";
const OPERATION_ACK_MESSAGE: &str = "push_consumer.ack_message";
const OPERATION_START_PUSH_CONSUMER: &str = "push_consumer.start";
const OPERATION_CHANGE_INVISIBLE_DURATION: &str = "push_consumer.change_invisible_duration";

pub type MessageListener = dyn Fn(&MessageView) -> ConsumeResult + Send + Sync;

pub struct PushConsumer {
    logger: Logger,
    client: Client<PushConsumerOption>,
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
    max_cache_message_count: usize,
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
        let option = Arc::new(RwLock::new(option));
        let client =
            Client::<PushConsumerOption>::new(&logger, client_option, Arc::clone(&option))?;
        Ok(Self {
            logger,
            client,
            message_listener: Arc::new(message_listener),
            option,
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
                            ExponentialBackOffRetryPolicy::new(strategy, retry_policy.max_attempts),
                        ))
                    }
                    pb::retry_policy::Strategy::CustomizedBackoff(strategy) => {
                        Some(BackOffRetryPolicy::Customized(
                            CustomizedBackOffRetryPolicy::new(strategy, retry_policy.max_attempts),
                        ))
                    }
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
        let rpc_client = self.client.get_session().await?;
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
            .sync_topic_routes(rpc_client.shadow_session(), topics)
            .await?;
        let logger = self.logger.clone();
        let mut actor_table: HashMap<MessageQueue, MessageQueueActor> = HashMap::new();

        let message_listener = Arc::clone(&self.message_listener);
        // must retrieve settings from server first.
        let command = telemetry_command_rx.recv().await.ok_or(ClientError::new(
            ErrorKind::Connect,
            "telemtry command channel closed.",
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
                                    if let Some(status) = response.status {
                                        if status.code == Code::Ok as i32 {
                                            Self::process_assignments(logger.clone(),
                                                rpc_client.shadow_session(),
                                                &consumer_option,
                                                Arc::clone(&message_listener),
                                                &mut actor_table,
                                                response.assignments,
                                                retry_policy_inner,
                                            ).await;
                                        } else {
                                            error!(logger, "query assignment failed: error status {:?}", status);
                                        }
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
    ) {
        let message_queues: Vec<MessageQueue> = assignments
            .iter_mut()
            .filter(|assignment| assignment.message_queue.is_some())
            .flat_map(|assignment| {
                MessageQueue::from_pb_message_queue(assignment.message_queue.take().unwrap())
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
        let mut max_cache_messages_per_queue =
            option.max_cache_message_count() as usize / message_queues.len();
        if max_cache_messages_per_queue < 1 {
            max_cache_messages_per_queue = 1;
        }
        for mut actor in actors {
            actor.set_max_cache_message_count(max_cache_messages_per_queue);
            actor.set_option(option.clone());
            actor.set_retry_policy(retry_policy.clone());
            actor_table.insert(actor.message_queue.clone(), actor);
        }

        // start new actors
        for message_queue in message_queues {
            if actor_table.contains_key(&message_queue) {
                continue;
            }
            let mut actor = MessageQueueActor::new(
                logger.clone(),
                rpc_client.shadow_session(),
                message_queue.clone(),
                option.clone(),
                Arc::clone(&message_listener),
                max_cache_messages_per_queue,
                retry_policy.clone(),
            );
            let result = actor.start().await;
            if result.is_ok() {
                actor_table.insert(message_queue, actor);
            }
        }
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

impl MessageQueueActor {
    pub(crate) fn new(
        logger: Logger,
        rpc_client: Session,
        message_queue: MessageQueue,
        option: PushConsumerOption,
        message_listener: Arc<Box<MessageListener>>,
        max_cache_message_count: usize,
        retry_policy: BackOffRetryPolicy,
    ) -> Self {
        Self {
            logger,
            rpc_client,
            message_queue,
            shutdown_tx: None,
            option,
            message_listener: Arc::clone(&message_listener),
            max_cache_message_count,
            retry_policy,
        }
    }

    // The following two methods won't affect the running actor in fact.
    pub(crate) fn set_max_cache_message_count(&mut self, max_cache_message_count: usize) {
        self.max_cache_message_count = max_cache_message_count;
    }

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
            max_cache_message_count: self.max_cache_message_count,
            retry_policy: self.retry_policy.clone(),
        }
    }

    pub(crate) fn get_consumer_group(&self) -> Resource {
        Resource {
            name: self.option.consumer_group().to_string(),
            resource_namespace: self.option.namespace().to_string(),
        }
    }

    pub(crate) fn get_filter_expression(&self) -> Option<&FilterExpression> {
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
                        let poll_result = actor.poll_messages().await;
                        if let Ok(messages) = poll_result {
                            for message in messages {
                                let consume_result = (actor.message_listener)(&message);
                                match consume_result {
                                    ConsumeResult::SUCCESS => {
                                        let result = actor.ack_message(&message).await;
                                        if result.is_err() {
                                            info!(logger, "Ack message failed, put it back to queue, result: {:?}", result.unwrap_err());
                                            // put it back to queue
                                            message_handler_queue.push_back(MessageHandler {
                                                message,
                                                status: ConsumeResult::SUCCESS,
                                                attempt: 0,
                                            });
                                        }
                                    }
                                    ConsumeResult::FAILURE => {
                                        let result = actor.change_invisible_duration(&message).await;
                                        if result.is_err() {
                                            info!(logger, "Change invisible duration failed, put it back to queue, result: {:?}", result.unwrap_err());
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
                        if message_handler_queue.len() < actor.max_cache_message_count {
                            let _ = poll_tx.try_send(());
                        } else {
                            error!(logger, "message queue has reached its limit {:?}, stop for a while.", actor.max_cache_message_count);
                        }
                    }
                    _ = ack_rx.recv() => {
                        if let Some(mut handler) = message_handler_queue.pop_front() {
                            match handler.status {
                                ConsumeResult::SUCCESS => {
                                    let result = actor.ack_message(&handler.message).await;
                                    if result.is_err() {
                                        handler.attempt += 1;
                                        warn!(logger, "{:?} attempt to ack message failed, result: {:?}", handler.attempt, result.unwrap_err());
                                        message_handler_queue.push_front(handler);
                                    } else if !message_handler_queue.is_empty() {
                                        let _ = ack_tx.try_send(());
                                    }
                                }
                                ConsumeResult::FAILURE => {
                                    let result = actor.change_invisible_duration(&handler.message).await;
                                    if result.is_err() {
                                        handler.attempt += 1;
                                        warn!(logger, "{:?} attempt to change invisible duration failed, result: {:?}", handler.attempt, result.unwrap_err());
                                        message_handler_queue.push_front(handler);
                                    } else if !message_handler_queue.is_empty() {
                                        let _ = ack_tx.try_send(());
                                    }
                                }
                            }
                        }
                    }
                    _ = queue_check_ticker.tick() => {
                        if !message_handler_queue.is_empty() {
                            let _ = ack_tx.try_send(());
                        }
                        if message_handler_queue.len() < actor.max_cache_message_count {
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

    async fn poll_messages(&mut self) -> Result<Vec<MessageView>, ClientError> {
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
        let responses = self.rpc_client.receive_message(request).await?;
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
                        messages.push(MessageView::from_pb_message(message, endpoints.clone()));
                    }
                }
            }
        }
        Ok(messages)
    }

    async fn ack_message(&mut self, ack_entry: &impl AckMessageEntry) -> Result<(), ClientError> {
        let request = AckMessageRequest {
            group: Some(self.get_consumer_group()),
            topic: Some(self.message_queue.topic.clone()),
            entries: vec![pb::AckMessageEntry {
                message_id: ack_entry.message_id().to_string(),
                receipt_handle: ack_entry.receipt_handle().to_string(),
            }],
        };
        let response = self.rpc_client.ack_message(request).await?;
        let status = response.status.ok_or(ClientError::new(
            ErrorKind::Server,
            "no status in response.",
            OPERATION_ACK_MESSAGE,
        ))?;
        if status.code != Code::Ok as i32 {
            return Err(ClientError::new(
                ErrorKind::Server,
                "server return an error",
                OPERATION_ACK_MESSAGE,
            )
            .with_context("code", format!("{}", status.code))
            .with_context("message", status.message));
        }

        Ok(())
    }

    async fn change_invisible_duration(
        &mut self,
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
        let response = self.rpc_client.change_invisible_duration(request).await?;
        let status = response.status.ok_or(ClientError::new(
            ErrorKind::Server,
            "no status in response.",
            OPERATION_CHANGE_INVISIBLE_DURATION,
        ))?;
        if status.code != Code::Ok as i32 {
            return Err(ClientError::new(
                ErrorKind::Server,
                "server return an error",
                OPERATION_CHANGE_INVISIBLE_DURATION,
            )
            .with_context("code", format!("{}", status.code))
            .with_context("message", status.message));
        }
        Ok(())
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
mod tests {}
