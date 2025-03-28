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

use std::fmt::Debug;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use mockall_double::double;
use parking_lot::RwLock;
use prost_types::Timestamp;
use slog::{error, info, warn, Logger};
use tokio::select;
use tokio::sync::{mpsc, oneshot};

#[double]
use crate::client::Client;
use crate::conf::{ClientOption, ProducerOption};
use crate::error::{ClientError, ErrorKind};
use crate::model::common::{ClientType, Endpoints, SendReceipt};
use crate::model::message::{self, MessageTypeAware, MessageView};
use crate::model::transaction::{
    Transaction, TransactionChecker, TransactionImpl, TransactionResolution,
};
use crate::pb::settings::PubSub;
use crate::pb::telemetry_command::Command::{RecoverOrphanedTransactionCommand, Settings};
use crate::pb::{Encoding, EndTransactionRequest, Resource, SystemProperties, TransactionSource};
use crate::session::RPCClient;
use crate::util::{
    build_endpoints_by_message_queue, build_producer_settings, handle_response_status,
    select_message_queue, select_message_queue_by_message_group, HOST_NAME,
};
use crate::{log, pb};

/// [`Producer`] is the core struct, to which application developers should turn, when publishing messages to RocketMQ proxy.
///
/// [`Producer`] is a thin wrapper of internal client struct that shoulders the actual workloads.
/// Most of its methods take shared reference so that application developers may use it at will.
///
/// [`Producer`] is `Send` and `Sync` by design, so that developers may get started easily.
pub struct Producer {
    option: Arc<RwLock<ProducerOption>>,
    logger: Logger,
    client: Client,
    transaction_checker: Option<Box<TransactionChecker>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl Debug for Producer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Producer")
            .field("option", &self.option)
            .field("client", &self.client)
            .finish()
    }
}

impl Producer {
    const OPERATION_SEND_MESSAGE: &'static str = "producer.send_message";
    const OPERATION_SEND_TRANSACTION_MESSAGE: &'static str = "producer.send_transaction_message";
    const OPERATION_END_TRANSACTION: &'static str = "producer.end_transaction";
    /// Create a new producer instance
    ///
    /// # Arguments
    ///
    /// * `option` - producer option
    /// * `client_option` - client option
    pub fn new(option: ProducerOption, client_option: ClientOption) -> Result<Self, ClientError> {
        let client_option = ClientOption {
            client_type: ClientType::Producer,
            namespace: option.namespace().to_string(),
            ..client_option
        };
        let logger = log::logger(option.logging_format());
        let client = Client::new(&logger, client_option, build_producer_settings(&option))?;
        Ok(Producer {
            option: Arc::new(RwLock::new(option)),
            logger,
            client,
            transaction_checker: None,
            shutdown_tx: None,
        })
    }

    /// Create a new transaction producer instance
    ///
    /// # Arguments
    ///
    /// * `option` - producer option
    /// * `client_option` - client option
    /// * `transaction_checker` - handle server query for uncommitted transaction status
    pub fn new_transaction_producer(
        option: ProducerOption,
        client_option: ClientOption,
        transaction_checker: Box<TransactionChecker>,
    ) -> Result<Self, ClientError> {
        let client_option = ClientOption {
            client_type: ClientType::Producer,
            namespace: option.namespace().to_string(),
            ..client_option
        };
        let logger = log::logger(option.logging_format());
        let client = Client::new(&logger, client_option, build_producer_settings(&option))?;
        Ok(Producer {
            option: Arc::new(RwLock::new(option)),
            logger,
            client,
            transaction_checker: Some(transaction_checker),
            shutdown_tx: None,
        })
    }

    fn get_resource_namespace(&self) -> String {
        self.option.read().namespace().to_string()
    }

    fn get_topics(&self) -> Option<Vec<String>> {
        let binding = self.option.read();
        let topics = binding.topics();
        if let Some(topics) = topics {
            return Some(topics.iter().map(|topic| topic.to_string()).collect());
        }
        None
    }

    /// Start the producer
    pub async fn start(&mut self) -> Result<(), ClientError> {
        let (telemetry_command_tx, mut telemetry_command_rx) = mpsc::channel(16);
        let telemetry_command_tx: mpsc::Sender<pb::telemetry_command::Command> =
            telemetry_command_tx;
        self.client.start(telemetry_command_tx).await?;
        let topics = self.get_topics();
        if let Some(topics) = topics {
            for topic in topics {
                self.client.topic_route(topic.as_str(), true).await?;
            }
        }
        let transaction_checker = self.transaction_checker.take();
        if transaction_checker.is_some() {
            self.transaction_checker = Some(Box::new(|_, _| TransactionResolution::UNKNOWN));
        }
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        self.shutdown_tx = Some(shutdown_tx);
        let rpc_client = self.client.get_session().await?;
        let endpoints = self.client.get_endpoints();
        let logger = self.logger.clone();
        let producer_option = Arc::clone(&self.option);
        tokio::spawn(async move {
            loop {
                select! {
                    command = telemetry_command_rx.recv() => {
                        if let Some(command) = command {
                            match command {
                                RecoverOrphanedTransactionCommand(command) => {
                                    let result = Self::handle_recover_orphaned_transaction_command(
                                            rpc_client.shadow_session(),
                                            command,
                                            &transaction_checker,
                                            endpoints.clone()).await;
                                    if let Err(error) = result {
                                        error!(logger, "handle trannsaction command failed: {:?}", error);
                                    };
                                }
                                Settings(command) => {
                                    let option = &mut producer_option.write();
                                    Self::handle_settings_command(command, option);
                                    info!(logger, "handle setting command success.");
                                }
                                _ => {
                                    warn!(logger, "unimplemented command {:?}", command);
                                }
                            }
                        }
                    }
                    _ = &mut shutdown_rx => {
                       break;
                    }
                }
            }
        });
        info!(
            self.logger,
            "start producer success, client_id: {}",
            self.client.client_id()
        );
        Ok(())
    }

    async fn handle_recover_orphaned_transaction_command<T: RPCClient + 'static>(
        mut rpc_client: T,
        command: pb::RecoverOrphanedTransactionCommand,
        transaction_checker: &Option<Box<TransactionChecker>>,
        endpoints: Endpoints,
    ) -> Result<(), ClientError> {
        let transaction_id = command.transaction_id;
        let message = command.message.clone().ok_or_else(|| {
            ClientError::new(
                ErrorKind::InvalidMessage,
                "no message in command",
                Self::OPERATION_END_TRANSACTION,
            )
        })?;
        let message_id = message
            .system_properties
            .as_ref()
            .map(|props| props.message_id.clone())
            .ok_or_else(|| {
                ClientError::new(
                    ErrorKind::InvalidMessage,
                    "no message id exists",
                    Self::OPERATION_END_TRANSACTION,
                )
            })?;
        let topic = message.topic.clone().ok_or_else(|| {
            ClientError::new(
                ErrorKind::InvalidMessage,
                "no topic exists in message",
                Self::OPERATION_END_TRANSACTION,
            )
        })?;
        if let Some(transaction_checker) = transaction_checker {
            let resolution = transaction_checker(
                transaction_id.clone(),
                MessageView::from_pb_message(message, endpoints).ok_or(ClientError::new(
                    ErrorKind::InvalidMessage,
                    "error parsing from pb",
                    Self::OPERATION_END_TRANSACTION,
                ))?,
            );
            let response = rpc_client
                .end_transaction(EndTransactionRequest {
                    topic: Some(topic),
                    message_id,
                    transaction_id,
                    resolution: resolution as i32,
                    source: TransactionSource::SourceServerCheck as i32,
                    trace_context: "".to_string(),
                })
                .await?;
            handle_response_status(response.status, Self::OPERATION_END_TRANSACTION)
        } else {
            Err(ClientError::new(
                ErrorKind::Config,
                "failed to get transaction checker",
                Self::OPERATION_END_TRANSACTION,
            ))
        }
    }

    fn handle_settings_command(settings: pb::Settings, option: &mut ProducerOption) {
        if let Some(PubSub::Publishing(publishing)) = settings.pub_sub {
            option.set_validate_message_type(publishing.validate_message_type);
        };
    }

    async fn transform_messages_to_protobuf(
        &self,
        messages: Vec<impl message::Message>,
    ) -> Result<(String, Option<String>, Vec<pb::Message>), ClientError> {
        if messages.is_empty() {
            return Err(ClientError::new(
                ErrorKind::InvalidMessage,
                "no message found",
                Self::OPERATION_SEND_MESSAGE,
            ));
        }

        let born_timestamp = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(duration) => Some(Timestamp {
                seconds: duration.as_secs() as i64,
                nanos: 0,
            }),
            Err(_) => None,
        };

        let mut pb_messages = Vec::with_capacity(messages.len());
        let mut last_topic: Option<String> = None;
        let mut last_message_group: Option<Option<String>> = None;

        for mut message in messages {
            if let Some(last_topic) = last_topic.clone() {
                if last_topic.ne(&message.take_topic()) {
                    return Err(ClientError::new(
                        ErrorKind::InvalidMessage,
                        "Not all messages have the same topic.",
                        Self::OPERATION_SEND_MESSAGE,
                    ));
                }
            } else {
                last_topic = Some(message.take_topic());
            }

            let mut message_group = message.take_message_group();
            if let Some(last_message_group) = last_message_group.clone() {
                if last_message_group.ne(&message_group) {
                    return Err(ClientError::new(
                        ErrorKind::InvalidMessage,
                        "not all messages have the same message group",
                        Self::OPERATION_SEND_MESSAGE,
                    ));
                }
            } else {
                last_message_group = Some(message_group.clone());
            }

            let mut delivery_timestamp = message
                .take_delivery_timestamp()
                .map(|seconds| Timestamp { seconds, nanos: 0 });

            if message.transaction_enabled() {
                message_group = None;
                delivery_timestamp = None;
            } else if delivery_timestamp.is_some() {
                message_group = None;
            } else if message_group.is_some() {
                delivery_timestamp = None;
            };

            // TODO: use a converter trait From or TryFrom
            let pb_message = pb::Message {
                topic: Some(Resource {
                    name: message.take_topic(),
                    resource_namespace: self.get_resource_namespace(),
                }),
                user_properties: message.take_properties(),
                system_properties: Some(SystemProperties {
                    tag: message.take_tag(),
                    keys: message.take_keys(),
                    message_id: message.take_message_id(),
                    message_group,
                    delivery_timestamp,
                    message_type: message.get_message_type() as i32,
                    born_host: HOST_NAME.clone(),
                    born_timestamp: born_timestamp.clone(),
                    body_digest: None,
                    body_encoding: Encoding::Identity as i32,
                    ..SystemProperties::default()
                }),
                body: message.take_body(),
            };
            pb_messages.push(pb_message);
        }

        let topic = last_topic.unwrap();
        if topic.is_empty() {
            return Err(ClientError::new(
                ErrorKind::InvalidMessage,
                "message topic is empty",
                Self::OPERATION_SEND_MESSAGE,
            ));
        }

        Ok((topic, last_message_group.unwrap(), pb_messages))
    }

    /// Send a single message
    ///
    /// # Arguments
    ///
    /// * `message` - the message to send
    pub async fn send(&self, message: impl message::Message) -> Result<SendReceipt, ClientError> {
        let results = self.batch_send(vec![message]).await?;
        Ok(results[0].clone())
    }

    /// Send a batch of messages
    ///
    /// # Arguments
    ///
    /// * `messages` - A vector that holds the messages to send
    pub async fn batch_send(
        &self,
        messages: Vec<impl message::Message>,
    ) -> Result<Vec<SendReceipt>, ClientError> {
        let message_types = messages
            .iter()
            .map(|message| message.get_message_type())
            .collect::<Vec<_>>();

        let (topic, message_group, mut pb_messages) =
            self.transform_messages_to_protobuf(messages).await?;

        let route = self.client.topic_route(&topic, true).await?;

        let message_queue = if let Some(message_group) = message_group {
            select_message_queue_by_message_group(route, message_group)
        } else {
            select_message_queue(route)
        };

        let validate_message_type = self.validate_message_type().await;
        if validate_message_type {
            for message_type in message_types {
                if !message_queue.accept_type(message_type) {
                    return Err(ClientError::new(
                        ErrorKind::MessageTypeNotMatch,
                        format!(
                            "Current message type {:?} not match with accepted types {:?}.",
                            message_type, message_queue.accept_message_types
                        )
                        .as_str(),
                        Self::OPERATION_SEND_MESSAGE,
                    ));
                }
            }
        }

        let endpoints =
            build_endpoints_by_message_queue(&message_queue, Self::OPERATION_SEND_MESSAGE)?;
        for message in pb_messages.iter_mut() {
            message.system_properties.as_mut().unwrap().queue_id = message_queue.id;
        }

        self.client.send_message(&endpoints, pb_messages).await
    }

    async fn validate_message_type(&self) -> bool {
        self.option.read().validate_message_type()
    }

    pub fn has_transaction_checker(&self) -> bool {
        self.transaction_checker.is_some()
    }

    /// Send message in a transaction
    pub async fn send_transaction_message(
        &self,
        mut message: impl message::Message,
    ) -> Result<impl Transaction, ClientError> {
        if !self.has_transaction_checker() {
            return Err(ClientError::new(
                ErrorKind::InvalidMessage,
                "this producer can not send transaction message, please create a transaction producer using producer::new_transaction_producer",
                Self::OPERATION_SEND_TRANSACTION_MESSAGE,
            ));
        }
        let topic = message.take_topic();
        let receipt = self.send(message).await?;
        let rpc_client = self.client.get_session().await?;
        Ok(TransactionImpl::new(
            Box::new(rpc_client),
            Resource {
                resource_namespace: self.get_resource_namespace(),
                name: topic,
            },
            receipt,
        ))
    }

    pub async fn shutdown(mut self) -> Result<(), ClientError> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        self.client.shutdown().await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::error::ErrorKind;
    use crate::log::terminal_logger;
    use crate::model::common::Route;
    use crate::model::message::{MessageBuilder, MessageImpl, MessageType};
    use crate::model::transaction::TransactionResolution;
    use crate::pb::{Broker, Code, EndTransactionResponse, MessageQueue, Status};
    use crate::session::MockRPCClient;
    #[double]
    use crate::session::Session;

    use super::*;

    fn new_producer_for_test() -> Producer {
        Producer {
            option: Default::default(),
            logger: terminal_logger(),
            client: Client::default(),
            shutdown_tx: None,
            transaction_checker: None,
        }
    }

    fn new_transaction_producer_for_test() -> Producer {
        Producer {
            option: Default::default(),
            logger: terminal_logger(),
            client: Client::default(),
            shutdown_tx: None,
            transaction_checker: Some(Box::new(|_, _| TransactionResolution::COMMIT)),
        }
    }

    #[tokio::test]
    async fn producer_start() -> Result<(), ClientError> {
        let _m = crate::client::tests::MTX.lock();

        let ctx = Client::new_context();
        ctx.expect().return_once(|_, _, _| {
            let mut client = Client::default();
            client.expect_topic_route().returning(|_, _| {
                Ok(Arc::new(Route {
                    index: Default::default(),
                    queue: vec![],
                }))
            });
            client.expect_start().returning(|_| Ok(()));
            client
                .expect_client_id()
                .return_const("fake_id".to_string());
            client
                .expect_get_session()
                .return_once(|| Ok(Session::default()));
            client
                .expect_get_endpoints()
                .return_once(|| Endpoints::from_url("foobar.com:8080").unwrap());
            Ok(client)
        });
        let mut producer_option = ProducerOption::default();
        producer_option.set_topics(vec!["DefaultCluster".to_string()]);
        Producer::new(producer_option, ClientOption::default())?
            .start()
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn transaction_producer_start() -> Result<(), ClientError> {
        let _m = crate::client::tests::MTX.lock();

        let ctx = Client::new_context();
        ctx.expect().return_once(|_, _, _| {
            let mut client = Client::default();
            client.expect_topic_route().returning(|_, _| {
                Ok(Arc::new(Route {
                    index: Default::default(),
                    queue: vec![],
                }))
            });
            client.expect_start().returning(|_| Ok(()));
            client
                .expect_client_id()
                .return_const("fake_id".to_string());
            client
                .expect_get_session()
                .return_once(|| Ok(Session::default()));
            client
                .expect_get_endpoints()
                .return_once(|| Endpoints::from_url("foobar.com:8080").unwrap());
            Ok(client)
        });
        let mut producer_option = ProducerOption::default();
        producer_option.set_topics(vec!["DefaultCluster".to_string()]);
        Producer::new_transaction_producer(
            producer_option,
            ClientOption::default(),
            Box::new(|_, _| TransactionResolution::COMMIT),
        )?
        .start()
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn producer_transform_messages_to_protobuf() {
        let producer = new_producer_for_test();
        let messages = vec![MessageBuilder::builder()
            .set_topic("DefaultCluster")
            .set_body("hello world".as_bytes().to_vec())
            .set_tag("tag")
            .set_keys(vec!["key"])
            .set_properties(vec![("key", "value")].into_iter().collect())
            .set_message_group("message_group".to_string())
            .build()
            .unwrap()];
        let result = producer.transform_messages_to_protobuf(messages).await;
        assert!(result.is_ok());

        let (topic, message_group, pb_messages) = result.unwrap();
        assert_eq!(topic, "DefaultCluster");
        assert_eq!(message_group, Some("message_group".to_string()));

        // check message
        assert_eq!(pb_messages.len(), 1);
        let message = pb_messages.get(0).unwrap().clone();
        assert_eq!(message.topic.unwrap().name, "DefaultCluster");
        let system_properties = message.system_properties.unwrap();
        assert_eq!(system_properties.tag.unwrap(), "tag");
        assert_eq!(system_properties.keys, vec!["key".to_string()]);
        assert_eq!(message.user_properties.get("key").unwrap(), "value");
        assert_eq!(std::str::from_utf8(&message.body).unwrap(), "hello world");
        assert!(!system_properties.message_id.is_empty());
    }

    #[tokio::test]
    async fn producer_transform_messages_to_protobuf_failed() {
        let producer = new_producer_for_test();

        let messages: Vec<MessageImpl> = vec![];
        let result = producer.transform_messages_to_protobuf(messages).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidMessage);
        assert_eq!(err.message, "no message found");

        let messages = vec![MessageImpl {
            message_id: "".to_string(),
            topic: "".to_string(),
            body: None,
            tag: None,
            keys: None,
            properties: None,
            message_group: None,
            delivery_timestamp: None,
            transaction_enabled: false,
            message_type: MessageType::TRANSACTION,
        }];
        let result = producer.transform_messages_to_protobuf(messages).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidMessage);
        assert_eq!(err.message, "message topic is empty");

        let messages = vec![
            MessageBuilder::builder()
                .set_topic("DefaultCluster")
                .set_body("hello world".as_bytes().to_vec())
                .build()
                .unwrap(),
            MessageBuilder::builder()
                .set_topic("DefaultCluster_dup")
                .set_body("hello world".as_bytes().to_vec())
                .build()
                .unwrap(),
        ];
        let result = producer.transform_messages_to_protobuf(messages).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidMessage);
        assert_eq!(err.message, "Not all messages have the same topic.");

        let messages = vec![
            MessageBuilder::builder()
                .set_topic("DefaultCluster")
                .set_body("hello world".as_bytes().to_vec())
                .set_message_group("message_group")
                .build()
                .unwrap(),
            MessageBuilder::builder()
                .set_topic("DefaultCluster")
                .set_body("hello world".as_bytes().to_vec())
                .set_message_group("message_group_dup")
                .build()
                .unwrap(),
        ];
        let result = producer.transform_messages_to_protobuf(messages).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidMessage);
        assert_eq!(err.message, "not all messages have the same message group");
    }

    #[tokio::test]
    async fn producer_send() -> Result<(), ClientError> {
        let mut producer = new_producer_for_test();
        producer.client.expect_topic_route().returning(|_, _| {
            Ok(Arc::new(Route {
                index: Default::default(),
                queue: vec![MessageQueue {
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
                            addresses: vec![],
                        }),
                    }),
                    accept_message_types: vec![MessageType::NORMAL as i32],
                }],
            }))
        });
        producer.client.expect_send_message().returning(|_, _| {
            Ok(vec![SendReceipt::from_pb_send_result(
                &pb::SendResultEntry {
                    message_id: "message_id".to_string(),
                    transaction_id: "transaction_id".to_string(),
                    ..pb::SendResultEntry::default()
                },
            )])
        });
        let result = producer
            .send(
                MessageBuilder::builder()
                    .set_topic("test_topic")
                    .set_body(vec![])
                    .build()
                    .unwrap(),
            )
            .await?;
        assert_eq!(result.message_id(), "message_id");
        assert_eq!(result.transaction_id(), "transaction_id");
        Ok(())
    }

    #[tokio::test]
    async fn producer_send_transaction_message() -> Result<(), ClientError> {
        let mut producer = new_transaction_producer_for_test();
        producer.client.expect_topic_route().returning(|_, _| {
            Ok(Arc::new(Route {
                index: Default::default(),
                queue: vec![MessageQueue {
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
                            addresses: vec![],
                        }),
                    }),
                    accept_message_types: vec![MessageType::TRANSACTION as i32],
                }],
            }))
        });
        producer.client.expect_send_message().returning(|_, _| {
            Ok(vec![SendReceipt::from_pb_send_result(
                &pb::SendResultEntry {
                    message_id: "message_id".to_string(),
                    transaction_id: "transaction_id".to_string(),
                    ..pb::SendResultEntry::default()
                },
            )])
        });
        producer
            .client
            .expect_get_session()
            .return_once(|| Ok(Session::default()));

        let _ = producer
            .send_transaction_message(
                MessageBuilder::transaction_message_builder("test_topic", vec![])
                    .build()
                    .unwrap(),
            )
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn client_handle_recover_orphaned_transaction_command() {
        let response = Ok(EndTransactionResponse {
            status: Some(Status {
                code: Code::Ok as i32,
                message: "".to_string(),
            }),
        });
        let mut mock = MockRPCClient::new();
        mock.expect_end_transaction()
            .return_once(|_| Box::pin(futures::future::ready(response)));

        let result = Producer::handle_recover_orphaned_transaction_command(
            mock,
            pb::RecoverOrphanedTransactionCommand {
                message: Some(pb::Message {
                    topic: Some(Resource::default()),
                    user_properties: Default::default(),
                    system_properties: Some(SystemProperties::default()),
                    body: vec![],
                }),
                transaction_id: "".to_string(),
            },
            &Some(Box::new(|_, _| TransactionResolution::COMMIT)),
            Endpoints::from_url("localhost:8081").unwrap(),
        )
        .await;
        assert!(result.is_ok())
    }
}
