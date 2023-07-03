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

use std::time::{SystemTime, UNIX_EPOCH};

use mockall_double::double;
use prost_types::Timestamp;
use slog::{info, Logger};

#[double]
use crate::client::Client;
use crate::conf::{ClientOption, ProducerOption};
use crate::error::{ClientError, ErrorKind};
use crate::model::common::{ClientType, SendReceipt};
use crate::model::message;
use crate::model::transaction::{Transaction, TransactionChecker, TransactionImpl};
use crate::pb::{Encoding, MessageType, Resource, SystemProperties};
use crate::util::{
    build_endpoints_by_message_queue, build_producer_settings, select_message_queue,
    select_message_queue_by_message_group, HOST_NAME,
};
use crate::{log, pb};

/// [`Producer`] is the core struct, to which application developers should turn, when publishing messages to RocketMQ proxy.
///
/// [`Producer`] is a thin wrapper of internal client struct that shoulders the actual workloads.
/// Most of its methods take shared reference so that application developers may use it at will.
///
/// [`Producer`] is `Send` and `Sync` by design, so that developers may get started easily.
#[derive(Debug)]
pub struct Producer {
    option: ProducerOption,
    logger: Logger,
    client: Client,
}

impl Producer {
    const OPERATION_SEND_MESSAGE: &'static str = "producer.send_message";
    const OPERATION_SEND_TRANSACTION_MESSAGE: &'static str = "producer.send_transaction_message";

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
        let settings = build_producer_settings(&option, &client_option);
        let client = Client::new(&logger, client_option, settings)?;
        Ok(Producer {
            option,
            logger,
            client,
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
        let settings = build_producer_settings(&option, &client_option);
        let mut client = Client::new(&logger, client_option, settings)?;
        client.set_transaction_checker(transaction_checker);
        Ok(Producer {
            option,
            logger,
            client,
        })
    }

    /// Start the producer
    pub async fn start(&mut self) -> Result<(), ClientError> {
        self.client.start().await;
        if let Some(topics) = self.option.topics() {
            for topic in topics {
                self.client.topic_route(topic, true).await?;
            }
        }
        info!(
            self.logger,
            "start producer success, client_id: {}",
            self.client.client_id()
        );
        Ok(())
    }

    fn transform_messages_to_protobuf(
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

            let message_type = if message.transaction_enabled() {
                message_group = None;
                delivery_timestamp = None;
                MessageType::Transaction as i32
            } else if delivery_timestamp.is_some() {
                message_group = None;
                MessageType::Delay as i32
            } else if message_group.is_some() {
                delivery_timestamp = None;
                MessageType::Fifo as i32
            } else {
                MessageType::Normal as i32
            };

            let pb_message = pb::Message {
                topic: Some(Resource {
                    name: message.take_topic(),
                    resource_namespace: self.option.namespace().to_string(),
                }),
                user_properties: message.take_properties(),
                system_properties: Some(SystemProperties {
                    tag: message.take_tag(),
                    keys: message.take_keys(),
                    message_id: message.take_message_id(),
                    message_group,
                    delivery_timestamp,
                    message_type,
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
        let (topic, message_group, mut pb_messages) =
            self.transform_messages_to_protobuf(messages)?;

        let route = self.client.topic_route(&topic, true).await?;

        let message_queue = if let Some(message_group) = message_group {
            select_message_queue_by_message_group(route, message_group)
        } else {
            select_message_queue(route)
        };

        let endpoints =
            build_endpoints_by_message_queue(&message_queue, Self::OPERATION_SEND_MESSAGE)?;
        for message in pb_messages.iter_mut() {
            message.system_properties.as_mut().unwrap().queue_id = message_queue.id;
        }

        self.client.send_message(&endpoints, pb_messages).await
    }

    /// Send message in a transaction
    pub async fn send_transaction_message(
        &self,
        mut message: impl message::Message,
    ) -> Result<impl Transaction, ClientError> {
        if !self.client.has_transaction_checker() {
            return Err(ClientError::new(
                ErrorKind::InvalidMessage,
                "this producer can not send transaction message, please create a transaction producer using producer::new_transaction_producer",
                Self::OPERATION_SEND_TRANSACTION_MESSAGE,
            ));
        }
        let topic = message.take_topic();
        let receipt = self.send(message).await?;
        Ok(TransactionImpl::new(
            Box::new(self.client.get_session().await.unwrap()),
            Resource {
                resource_namespace: self.option.namespace().to_string(),
                name: topic,
            },
            receipt,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::error::ErrorKind;
    use crate::log::terminal_logger;
    use crate::model::common::Route;
    use crate::model::message::{MessageBuilder, MessageImpl};
    use crate::model::transaction::TransactionResolution;
    use crate::pb::{Broker, MessageQueue};
    use crate::session::Session;

    use super::*;

    fn new_producer_for_test() -> Producer {
        Producer {
            option: Default::default(),
            logger: terminal_logger(),
            client: Client::default(),
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
            client.expect_start().returning(|| ());
            client
                .expect_client_id()
                .return_const("fake_id".to_string());
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
            client.expect_start().returning(|| ());
            client.expect_set_transaction_checker().returning(|_| ());
            client
                .expect_client_id()
                .return_const("fake_id".to_string());
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
        let result = producer.transform_messages_to_protobuf(messages);
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
        let result = producer.transform_messages_to_protobuf(messages);
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
        }];
        let result = producer.transform_messages_to_protobuf(messages);
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
        let result = producer.transform_messages_to_protobuf(messages);
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
        let result = producer.transform_messages_to_protobuf(messages);
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
                    accept_message_types: vec![],
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
                    accept_message_types: vec![],
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
            .return_once(|| Ok(Session::mock()));

        let _ = producer
            .send_transaction_message(
                MessageBuilder::builder()
                    .set_topic("test_topic")
                    .set_body(vec![])
                    .build()
                    .unwrap(),
            )
            .await?;
        Ok(())
    }
}
