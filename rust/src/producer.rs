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

//! Publish messages of various types to brokers.
//!
//! `Producer` is a thin wrapper of internal `Client` struct that shoulders the actual workloads.
//! Most of its methods take shared reference so that application developers may use it at will.

use std::time::{SystemTime, UNIX_EPOCH};

use mockall_double::double;
use prost_types::Timestamp;
use slog::{info, Logger};

#[double]
use crate::client::Client;
use crate::conf::{ClientOption, ProducerOption};
use crate::error::{ClientError, ErrorKind};
use crate::model::common::ClientType;
use crate::model::message;
use crate::pb::{Encoding, Resource, SendResultEntry, SystemProperties};
use crate::util::{
    build_endpoints_by_message_queue, build_producer_settings, select_message_queue,
    select_message_queue_by_message_group, HOST_NAME,
};
use crate::{log, pb};

/// `Producer` is the core struct, to which application developers should turn, when publishing messages to brokers.
///
/// `Producer` is `Send` and `Sync` by design, so that developers may get started easily.
#[derive(Debug)]
pub struct Producer {
    option: ProducerOption,
    logger: Logger,
    client: Client,
}

impl Producer {
    const OPERATION_SEND_MESSAGE: &'static str = "producer.send_message";

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

    pub async fn start(&self) -> Result<(), ClientError> {
        if let Some(topics) = self.option.topics() {
            for topic in topics {
                self.client.topic_route(topic, true).await?;
            }
        }
        self.client.start();
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

            let message_group = message.take_message_group();
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

            let delivery_timestamp = message
                .take_delivery_timestamp()
                .map(|seconds| Timestamp { seconds, nanos: 0 });

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

    pub async fn send_one(
        &self,
        message: impl message::Message,
    ) -> Result<SendResultEntry, ClientError> {
        let results = self.send(vec![message]).await?;
        Ok(results[0].clone())
    }

    pub async fn send(
        &self,
        messages: Vec<impl message::Message>,
    ) -> Result<Vec<SendResultEntry>, ClientError> {
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
}

#[cfg(test)]
mod tests {
    use crate::error::ErrorKind;
    use crate::log::terminal_logger;
    use crate::model::message::MessageImpl;

    use super::*;

    fn new_producer_for_test() -> Producer {
        Producer {
            option: Default::default(),
            logger: terminal_logger(),
            client: Client::default(),
        }
    }

    // #[tokio::test]
    // async fn producer_start() {
    //     let mut producer_option = ProducerOption::default();
    //     producer_option.set_topics(vec!["DefaultCluster".to_string()]);
    //     let producer = Producer::new(producer_option, ClientOption::default())
    //         .await
    //         .unwrap();
    //     producer.start().await.unwrap();
    // }

    #[tokio::test]
    async fn producer_transform_messages_to_protobuf() {
        let producer = new_producer_for_test();
        let messages = vec![MessageImpl::builder()
            .set_topic("DefaultCluster")
            .set_body("hello world".as_bytes().to_vec())
            .set_tags("tag")
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
            tags: None,
            keys: None,
            properties: None,
            message_group: None,
            delivery_timestamp: None,
        }];
        let result = producer.transform_messages_to_protobuf(messages);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidMessage);
        assert_eq!(err.message, "message topic is empty");

        let messages = vec![
            MessageImpl::builder()
                .set_topic("DefaultCluster")
                .set_body("hello world".as_bytes().to_vec())
                .build()
                .unwrap(),
            MessageImpl::builder()
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
            MessageImpl::builder()
                .set_topic("DefaultCluster")
                .set_body("hello world".as_bytes().to_vec())
                .set_message_group("message_group")
                .build()
                .unwrap(),
            MessageImpl::builder()
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
}
