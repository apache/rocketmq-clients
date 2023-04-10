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

use std::hash::Hasher;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use prost_types::Timestamp;
use siphasher::sip::SipHasher24;
use slog::{info, Logger};

use crate::client::Client;
use crate::conf::{ClientOption, ProducerOption};
use crate::error::{ClientError, ErrorKind};
use crate::model::common::{Endpoints, Route};
use crate::model::message;
use crate::pb::{Encoding, MessageQueue, Resource, SendResultEntry, SystemProperties};
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

lazy_static::lazy_static! {
    static ref HOST_NAME: String = match hostname::get() {
        Ok(name) => name.to_str().unwrap_or("localhost").to_string(),
        Err(_) => "localhost".to_string(),
    };
}

impl Producer {
    const OPERATION_SEND_MESSAGE: &'static str = "producer.send_message";

    pub async fn new(
        option: ProducerOption,
        client_option: ClientOption,
    ) -> Result<Self, ClientError> {
        let logger = log::logger(&option);
        let client = Client::new(&logger, client_option)?;
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
                "No message found.",
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
                if last_topic.ne(&message.get_topic()) {
                    return Err(ClientError::new(
                        ErrorKind::InvalidMessage,
                        "Not all messages have the same topic.",
                        Self::OPERATION_SEND_MESSAGE,
                    ));
                }
            } else {
                last_topic = Some(message.get_topic());
            }

            let message_group = message.get_message_group();
            if let Some(last_message_group) = last_message_group.clone() {
                if last_message_group.ne(&message_group) {
                    return Err(ClientError::new(
                        ErrorKind::InvalidMessage,
                        "Not all messages have the same message group.",
                        Self::OPERATION_SEND_MESSAGE,
                    ));
                }
            } else {
                last_message_group = Some(message_group.clone());
            }

            let delivery_timestamp = message
                .get_delivery_timestamp()
                .map(|seconds| Timestamp { seconds, nanos: 0 });

            let pb_message = pb::Message {
                topic: Some(Resource {
                    name: message.get_topic(),
                    resource_namespace: self.option.name_space().to_string(),
                }),
                user_properties: message.get_properties(),
                system_properties: Some(SystemProperties {
                    tag: message.get_tag(),
                    keys: message.get_keys(),
                    message_id: message.get_message_id(),
                    message_group,
                    delivery_timestamp,
                    born_host: HOST_NAME.clone(),
                    born_timestamp: born_timestamp.clone(),
                    body_digest: None,
                    body_encoding: Encoding::Identity as i32,
                    ..SystemProperties::default()
                }),
                body: message.get_body(),
            };
            pb_messages.push(pb_message);
        }

        let topic = last_topic.unwrap();
        if topic.is_empty() {
            return Err(ClientError::new(
                ErrorKind::InvalidMessage,
                "Message topic is empty.",
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
        let (topic, message_group, pb_messages) = self.transform_messages_to_protobuf(messages)?;

        let route = self.client.topic_route(&topic, true).await?;

        let message_queue = if let Some(message_group) = message_group {
            self.select_message_queue_by_message_group(route, message_group)
        } else {
            self.select_message_queue(route)
        };

        if message_queue.broker.is_none() {
            return Err(ClientError::new(
                ErrorKind::NoBrokerAvailable,
                "Message queue do not have a available endpoint.",
                Self::OPERATION_SEND_MESSAGE,
            )
            .with_context("message_queue", format!("{:?}", message_queue)));
        }

        let broker = message_queue.broker.unwrap();
        if broker.endpoints.is_none() {
            return Err(ClientError::new(
                ErrorKind::NoBrokerAvailable,
                "Message queue do not have a available endpoint.",
                Self::OPERATION_SEND_MESSAGE,
            )
            .with_context("broker", broker.name)
            .with_context("topic", topic)
            .with_context("queue_id", message_queue.id.to_string()));
        }

        let endpoints = Endpoints::from_pb_endpoints(broker.endpoints.unwrap());
        self.client.send_message(pb_messages, &endpoints).await
    }

    fn select_message_queue(&self, route: Arc<Route>) -> MessageQueue {
        let i = route.index.fetch_add(1, Ordering::Relaxed);
        route.queue[i % route.queue.len()].clone()
    }

    fn select_message_queue_by_message_group(
        &self,
        route: Arc<Route>,
        message_group: String,
    ) -> MessageQueue {
        let mut sip_hasher24 = SipHasher24::default();
        sip_hasher24.write(message_group.as_bytes());
        let index = sip_hasher24.finish() % route.queue.len() as u64;
        route.queue[index as usize].clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::conf::{ClientOption, ProducerOption};
    use crate::error::ErrorKind;
    use crate::model::message::MessageImpl;
    use crate::producer::Producer;

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
        let producer = Producer::new(ProducerOption::default(), ClientOption::default())
            .await
            .unwrap();
        let messages = vec![MessageImpl::builder()
            .set_topic("DefaultCluster".to_string())
            .set_body("hello world".as_bytes().to_vec())
            .set_tags("tag".to_string())
            .set_keys(vec!["key".to_string()])
            .set_properties(
                vec![("key".to_string(), "value".to_string())]
                    .into_iter()
                    .collect(),
            )
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
        let producer = Producer::new(ProducerOption::default(), ClientOption::default())
            .await
            .unwrap();

        let messages: Vec<MessageImpl> = vec![];
        let result = producer.transform_messages_to_protobuf(messages);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidMessage);
        assert_eq!(err.message, "No message found.");

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
        assert_eq!(err.message, "Message topic is empty.");

        let messages = vec![
            MessageImpl::builder()
                .set_topic("DefaultCluster".to_string())
                .set_body("hello world".as_bytes().to_vec())
                .build()
                .unwrap(),
            MessageImpl::builder()
                .set_topic("DefaultCluster_dup".to_string())
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
                .set_topic("DefaultCluster".to_string())
                .set_body("hello world".as_bytes().to_vec())
                .set_message_group("message_group".to_string())
                .build()
                .unwrap(),
            MessageImpl::builder()
                .set_topic("DefaultCluster".to_string())
                .set_body("hello world".as_bytes().to_vec())
                .set_message_group("message_group_dup".to_string())
                .build()
                .unwrap(),
        ];
        let result = producer.transform_messages_to_protobuf(messages);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidMessage);
        assert_eq!(err.message, "Not all messages have the same message group.");
    }
}
