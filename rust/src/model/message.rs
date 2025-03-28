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

//! Message data model of RocketMQ rust client.

use std::collections::HashMap;

use crate::error::{ClientError, ErrorKind};
use crate::model::common::Endpoints;
use crate::model::message::MessageType::{DELAY, FIFO, NORMAL, TRANSACTION};
use crate::model::message_id::UNIQ_ID_GENERATOR;
use crate::pb;

#[derive(Clone, Copy, Debug)]
pub enum MessageType {
    NORMAL = 1,
    FIFO = 2,
    DELAY = 3,
    TRANSACTION = 4,
}

/// [`Message`] is the data model for sending.
pub trait Message {
    fn take_message_id(&mut self) -> String;
    fn take_topic(&mut self) -> String;
    fn take_body(&mut self) -> Vec<u8>;
    fn take_tag(&mut self) -> Option<String>;
    fn take_keys(&mut self) -> Vec<String>;
    fn take_properties(&mut self) -> HashMap<String, String>;
    fn take_message_group(&mut self) -> Option<String>;
    fn take_delivery_timestamp(&mut self) -> Option<i64>;
    fn transaction_enabled(&mut self) -> bool;
    fn get_message_type(&self) -> MessageType;
}

pub trait MessageTypeAware {
    fn accept_type(&self, message_type: MessageType) -> bool;
}

impl MessageTypeAware for pb::MessageQueue {
    fn accept_type(&self, message_type: MessageType) -> bool {
        self.accept_message_types.contains(&(message_type as i32))
    }
}

pub(crate) struct MessageImpl {
    pub(crate) message_id: String,
    pub(crate) topic: String,
    pub(crate) body: Option<Vec<u8>>,
    pub(crate) tag: Option<String>,
    pub(crate) keys: Option<Vec<String>>,
    pub(crate) properties: Option<HashMap<String, String>>,
    pub(crate) message_group: Option<String>,
    pub(crate) delivery_timestamp: Option<i64>,
    pub(crate) transaction_enabled: bool,
    pub(crate) message_type: MessageType,
}

impl Message for MessageImpl {
    fn take_message_id(&mut self) -> String {
        self.message_id.clone()
    }

    fn take_topic(&mut self) -> String {
        self.topic.clone()
    }

    fn take_body(&mut self) -> Vec<u8> {
        self.body.take().unwrap_or_default()
    }

    fn take_tag(&mut self) -> Option<String> {
        self.tag.take()
    }

    fn take_keys(&mut self) -> Vec<String> {
        self.keys.take().unwrap_or_default()
    }

    fn take_properties(&mut self) -> HashMap<String, String> {
        self.properties.take().unwrap_or_default()
    }

    fn take_message_group(&mut self) -> Option<String> {
        self.message_group.take()
    }

    fn take_delivery_timestamp(&mut self) -> Option<i64> {
        self.delivery_timestamp.take()
    }

    fn transaction_enabled(&mut self) -> bool {
        self.transaction_enabled
    }

    fn get_message_type(&self) -> MessageType {
        self.message_type
    }
}

/// [`MessageBuilder`] is the builder for [`Message`].
pub struct MessageBuilder {
    message: MessageImpl,
}

impl MessageBuilder {
    const OPERATION_BUILD_MESSAGE: &'static str = "build_message";

    /// Create a new [`MessageBuilder`] for building a message. [Read more](https://rocketmq.apache.org/docs/domainModel/04message/)
    pub fn builder() -> MessageBuilder {
        MessageBuilder {
            message: MessageImpl {
                message_id: UNIQ_ID_GENERATOR.lock().next_id(),
                topic: "".to_string(),
                body: None,
                tag: None,
                keys: None,
                properties: None,
                message_group: None,
                delivery_timestamp: None,
                transaction_enabled: false,
                message_type: NORMAL,
            },
        }
    }

    /// Create a new [`MessageBuilder`] for building a fifo message. [Read more](https://rocketmq.apache.org/docs/featureBehavior/03fifomessage)
    ///
    /// # Arguments
    ///
    /// * `topic` - topic of the message
    /// * `body` - message body
    /// * `message_group` - message group, messages with same message group will be delivered in FIFO order
    pub fn fifo_message_builder(
        topic: impl Into<String>,
        body: Vec<u8>,
        message_group: impl Into<String>,
    ) -> MessageBuilder {
        MessageBuilder {
            message: MessageImpl {
                message_id: UNIQ_ID_GENERATOR.lock().next_id(),
                topic: topic.into(),
                body: Some(body),
                tag: None,
                keys: None,
                properties: None,
                message_group: Some(message_group.into()),
                delivery_timestamp: None,
                transaction_enabled: false,
                message_type: FIFO,
            },
        }
    }

    /// Create a new [`MessageBuilder`] for building a delay message. [Read more](https://rocketmq.apache.org/docs/featureBehavior/02delaymessage)
    ///
    /// # Arguments
    ///
    /// * `topic` - topic of the message
    /// * `body` - message body
    /// * `delay_time` - delivery timestamp of message, specify when to deliver the message
    pub fn delay_message_builder(
        topic: impl Into<String>,
        body: Vec<u8>,
        delay_time: i64,
    ) -> MessageBuilder {
        MessageBuilder {
            message: MessageImpl {
                message_id: UNIQ_ID_GENERATOR.lock().next_id(),
                topic: topic.into(),
                body: Some(body),
                tag: None,
                keys: None,
                properties: None,
                message_group: None,
                delivery_timestamp: Some(delay_time),
                transaction_enabled: false,
                message_type: DELAY,
            },
        }
    }

    /// Create a new [`MessageBuilder`] for building a transaction message. [Read more](https://rocketmq.apache.org/docs/featureBehavior/04transactionmessage)
    ///
    /// # Arguments
    ///
    /// * `topic` - topic of the message
    /// * `body` - message body
    pub fn transaction_message_builder(topic: impl Into<String>, body: Vec<u8>) -> MessageBuilder {
        MessageBuilder {
            message: MessageImpl {
                message_id: UNIQ_ID_GENERATOR.lock().next_id(),
                topic: topic.into(),
                body: Some(body),
                tag: None,
                keys: None,
                properties: None,
                message_group: None,
                delivery_timestamp: None,
                transaction_enabled: true,
                message_type: TRANSACTION,
            },
        }
    }

    /// Set topic for message, which is required
    pub fn set_topic(mut self, topic: impl Into<String>) -> Self {
        self.message.topic = topic.into();
        self
    }

    /// Set message body, which is required
    pub fn set_body(mut self, body: Vec<u8>) -> Self {
        self.message.body = Some(body);
        self
    }

    /// Set message tag
    pub fn set_tag(mut self, tag: impl Into<String>) -> Self {
        self.message.tag = Some(tag.into());
        self
    }

    /// Set message keys
    pub fn set_keys(mut self, keys: Vec<impl Into<String>>) -> Self {
        self.message.keys = Some(keys.into_iter().map(|k| k.into()).collect());
        self
    }

    /// Set message properties
    pub fn set_properties(
        mut self,
        properties: HashMap<impl Into<String>, impl Into<String>>,
    ) -> Self {
        self.message.properties = Some(
            properties
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        );
        self
    }

    /// Set message group, which is required for fifo message. [Read more](https://rocketmq.apache.org/docs/featureBehavior/03fifomessage)
    ///
    /// The message group could not be set with delivery timestamp at the same time
    pub fn set_message_group(mut self, message_group: impl Into<String>) -> Self {
        self.message.message_group = Some(message_group.into());
        self
    }

    /// Set delivery timestamp, which is required for delay message. [Read more](https://rocketmq.apache.org/docs/featureBehavior/02delaymessage)
    ///
    /// The delivery timestamp could not be set with message group at the same time
    pub fn set_delivery_timestamp(mut self, delivery_timestamp: i64) -> Self {
        self.message.delivery_timestamp = Some(delivery_timestamp);
        self
    }

    /// Mark this message as the beginning transaction, which is required for the transaction message. [Read more](https://rocketmq.apache.org/docs/featureBehavior/04transactionmessage)
    ///
    /// The transaction message could not have message group and delivery timestamp
    pub fn enable_transaction(mut self) -> Self {
        self.message.transaction_enabled = true;
        self
    }

    fn check_message(&self) -> Result<(), String> {
        if self.message.topic.is_empty() {
            return Err("Topic is empty.".to_string());
        }
        if self.message.body.is_none() {
            return Err("Body is empty.".to_string());
        }
        if self.message.message_group.is_some() && self.message.delivery_timestamp.is_some() {
            return Err(
                "message_group and delivery_timestamp can not be set at the same time.".to_string(),
            );
        }
        if self.message.transaction_enabled
            && (self.message.message_group.is_some() || self.message.delivery_timestamp.is_some())
        {
            return Err(
                "message_group and delivery_timestamp can not be set for transaction message."
                    .to_string(),
            );
        }
        Ok(())
    }

    /// Build message
    pub fn build(self) -> Result<impl Message, ClientError> {
        self.check_message().map_err(|e| {
            ClientError::new(ErrorKind::InvalidMessage, &e, Self::OPERATION_BUILD_MESSAGE)
        })?;
        Ok(self.message)
    }
}

/// [`AckMessageEntry`] is the data model for ack message.
pub trait AckMessageEntry {
    fn topic(&self) -> String;
    fn message_id(&self) -> String;
    fn receipt_handle(&self) -> String;
    fn endpoints(&self) -> &Endpoints;
}

/// [`MessageView`] is the data model for receive message.
///
/// [`MessageView`] has implemented [`AckMessageEntry`] trait.
#[derive(Debug)]
pub struct MessageView {
    pub(crate) message_id: String,
    pub(crate) receipt_handle: Option<String>,
    pub(crate) namespace: String,
    pub(crate) topic: String,
    pub(crate) body: Vec<u8>,
    pub(crate) tag: Option<String>,
    pub(crate) keys: Vec<String>,
    pub(crate) properties: HashMap<String, String>,
    pub(crate) message_group: Option<String>,
    pub(crate) delivery_timestamp: Option<i64>,
    pub(crate) born_host: String,
    pub(crate) born_timestamp: i64,
    pub(crate) delivery_attempt: i32,
    pub(crate) endpoints: Endpoints,
}

impl AckMessageEntry for MessageView {
    fn topic(&self) -> String {
        self.topic.clone()
    }

    fn message_id(&self) -> String {
        self.message_id.clone()
    }

    fn receipt_handle(&self) -> String {
        self.receipt_handle.clone().unwrap()
    }

    fn endpoints(&self) -> &Endpoints {
        &self.endpoints
    }
}

impl MessageView {
    pub(crate) fn from_pb_message(message: pb::Message, endpoints: Endpoints) -> Option<Self> {
        let system_properties = message.system_properties?;
        let topic = message.topic?;
        Some(MessageView {
            message_id: system_properties.message_id,
            receipt_handle: system_properties.receipt_handle,
            namespace: topic.resource_namespace,
            topic: topic.name,
            body: message.body,
            tag: system_properties.tag,
            keys: system_properties.keys,
            properties: message.user_properties,
            message_group: system_properties.message_group,
            delivery_timestamp: system_properties.delivery_timestamp.map(|t| t.seconds),
            born_host: system_properties.born_host,
            born_timestamp: system_properties.born_timestamp.map_or(0, |t| t.seconds),
            delivery_attempt: system_properties.delivery_attempt.unwrap_or(0),
            endpoints,
        })
    }

    /// Get message id
    pub fn message_id(&self) -> &str {
        &self.message_id
    }

    /// Get topic namespace of message
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Get topic name of message
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Get message body
    pub fn body(&self) -> &[u8] {
        &self.body
    }

    /// Get message tag
    pub fn tag(&self) -> Option<&str> {
        self.tag.as_deref()
    }

    /// Get message keys
    pub fn keys(&self) -> &[String] {
        &self.keys
    }

    /// Get message properties
    pub fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }

    /// Get message group of fifo message. [Read more](https://rocketmq.apache.org/docs/featureBehavior/03fifomessage)
    pub fn message_group(&self) -> Option<&str> {
        self.message_group.as_deref()
    }

    /// Get delivery timestamp of delay message. [Read more](https://rocketmq.apache.org/docs/featureBehavior/02delaymessage)
    pub fn delivery_timestamp(&self) -> Option<i64> {
        self.delivery_timestamp
    }

    /// Get born host of message
    pub fn born_host(&self) -> &str {
        &self.born_host
    }

    /// Get born timestamp of message
    pub fn born_timestamp(&self) -> i64 {
        self.born_timestamp
    }

    /// Get delivery attempt of message
    pub fn delivery_attempt(&self) -> i32 {
        self.delivery_attempt
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn common_test_message() {
        let mut properties = HashMap::new();
        properties.insert("key", "value".to_string());
        let message = MessageBuilder::builder()
            .set_topic("test")
            .set_body(vec![1, 2, 3])
            .set_tag("tag")
            .set_keys(vec!["key"])
            .set_properties(properties)
            .build();
        assert!(message.is_ok());

        let mut message = message.unwrap();
        assert_eq!(message.take_topic(), "test");
        assert_eq!(message.take_body(), vec![1, 2, 3]);
        assert_eq!(message.take_tag(), Some("tag".to_string()));
        assert_eq!(message.take_keys(), vec!["key".to_string()]);
        assert_eq!(message.take_properties(), {
            let mut properties = HashMap::new();
            properties.insert("key".to_string(), "value".to_string());
            properties
        });

        let message = MessageBuilder::builder()
            .set_topic("test")
            .set_body(vec![1, 2, 3])
            .set_message_group("message_group")
            .set_delivery_timestamp(123456789)
            .build();
        assert!(message.is_err());
        let err = message.err().unwrap();
        assert_eq!(err.kind, ErrorKind::InvalidMessage);
        assert_eq!(
            err.message,
            "message_group and delivery_timestamp can not be set at the same time."
        );

        let message =
            MessageBuilder::fifo_message_builder("test", vec![1, 2, 3], "message_group").build();
        let mut message = message.unwrap();
        assert_eq!(
            message.take_message_group(),
            Some("message_group".to_string())
        );

        let message =
            MessageBuilder::delay_message_builder("test", vec![1, 2, 3], 123456789).build();
        let mut message = message.unwrap();
        assert_eq!(message.take_delivery_timestamp(), Some(123456789));

        let message = MessageBuilder::transaction_message_builder("test", vec![1, 2, 3]).build();
        let mut message = message.unwrap();
        assert!(message.transaction_enabled());
    }

    #[test]
    fn common_message() {
        let message_view = MessageView::from_pb_message(
            pb::Message {
                topic: Some(pb::Resource {
                    name: "test".to_string(),
                    ..Default::default()
                }),
                body: vec![1, 2, 3],
                user_properties: {
                    let mut properties = HashMap::new();
                    properties.insert("key".to_string(), "value".to_string());
                    properties
                },
                system_properties: Some(pb::SystemProperties {
                    message_id: "message_id".to_string(),
                    receipt_handle: Some("receipt_handle".to_string()),
                    tag: Some("tag".to_string()),
                    keys: vec!["key".to_string()],
                    message_group: Some("message_group".to_string()),
                    delivery_timestamp: Some(prost_types::Timestamp {
                        seconds: 123456789,
                        ..Default::default()
                    }),
                    born_host: "born_host".to_string(),
                    born_timestamp: Some(prost_types::Timestamp {
                        seconds: 987654321,
                        ..Default::default()
                    }),
                    delivery_attempt: Some(1),
                    ..Default::default()
                }),
            },
            Endpoints::from_url("localhost:8081").unwrap(),
        )
        .unwrap();

        assert_eq!(message_view.message_id(), "message_id");
        assert_eq!(message_view.topic(), "test");
        assert_eq!(message_view.body(), &[1, 2, 3]);
        assert_eq!(message_view.tag(), Some("tag"));
        assert_eq!(message_view.keys(), &["key"]);
        assert_eq!(message_view.properties(), &{
            let mut properties = HashMap::new();
            properties.insert("key".to_string(), "value".to_string());
            properties
        });
        assert_eq!(message_view.message_group(), Some("message_group"));
        assert_eq!(message_view.delivery_timestamp(), Some(123456789));
        assert_eq!(message_view.born_host(), "born_host");
        assert_eq!(message_view.born_timestamp(), 987654321);
        assert_eq!(message_view.delivery_attempt(), 1);

        assert_eq!(AckMessageEntry::message_id(&message_view), "message_id");
        assert_eq!(AckMessageEntry::topic(&message_view), "test");
        assert_eq!(
            AckMessageEntry::receipt_handle(&message_view),
            "receipt_handle"
        );
        assert_eq!(
            AckMessageEntry::endpoints(&message_view).endpoint_url(),
            "localhost:8081"
        );
    }
}
