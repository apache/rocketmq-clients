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
use crate::error::{ClientError, ErrorKind};
use crate::model::common::Endpoints;
use crate::model::message_id::UNIQ_ID_GENERATOR;
use crate::pb;
use std::collections::HashMap;

pub trait Message {
    fn take_message_id(&mut self) -> String;
    fn take_topic(&mut self) -> String;
    fn take_body(&mut self) -> Vec<u8>;
    fn take_tag(&mut self) -> Option<String>;
    fn take_keys(&mut self) -> Vec<String>;
    fn take_properties(&mut self) -> HashMap<String, String>;
    fn take_message_group(&mut self) -> Option<String>;
    fn take_delivery_timestamp(&mut self) -> Option<i64>;
}

#[derive(Debug)]
pub struct MessageImpl {
    pub(crate) message_id: String,
    pub(crate) topic: String,
    pub(crate) body: Option<Vec<u8>>,
    pub(crate) tags: Option<String>,
    pub(crate) keys: Option<Vec<String>>,
    pub(crate) properties: Option<HashMap<String, String>>,
    pub(crate) message_group: Option<String>,
    pub(crate) delivery_timestamp: Option<i64>,
}

impl Message for MessageImpl {
    fn take_message_id(&mut self) -> String {
        self.message_id.clone()
    }

    fn take_topic(&mut self) -> String {
        self.topic.clone()
    }

    fn take_body(&mut self) -> Vec<u8> {
        self.body.take().unwrap_or(vec![])
    }

    fn take_tag(&mut self) -> Option<String> {
        self.tags.take()
    }

    fn take_keys(&mut self) -> Vec<String> {
        self.keys.take().unwrap_or(vec![])
    }

    fn take_properties(&mut self) -> HashMap<String, String> {
        self.properties.take().unwrap_or(HashMap::new())
    }

    fn take_message_group(&mut self) -> Option<String> {
        self.message_group.take()
    }

    fn take_delivery_timestamp(&mut self) -> Option<i64> {
        self.delivery_timestamp.take()
    }
}

impl MessageImpl {
    pub fn builder() -> MessageBuilder {
        MessageBuilder {
            message: MessageImpl {
                message_id: UNIQ_ID_GENERATOR.lock().next_id(),
                topic: "".to_string(),
                body: None,
                tags: None,
                keys: None,
                properties: None,
                message_group: None,
                delivery_timestamp: None,
            },
        }
    }

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
                tags: None,
                keys: None,
                properties: None,
                message_group: Some(message_group.into()),
                delivery_timestamp: None,
            },
        }
    }

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
                tags: None,
                keys: None,
                properties: None,
                message_group: None,
                delivery_timestamp: Some(delay_time),
            },
        }
    }
}

pub struct MessageBuilder {
    message: MessageImpl,
}

impl MessageBuilder {
    const OPERATION_BUILD_MESSAGE: &'static str = "build_message";

    pub fn set_topic(mut self, topic: impl Into<String>) -> Self {
        self.message.topic = topic.into();
        self
    }

    pub fn set_body(mut self, body: Vec<u8>) -> Self {
        self.message.body = Some(body);
        self
    }

    pub fn set_tags(mut self, tags: impl Into<String>) -> Self {
        self.message.tags = Some(tags.into());
        self
    }

    pub fn set_keys(mut self, keys: Vec<impl Into<String>>) -> Self {
        self.message.keys = Some(keys.into_iter().map(|k| k.into()).collect());
        self
    }

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

    pub fn set_message_group(mut self, message_group: impl Into<String>) -> Self {
        self.message.message_group = Some(message_group.into());
        self
    }

    pub fn set_delivery_timestamp(mut self, delivery_timestamp: i64) -> Self {
        self.message.delivery_timestamp = Some(delivery_timestamp);
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
        Ok(())
    }

    pub fn build(self) -> Result<MessageImpl, ClientError> {
        self.check_message().map_err(|e| {
            ClientError::new(ErrorKind::InvalidMessage, &e, Self::OPERATION_BUILD_MESSAGE)
        })?;
        Ok(self.message)
    }
}

pub trait AckMessageEntry {
    fn topic(&self) -> String;
    fn message_id(&self) -> String;
    fn receipt_handle(&self) -> String;
    fn endpoints(&self) -> &Endpoints;
}

#[derive(Debug)]
pub struct MessageView {
    pub(crate) message_id: String,
    pub(crate) receipt_handle: Option<String>,
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
    pub(crate) fn from_pb_message(message: pb::Message, endpoints: Endpoints) -> Self {
        let system_properties = message.system_properties.unwrap();
        MessageView {
            message_id: system_properties.message_id,
            receipt_handle: system_properties.receipt_handle,
            topic: message.topic.unwrap().name,
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
        }
    }

    pub fn message_id(&self) -> &str {
        &self.message_id
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn body(&self) -> &[u8] {
        &self.body
    }

    pub fn tag(&self) -> Option<&str> {
        self.tag.as_deref()
    }

    pub fn keys(&self) -> &[String] {
        &self.keys
    }

    pub fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }

    pub fn message_group(&self) -> Option<&str> {
        self.message_group.as_deref()
    }

    pub fn delivery_timestamp(&self) -> Option<i64> {
        self.delivery_timestamp
    }

    pub fn born_host(&self) -> &str {
        &self.born_host
    }

    pub fn born_timestamp(&self) -> i64 {
        self.born_timestamp
    }

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
        let message = MessageImpl::builder()
            .set_topic("test")
            .set_body(vec![1, 2, 3])
            .set_tags("tag")
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

        let message = MessageImpl::builder()
            .set_topic("test")
            .set_body(vec![1, 2, 3])
            .set_message_group("message_group")
            .set_delivery_timestamp(123456789)
            .build();
        assert!(message.is_err());
        let err = message.unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidMessage);
        assert_eq!(
            err.message,
            "message_group and delivery_timestamp can not be set at the same time."
        );

        let message =
            MessageImpl::fifo_message_builder("test", vec![1, 2, 3], "message_group").build();
        let mut message = message.unwrap();
        assert_eq!(
            message.take_message_group(),
            Some("message_group".to_string())
        );

        let message = MessageImpl::delay_message_builder("test", vec![1, 2, 3], 123456789).build();
        let mut message = message.unwrap();
        assert_eq!(message.take_delivery_timestamp(), Some(123456789));
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
        );

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
