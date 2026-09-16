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
use std::io::Read;

use tracing::error;

use crate::error::{ClientError, ErrorKind};
use crate::model::common::Endpoints;
use crate::model::message::MessageType::{DELAY, FIFO, LITE, NORMAL, PRIORITY, TRANSACTION};
use crate::model::message_id::UNIQ_ID_GENERATOR;
use crate::pb;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum MessageType {
    NORMAL = 1,
    FIFO = 2,
    DELAY = 3,
    TRANSACTION = 4,
    LITE = 5,
    PRIORITY = 6,
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
    fn take_priority(&mut self) -> Option<i32>;
    fn take_lite_topic(&mut self) -> Option<String>;
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
    pub(crate) priority: Option<i32>,
    pub(crate) lite_topic: Option<String>,
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

    fn take_priority(&mut self) -> Option<i32> {
        self.priority.take()
    }

    fn take_lite_topic(&mut self) -> Option<String> {
        self.lite_topic.take()
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
                priority: None,
                lite_topic: None,
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
                priority: None,
                lite_topic: None,
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
                priority: None,
                lite_topic: None,
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
                priority: None,
                lite_topic: None,
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

    /// Create a new [`MessageBuilder`] for building a priority message.
    ///
    /// # Arguments
    ///
    /// * `topic` - topic of the message
    /// * `body` - message body
    /// * `priority` - message priority (1-10, higher value means higher priority)
    pub fn priority_message_builder(
        topic: impl Into<String>,
        body: Vec<u8>,
        priority: i32,
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
                delivery_timestamp: None,
                transaction_enabled: false,
                message_type: PRIORITY,
                priority: Some(priority),
                lite_topic: None,
            },
        }
    }

    /// Set message priority (1-10, higher value means higher priority).
    /// Only valid for priority messages.
    pub fn set_priority(mut self, priority: i32) -> Self {
        self.message.priority = Some(priority);
        self.message.message_type = PRIORITY;
        self
    }

    /// Create a new [`MessageBuilder`] for building a lite message.
    ///
    /// # Arguments
    ///
    /// * `topic` - parent topic of the message
    /// * `body` - message body
    /// * `lite_topic` - lite topic name, should not be blank
    pub fn lite_message_builder(
        topic: impl Into<String>,
        body: Vec<u8>,
        lite_topic: impl Into<String>,
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
                delivery_timestamp: None,
                transaction_enabled: false,
                message_type: LITE,
                priority: None,
                lite_topic: Some(lite_topic.into()),
            },
        }
    }

    /// Set lite topic for lite message.
    /// Lite topic cannot be blank, and cannot be set with message_group, delivery_timestamp, or
    /// priority at the same time; [`MessageBuilder::build`] fails otherwise.
    pub fn set_lite_topic(mut self, lite_topic: impl Into<String>) -> Self {
        self.message.lite_topic = Some(lite_topic.into());
        self.message.message_type = LITE;
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
        // Lite topic validation
        if let Some(lite_topic) = &self.message.lite_topic {
            // Reference Java: MessageBuilderImpl#setLiteTopic checks StringUtils.isNotBlank.
            if lite_topic.trim().is_empty() {
                return Err("lite_topic should not be blank.".to_string());
            }
            if self.message.message_group.is_some() {
                return Err(
                    "lite_topic and message_group can not be set at the same time.".to_string(),
                );
            }
            if self.message.delivery_timestamp.is_some() {
                return Err(
                    "lite_topic and delivery_timestamp can not be set at the same time."
                        .to_string(),
                );
            }
            if self.message.priority.is_some() {
                return Err("lite_topic and priority can not be set at the same time.".to_string());
            }
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

/// Verify the body digest and decode the body according to its encoding.
///
/// Returns the body to expose and whether the message is corrupted. The digest is computed by
/// the server on the body as stored, so it is checked before decoding.
///
/// Reference Java: `MessageViewImpl#fromProtobuf`.
fn decode_body(
    body: Vec<u8>,
    system_properties: &pb::SystemProperties,
    topic: &str,
    message_id: &str,
) -> (Vec<u8>, bool) {
    let mut corrupted = false;
    if let Some(digest) = &system_properties.body_digest {
        match pb::DigestType::from_i32(digest.r#type) {
            Some(pb::DigestType::Crc32) => {
                corrupted |= format!("{:X}", crc32fast::hash(&body)) != digest.checksum;
            }
            Some(pb::DigestType::Md5) => {
                use md5::Digest;
                corrupted |= hex::encode_upper(md5::Md5::digest(&body)) != digest.checksum;
            }
            Some(pb::DigestType::Sha1) => {
                let sha1 = ring::digest::digest(&ring::digest::SHA1_FOR_LEGACY_USE_ONLY, &body);
                corrupted |= hex::encode_upper(sha1.as_ref()) != digest.checksum;
            }
            _ => error!(
                "Unsupported message body digest algorithm, digest_type={}, topic={}, message_id={}",
                digest.r#type, topic, message_id
            ),
        }
    }
    if corrupted {
        error!(
            "Message body digest mismatch, topic={}, message_id={}",
            topic, message_id
        );
    }
    match pb::Encoding::from_i32(system_properties.body_encoding) {
        Some(pb::Encoding::Gzip) => match decompress(&body) {
            Ok(decompressed) => (decompressed, corrupted),
            Err(e) => {
                error!(
                    "Failed to uncompress message body, topic={}, message_id={}, error={}",
                    topic, message_id, e
                );
                (body, true)
            }
        },
        Some(pb::Encoding::Identity) => (body, corrupted),
        _ => {
            error!(
                "Unsupported message encoding algorithm, topic={}, message_id={}, body_encoding={}",
                topic, message_id, system_properties.body_encoding
            );
            (body, corrupted)
        }
    }
}

/// Decompress a body by its magic number.
///
/// The proxy marks every body that was stored compressed as `GZIP`, whatever the algorithm. The
/// remoting producer compresses with ZLIB by default. LZ4 and ZSTD are not supported.
///
/// Reference Java: `Utilities#decompressBytes`.
fn decompress(body: &[u8]) -> std::io::Result<Vec<u8>> {
    let mut decompressed = Vec::new();
    match body {
        [0x1f, 0x8b, ..] => {
            flate2::read::MultiGzDecoder::new(body).read_to_end(&mut decompressed)?;
        }
        [0x78, ..] => {
            flate2::read::ZlibDecoder::new(body).read_to_end(&mut decompressed)?;
        }
        _ => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unsupported compression format",
            ))
        }
    }
    Ok(decompressed)
}

/// [`AckMessageEntry`] is the data model for ack message.
pub trait AckMessageEntry {
    fn topic(&self) -> String;
    fn message_id(&self) -> String;
    fn receipt_handle(&self) -> String;
    fn endpoints(&self) -> &Endpoints;

    /// Lite topic the message belongs to, `None` for non-lite messages.
    ///
    /// Lite messages MUST be acknowledged with their lite topic, otherwise the proxy is unable
    /// to resolve the LMQ receipt handle and answers `INTERNAL_SERVER_ERROR`.
    fn lite_topic(&self) -> Option<&str> {
        None
    }
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
    pub(crate) priority: Option<i32>,
    pub(crate) lite_topic: Option<String>,
    pub(crate) corrupted: bool,
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

    fn lite_topic(&self) -> Option<&str> {
        self.lite_topic.as_deref()
    }
}

impl MessageView {
    pub(crate) fn from_pb_message(message: pb::Message, endpoints: Endpoints) -> Option<Self> {
        let system_properties = message.system_properties?;
        let topic = message.topic?;
        let (body, corrupted) = decode_body(
            message.body,
            &system_properties,
            &topic.name,
            &system_properties.message_id,
        );
        Some(MessageView {
            message_id: system_properties.message_id,
            receipt_handle: system_properties.receipt_handle,
            namespace: topic.resource_namespace,
            topic: topic.name,
            body,
            tag: system_properties.tag,
            keys: system_properties.keys,
            properties: message.user_properties,
            message_group: system_properties.message_group,
            delivery_timestamp: system_properties.delivery_timestamp.map(|t| t.seconds),
            born_host: system_properties.born_host,
            born_timestamp: system_properties.born_timestamp.map_or(0, |t| t.seconds),
            delivery_attempt: system_properties.delivery_attempt.unwrap_or(0),
            endpoints,
            priority: system_properties.priority,
            lite_topic: system_properties.lite_topic,
            corrupted,
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

    /// Whether the message body failed its digest check or could not be decompressed.
    ///
    /// A corrupted message keeps the body as received. Reference Java: `MessageViewImpl#isCorrupted`.
    pub fn is_corrupted(&self) -> bool {
        self.corrupted
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

    /// Increase the delivery attempt before a local redelivery and return the new value.
    ///
    /// Reference Java: `MessageViewImpl#incrementAndGetDeliveryAttempt`.
    pub(crate) fn increment_and_get_delivery_attempt(&mut self) -> i32 {
        self.delivery_attempt += 1;
        self.delivery_attempt
    }

    /// Get message priority (1-10, higher value means higher priority)
    pub fn priority(&self) -> Option<i32> {
        self.priority
    }

    /// Get lite topic of lite message
    pub fn lite_topic(&self) -> Option<&str> {
        self.lite_topic.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod body_decoding {
        use std::io::Write;

        use super::*;

        const PLAIN: &[u8] = b"hello compressed body hello compressed body hello compressed body";

        fn gzip(data: &[u8]) -> Vec<u8> {
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            encoder.write_all(data).unwrap();
            encoder.finish().unwrap()
        }

        fn zlib(data: &[u8]) -> Vec<u8> {
            let mut encoder =
                flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
            encoder.write_all(data).unwrap();
            encoder.finish().unwrap()
        }

        fn md5_hex(data: &[u8]) -> String {
            use md5::Digest;
            hex::encode_upper(md5::Md5::digest(data))
        }

        fn view(body: Vec<u8>, encoding: pb::Encoding, digest: Option<pb::Digest>) -> MessageView {
            MessageView::from_pb_message(
                pb::Message {
                    topic: Some(pb::Resource {
                        name: "topic".to_string(),
                        resource_namespace: "".to_string(),
                    }),
                    user_properties: Default::default(),
                    system_properties: Some(pb::SystemProperties {
                        body_encoding: encoding as i32,
                        body_digest: digest,
                        ..Default::default()
                    }),
                    body,
                },
                Endpoints::from_url("localhost:8081").unwrap(),
            )
            .unwrap()
        }

        fn digest(r#type: pb::DigestType, checksum: String) -> Option<pb::Digest> {
            Some(pb::Digest {
                r#type: r#type as i32,
                checksum,
            })
        }

        #[test]
        fn identity_body_is_kept() {
            let message = view(PLAIN.to_vec(), pb::Encoding::Identity, None);
            assert_eq!(message.body(), PLAIN);
            assert!(!message.is_corrupted());
        }

        #[test]
        fn gzip_body_is_decompressed() {
            let message = view(gzip(PLAIN), pb::Encoding::Gzip, None);
            assert_eq!(message.body(), PLAIN);
            assert!(!message.is_corrupted());
        }

        #[test]
        fn zlib_body_marked_as_gzip_is_decompressed() {
            // The proxy marks every body stored with COMPRESSED_FLAG as GZIP, but the remoting
            // producer compresses with ZLIB by default.
            let message = view(zlib(PLAIN), pb::Encoding::Gzip, None);
            assert_eq!(message.body(), PLAIN);
            assert!(!message.is_corrupted());
        }

        #[test]
        fn undecodable_body_is_corrupted() {
            let garbage = vec![0x1f, 0x8b, 0x00, 0x01, 0x02];
            let message = view(garbage.clone(), pb::Encoding::Gzip, None);
            assert_eq!(message.body(), garbage.as_slice());
            assert!(message.is_corrupted());

            let unknown_format = b"not compressed".to_vec();
            let message = view(unknown_format.clone(), pb::Encoding::Gzip, None);
            assert_eq!(message.body(), unknown_format.as_slice());
            assert!(message.is_corrupted());
        }

        #[test]
        fn digest_is_verified_on_the_received_body() {
            // The proxy computes the digest on the stored, i.e. still compressed, body.
            let compressed = zlib(PLAIN);
            let message = view(
                compressed.clone(),
                pb::Encoding::Gzip,
                digest(pb::DigestType::Md5, md5_hex(&compressed)),
            );
            assert_eq!(message.body(), PLAIN);
            assert!(!message.is_corrupted());

            let message = view(
                compressed,
                pb::Encoding::Gzip,
                digest(pb::DigestType::Md5, md5_hex(PLAIN)),
            );
            assert!(message.is_corrupted());
        }

        #[test]
        fn every_digest_type_is_verified() {
            let crc32 = format!("{:X}", crc32fast::hash(PLAIN));
            let sha1 = hex::encode_upper(
                ring::digest::digest(&ring::digest::SHA1_FOR_LEGACY_USE_ONLY, PLAIN).as_ref(),
            );
            for (r#type, checksum) in [
                (pb::DigestType::Crc32, crc32),
                (pb::DigestType::Md5, md5_hex(PLAIN)),
                (pb::DigestType::Sha1, sha1),
            ] {
                let message = view(
                    PLAIN.to_vec(),
                    pb::Encoding::Identity,
                    digest(r#type, checksum.clone()),
                );
                assert!(!message.is_corrupted(), "{:?} {}", r#type, checksum);

                let message = view(
                    PLAIN.to_vec(),
                    pb::Encoding::Identity,
                    digest(r#type, "0".to_string()),
                );
                assert!(message.is_corrupted(), "{:?}", r#type);
            }
        }
    }

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

        // Test lite message builder
        let message =
            MessageBuilder::lite_message_builder("parent_topic", vec![1, 2, 3], "lite_topic_001")
                .build();
        assert!(message.is_ok());
        let mut message = message.unwrap();
        assert_eq!(
            message.take_lite_topic(),
            Some("lite_topic_001".to_string())
        );
        assert_eq!(message.get_message_type(), LITE);

        // Test lite topic conflicts with message_group
        let message = MessageBuilder::builder()
            .set_topic("test")
            .set_body(vec![1, 2, 3])
            .set_lite_topic("lite_topic")
            .set_message_group("group")
            .build();
        assert!(message.is_err());
        let err = message.err().unwrap();
        assert_eq!(err.kind, ErrorKind::InvalidMessage);
        assert_eq!(
            err.message,
            "lite_topic and message_group can not be set at the same time."
        );

        // Test lite topic conflicts with delivery_timestamp
        let message = MessageBuilder::builder()
            .set_topic("test")
            .set_body(vec![1, 2, 3])
            .set_lite_topic("lite_topic")
            .set_delivery_timestamp(123456789)
            .build();
        assert!(message.is_err());
        let err = message.err().unwrap();
        assert_eq!(err.kind, ErrorKind::InvalidMessage);
        assert_eq!(
            err.message,
            "lite_topic and delivery_timestamp can not be set at the same time."
        );

        // Test lite topic conflicts with priority
        let message = MessageBuilder::builder()
            .set_topic("test")
            .set_body(vec![1, 2, 3])
            .set_lite_topic("lite_topic")
            .set_priority(5)
            .build();
        assert!(message.is_err());
        let err = message.err().unwrap();
        assert_eq!(err.kind, ErrorKind::InvalidMessage);
        assert_eq!(
            err.message,
            "lite_topic and priority can not be set at the same time."
        );
    }

    #[test]
    fn test_build_message_with_blank_lite_topic() {
        for lite_topic in ["", " ", "\t\n "] {
            let message =
                MessageBuilder::lite_message_builder("parent_topic", vec![1, 2, 3], lite_topic)
                    .build();
            let err = message.err().unwrap();
            assert_eq!(err.kind, ErrorKind::InvalidMessage);
            assert_eq!(err.message, "lite_topic should not be blank.");

            let message = MessageBuilder::builder()
                .set_topic("parent_topic")
                .set_body(vec![1, 2, 3])
                .set_lite_topic(lite_topic)
                .build();
            let err = message.err().unwrap();
            assert_eq!(err.kind, ErrorKind::InvalidMessage);
            assert_eq!(err.message, "lite_topic should not be blank.");
        }

        // Surrounding whitespace is kept as is, like the Java client.
        let mut message =
            MessageBuilder::lite_message_builder("parent_topic", vec![1, 2, 3], " lite_topic ")
                .build()
                .unwrap();
        assert_eq!(message.take_lite_topic(), Some(" lite_topic ".to_string()));
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

    #[test]
    fn test_lite_message_view() {
        // Test MessageView with lite_topic
        let message_view = MessageView::from_pb_message(
            pb::Message {
                topic: Some(pb::Resource {
                    name: "parent_topic".to_string(),
                    ..Default::default()
                }),
                body: vec![1, 2, 3],
                user_properties: HashMap::new(),
                system_properties: Some(pb::SystemProperties {
                    message_id: "lite_msg_id".to_string(),
                    receipt_handle: Some("receipt_handle".to_string()),
                    lite_topic: Some("lite_topic_001".to_string()),
                    message_type: LITE as i32,
                    ..Default::default()
                }),
            },
            Endpoints::from_url("localhost:8081").unwrap(),
        )
        .unwrap();

        assert_eq!(message_view.message_id(), "lite_msg_id");
        assert_eq!(message_view.topic(), "parent_topic");
        assert_eq!(message_view.lite_topic(), Some("lite_topic_001"));
        assert_eq!(message_view.body(), &[1, 2, 3]);
    }

    #[test]
    fn test_lite_message_without_lite_topic() {
        // Test MessageView without lite_topic
        let message_view = MessageView::from_pb_message(
            pb::Message {
                topic: Some(pb::Resource {
                    name: "normal_topic".to_string(),
                    ..Default::default()
                }),
                body: vec![4, 5, 6],
                user_properties: HashMap::new(),
                system_properties: Some(pb::SystemProperties {
                    message_id: "normal_msg_id".to_string(),
                    receipt_handle: Some("receipt_handle".to_string()),
                    lite_topic: None,
                    message_type: NORMAL as i32,
                    ..Default::default()
                }),
            },
            Endpoints::from_url("localhost:8081").unwrap(),
        )
        .unwrap();

        assert_eq!(message_view.message_id(), "normal_msg_id");
        assert_eq!(message_view.topic(), "normal_topic");
        assert_eq!(message_view.lite_topic(), None);
        assert_eq!(message_view.body(), &[4, 5, 6]);
    }

    #[test]
    fn test_lite_message_builder_with_all_fields() {
        // Test lite message builder with additional fields
        let message =
            MessageBuilder::lite_message_builder("parent_topic", vec![7, 8, 9], "lite_topic_full")
                .set_tag("test-tag")
                .set_keys(vec!["key1".to_string(), "key2".to_string()])
                .set_properties({
                    let mut props = HashMap::new();
                    props.insert("prop1".to_string(), "value1".to_string());
                    props
                })
                .build();

        assert!(message.is_ok());
        let mut msg = message.unwrap();
        assert_eq!(msg.take_topic(), "parent_topic");
        assert_eq!(msg.take_lite_topic(), Some("lite_topic_full".to_string()));
        assert_eq!(msg.take_body(), vec![7, 8, 9]);
        assert_eq!(msg.take_tag(), Some("test-tag".to_string()));
        assert_eq!(
            msg.take_keys(),
            vec!["key1".to_string(), "key2".to_string()]
        );
        assert_eq!(msg.get_message_type(), LITE);
    }

    #[test]
    fn test_lite_message_type_enum_value() {
        // Verify MessageType enum values match proto definition
        assert_eq!(NORMAL as i32, 1);
        assert_eq!(FIFO as i32, 2);
        assert_eq!(DELAY as i32, 3);
        assert_eq!(TRANSACTION as i32, 4);
        assert_eq!(LITE as i32, 5); // LITE should be 5
        assert_eq!(PRIORITY as i32, 6); // PRIORITY should be 6
    }
}
