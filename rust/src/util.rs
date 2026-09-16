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
use std::hash::Hasher;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use once_cell::sync::Lazy;
use siphasher::sip::SipHasher24;

use crate::conf::{ProducerOption, PushConsumerOption, SimpleConsumerOption};
use crate::error::{ClientError, ErrorKind};
use crate::model::common::{ClientType, Endpoints, Route};
use crate::model::message::AckMessageEntry;
use crate::pb::settings::PubSub;
use crate::pb::telemetry_command::Command;
use crate::pb::{
    Code, FilterExpression, Language, MessageQueue, Publishing, Resource, Settings, Status,
    Subscription, SubscriptionEntry, TelemetryCommand, Ua,
};

pub(crate) static SDK_LANGUAGE: Language = Language::Rust;
pub(crate) static SDK_VERSION: &str = "5.0.0";
pub(crate) static PROTOCOL_VERSION: &str = "2.0.0";

pub(crate) static HOST_NAME: Lazy<String> = Lazy::new(|| match hostname::get() {
    Ok(name) => name.to_str().unwrap_or("localhost").to_string(),
    Err(_) => "localhost".to_string(),
});

pub(crate) fn select_message_queue(route: Arc<Route>) -> MessageQueue {
    let i = route.index.fetch_add(1, Ordering::Relaxed);
    route.queue[i % route.queue.len()].clone()
}

pub(crate) fn select_message_queue_by_message_group(
    route: Arc<Route>,
    message_group: String,
) -> MessageQueue {
    let mut sip_hasher24 = SipHasher24::default();
    sip_hasher24.write(message_group.as_bytes());
    let index = sip_hasher24.finish() % route.queue.len() as u64;
    route.queue[index as usize].clone()
}

/// Check whether a message queue is readable and served by a master broker.
///
/// Reference Java: `SubscriptionLoadBalancer.isReadableMasterQueue(MessageQueueImpl)`
fn is_readable_master_queue(message_queue: &MessageQueue) -> bool {
    // Queues served by a slave broker are never readable.
    if message_queue.broker.as_ref().map(|broker| broker.id) != Some(0) {
        return false;
    }
    match crate::pb::Permission::from_i32(message_queue.permission) {
        // `Unspecified` is tolerated: some route responses omit the permission field.
        Some(crate::pb::Permission::Unspecified)
        | Some(crate::pb::Permission::Read)
        | Some(crate::pb::Permission::ReadWrite) => true,
        _ => false,
    }
}

/// Keep only the first readable master queue of a route.
///
/// Lite consumers only need a route to *a* broker of the parent topic: the proxy resolves the
/// real lite topic queues on the server side. Mirrors
/// `LiteSimpleConsumerImpl#updateSubscriptionLoadBalancer` in Java.
pub(crate) fn prune_lite_route(
    route: Arc<Route>,
    topic: &str,
    operation: &'static str,
) -> Result<Arc<Route>, ClientError> {
    let queue = route
        .queue
        .iter()
        .find(|message_queue| is_readable_master_queue(message_queue))
        .cloned()
        .map(|message_queue| vec![message_queue])
        .unwrap_or_default();

    if queue.is_empty() {
        return Err(ClientError::new(
            ErrorKind::NoBrokerAvailable,
            &format!(
                "no readable message queue found for lite topic route, topic={}",
                topic
            ),
            operation,
        ));
    }

    Ok(Arc::new(Route {
        index: Default::default(),
        queue,
    }))
}

/// Build the [`AckMessageEntry`] protobuf of an ack entry.
///
/// `lite_topic` MUST be carried for lite messages, otherwise the proxy is unable to resolve the
/// LMQ receipt handle and answers `INTERNAL_SERVER_ERROR`.
pub(crate) fn build_ack_message_entry<T: AckMessageEntry + ?Sized>(
    ack_entry: &T,
) -> crate::pb::AckMessageEntry {
    crate::pb::AckMessageEntry {
        message_id: ack_entry.message_id(),
        receipt_handle: ack_entry.receipt_handle(),
        lite_topic: ack_entry.lite_topic().map(|topic| topic.to_string()),
    }
}

pub(crate) fn build_endpoints_by_message_queue(
    message_queue: &MessageQueue,
    operation: &'static str,
) -> Result<Endpoints, ClientError> {
    let topic = message_queue.topic.clone().unwrap().name;
    if message_queue.broker.is_none() {
        return Err(ClientError::new(
            ErrorKind::NoBrokerAvailable,
            "message queue do not have a available endpoint",
            operation,
        )
        .with_context("topic", topic)
        .with_context("queue_id", message_queue.id.to_string()));
    }

    let broker = message_queue.broker.clone().unwrap();
    if broker.endpoints.is_none() {
        return Err(ClientError::new(
            ErrorKind::NoBrokerAvailable,
            "message queue do not have a available endpoint",
            operation,
        )
        .with_context("broker", broker.name)
        .with_context("topic", topic)
        .with_context("queue_id", message_queue.id.to_string()));
    }

    Ok(Endpoints::from_pb_endpoints(broker.endpoints.unwrap()))
}

pub(crate) fn build_producer_settings(option: &ProducerOption) -> TelemetryCommand {
    let topics = option
        .topics()
        .clone()
        .unwrap_or_default()
        .iter()
        .map(|topic| Resource {
            name: topic.to_string(),
            resource_namespace: option.namespace().to_string(),
        })
        .collect();
    let platform = os_info::get();
    TelemetryCommand {
        command: Some(Command::Settings(Settings {
            client_type: Some(ClientType::Producer as i32),
            request_timeout: Some(prost_types::Duration {
                seconds: option.timeout().as_secs() as i64,
                nanos: option.timeout().subsec_nanos() as i32,
            }),
            pub_sub: Some(PubSub::Publishing(Publishing {
                topics,
                validate_message_type: option.validate_message_type(),
                ..Publishing::default()
            })),
            user_agent: Some(Ua {
                language: SDK_LANGUAGE as i32,
                version: SDK_VERSION.to_string(),
                platform: format!("{} {}", platform.os_type(), platform.version()),
                hostname: HOST_NAME.clone(),
            }),
            ..Settings::default()
        })),
        ..TelemetryCommand::default()
    }
}

pub(crate) fn build_simple_consumer_settings(option: &SimpleConsumerOption) -> TelemetryCommand {
    let platform = os_info::get();
    TelemetryCommand {
        command: Some(Command::Settings(Settings {
            client_type: Some(ClientType::SimpleConsumer as i32),
            request_timeout: Some(prost_types::Duration {
                seconds: option.timeout().as_secs() as i64,
                nanos: option.timeout().subsec_nanos() as i32,
            }),
            pub_sub: Some(PubSub::Subscription(Subscription {
                group: Some(Resource {
                    name: option.consumer_group().to_string(),
                    resource_namespace: option.namespace().to_string(),
                }),
                subscriptions: vec![],
                fifo: Some(false),
                receive_batch_size: None,
                long_polling_timeout: Some(prost_types::Duration {
                    seconds: option.long_polling_timeout().as_secs() as i64,
                    nanos: option.long_polling_timeout().subsec_nanos() as i32,
                }),
                lite_subscription_quota: None,
                max_lite_topic_size: None,
            })),
            user_agent: Some(Ua {
                language: SDK_LANGUAGE as i32,
                version: SDK_VERSION.to_string(),
                platform: format!("{} {}", platform.os_type(), platform.version()),
                hostname: HOST_NAME.clone(),
            }),
            ..Settings::default()
        })),
        ..TelemetryCommand::default()
    }
}

/// Build the settings of a `LiteSimpleConsumer`.
///
/// Same as [`build_simple_consumer_settings`] except that the client type is
/// `LITE_SIMPLE_CONSUMER` and the subscription table carries the bind topic with a `SUB_ALL`
/// filter expression (reference Java: `LiteSimpleConsumerBuilderImpl`).
pub(crate) fn build_lite_simple_consumer_settings(
    option: &SimpleConsumerOption,
    bind_topic: &str,
) -> TelemetryCommand {
    let platform = os_info::get();
    TelemetryCommand {
        command: Some(Command::Settings(Settings {
            client_type: Some(ClientType::LiteSimpleConsumer as i32),
            request_timeout: Some(prost_types::Duration {
                seconds: option.timeout().as_secs() as i64,
                nanos: option.timeout().subsec_nanos() as i32,
            }),
            pub_sub: Some(PubSub::Subscription(Subscription {
                group: Some(Resource {
                    name: option.consumer_group().to_string(),
                    resource_namespace: option.namespace().to_string(),
                }),
                subscriptions: vec![SubscriptionEntry {
                    topic: Some(Resource {
                        name: bind_topic.to_string(),
                        resource_namespace: option.namespace().to_string(),
                    }),
                    expression: Some(FilterExpression {
                        expression: crate::model::common::FilterExpression::sub_all()
                            .expression()
                            .to_string(),
                        r#type: crate::model::common::FilterExpression::sub_all().filter_type()
                            as i32,
                    }),
                }],
                fifo: Some(false),
                receive_batch_size: None,
                long_polling_timeout: Some(prost_types::Duration {
                    seconds: option.long_polling_timeout().as_secs() as i64,
                    nanos: option.long_polling_timeout().subsec_nanos() as i32,
                }),
                lite_subscription_quota: None,
                max_lite_topic_size: None,
            })),
            user_agent: Some(Ua {
                language: SDK_LANGUAGE as i32,
                version: SDK_VERSION.to_string(),
                platform: format!("{} {}", platform.os_type(), platform.version()),
                hostname: HOST_NAME.clone(),
            }),
            ..Settings::default()
        })),
        ..TelemetryCommand::default()
    }
}

pub(crate) fn build_push_consumer_settings(option: &PushConsumerOption) -> TelemetryCommand {
    let subscriptions: Vec<SubscriptionEntry> = option
        .subscription_expressions()
        .iter()
        .map(|(topic, filter_expression)| SubscriptionEntry {
            topic: Some(Resource {
                name: topic.to_string(),
                resource_namespace: option.namespace().to_string(),
            }),
            expression: Some(FilterExpression {
                expression: filter_expression.expression().to_string(),
                r#type: filter_expression.filter_type() as i32,
            }),
        })
        .collect();
    let platform = os_info::get();
    TelemetryCommand {
        command: Some(Command::Settings(Settings {
            client_type: Some(ClientType::PushConsumer as i32),
            request_timeout: Some(prost_types::Duration {
                seconds: option.timeout().as_secs() as i64,
                nanos: option.timeout().subsec_nanos() as i32,
            }),
            pub_sub: Some(PubSub::Subscription(Subscription {
                group: Some(Resource {
                    name: option.consumer_group().to_string(),
                    resource_namespace: option.namespace().to_string(),
                }),
                subscriptions,
                fifo: Some(option.fifo()),
                receive_batch_size: None,
                long_polling_timeout: Some(prost_types::Duration {
                    seconds: option.long_polling_timeout().as_secs() as i64,
                    nanos: option.long_polling_timeout().subsec_nanos() as i32,
                }),
                lite_subscription_quota: None,
                max_lite_topic_size: None,
            })),
            user_agent: Some(Ua {
                language: SDK_LANGUAGE as i32,
                version: SDK_VERSION.to_string(),
                platform: format!("{} {}", platform.os_type(), platform.version()),
                hostname: HOST_NAME.clone(),
            }),
            ..Settings::default()
        })),
        ..TelemetryCommand::default()
    }
}

pub fn handle_response_status(
    status: Option<Status>,
    operation: &'static str,
) -> Result<(), ClientError> {
    let status = status.ok_or(ClientError::new(
        ErrorKind::Server,
        "server do not return status, this may be a bug",
        operation,
    ))?;

    if status.code != Code::Ok as i32 {
        return Err(
            ClientError::new(ErrorKind::Server, "server return an error", operation)
                .with_context("code", format!("{}", status.code))
                .with_context("message", status.message),
        );
    }
    Ok(())
}

/// Handle status messages in receive message responses, similar to Java StatusChecker
/// This function handles status codes appropriately based on the context
pub fn handle_receive_message_status(
    status: &Status,
    operation: &'static str,
) -> Result<(), ClientError> {
    let code = match Code::from_i32(status.code) {
        Some(code) => code,
        None => {
            // Handle unrecognized status codes.
            tracing::warn!(
                "Unrecognized status code={}, statusMessage={}, operation={}",
                status.code,
                status.message,
                operation
            );
            return Err(
                ClientError::new(ErrorKind::Server, "unsupported status code", operation)
                    .with_context("code", format!("{}", status.code))
                    .with_context("message", status.message.clone()),
            );
        }
    };

    match code {
        // Unused, unrecognized status codes
        Code::Unspecified
        | Code::PreconditionFailed
        | Code::NotImplemented
        | Code::FailedToConsumeMessage => {
            Err(
                ClientError::new(ErrorKind::Server, "unsupported status code", operation)
                    .with_context("code", format!("{}", status.code))
                    .with_context("message", status.message.clone()),
            )
        }

        // OK and MULTIPLE_RESULTS are acceptable for receive message
        Code::Ok | Code::MultipleResults => Ok(()),
        // MESSAGE_NOT_FOUND is acceptable for receive message - no messages available
        // This is not an error, just indicates no new messages
        Code::MessageNotFound => Ok(()),

        Code::BadRequest
        | Code::IllegalAccessPoint
        | Code::IllegalTopic
        | Code::IllegalConsumerGroup
        | Code::IllegalMessageTag
        | Code::IllegalMessageKey
        | Code::IllegalMessageGroup
        | Code::IllegalMessagePropertyKey
        | Code::InvalidTransactionId
        | Code::IllegalMessageId
        | Code::IllegalFilterExpression
        | Code::IllegalInvisibleTime
        | Code::IllegalDeliveryTime
        | Code::InvalidReceiptHandle
        | Code::MessagePropertyConflictWithType
        | Code::UnrecognizedClientType
        | Code::MessageCorrupted
        | Code::ClientIdRequired
        | Code::IllegalPollingTime
        | Code::IllegalOffset
        | Code::IllegalLiteTopic => {
            Err(
                ClientError::new(ErrorKind::Config, "bad request", operation)
                    .with_context("code", format!("{}", status.code))
                    .with_context("message", status.message.clone()),
            )
        }
        Code::Unauthorized => Err(
            ClientError::new(ErrorKind::Server, "unauthorized", operation)
                .with_context("code", format!("{}", status.code))
                .with_context("message", status.message.clone()),
        ),
        Code::PaymentRequired => {
            Err(
                ClientError::new(ErrorKind::Server, "payment required", operation)
                    .with_context("code", format!("{}", status.code))
                    .with_context("message", status.message.clone()),
            )
        }
        Code::Forbidden => Err(ClientError::new(ErrorKind::Server, "forbidden", operation)
            .with_context("code", format!("{}", status.code))
            .with_context("message", status.message.clone())),
        Code::NotFound
        | Code::TopicNotFound
        | Code::ConsumerGroupNotFound
        | Code::OffsetNotFound => Err(ClientError::new(ErrorKind::Server, "not found", operation)
            .with_context("code", format!("{}", status.code))
            .with_context("message", status.message.clone())),
        Code::PayloadTooLarge | Code::MessageBodyTooLarge | Code::MessageBodyEmpty => Err(
            ClientError::new(ErrorKind::Server, "payload too large", operation)
                .with_context("code", format!("{}", status.code))
                .with_context("message", status.message.clone()),
        ),
        Code::TooManyRequests
        | Code::LiteTopicQuotaExceeded
        | Code::LiteSubscriptionQuotaExceeded => {
            Err(
                ClientError::new(ErrorKind::Server, "too many requests", operation)
                    .with_context("code", format!("{}", status.code))
                    .with_context("message", status.message.clone()),
            )
        }
        Code::RequestHeaderFieldsTooLarge | Code::MessagePropertiesTooLarge => {
            Err(ClientError::new(
                ErrorKind::Server,
                "request header fields too large",
                operation,
            )
            .with_context("code", format!("{}", status.code))
            .with_context("message", status.message.clone()))
        }
        Code::InternalError | Code::InternalServerError | Code::HaNotAvailable => Err(
            ClientError::new(ErrorKind::Server, "internal error", operation)
                .with_context("code", format!("{}", status.code))
                .with_context("message", status.message.clone()),
        ),
        Code::RequestTimeout
        | Code::ProxyTimeout
        | Code::MasterPersistenceTimeout
        | Code::SlavePersistenceTimeout => {
            Err(ClientError::new(ErrorKind::Server, "timeout", operation))
        }
        Code::Unsupported | Code::VersionUnsupported | Code::VerifyFifoMessageUnsupported => Err(
            ClientError::new(ErrorKind::Server, "unsupported", operation)
                .with_context("code", format!("{}", status.code))
                .with_context("message", status.message.clone()),
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;

    use crate::model::common::Route;
    use crate::pb;
    use crate::pb::{Broker, MessageQueue};

    use super::*;

    fn build_route() -> Arc<Route> {
        let message_queue_1 = MessageQueue {
            topic: None,
            id: 1,
            permission: 0,
            broker: None,
            accept_message_types: vec![],
        };

        let message_queue_2 = MessageQueue {
            topic: None,
            id: 2,
            permission: 0,
            broker: None,
            accept_message_types: vec![],
        };

        Arc::new(Route {
            index: AtomicUsize::new(0),
            queue: vec![message_queue_1, message_queue_2],
        })
    }

    #[test]
    fn util_select_message_queue() {
        let route = build_route();
        let message_queue = select_message_queue(route.clone());
        assert_eq!(message_queue.id, 1);
        let message_queue = select_message_queue(route.clone());
        assert_eq!(message_queue.id, 2);
        let message_queue = select_message_queue(route);
        assert_eq!(message_queue.id, 1);
    }

    #[test]
    fn util_select_message_queue_by_message_group() {
        let route = build_route();
        let message_queue =
            select_message_queue_by_message_group(route.clone(), "group1".to_string());
        assert_eq!(message_queue.id, 1);
        let message_queue =
            select_message_queue_by_message_group(route.clone(), "group1".to_string());
        assert_eq!(message_queue.id, 1);
        let message_queue =
            select_message_queue_by_message_group(route, "another_group".to_string());
        assert_eq!(message_queue.id, 2);
    }

    #[test]
    fn util_build_endpoints_by_message_queue() {
        let mut message_queue = MessageQueue {
            topic: Some(Resource {
                name: "topic".to_string(),
                resource_namespace: "".to_string(),
            }),
            id: 1,
            permission: 0,
            broker: Some(Broker {
                name: "".to_string(),
                id: 0,
                endpoints: Some(pb::Endpoints {
                    scheme: pb::AddressScheme::DomainName as i32,
                    addresses: vec![],
                }),
            }),
            accept_message_types: vec![],
        };
        let result = build_endpoints_by_message_queue(&message_queue, "test");
        assert!(result.is_ok());
        assert_eq!(result.unwrap().scheme(), pb::AddressScheme::DomainName);

        message_queue.broker = Some(Broker {
            name: "".to_string(),
            id: 0,
            endpoints: None,
        });
        let result = build_endpoints_by_message_queue(&message_queue, "test");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.kind, ErrorKind::NoBrokerAvailable);
        assert_eq!(error.operation, "test");
        assert_eq!(
            error.message,
            "message queue do not have a available endpoint"
        );
        assert_eq!(error.context.len(), 3);

        message_queue.broker.take();
        let result = build_endpoints_by_message_queue(&message_queue, "test");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.kind, ErrorKind::NoBrokerAvailable);
        assert_eq!(error.operation, "test");
        assert_eq!(
            error.message,
            "message queue do not have a available endpoint"
        );
        assert_eq!(error.context.len(), 2);
    }

    #[test]
    fn util_build_producer_settings() {
        build_producer_settings(&ProducerOption::default());
    }

    #[test]
    fn util_build_simple_consumer_settings() {
        build_simple_consumer_settings(&SimpleConsumerOption::default());
    }

    #[test]
    fn util_build_lite_simple_consumer_settings() {
        let mut option = SimpleConsumerOption::default();
        option.set_consumer_group("test_group");
        let settings = build_lite_simple_consumer_settings(&option, "parent_topic");
        match settings.command {
            Some(Command::Settings(settings)) => {
                assert_eq!(
                    settings.client_type,
                    Some(ClientType::LiteSimpleConsumer as i32)
                );
                match settings.pub_sub {
                    Some(PubSub::Subscription(subscription)) => {
                        assert_eq!(subscription.subscriptions.len(), 1);
                        assert_eq!(
                            subscription.subscriptions[0].topic.as_ref().unwrap().name,
                            "parent_topic"
                        );
                        let expression = subscription.subscriptions[0].expression.as_ref().unwrap();
                        assert_eq!(expression.expression, "*");
                    }
                    other => panic!("expect a subscription settings, got {:?}", other),
                }
            }
            other => panic!("expect a settings command, got {:?}", other),
        }
    }

    fn message_queue(id: i32, broker_id: i32, permission: crate::pb::Permission) -> MessageQueue {
        MessageQueue {
            topic: Some(Resource {
                name: "parent_topic".to_string(),
                resource_namespace: "".to_string(),
            }),
            id,
            permission: permission as i32,
            broker: Some(Broker {
                name: "broker-a".to_string(),
                id: broker_id,
                endpoints: Some(pb::Endpoints {
                    scheme: pb::AddressScheme::DomainName as i32,
                    addresses: vec![],
                }),
            }),
            accept_message_types: vec![],
        }
    }

    #[test]
    fn util_prune_lite_route_keeps_first_readable_master_queue() {
        let route = Arc::new(Route {
            index: Default::default(),
            queue: vec![
                message_queue(0, 1, crate::pb::Permission::ReadWrite), // slave
                message_queue(1, 0, crate::pb::Permission::Write),     // write only master
                message_queue(2, 0, crate::pb::Permission::ReadWrite), // first readable master
                message_queue(3, 0, crate::pb::Permission::ReadWrite),
            ],
        });
        let pruned = prune_lite_route(route, "parent_topic", "test").unwrap();
        assert_eq!(pruned.queue.len(), 1);
        assert_eq!(pruned.queue[0].id, 2);
    }

    #[test]
    fn util_prune_lite_route_fails_without_readable_queue() {
        let route = Arc::new(Route {
            index: Default::default(),
            queue: vec![message_queue(0, 0, crate::pb::Permission::None)],
        });
        let result = prune_lite_route(route, "parent_topic", "test");
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert_eq!(err.kind, ErrorKind::NoBrokerAvailable);

        let empty = Arc::new(Route {
            index: Default::default(),
            queue: vec![],
        });
        assert!(prune_lite_route(empty, "parent_topic", "test").is_err());
    }

    #[test]
    fn util_build_ack_message_entry_carries_lite_topic() {
        let message_view = crate::model::message::MessageView::from_pb_message(
            crate::pb::Message {
                topic: Some(Resource {
                    name: "parent_topic".to_string(),
                    ..Default::default()
                }),
                body: vec![1, 2, 3],
                user_properties: Default::default(),
                system_properties: Some(crate::pb::SystemProperties {
                    message_id: "lite_msg_id".to_string(),
                    receipt_handle: Some("receipt_handle".to_string()),
                    lite_topic: Some("lite_topic_001".to_string()),
                    ..Default::default()
                }),
            },
            Endpoints::from_url("localhost:8081").unwrap(),
        )
        .unwrap();

        let entry = build_ack_message_entry(&message_view);
        assert_eq!(entry.message_id, "lite_msg_id");
        assert_eq!(entry.receipt_handle, "receipt_handle");
        assert_eq!(entry.lite_topic, Some("lite_topic_001".to_string()));

        // A normal message must not carry a lite topic.
        let message_view = crate::model::message::MessageView::from_pb_message(
            crate::pb::Message {
                topic: Some(Resource {
                    name: "normal_topic".to_string(),
                    ..Default::default()
                }),
                body: vec![1, 2, 3],
                user_properties: Default::default(),
                system_properties: Some(crate::pb::SystemProperties {
                    message_id: "normal_msg_id".to_string(),
                    receipt_handle: Some("receipt_handle".to_string()),
                    ..Default::default()
                }),
            },
            Endpoints::from_url("localhost:8081").unwrap(),
        )
        .unwrap();
        assert_eq!(build_ack_message_entry(&message_view).lite_topic, None);
    }

    #[test]
    fn test_handle_response_status() {
        let result = handle_response_status(None, "test");
        assert!(result.is_err(), "should return error when status is None");
        let result = result.unwrap_err();
        assert_eq!(result.kind, ErrorKind::Server);
        assert_eq!(
            result.message,
            "server do not return status, this may be a bug"
        );
        assert_eq!(result.operation, "test");

        let result = handle_response_status(
            Some(Status {
                code: Code::BadRequest as i32,
                message: "test failed".to_string(),
            }),
            "test failed",
        );
        assert!(
            result.is_err(),
            "should return error when status is BadRequest"
        );
        let result = result.unwrap_err();
        assert_eq!(result.kind, ErrorKind::Server);
        assert_eq!(result.message, "server return an error");
        assert_eq!(result.operation, "test failed");
        assert_eq!(
            result.context,
            vec![
                ("code", format!("{}", Code::BadRequest as i32)),
                ("message", "test failed".to_string()),
            ]
        );

        let result = handle_response_status(
            Some(Status {
                code: Code::Ok as i32,
                message: "test success".to_string(),
            }),
            "test success",
        );
        assert!(result.is_ok(), "should not return error when status is Ok");
    }

    #[test]
    fn test_handle_receive_message_status() {
        // Test OK status
        let result = handle_receive_message_status(
            &Status {
                code: Code::Ok as i32,
                message: "OK".to_string(),
            },
            "test",
        );
        assert!(result.is_ok(), "should not return error when status is Ok");

        // Test MultipleResults status
        let result = handle_receive_message_status(
            &Status {
                code: Code::MultipleResults as i32,
                message: "Multiple results".to_string(),
            },
            "test",
        );
        assert!(
            result.is_ok(),
            "should not return error when status is MultipleResults"
        );

        // Test MessageNotFound status - should be OK for receive message
        let result = handle_receive_message_status(
            &Status {
                code: Code::MessageNotFound as i32,
                message: "no new message".to_string(),
            },
            "test",
        );
        assert!(
            result.is_ok(),
            "should not return error when status is MessageNotFound"
        );

        // Test BadRequest status
        let result = handle_receive_message_status(
            &Status {
                code: Code::BadRequest as i32,
                message: "bad request".to_string(),
            },
            "test",
        );
        assert!(
            result.is_err(),
            "should return error when status is BadRequest"
        );
        let result = result.unwrap_err();
        assert_eq!(result.kind, ErrorKind::Config);
        assert_eq!(result.message, "bad request");

        // Test NotFound status
        let result = handle_receive_message_status(
            &Status {
                code: Code::NotFound as i32,
                message: "not found".to_string(),
            },
            "test",
        );
        assert!(
            result.is_err(),
            "should return error when status is NotFound"
        );
        let result = result.unwrap_err();
        assert_eq!(result.kind, ErrorKind::Server);
        assert_eq!(result.message, "not found");

        // Test Unauthorized status
        let result = handle_receive_message_status(
            &Status {
                code: Code::Unauthorized as i32,
                message: "unauthorized".to_string(),
            },
            "test",
        );
        assert!(
            result.is_err(),
            "should return error when status is Unauthorized"
        );
        let result = result.unwrap_err();
        assert_eq!(result.kind, ErrorKind::Server);
        assert_eq!(result.message, "unauthorized");
    }
}
