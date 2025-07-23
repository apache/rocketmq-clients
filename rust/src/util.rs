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
        | Code::IllegalPollingTime => {
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
        Code::NotFound | Code::TopicNotFound | Code::ConsumerGroupNotFound => {
            Err(ClientError::new(ErrorKind::Server, "not found", operation)
                .with_context("code", format!("{}", status.code))
                .with_context("message", status.message.clone()))
        }
        Code::PayloadTooLarge | Code::MessageBodyTooLarge => {
            Err(
                ClientError::new(ErrorKind::Server, "payload too large", operation)
                    .with_context("code", format!("{}", status.code))
                    .with_context("message", status.message.clone()),
            )
        }
        Code::TooManyRequests => {
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
