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

use crate::conf::{ClientOption, ProducerOption, SimpleConsumerOption};
use siphasher::sip::SipHasher24;

use crate::error::{ClientError, ErrorKind};
use crate::model::common::{Endpoints, Route};
use crate::pb::settings::PubSub;
use crate::pb::telemetry_command::Command;
use crate::pb::{
    Language, MessageQueue, Publishing, Resource, Settings, Subscription, TelemetryCommand, Ua,
};

pub(crate) static SDK_LANGUAGE: Language = Language::Rust;
pub(crate) static SDK_VERSION: &str = "5.0.0";
pub(crate) static PROTOCOL_VERSION: &str = "2.0.0";

lazy_static::lazy_static! {
    pub(crate) static ref HOST_NAME: String = match hostname::get() {
        Ok(name) => name.to_str().unwrap_or("localhost").to_string(),
        Err(_) => "localhost".to_string(),
    };
}

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

pub(crate) fn build_producer_settings(
    option: &ProducerOption,
    client_options: &ClientOption,
) -> TelemetryCommand {
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
            client_type: Some(client_options.client_type.clone() as i32),
            request_timeout: Some(prost_types::Duration {
                seconds: client_options.timeout().as_secs() as i64,
                nanos: client_options.timeout().subsec_nanos() as i32,
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

pub(crate) fn build_simple_consumer_settings(
    option: &SimpleConsumerOption,
    client_option: &ClientOption,
) -> TelemetryCommand {
    let platform = os_info::get();
    TelemetryCommand {
        command: Some(Command::Settings(Settings {
            client_type: Some(client_option.client_type.clone() as i32),
            request_timeout: Some(prost_types::Duration {
                seconds: client_option.timeout().as_secs() as i64,
                nanos: client_option.timeout().subsec_nanos() as i32,
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
                    seconds: client_option.long_polling_timeout().as_secs() as i64,
                    nanos: client_option.long_polling_timeout().subsec_nanos() as i32,
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

#[cfg(test)]
mod tests {
    use crate::model::common::Route;
    use crate::pb;
    use crate::pb::{Broker, MessageQueue};
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;

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
        build_producer_settings(&ProducerOption::default(), &ClientOption::default());
    }

    #[test]
    fn util_build_simple_consumer_settings() {
        build_simple_consumer_settings(&SimpleConsumerOption::default(), &ClientOption::default());
    }
}
