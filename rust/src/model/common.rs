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

//! Common data model of RocketMQ rust client.

use std::hash::Hash;
use std::net::IpAddr;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use tokio::sync::oneshot;

use crate::error::{ClientError, ErrorKind};
use crate::pb::{self, Broker, Resource};
use crate::pb::{Address, AddressScheme};

#[derive(Debug, Clone)]
pub(crate) enum ClientType {
    Producer = 1,
    #[allow(dead_code)]
    PushConsumer = 2,
    SimpleConsumer = 3,
    #[allow(dead_code)]
    PullConsumer = 4,
}

#[derive(Debug)]
pub(crate) struct Route {
    pub(crate) index: AtomicUsize,
    pub queue: Vec<pb::MessageQueue>,
}

type InflightRequest = Option<Vec<oneshot::Sender<Result<Arc<Route>, ClientError>>>>;

#[derive(Debug)]
pub(crate) enum RouteStatus {
    Querying(InflightRequest),
    Found(Arc<Route>),
}

/// Access points for receive messages or querying topic routes.
#[derive(Debug, Clone)]
pub struct Endpoints {
    endpoint_url: String,
    scheme: AddressScheme,
    inner: pb::Endpoints,
}

impl Endpoints {
    const OPERATION_PARSE: &'static str = "endpoint.parse";

    const ENDPOINT_SEPARATOR: &'static str = ",";
    const ADDRESS_SEPARATOR: &'static str = ":";

    pub(crate) fn from_url(endpoint_url: &str) -> Result<Self, ClientError> {
        if endpoint_url.is_empty() {
            return Err(ClientError::new(
                ErrorKind::Config,
                "endpoint url is empty",
                Self::OPERATION_PARSE,
            )
            .with_context("url", endpoint_url));
        }

        let mut scheme = AddressScheme::DomainName;
        let mut urls = Vec::new();
        for url in endpoint_url.split(Self::ENDPOINT_SEPARATOR) {
            if let Some((host, port)) = url.rsplit_once(Self::ADDRESS_SEPARATOR) {
                let port_i32 = port.parse::<i32>().map_err(|e| {
                    ClientError::new(
                        ErrorKind::Config,
                        &format!("port {} in endpoint url is invalid", port),
                        Self::OPERATION_PARSE,
                    )
                    .with_context("url", endpoint_url)
                    .set_source(e)
                })?;
                urls.push((host.to_string(), port_i32));
            } else {
                return Err(ClientError::new(
                    ErrorKind::Config,
                    "port in endpoint url is missing",
                    Self::OPERATION_PARSE,
                )
                .with_context("url", endpoint_url));
            }
        }

        let mut addresses = Vec::new();
        let urls_len = urls.len();
        for (host, port) in urls.into_iter() {
            match host.parse::<IpAddr>() {
                Ok(ip_addr) => {
                    match ip_addr {
                        IpAddr::V4(_) => {
                            if scheme == AddressScheme::IPv6 {
                                return Err(ClientError::new(
                                    ErrorKind::Config,
                                    "multiple addresses not in the same schema",
                                    Self::OPERATION_PARSE,
                                )
                                .with_context("url", endpoint_url));
                            }
                            scheme = AddressScheme::IPv4
                        }
                        IpAddr::V6(_) => {
                            if scheme == AddressScheme::IPv4 {
                                return Err(ClientError::new(
                                    ErrorKind::Config,
                                    "multiple addresses not in the same schema",
                                    Self::OPERATION_PARSE,
                                )
                                .with_context("url", endpoint_url));
                            }
                            scheme = AddressScheme::IPv6
                        }
                    }
                    addresses.push(Address { host, port });
                }
                Err(_) => {
                    if urls_len > 1 {
                        return Err(ClientError::new(
                            ErrorKind::Config,
                            "multiple addresses not allowed in domain schema",
                            Self::OPERATION_PARSE,
                        )
                        .with_context("url", endpoint_url));
                    }
                    scheme = AddressScheme::DomainName;
                    addresses.push(Address { host, port });
                }
            }
        }

        Ok(Endpoints {
            endpoint_url: endpoint_url.to_string(),
            scheme,
            inner: pb::Endpoints {
                scheme: scheme as i32,
                addresses,
            },
        })
    }

    pub(crate) fn from_pb_endpoints(endpoints: pb::Endpoints) -> Self {
        let mut addresses = Vec::new();
        for address in endpoints.addresses.iter() {
            addresses.push(format!("{}:{}", address.host, address.port));
        }

        Endpoints {
            endpoint_url: addresses.join(Self::ENDPOINT_SEPARATOR),
            scheme: endpoints.scheme(),
            inner: endpoints,
        }
    }

    /// Get endpoint url
    pub fn endpoint_url(&self) -> &str {
        &self.endpoint_url
    }

    /// Get address scheme of endpoint
    pub fn scheme(&self) -> AddressScheme {
        self.scheme
    }

    pub(crate) fn inner(&self) -> &pb::Endpoints {
        &self.inner
    }

    #[allow(dead_code)]
    pub(crate) fn into_inner(self) -> pb::Endpoints {
        self.inner
    }
}

/// Filter type for message filtering.
///
/// RocketMQ allows to filter messages by tag or SQL.
#[derive(Clone, Copy, Debug)]
#[repr(i32)]
pub enum FilterType {
    /// Filter by tag
    Tag = 1,
    /// Filter by SQL
    Sql = 2,
}

/// Filter expression for message filtering.
#[derive(Clone, Debug)]
pub struct FilterExpression {
    filter_type: FilterType,
    expression: String,
}

impl FilterExpression {
    /// Create a new filter expression
    ///
    /// # Arguments
    ///
    /// * `filter_type` - set filter type
    /// * `expression` - set message tag or SQL query string
    pub fn new(filter_type: FilterType, expression: impl Into<String>) -> Self {
        FilterExpression {
            filter_type,
            expression: expression.into(),
        }
    }

    /// Get filter type
    pub fn filter_type(&self) -> FilterType {
        self.filter_type
    }

    /// Get message tag or SQL query string
    pub fn expression(&self) -> &str {
        &self.expression
    }
}

/// Send result returned by producer.
#[derive(Clone, Debug)]
pub struct SendReceipt {
    message_id: String,
    transaction_id: String,
}

impl SendReceipt {
    pub(crate) fn from_pb_send_result(entry: &pb::SendResultEntry) -> Self {
        SendReceipt {
            message_id: entry.message_id.clone(),
            transaction_id: entry.transaction_id.clone(),
        }
    }

    /// Get message id
    pub fn message_id(&self) -> &str {
        &self.message_id
    }

    /// Get transaction id
    pub fn transaction_id(&self) -> &str {
        &self.transaction_id
    }
}

#[derive(Debug)]
pub enum ConsumeResult {
    SUCCESS,
    FAILURE,
}

impl Eq for Resource {}

impl Hash for Resource {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.resource_namespace.hash(state);
    }
}

impl Eq for Broker {}

impl Hash for Broker {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct MessageQueue {
    pub(crate) id: i32,
    pub(crate) topic: Resource,
    pub(crate) broker: Broker,
    pub(crate) accept_message_types: Vec<i32>,
    pub(crate) permission: i32,
}

impl MessageQueue {
    pub(crate) fn from_pb_message_queue(
        message_queue: pb::MessageQueue,
    ) -> Result<Self, ClientError> {
        if let Some(broker) = message_queue.broker {
            if let Some(topic) = message_queue.topic {
                return Ok(Self {
                    id: message_queue.id,
                    topic,
                    broker,
                    accept_message_types: message_queue.accept_message_types,
                    permission: message_queue.permission,
                });
            }
        }
        Err(ClientError::new(
            ErrorKind::InvalidMessageQueue,
            "message queue is not valid.",
            "",
        ))
    }

    pub(crate) fn to_pb_message_queue(&self) -> pb::MessageQueue {
        pb::MessageQueue {
            id: self.id,
            topic: Some(self.topic.clone()),
            broker: Some(self.broker.clone()),
            accept_message_types: self.accept_message_types.clone(),
            permission: self.permission,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::error::ErrorKind;
    use crate::model::common::Endpoints;
    use crate::pb;
    use crate::pb::{Address, AddressScheme};

    #[test]
    fn parse_domain_endpoint_url() {
        let endpoints = Endpoints::from_url("localhost:8080").unwrap();
        assert_eq!(endpoints.endpoint_url(), "localhost:8080");
        assert_eq!(endpoints.scheme(), AddressScheme::DomainName);
        let inner = endpoints.into_inner();
        assert_eq!(inner.addresses.len(), 1);
        assert_eq!(inner.addresses[0].host, "localhost");
        assert_eq!(inner.addresses[0].port, 8080);
    }

    #[test]
    fn parse_ipv4_endpoint_url() {
        let endpoints = Endpoints::from_url("127.0.0.1:8080").unwrap();
        assert_eq!(endpoints.endpoint_url(), "127.0.0.1:8080");
        assert_eq!(endpoints.scheme(), AddressScheme::IPv4);
        let inner = endpoints.into_inner();
        assert_eq!(inner.addresses.len(), 1);
        assert_eq!(inner.addresses[0].host, "127.0.0.1");
        assert_eq!(inner.addresses[0].port, 8080);
    }

    #[test]
    fn parse_ipv6_endpoint_url() {
        let endpoints = Endpoints::from_url("::1:8080").unwrap();
        assert_eq!(endpoints.endpoint_url(), "::1:8080");
        assert_eq!(endpoints.scheme(), AddressScheme::IPv6);
        let inner = endpoints.into_inner();
        assert_eq!(inner.addresses.len(), 1);
        assert_eq!(inner.addresses[0].host, "::1");
        assert_eq!(inner.addresses[0].port, 8080);
    }

    #[test]
    fn parse_endpoint_url_failed() {
        let err = Endpoints::from_url("").err().unwrap();
        assert_eq!(err.kind, ErrorKind::Config);
        assert_eq!(err.operation, "endpoint.parse");
        assert_eq!(err.message, "endpoint url is empty");

        let err = Endpoints::from_url("localhost:<port>").err().unwrap();
        assert_eq!(err.kind, ErrorKind::Config);
        assert_eq!(err.operation, "endpoint.parse");
        assert_eq!(err.message, "port <port> in endpoint url is invalid");

        let err = Endpoints::from_url("localhost").err().unwrap();
        assert_eq!(err.kind, ErrorKind::Config);
        assert_eq!(err.operation, "endpoint.parse");
        assert_eq!(err.message, "port in endpoint url is missing");

        let err = Endpoints::from_url("127.0.0.1:8080,::1:8080")
            .err()
            .unwrap();
        assert_eq!(err.kind, ErrorKind::Config);
        assert_eq!(err.operation, "endpoint.parse");
        assert_eq!(err.message, "multiple addresses not in the same schema");

        let err = Endpoints::from_url("::1:8080,127.0.0.1:8080")
            .err()
            .unwrap();
        assert_eq!(err.kind, ErrorKind::Config);
        assert_eq!(err.operation, "endpoint.parse");
        assert_eq!(err.message, "multiple addresses not in the same schema");

        let err = Endpoints::from_url("localhost:8080,localhost:8081")
            .err()
            .unwrap();
        assert_eq!(err.kind, ErrorKind::Config);
        assert_eq!(err.operation, "endpoint.parse");
        assert_eq!(
            err.message,
            "multiple addresses not allowed in domain schema"
        );
    }

    #[test]
    fn parse_pb_endpoints() {
        let pb_endpoints = pb::Endpoints {
            scheme: AddressScheme::IPv4 as i32,
            addresses: vec![
                Address {
                    host: "localhost".to_string(),
                    port: 8080,
                },
                Address {
                    host: "127.0.0.1".to_string(),
                    port: 8081,
                },
            ],
        };

        let endpoints = Endpoints::from_pb_endpoints(pb_endpoints);
        assert_eq!(endpoints.endpoint_url(), "localhost:8080,127.0.0.1:8081");
        assert_eq!(endpoints.scheme(), AddressScheme::IPv4);
        assert_eq!(endpoints.into_inner().addresses.len(), 2);
    }
}
