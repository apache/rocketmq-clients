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
use std::net::IpAddr;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use tokio::sync::oneshot;

use crate::error::{ClientError, ErrorKind};
use crate::pb;
use crate::pb::{Address, AddressScheme, MessageQueue};

#[derive(Debug)]
pub struct Route {
    pub(crate) index: AtomicUsize,
    pub queue: Vec<MessageQueue>,
}

#[derive(Debug)]
pub(crate) enum RouteStatus {
    Querying(Option<Vec<oneshot::Sender<Result<Arc<Route>, ClientError>>>>),
    Found(Arc<Route>),
}

#[derive(Debug)]
pub(crate) struct Endpoints {
    endpoint_url: String,
    scheme: AddressScheme,
    inner: pb::Endpoints,
}

impl Endpoints {
    const OPERATION_PARSE: &'static str = "endpoint.parse";

    const ENDPOINT_SEPARATOR: &'static str = ",";
    const ADDRESS_SEPARATOR: &'static str = ":";

    pub fn from_url(endpoint_url: &str) -> Result<Self, ClientError> {
        if endpoint_url.is_empty() {
            return Err(ClientError::new(
                ErrorKind::Config,
                "Endpoint url is empty.",
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
                        &format!("Port {} in endpoint url is invalid.", port),
                        Self::OPERATION_PARSE,
                    )
                    .with_context("url", endpoint_url)
                    .set_source(e)
                })?;
                urls.push((host.to_string(), port_i32));
            } else {
                return Err(ClientError::new(
                    ErrorKind::Config,
                    "Port in endpoint url is missing.",
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
                                    "Multiple addresses not in the same schema.",
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
                                    "Multiple addresses not in the same schema.",
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
                            "Multiple addresses not allowed in domain schema.",
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

    pub fn endpoint_url(&self) -> &str {
        &self.endpoint_url
    }

    pub fn scheme(&self) -> AddressScheme {
        self.scheme
    }

    pub fn inner(&self) -> &pb::Endpoints {
        &self.inner
    }

    pub fn into_inner(self) -> pb::Endpoints {
        self.inner
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
        assert_eq!(err.message, "Endpoint url is empty.");

        let err = Endpoints::from_url("localhost:<port>").err().unwrap();
        assert_eq!(err.kind, ErrorKind::Config);
        assert_eq!(err.operation, "endpoint.parse");
        assert_eq!(err.message, "Port <port> in endpoint url is invalid.");

        let err = Endpoints::from_url("localhost").err().unwrap();
        assert_eq!(err.kind, ErrorKind::Config);
        assert_eq!(err.operation, "endpoint.parse");
        assert_eq!(err.message, "Port in endpoint url is missing.");

        let err = Endpoints::from_url("127.0.0.1:8080,::1:8080")
            .err()
            .unwrap();
        assert_eq!(err.kind, ErrorKind::Config);
        assert_eq!(err.operation, "endpoint.parse");
        assert_eq!(err.message, "Multiple addresses not in the same schema.");

        let err = Endpoints::from_url("::1:8080,127.0.0.1:8080")
            .err()
            .unwrap();
        assert_eq!(err.kind, ErrorKind::Config);
        assert_eq!(err.operation, "endpoint.parse");
        assert_eq!(err.message, "Multiple addresses not in the same schema.");

        let err = Endpoints::from_url("localhost:8080,localhost:8081")
            .err()
            .unwrap();
        assert_eq!(err.kind, ErrorKind::Config);
        assert_eq!(err.operation, "endpoint.parse");
        assert_eq!(
            err.message,
            "Multiple addresses not allowed in domain schema."
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
