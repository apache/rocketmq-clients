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
use std::net::ToSocketAddrs;
use std::{collections::HashMap, sync::atomic::AtomicUsize, sync::Arc};

use parking_lot::Mutex;
use slog::{debug, info, o, Logger};
use tokio::sync::oneshot;

use crate::conf::ClientOption;
use crate::error::{ClientError, ErrorKind};
use crate::model::{Route, RouteStatus};
use crate::pb::{
    self, Code, Message, QueryRouteRequest, Resource, SendMessageRequest, SendResultEntry, Status,
};
use crate::session::{Session, SessionManager};

pub(crate) struct Client {
    logger: Logger,
    option: ClientOption,
    session_manager: SessionManager,
    route_table: Mutex<HashMap<String /* topic */, RouteStatus>>,
    id: String,
    endpoints: pb::Endpoints,
}

static CLIENT_ID_SEQUENCE: AtomicUsize = AtomicUsize::new(0);

impl Client {
    const OPERATION_CLIENT_NEW: &'static str = "client.new";
    const OPERATION_QUERY_ROUTE: &'static str = "client.query_route";
    const OPERATION_SEND_MESSAGE: &'static str = "client.send_message";

    pub(crate) fn new(logger: &Logger, option: ClientOption) -> Result<Self, ClientError> {
        let id = Self::generate_client_id();
        let mut endpoints = pb::Endpoints {
            scheme: pb::AddressScheme::IPv4 as i32,
            addresses: vec![],
        };

        let socket_addrs = option.access_url().to_socket_addrs().map_err(|e| {
            ClientError::new(
                ErrorKind::Config,
                "Failed to resolve access url.".to_string(),
                Self::OPERATION_CLIENT_NEW,
            )
            .with_context("access_url", option.access_url())
            .set_source(e)
        })?;

        for socket_addr in socket_addrs {
            if socket_addr.is_ipv4() {
                endpoints.scheme = pb::AddressScheme::IPv4 as i32;
            } else {
                endpoints.scheme = pb::AddressScheme::IPv6 as i32;
            }

            let addr = pb::Address {
                host: socket_addr.ip().to_string(),
                port: socket_addr.port() as i32,
            };
            endpoints.addresses.push(addr);
        }

        let session_manager = SessionManager::new(&logger);
        Ok(Client {
            logger: logger.new(o!("component" => "client")),
            option,
            session_manager,
            route_table: Mutex::new(HashMap::new()),
            id,
            endpoints,
        })
    }

    pub(crate) fn client_id(&self) -> &str {
        &self.id
    }

    fn generate_client_id() -> String {
        let host = match hostname::get() {
            Ok(name) => name,
            Err(_) => "localhost".into(),
        };

        let host = match host.into_string() {
            Ok(host) => host,
            Err(_) => String::from("localhost"),
        };

        format!(
            "{}@{}#{}",
            host,
            std::process::id(),
            CLIENT_ID_SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        )
    }

    fn sign(&self, metadata: &mut tonic::metadata::MetadataMap) {
        let _ = tonic::metadata::AsciiMetadataValue::try_from(&self.id).and_then(|v| {
            metadata.insert("x-mq-client-id", v);
            Ok(())
        });

        metadata.insert(
            "x-mq-language",
            tonic::metadata::AsciiMetadataValue::from_static("RUST"),
        );
        metadata.insert(
            "x-mq-client-version",
            tonic::metadata::AsciiMetadataValue::from_static("5.0.0"),
        );
        metadata.insert(
            "x-mq-protocol-version",
            tonic::metadata::AsciiMetadataValue::from_static("2.0.0"),
        );
    }

    async fn get_session(&self) -> Result<Session, ClientError> {
        // TODO: support multiple endpoints
        let endpoint = self.endpoints.addresses[0].clone();
        let url = if self.option.enable_tls() {
            format!("https://{}:{}", endpoint.host, endpoint.port)
        } else {
            format!("http://{}:{}", endpoint.host, endpoint.port)
        };
        Ok(self.session_manager.get_session(url).await?)
    }

    fn handle_response_status(
        status: Option<Status>,
        operation: &'static str,
    ) -> Result<(), ClientError> {
        if status.is_none() {
            return Err(ClientError::new(
                ErrorKind::Server,
                "Server do not return status, this may be a bug.".to_string(),
                operation,
            ));
        }

        let status = status.unwrap();
        let status_code = Code::from_i32(status.code).unwrap();
        if !status_code.eq(&Code::Ok) {
            return Err(ClientError::new(
                ErrorKind::Server,
                "Server return an error.".to_string(),
                operation,
            )
            .with_context("code", status_code.as_str_name())
            .with_context("message", status.message));
        }
        Ok(())
    }

    pub(crate) async fn topic_route(
        &self,
        topic: &str,
        lookup_cache: bool,
    ) -> Result<Arc<Route>, ClientError> {
        debug!(self.logger, "query route for topic={}", topic);
        let rx = match self
            .route_table
            .lock()
            .entry(topic.to_owned())
            .or_insert_with(|| RouteStatus::Querying(Vec::new()))
        {
            RouteStatus::Found(route) => {
                if lookup_cache {
                    return Ok(Arc::clone(route));
                }
                None
            }
            RouteStatus::Querying(ref mut v) => {
                if v.is_empty() {
                    None
                } else {
                    let (tx, rx) = oneshot::channel();
                    v.push(tx);
                    Some(rx)
                }
            }
        };

        if let Some(rx) = rx {
            return match rx.await {
                Ok(route) => route,
                Err(e) => Err(ClientError::new(
                    ErrorKind::ChannelReceive,
                    "Wait inflight query request failed.".to_string(),
                    Self::OPERATION_QUERY_ROUTE,
                )
                .set_source(e)),
            };
        }

        let request = QueryRouteRequest {
            topic: Some(Resource {
                name: topic.to_owned(),
                resource_namespace: self.option.name_space().to_string(),
            }),
            endpoints: Some(self.endpoints.clone()),
        };
        let mut request = tonic::Request::new(request);
        self.sign(request.metadata_mut());

        let mut session = self.get_session().await?;
        match session.get_rpc_client().query_route(request).await {
            Ok(response) => {
                let response = response.into_inner();
                Self::handle_response_status(response.status, Self::OPERATION_QUERY_ROUTE)?;

                let route = Route {
                    queue: response.message_queues,
                };
                debug!(
                    self.logger,
                    "query route for topic={} success: route={:?}", topic, route
                );
                let route = Arc::new(route);
                let prev = self
                    .route_table
                    .lock()
                    .insert(topic.to_owned(), RouteStatus::Found(Arc::clone(&route)));
                info!(self.logger, "update route for topic={}", topic);

                match prev {
                    Some(RouteStatus::Found(_)) => {}
                    Some(RouteStatus::Querying(mut v)) => {
                        for item in v.drain(..) {
                            let _ = item.send(Ok(Arc::clone(&route)));
                        }
                    }
                    None => {}
                };
                Ok(route)
            }
            Err(e) => {
                let error = ClientError::new(
                    ErrorKind::ClientInternal,
                    "Query topic route rpc failed.".to_string(),
                    Self::OPERATION_QUERY_ROUTE,
                )
                .set_source(e);
                Err(error)
            }
        }
    }

    pub(crate) async fn send_message(
        &self,
        message: Message,
    ) -> Result<SendResultEntry, ClientError> {
        if let Some(properties) = &message.system_properties {
            debug!(
                self.logger,
                "send for topic={:?} message_id={}", message.topic, properties.message_id
            );
        } else {
            return Err(ClientError::new(
                ErrorKind::ClientInternal,
                "Message do not have system properties.".to_string(),
                Self::OPERATION_SEND_MESSAGE,
            ));
        }

        let request = SendMessageRequest {
            messages: vec![message],
        };
        let mut request = tonic::Request::new(request);
        self.sign(request.metadata_mut());

        let mut session = self.get_session().await?;
        match session.get_rpc_client().send_message(request).await {
            Ok(response) => {
                let response = response.into_inner();
                Self::handle_response_status(response.status, Self::OPERATION_SEND_MESSAGE)?;

                let send_result = response.entries.get(0);
                match send_result {
                    Some(send_result) => Ok(send_result.clone()),
                    None => Err(ClientError::new(
                        ErrorKind::Server,
                        "Server do not return send result, this may be a bug.".to_string(),
                        Self::OPERATION_SEND_MESSAGE,
                    )),
                }
            }
            Err(e) => {
                let error = ClientError::new(
                    ErrorKind::ClientInternal,
                    "Send message rpc failed.".to_string(),
                    Self::OPERATION_SEND_MESSAGE,
                )
                .set_source(e);
                Err(error)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use slog::debug;

    use crate::client::Client;
    use crate::conf::ClientOption;
    use crate::log::terminal_logger;

    #[tokio::test]
    async fn test_client_query_route() {
        let logger = terminal_logger();
        let client = Client::new(&logger, ClientOption::default()).unwrap();
        match client.topic_route("DefaultCluster", true).await {
            Ok(route) => {
                debug!(logger, "route: {:?}", route);
            }
            Err(e) => {
                debug!(logger, "err: {:?}", e);
            }
        }
    }
}
