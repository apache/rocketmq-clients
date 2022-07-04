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
use crate::command;
use crate::error;
use crate::pb::{self, QueryRouteRequest, Resource};
use crate::{error::ClientError, pb::messaging_service_client::MessagingServiceClient};
use parking_lot::Mutex;
use slog::info;
use slog::{debug, error, o, warn, Logger};
use std::{
    collections::HashMap,
    sync::Arc,
    sync::{atomic::AtomicUsize, Weak},
};
use tokio::sync::oneshot;
use tonic::transport::{Certificate, Channel, ClientTlsConfig};

#[derive(Debug, Clone)]
struct Session {
    stub: MessagingServiceClient<Channel>,
    logger: Logger,
}

impl Session {
    async fn new(endpoint: String, logger: &Logger) -> Result<Self, error::ClientError> {
        debug!(logger, "Creating session to {}", endpoint);
        let peer_addr = endpoint.clone();

        let tls = ClientTlsConfig::default();

        let channel = Channel::from_shared(endpoint)
            .map_err(|e| {
                error!(logger, "Failed to create channel. Cause: {:?}", e);
                error::ClientError::Connect
            })?
            .tls_config(tls)
            .map_err(|e| {
                error!(logger, "Failed to configure TLS. Cause: {:?}", e);
                error::ClientError::Connect
            })?
            .connect_timeout(std::time::Duration::from_secs(3))
            .tcp_nodelay(true)
            .connect()
            .await
            .map_err(|e| {
                error!(logger, "Failed to connect to {}. Cause: {:?}", peer_addr, e);
                error::ClientError::Connect
            })?;

        let stub = MessagingServiceClient::new(channel);

        Ok(Session {
            stub,
            logger: logger.new(o!("peer" => peer_addr)),
        })
    }

    async fn query_route(
        &mut self,
        request: tonic::Request<pb::QueryRouteRequest>,
    ) -> Result<tonic::Response<pb::QueryRouteResponse>, error::ClientError> {
        match self.stub.query_route(request).await {
            Ok(response) => {
                return Ok(response);
            }
            Err(e) => {
                error!(self.logger, "QueryRoute failed. Cause: {:?}", e);
                return Err(error::ClientError::ClientInternal);
            }
        }
    }
}

#[derive(Debug)]
struct SessionManager {
    logger: Logger,
    tx: tokio::sync::mpsc::Sender<command::Command>,
}

impl SessionManager {
    fn new(logger: Logger) -> Self {
        let (tx, mut rx) = tokio::sync::mpsc::channel(256);

        let submitter_logger = logger.new(o!("component" => "submitter"));
        tokio::spawn(async move {
            let mut session_map: HashMap<String, Session> = HashMap::new();
            loop {
                match rx.recv().await {
                    Some(command) => match command {
                        command::Command::QueryRoute { peer, request, tx } => {
                            if !session_map.contains_key(&peer) {
                                match Session::new(peer.clone(), &submitter_logger).await {
                                    Ok(session) => {
                                        session_map.insert(peer.clone(), session);
                                    }
                                    Err(e) => {
                                        error!(
                                            submitter_logger,
                                            "Failed to create session to {}. Cause: {:?}", peer, e
                                        );
                                        let _ = tx.send(Err(ClientError::Connect));
                                        continue;
                                    }
                                }
                            }

                            match session_map.get(&peer) {
                                Some(session) => {
                                    // Cloning Channel is cheap and encouraged
                                    // https://docs.rs/tonic/0.7.2/tonic/transport/struct.Channel.html#multiplexing-requests
                                    let mut session = session.clone();
                                    tokio::spawn(async move {
                                        let result = session.query_route(request).await;
                                        let _ = tx.send(result);
                                    });
                                }
                                None => {}
                            }
                        }
                    },
                    None => {
                        info!(submitter_logger, "Submit loop exit");
                        break;
                    }
                }
            }
        });

        SessionManager { logger, tx }
    }

    async fn route(
        &self,
        endpoint: &str,
        topic: &str,
        client: Weak<&Client>,
    ) -> Result<Route, error::ClientError> {
        let client = match client.upgrade() {
            Some(client) => client,
            None => {
                return Err(error::ClientError::ClientInternal);
            }
        };

        let request = QueryRouteRequest {
            topic: Some(Resource {
                name: topic.to_owned(),
                resource_namespace: client.arn.clone(),
            }),
            endpoints: Some(client.access_point.clone()),
        };

        let mut request = tonic::Request::new(request);
        client.sign(request.metadata_mut());

        let (tx1, rx1) = oneshot::channel();
        let command = command::Command::QueryRoute {
            peer: endpoint.to_owned(),
            request,
            tx: tx1,
        };

        match self.tx.send(command).await {
            Ok(_) => {}
            Err(e) => {
                error!(self.logger, "Failed to submit request");
            }
        }

        match rx1.await {
            Ok(result) => result.map(|_response| Route {}),
            Err(e) => {
                error!(self.logger, "oneshot channel error. Cause: {:?}", e);
                Err(ClientError::ClientInternal)
            }
        }
    }
}

#[derive(Debug)]
struct Route {}

#[derive(Debug)]
enum RouteStatus {
    Querying(Vec<oneshot::Sender<Result<Arc<Route>, error::ClientError>>>),
    Found(Arc<Route>),
}

#[derive(Debug)]
pub(crate) struct Client {
    session_manager: SessionManager,
    logger: Logger,
    route_table: Mutex<HashMap<String /* topic */, RouteStatus>>,
    arn: String,
    id: String,
    access_point: pb::Endpoints,
}

static CLIENT_ID_SEQUENCE: AtomicUsize = AtomicUsize::new(0);

impl Client {
    fn client_id() -> String {
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

    pub(crate) fn new(
        logger: Logger,
        access_url: impl std::net::ToSocketAddrs,
    ) -> Result<Self, error::ClientError> {
        let id = Self::client_id();
        let mut access_point = pb::Endpoints {
            scheme: pb::AddressScheme::IPv4 as i32,
            addresses: vec![],
        };

        for socket_addr in access_url
            .to_socket_addrs()
            .map_err(|e| error::ClientError::ClientInternal)?
        {
            if socket_addr.is_ipv4() {
                access_point.scheme = pb::AddressScheme::IPv4 as i32;
            } else {
                access_point.scheme = pb::AddressScheme::IPv6 as i32;
            }

            let addr = pb::Address {
                host: socket_addr.ip().to_string(),
                port: socket_addr.port() as i32,
            };
            access_point.addresses.push(addr);
        }

        Ok(Client {
            session_manager: SessionManager::new(logger.new(o!("component" => "session_manager"))),
            logger,
            route_table: Mutex::new(HashMap::new()),
            arn: String::from(""),
            id,
            access_point,
        })
    }

    async fn query_route(
        &self,
        topic: &str,
        lookup_cache: bool,
    ) -> Result<Arc<Route>, error::ClientError> {
        debug!(self.logger, "Query route for topic={}", topic);
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
            match rx.await {
                Ok(route) => {
                    return route;
                }
                Err(_e) => {
                    return Err(error::ClientError::ClientInternal);
                }
            }
        }

        let client = Arc::new(*&self);
        let client_weak = Arc::downgrade(&client);
        let endpoint = "https://127.0.0.1:8081";
        match self
            .session_manager
            .route(endpoint, topic, client_weak)
            .await
        {
            Ok(route) => {
                let route = Arc::new(route);
                let prev = self
                    .route_table
                    .lock()
                    .insert(topic.to_owned(), RouteStatus::Found(Arc::clone(&route)));

                match prev {
                    Some(RouteStatus::Found(_)) => {}
                    Some(RouteStatus::Querying(mut v)) => {
                        for item in v.drain(..) {
                            let _ = item.send(Ok(Arc::clone(&route)));
                        }
                    }
                    None => {}
                };
                return Ok(route);
            }
            Err(_e) => {
                let prev = self.route_table.lock().remove(topic);
                match prev {
                    Some(RouteStatus::Found(route)) => {
                        self.route_table
                            .lock()
                            .insert(topic.to_owned(), RouteStatus::Found(Arc::clone(&route)));
                        return Ok(route);
                    }
                    Some(RouteStatus::Querying(mut v)) => {
                        for tx in v.drain(..) {
                            let _ = tx.send(Err(error::ClientError::ClientInternal));
                        }
                        return Err(error::ClientError::ClientInternal);
                    }
                    None => {
                        return Err(error::ClientError::ClientInternal);
                    }
                };
            }
        }
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
}

#[cfg(test)]
mod tests {

    use super::*;
    use slog::Drain;

    #[test]
    fn test_client_id() {
        let mut set = std::collections::HashSet::new();
        (0..256).for_each(|_| {
            let id = Client::client_id();
            assert_eq!(false, set.contains(&id));
            set.insert(id);
        });
    }

    #[tokio::test]
    async fn test_session_manager_new() {
        let _session_manager = SessionManager::new(create_logger());
        drop(_session_manager);
    }

    fn create_logger() -> Logger {
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        slog::Logger::root(drain, o!())
    }
}
