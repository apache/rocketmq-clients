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
use std::collections::HashMap;

use async_trait::async_trait;
use mockall::automock;
use slog::{debug, info, o, Logger};
use tokio::sync::Mutex;
use tonic::transport::{Channel, Endpoint};

use crate::conf::ClientOption;
use crate::error::ErrorKind;
use crate::model::Endpoints;
use crate::pb::{QueryRouteRequest, QueryRouteResponse, SendMessageRequest, SendMessageResponse};
use crate::{error::ClientError, pb::messaging_service_client::MessagingServiceClient};

#[async_trait]
#[automock]
pub(crate) trait RPCClient {
    const OPERATION_QUERY_ROUTE: &'static str = "rpc.query_route";
    const OPERATION_SEND_MESSAGE: &'static str = "rpc.send_message";

    async fn query_route(
        &mut self,
        request: QueryRouteRequest,
    ) -> Result<QueryRouteResponse, ClientError>;
    async fn send_message(
        &mut self,
        request: SendMessageRequest,
    ) -> Result<SendMessageResponse, ClientError>;
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct Session {
    logger: Logger,
    client_id: String,
    stub: MessagingServiceClient<Channel>,
}

impl Session {
    const OPERATION_CREATE: &'static str = "session.create_session";

    const HTTP_SCHEMA: &'static str = "http";
    const HTTPS_SCHEMA: &'static str = "https";

    async fn new(
        logger: &Logger,
        endpoints: &Endpoints,
        client_id: String,
        option: &ClientOption,
    ) -> Result<Self, ClientError> {
        let peer = endpoints.access_url().clone();
        debug!(logger, "creating session, peer={}", peer);

        let mut channel_endpoints = Vec::new();
        for endpoint in endpoints.inner().addresses.clone() {
            channel_endpoints.push(Self::build_endpoint(endpoint.host, endpoint.port, option)?);
        }

        if channel_endpoints.is_empty() {
            return Err(ClientError::new(
                ErrorKind::Connect,
                "No endpoint available.".to_string(),
                Self::OPERATION_CREATE,
            )
            .with_context("peer", peer));
        }

        let channel = if channel_endpoints.len() == 1 {
            channel_endpoints[0].connect().await.map_err(|e| {
                ClientError::new(
                    ErrorKind::Connect,
                    "Failed to connect to peer.".to_string(),
                    Self::OPERATION_CREATE,
                )
                .set_source(e)
                .with_context("peer", peer)
            })?
        } else {
            Channel::balance_list(channel_endpoints.into_iter())
        };

        let stub = MessagingServiceClient::new(channel);

        info!(
            logger,
            "create session success, peer={}",
            endpoints.access_url()
        );

        Ok(Session {
            logger: logger.new(o!("component" => "session", "peer" => peer.to_owned())),
            client_id,
            stub,
        })
    }

    fn build_endpoint(
        host: String,
        port: i32,
        option: &ClientOption,
    ) -> Result<Endpoint, ClientError> {
        let url = if option.enable_tls() {
            format!("{}://{}:{}", Self::HTTPS_SCHEMA, host, port)
        } else {
            format!("{}://{}:{}", Self::HTTP_SCHEMA, host, port)
        };
        let endpoint = Endpoint::from_shared(url.clone())
            .map_err(|e| {
                ClientError::new(
                    ErrorKind::Connect,
                    "Failed to create channel endpoint.".to_string(),
                    Self::OPERATION_CREATE,
                )
                .set_source(e)
                .with_context("peer", url)
            })?
            // TODO tls config
            // .tls_config(tls)
            // .map_err(|e| {
            //     ClientError::new(
            //         ErrorKind::Connect,
            //         "Failed to configure TLS.".to_string(),
            //         OPERATION,
            //     )
            //     .set_source(e)
            //     .with_context("peer", &peer_addr)
            // })?
            .connect_timeout(std::time::Duration::from_secs(3))
            .tcp_nodelay(true);
        Ok(endpoint)
    }

    fn sign(&self, metadata: &mut tonic::metadata::MetadataMap) {
        // let _ = tonic::metadata::AsciiMetadataValue::try_from(&self.id).and_then(|v| {
        //     metadata.insert("x-mq-client-id", v);
        //     Ok(())
        // });

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

#[async_trait]
impl RPCClient for Session {
    async fn query_route(
        &mut self,
        request: QueryRouteRequest,
    ) -> Result<QueryRouteResponse, ClientError> {
        let mut request = tonic::Request::new(request);
        self.sign(request.metadata_mut());
        let response = self.stub.query_route(request).await.map_err(|e| {
            ClientError::new(
                ErrorKind::ClientInternal,
                "Query topic route rpc failed.".to_string(),
                Self::OPERATION_QUERY_ROUTE,
            )
            .set_source(e)
        })?;
        Ok(response.into_inner())
    }

    async fn send_message(
        &mut self,
        request: SendMessageRequest,
    ) -> Result<SendMessageResponse, ClientError> {
        let response = self.stub.send_message(request).await.map_err(|e| {
            ClientError::new(
                ErrorKind::ClientInternal,
                "Send message rpc failed.".to_string(),
                Self::OPERATION_SEND_MESSAGE,
            )
            .set_source(e)
        })?;
        Ok(response.into_inner())
    }
}

#[derive(Debug)]
pub(crate) struct SessionManager {
    logger: Logger,
    client_id: String,
    option: ClientOption,
    session_map: Mutex<HashMap<String, Session>>,
}

impl SessionManager {
    pub(crate) fn new(logger: &Logger, client_id: String, option: &ClientOption) -> Self {
        let logger = logger.new(o!("component" => "session_manager"));
        let session_map = Mutex::new(HashMap::new());
        SessionManager {
            logger,
            client_id,
            option: option.clone(),
            session_map,
        }
    }

    pub(crate) async fn get_session(&self, endpoints: &Endpoints) -> Result<Session, ClientError> {
        let mut session_map = self.session_map.lock().await;
        let access_url = endpoints.access_url().to_string();
        return if session_map.contains_key(&access_url) {
            Ok(session_map.get(&access_url).unwrap().clone())
        } else {
            let session = Session::new(
                &self.logger,
                endpoints,
                self.client_id.clone(),
                &self.option,
            )
            .await?;
            session_map.insert(access_url.clone(), session.clone());
            Ok(session)
        };
    }
}

#[cfg(test)]
mod tests {
    use crate::log::terminal_logger;
    use wiremock_grpc::generate;

    use super::*;

    generate!("apache.rocketmq.v2", RocketMQMockServer);

    #[tokio::test]
    async fn session_new() {
        let server = RocketMQMockServer::start_default().await;
        let logger = terminal_logger();
        let session = Session::new(
            &logger,
            &Endpoints::from_access_url(format!("localhost:{}", server.address().port())).unwrap(),
            "test_client".to_string(),
            &ClientOption::default(),
        )
        .await;
        debug!(logger, "session: {:?}", session);
    }

    #[tokio::test]
    async fn session_new_multi_addr() {
        let logger = terminal_logger();
        let session = Session::new(
            &logger,
            &Endpoints::from_access_url("127.0.0.1:8080,127.0.0.1:8081".to_string()).unwrap(),
            "test_client".to_string(),
            &ClientOption::default(),
        )
        .await;
        debug!(logger, "session: {:?}", session);
    }

    #[tokio::test]
    async fn session_manager_new() {
        let server = RocketMQMockServer::start_default().await;
        let logger = terminal_logger();
        let session_manager =
            SessionManager::new(&logger, "test_client".to_string(), &ClientOption::default());
        let session = session_manager
            .get_session(
                &Endpoints::from_access_url(format!("localhost:{}", server.address().port()))
                    .unwrap(),
            )
            .await
            .unwrap();
        debug!(logger, "session: {:?}", session);
    }
}
