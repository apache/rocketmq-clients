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

use slog::{debug, info, o, Logger};
use tokio::sync::Mutex;
use tonic::transport::{Channel, ClientTlsConfig};

use crate::error::ErrorKind;
use crate::pb::{self, QueryRouteRequest};
use crate::{error::ClientError, pb::messaging_service_client::MessagingServiceClient};

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct Session {
    stub: MessagingServiceClient<Channel>,
    logger: Logger,
}

impl Session {
    async fn new(endpoint: String, logger: &Logger) -> Result<Self, ClientError> {
        const OPERATION: &str = "session.create_session";

        debug!(logger, "creating session to {}", endpoint);
        let peer_addr = endpoint.clone();
        let _tls = ClientTlsConfig::default();
        let channel = Channel::from_shared(endpoint)
            .map_err(|e| {
                ClientError::new(
                    ErrorKind::Connect,
                    "Failed to create channel.".to_string(),
                    OPERATION,
                )
                .set_source(e)
                .with_context("peer", &peer_addr)
            })?
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
            .tcp_nodelay(true)
            .connect()
            .await
            .map_err(|e| {
                ClientError::new(
                    ErrorKind::Connect,
                    "Failed to connect to peer.".to_string(),
                    OPERATION,
                )
                .set_source(e)
                .with_context("peer", &peer_addr)
            })?;
        info!(logger, "create session success, peer={}", peer_addr);

        let stub = MessagingServiceClient::new(channel);

        Ok(Session {
            stub,
            logger: logger.new(o!("component" => "session", "peer" => peer_addr)),
        })
    }

    pub(crate) fn get_rpc_client(&mut self) -> &mut MessagingServiceClient<Channel> {
        &mut self.stub
    }

    pub(crate) async fn query_route(
        &mut self,
        request: tonic::Request<QueryRouteRequest>,
    ) -> Result<tonic::Response<pb::QueryRouteResponse>, ClientError> {
        const OPERATION: &str = "query_route";
        match self.stub.query_route(request).await {
            Ok(response) => Ok(response),
            Err(e) => {
                let error = ClientError::new(
                    ErrorKind::ClientInternal,
                    "QueryRoute failed.".to_string(),
                    OPERATION,
                )
                .set_source(e);
                Err(error)
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct SessionManager {
    logger: Logger,
    session_map: Mutex<HashMap<String, Session>>,
}

impl SessionManager {
    pub(crate) fn new(logger: &Logger) -> Self {
        let logger = logger.new(o!("component" => "session_manager"));
        let session_map = Mutex::new(HashMap::new());
        SessionManager {
            logger,
            session_map,
        }
    }

    pub(crate) async fn get_session(&self, endpoint: String) -> Result<Session, ClientError> {
        let mut session_map = self.session_map.lock().await;
        return if session_map.contains_key(&endpoint) {
            Ok(session_map.get(&endpoint).unwrap().clone())
        } else {
            let session = Session::new(endpoint.clone(), &self.logger).await?;
            session_map.insert(endpoint.clone(), session.clone());
            Ok(session)
        };
    }
}

#[cfg(test)]
mod tests {
    use crate::log::terminal_logger;
    use crate::pb::Resource;

    use super::*;

    #[tokio::test]
    async fn test_session_new() {
        let logger = terminal_logger();
        let session = Session::new("http://localhost:8081".to_string(), &logger).await;
        debug!(logger, "session: {:?}", session);
    }

    #[tokio::test]
    async fn test_session_request() {
        let logger = terminal_logger();
        let session = Session::new("http://localhost:8081".to_string(), &logger).await;
        let request = QueryRouteRequest {
            topic: Some(Resource {
                name: "DefaultCluster".to_string(),
                resource_namespace: "".to_string(),
            }),
            endpoints: None,
        };
        let response = session
            .unwrap()
            .query_route(tonic::Request::new(request))
            .await
            .unwrap();
        debug!(logger, "response: {:?}", response);
    }

    #[tokio::test]
    async fn test_session_manager_new() {
        let logger = terminal_logger();
        let session_manager = SessionManager::new(&logger);
        let session = session_manager
            .get_session("http://localhost:8081".to_string())
            .await
            .unwrap();
        debug!(logger, "session: {:?}", session);
    }
}
