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
use slog::{debug, error, info, o, Logger};
use tokio::sync::{mpsc, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::metadata::AsciiMetadataValue;
use tonic::transport::{Channel, Endpoint};

use crate::conf::ClientOption;
use crate::error::ErrorKind;
use crate::log::terminal_logger;
use crate::model::common::Endpoints;
use crate::pb::{
    AckMessageRequest, AckMessageResponse, HeartbeatRequest, HeartbeatResponse, QueryRouteRequest,
    QueryRouteResponse, ReceiveMessageRequest, ReceiveMessageResponse, SendMessageRequest,
    SendMessageResponse, TelemetryCommand,
};
use crate::util::{PROTOCOL_VERSION, SDK_LANGUAGE, SDK_VERSION};
use crate::{error::ClientError, pb::messaging_service_client::MessagingServiceClient};

#[async_trait]
#[automock]
pub(crate) trait RPCClient {
    const OPERATION_START: &'static str = "session.start";
    const OPERATION_UPDATE_SETTINGS: &'static str = "session.update_settings";

    const OPERATION_QUERY_ROUTE: &'static str = "rpc.query_route";
    const OPERATION_HEARTBEAT: &'static str = "rpc.heartbeat";
    const OPERATION_SEND_MESSAGE: &'static str = "rpc.send_message";
    const OPERATION_RECEIVE_MESSAGE: &'static str = "rpc.receive_message";
    const OPERATION_ACK_MESSAGE: &'static str = "rpc.ack_message";

    async fn query_route(
        &mut self,
        request: QueryRouteRequest,
    ) -> Result<QueryRouteResponse, ClientError>;
    async fn heartbeat(
        &mut self,
        request: HeartbeatRequest,
    ) -> Result<HeartbeatResponse, ClientError>;
    async fn send_message(
        &mut self,
        request: SendMessageRequest,
    ) -> Result<SendMessageResponse, ClientError>;
    async fn receive_message(
        &mut self,
        request: ReceiveMessageRequest,
    ) -> Result<Vec<ReceiveMessageResponse>, ClientError>;
    async fn ack_message(
        &mut self,
        request: AckMessageRequest,
    ) -> Result<AckMessageResponse, ClientError>;
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct Session {
    logger: Logger,
    client_id: String,
    option: ClientOption,
    endpoints: Endpoints,
    stub: MessagingServiceClient<Channel>,
    telemetry_tx: Box<Option<mpsc::Sender<TelemetryCommand>>>,
}

impl Session {
    const OPERATION_CREATE: &'static str = "session.create_session";

    const HTTP_SCHEMA: &'static str = "http";
    const HTTPS_SCHEMA: &'static str = "https";

    #[cfg(test)]
    pub(crate) fn mock() -> Self {
        Session {
            logger: terminal_logger(),
            client_id: "fake_id".to_string(),
            option: ClientOption::default(),
            endpoints: Endpoints::from_url("http://localhost:8081").unwrap(),
            stub: MessagingServiceClient::new(
                Channel::from_static("http://localhost:8081").connect_lazy(),
            ),
            telemetry_tx: Box::new(None),
        }
    }

    async fn new(
        logger: &Logger,
        endpoints: &Endpoints,
        client_id: String,
        option: &ClientOption,
    ) -> Result<Self, ClientError> {
        let peer = endpoints.endpoint_url().to_owned();

        let mut channel_endpoints = Vec::new();
        for endpoint in endpoints.inner().addresses.clone() {
            channel_endpoints.push(Self::build_endpoint(endpoint.host, endpoint.port, option)?);
        }

        if channel_endpoints.is_empty() {
            return Err(ClientError::new(
                ErrorKind::Connect,
                "no endpoint available",
                Self::OPERATION_CREATE,
            )
            .with_context("peer", peer.clone()));
        }

        let channel = if channel_endpoints.len() == 1 {
            channel_endpoints[0].connect().await.map_err(|e| {
                ClientError::new(
                    ErrorKind::Connect,
                    "failed to connect to peer",
                    Self::OPERATION_CREATE,
                )
                .set_source(e)
                .with_context("peer", peer.clone())
            })?
        } else {
            Channel::balance_list(channel_endpoints.into_iter())
        };

        let stub = MessagingServiceClient::new(channel);

        let logger = logger.new(o!("peer" => peer.clone()));
        info!(logger, "create session success");

        Ok(Session {
            logger,
            option: option.clone(),
            endpoints: endpoints.clone(),
            client_id,
            stub,
            telemetry_tx: Box::new(None),
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
                    "failed to create channel endpoint",
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
            .connect_timeout(option.timeout)
            .tcp_nodelay(true);
        Ok(endpoint)
    }

    pub(crate) fn peer(&self) -> &str {
        self.endpoints.endpoint_url()
    }

    fn sign<T>(&self, mut request: tonic::Request<T>) -> tonic::Request<T> {
        let metadata = request.metadata_mut();
        let _ = AsciiMetadataValue::try_from(&self.client_id)
            .map(|v| metadata.insert("x-mq-client-id", v));

        metadata.insert(
            "x-mq-language",
            AsciiMetadataValue::from_static(SDK_LANGUAGE.as_str_name()),
        );
        metadata.insert(
            "x-mq-client-version",
            AsciiMetadataValue::from_static(SDK_VERSION),
        );
        metadata.insert(
            "x-mq-protocol-version",
            AsciiMetadataValue::from_static(PROTOCOL_VERSION),
        );

        request.set_timeout(*self.option.timeout());
        request
    }

    pub(crate) async fn start(&mut self, settings: TelemetryCommand) -> Result<(), ClientError> {
        let (tx, rx) = mpsc::channel(16);
        tx.send(settings).await.map_err(|e| {
            ClientError::new(
                ErrorKind::ChannelSend,
                "failed to send telemetry command",
                Self::OPERATION_START,
            )
            .set_source(e)
        })?;

        let request = self.sign(tonic::Request::new(ReceiverStream::new(rx)));
        let response = self.stub.telemetry(request).await.map_err(|e| {
            ClientError::new(
                ErrorKind::ClientInternal,
                "send rpc telemetry failed",
                Self::OPERATION_START,
            )
            .set_source(e)
        })?;

        let logger = self.logger.clone();
        tokio::spawn(async move {
            let mut stream = response.into_inner();
            loop {
                // TODO handle server stream
                match stream.message().await {
                    Ok(Some(item)) => {
                        debug!(logger, "telemetry command: {:?}", item);
                    }
                    Ok(None) => {
                        debug!(logger, "request stream closed");
                        break;
                    }
                    Err(e) => {
                        error!(logger, "telemetry response error: {:?}", e);
                    }
                }
            }
        });
        let _ = self.telemetry_tx.insert(tx);
        debug!(self.logger, "start session success");
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn is_started(&self) -> bool {
        self.telemetry_tx.is_some()
    }

    #[allow(dead_code)]
    pub(crate) async fn update_settings(
        &mut self,
        settings: TelemetryCommand,
    ) -> Result<(), ClientError> {
        if !self.is_started() {
            return Err(ClientError::new(
                ErrorKind::ClientIsNotRunning,
                "session is not started",
                Self::OPERATION_UPDATE_SETTINGS,
            ));
        }

        if let Some(tx) = self.telemetry_tx.as_ref() {
            tx.send(settings).await.map_err(|e| {
                ClientError::new(
                    ErrorKind::ChannelSend,
                    "failed to send telemetry command",
                    Self::OPERATION_UPDATE_SETTINGS,
                )
                .set_source(e)
            })?;
        }
        Ok(())
    }
}

#[automock]
#[async_trait]
impl RPCClient for Session {
    async fn query_route(
        &mut self,
        request: QueryRouteRequest,
    ) -> Result<QueryRouteResponse, ClientError> {
        let request = self.sign(tonic::Request::new(request));
        let response = self.stub.query_route(request).await.map_err(|e| {
            ClientError::new(
                ErrorKind::ClientInternal,
                "send rpc query_route failed",
                Self::OPERATION_QUERY_ROUTE,
            )
            .set_source(e)
        })?;
        Ok(response.into_inner())
    }

    async fn heartbeat(
        &mut self,
        request: HeartbeatRequest,
    ) -> Result<HeartbeatResponse, ClientError> {
        let request = self.sign(tonic::Request::new(request));
        let response = self.stub.heartbeat(request).await.map_err(|e| {
            ClientError::new(
                ErrorKind::ClientInternal,
                "send rpc heartbeat failed",
                Self::OPERATION_HEARTBEAT,
            )
            .set_source(e)
        })?;
        Ok(response.into_inner())
    }

    async fn send_message(
        &mut self,
        request: SendMessageRequest,
    ) -> Result<SendMessageResponse, ClientError> {
        let request = self.sign(tonic::Request::new(request));
        let response = self.stub.send_message(request).await.map_err(|e| {
            ClientError::new(
                ErrorKind::ClientInternal,
                "send rpc send_message failed",
                Self::OPERATION_SEND_MESSAGE,
            )
            .set_source(e)
        })?;
        Ok(response.into_inner())
    }

    async fn receive_message(
        &mut self,
        request: ReceiveMessageRequest,
    ) -> Result<Vec<ReceiveMessageResponse>, ClientError> {
        let batch_size = request.batch_size;
        let mut request = self.sign(tonic::Request::new(request));
        request.set_timeout(*self.option.long_polling_timeout());
        let mut stream = self
            .stub
            .receive_message(request)
            .await
            .map_err(|e| {
                ClientError::new(
                    ErrorKind::ClientInternal,
                    "send rpc receive_message failed",
                    Self::OPERATION_RECEIVE_MESSAGE,
                )
                .set_source(e)
            })?
            .into_inner();

        let mut responses = Vec::with_capacity(batch_size as usize);
        while let Some(item) = stream.next().await {
            let response = item.map_err(|e| {
                ClientError::new(
                    ErrorKind::ClientInternal,
                    "receive message failed: error in reading stream",
                    Self::OPERATION_RECEIVE_MESSAGE,
                )
                .set_source(e)
            })?;
            responses.push(response);
        }
        Ok(responses)
    }

    async fn ack_message(
        &mut self,
        request: AckMessageRequest,
    ) -> Result<AckMessageResponse, ClientError> {
        let request = self.sign(tonic::Request::new(request));
        let response = self.stub.ack_message(request).await.map_err(|e| {
            ClientError::new(
                ErrorKind::ClientInternal,
                "send rpc ack_message failed",
                Self::OPERATION_ACK_MESSAGE,
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

#[automock]
impl SessionManager {
    pub(crate) fn new(logger: &Logger, client_id: String, option: &ClientOption) -> Self {
        let logger = logger.new(o!("component" => "session"));
        let session_map = Mutex::new(HashMap::new());
        SessionManager {
            logger,
            client_id,
            option: option.clone(),
            session_map,
        }
    }

    pub(crate) async fn get_or_create_session(
        &self,
        endpoints: &Endpoints,
        settings: TelemetryCommand,
    ) -> Result<Session, ClientError> {
        let mut session_map = self.session_map.lock().await;
        let endpoint_url = endpoints.endpoint_url().to_string();
        return if session_map.contains_key(&endpoint_url) {
            Ok(session_map.get(&endpoint_url).unwrap().clone())
        } else {
            let mut session = Session::new(
                &self.logger,
                endpoints,
                self.client_id.clone(),
                &self.option,
            )
            .await?;
            session.start(settings).await?;
            session_map.insert(endpoint_url.clone(), session.clone());
            Ok(session)
        };
    }

    pub(crate) async fn get_all_sessions(&self) -> Result<Vec<Session>, ClientError> {
        let session_map = self.session_map.lock().await;
        let mut sessions = Vec::new();
        for (_, session) in session_map.iter() {
            sessions.push(session.clone());
        }
        Ok(sessions)
    }
}

#[cfg(test)]
mod tests {
    use crate::conf::ProducerOption;
    use slog::debug;
    use wiremock_grpc::generate;

    use crate::log::terminal_logger;
    use crate::util::build_producer_settings;

    use super::*;

    generate!("apache.rocketmq.v2", RocketMQMockServer);

    #[tokio::test]
    async fn session_new() {
        let server = RocketMQMockServer::start_default().await;
        let logger = terminal_logger();
        let session = Session::new(
            &logger,
            &Endpoints::from_url(&format!("localhost:{}", server.address().port())).unwrap(),
            "test_client".to_string(),
            &ClientOption::default(),
        )
        .await;
        debug!(logger, "session: {:?}", session);
        assert!(session.is_ok());
    }

    #[tokio::test]
    async fn session_new_multi_addr() {
        let logger = terminal_logger();
        let session = Session::new(
            &logger,
            &Endpoints::from_url("127.0.0.1:8080,127.0.0.1:8081").unwrap(),
            "test_client".to_string(),
            &ClientOption::default(),
        )
        .await;
        debug!(logger, "session: {:?}", session);
        assert!(session.is_ok());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn session_start() {
        let mut server = RocketMQMockServer::start_default().await;
        server.setup(
            MockBuilder::when()
                //    👇 RPC prefix
                .path("/apache.rocketmq.v2.MessagingService/Telemetry")
                .then()
                .return_status(Code::Ok),
        );

        let logger = terminal_logger();
        let session = Session::new(
            &logger,
            &Endpoints::from_url(&format!("localhost:{}", server.address().port())).unwrap(),
            "test_client".to_string(),
            &ClientOption::default(),
        )
        .await;
        debug!(logger, "session: {:?}", session);
        assert!(session.is_ok());

        let mut session = session.unwrap();

        let result = session
            .start(build_producer_settings(
                &ProducerOption::default(),
                &ClientOption::default(),
            ))
            .await;
        assert!(result.is_ok());
        assert!(session.is_started());
    }

    #[tokio::test]
    async fn session_manager_new() {
        let mut server = RocketMQMockServer::start_default().await;
        server.setup(
            MockBuilder::when()
                //    👇 RPC prefix
                .path("/apache.rocketmq.v2.MessagingService/Telemetry")
                .then()
                .return_status(Code::Ok),
        );

        let logger = terminal_logger();
        let session_manager =
            SessionManager::new(&logger, "test_client".to_string(), &ClientOption::default());
        let session = session_manager
            .get_or_create_session(
                &Endpoints::from_url(&format!("localhost:{}", server.address().port())).unwrap(),
                build_producer_settings(&ProducerOption::default(), &ClientOption::default()),
            )
            .await
            .unwrap();
        debug!(logger, "session: {:?}", session);
    }
}
