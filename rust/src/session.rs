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

use async_trait::async_trait;
use mockall::{automock, mock};
use ring::hmac;
use slog::{debug, error, info, o, Logger};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::metadata::{AsciiMetadataValue, MetadataMap};
use tonic::transport::{Channel, Endpoint};

use crate::conf::ClientOption;
use crate::error::ErrorKind;
use crate::model::common::Endpoints;
use crate::pb::telemetry_command::Command;
use crate::pb::{
    AckMessageRequest, AckMessageResponse, ChangeInvisibleDurationRequest,
    ChangeInvisibleDurationResponse, EndTransactionRequest, EndTransactionResponse,
    ForwardMessageToDeadLetterQueueRequest, ForwardMessageToDeadLetterQueueResponse,
    HeartbeatRequest, HeartbeatResponse, NotifyClientTerminationRequest,
    NotifyClientTerminationResponse, QueryAssignmentRequest, QueryAssignmentResponse,
    QueryRouteRequest, QueryRouteResponse, ReceiveMessageRequest, ReceiveMessageResponse,
    SendMessageRequest, SendMessageResponse, TelemetryCommand,
};
use crate::util::{PROTOCOL_VERSION, SDK_LANGUAGE, SDK_VERSION};
use crate::{error::ClientError, pb::messaging_service_client::MessagingServiceClient};

const OPERATION_START: &str = "session.start";
const OPERATION_UPDATE_SETTINGS: &str = "session.update_settings";

const OPERATION_QUERY_ROUTE: &str = "rpc.query_route";
const OPERATION_HEARTBEAT: &str = "rpc.heartbeat";
const OPERATION_SEND_MESSAGE: &str = "rpc.send_message";
const OPERATION_RECEIVE_MESSAGE: &str = "rpc.receive_message";
const OPERATION_ACK_MESSAGE: &str = "rpc.ack_message";
const OPERATION_CHANGE_INVISIBLE_DURATION: &str = "rpc.change_invisible_duration";
const OPERATION_END_TRANSACTION: &str = "rpc.end_transaction";
const OPERATION_NOTIFY_CLIENT_TERMINATION: &str = "rpc.notify_client_termination";
const OPERATION_QUERY_ASSIGNMENT: &str = "rpc.query_assignment";
const OPERATION_FORWARD_TO_DEADLETTER_QUEUE: &str = "rpc.forward_to_deadletter_queue";

#[async_trait]
#[automock]
pub(crate) trait RPCClient {
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
    async fn change_invisible_duration(
        &mut self,
        request: ChangeInvisibleDurationRequest,
    ) -> Result<ChangeInvisibleDurationResponse, ClientError>;
    async fn end_transaction(
        &mut self,
        request: EndTransactionRequest,
    ) -> Result<EndTransactionResponse, ClientError>;
    async fn notify_shutdown(
        &mut self,
        request: NotifyClientTerminationRequest,
    ) -> Result<NotifyClientTerminationResponse, ClientError>;
    async fn query_assignment(
        &mut self,
        request: QueryAssignmentRequest,
    ) -> Result<QueryAssignmentResponse, ClientError>;
    async fn forward_to_deadletter_queue(
        &mut self,
        request: ForwardMessageToDeadLetterQueueRequest,
    ) -> Result<ForwardMessageToDeadLetterQueueResponse, ClientError>;
}

#[derive(Debug)]
pub(crate) struct Session {
    logger: Logger,
    client_id: String,
    option: ClientOption,
    endpoints: Endpoints,
    stub: MessagingServiceClient<Channel>,
    telemetry_tx: Option<mpsc::Sender<TelemetryCommand>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl Session {
    const OPERATION_CREATE: &'static str = "session.create_session";

    const HTTP_SCHEMA: &'static str = "http";
    const HTTPS_SCHEMA: &'static str = "https";

    pub(crate) fn shadow_session(&self) -> Self {
        Session {
            logger: self.logger.clone(),
            client_id: self.client_id.clone(),
            option: self.option.clone(),
            endpoints: self.endpoints.clone(),
            stub: self.stub.clone(),
            telemetry_tx: self.telemetry_tx.clone(),
            shutdown_tx: None,
        }
    }

    pub(crate) async fn new(
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
            telemetry_tx: None,
            shutdown_tx: None,
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

    fn sign<T>(&self, message: T) -> tonic::Request<T> {
        let mut request = tonic::Request::new(message);
        let metadata = request.metadata_mut();
        self.sign_without_timeout(metadata);
        request.set_timeout(*self.option.timeout());
        request
    }

    fn sign_without_timeout(&self, metadata: &mut MetadataMap) {
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

        let date_time_result = OffsetDateTime::now_local();
        let date_time = if let Ok(result) = date_time_result {
            result
        } else {
            OffsetDateTime::now_utc()
        };

        let date_time = date_time.format(&Rfc3339).unwrap();

        metadata.insert(
            "x-mq-date-time",
            AsciiMetadataValue::try_from(&date_time).unwrap(),
        );

        if let Some((access_key, access_secret)) =
            self.option.access_key().zip(self.option.secret_key())
        {
            let key = hmac::Key::new(
                hmac::HMAC_SHA1_FOR_LEGACY_USE_ONLY,
                access_secret.as_bytes(),
            );
            let signature = hmac::sign(&key, date_time.as_bytes());
            let signature = hex::encode(signature.as_ref());
            let authorization = format!(
                "MQv2-HMAC-SHA1 Credential={}, SignedHeaders=x-mq-date-time, Signature={}",
                access_key, signature
            );
            metadata.insert(
                "authorization",
                AsciiMetadataValue::try_from(authorization).unwrap(),
            );
        }
    }

    pub(crate) async fn start(
        &mut self,
        settings: TelemetryCommand,
        telemetry_command_tx: mpsc::Sender<Command>,
    ) -> Result<(), ClientError> {
        let (tx, rx) = mpsc::channel(16);
        tx.send(settings).await.map_err(|e| {
            ClientError::new(
                ErrorKind::ChannelSend,
                "failed to send telemetry command",
                OPERATION_START,
            )
            .set_source(e)
        })?;

        let mut request = tonic::Request::new(ReceiverStream::new(rx));
        self.sign_without_timeout(request.metadata_mut());
        let response = self.stub.telemetry(request).await.map_err(|e| {
            ClientError::new(
                ErrorKind::ClientInternal,
                "send rpc telemetry failed",
                OPERATION_START,
            )
            .set_source(e)
        })?;

        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        self.shutdown_tx = Some(shutdown_tx);

        let logger = self.logger.clone();
        tokio::spawn(async move {
            let mut stream = response.into_inner();
            loop {
                tokio::select! {
                    message = stream.message() => {
                        match message {
                            Ok(Some(item)) => {
                                debug!(logger, "receive telemetry command: {:?}", item);
                                if let Some(command) = item.command {
                                    _ = telemetry_command_tx.send(command).await;
                                }
                            }
                            Ok(None) => {
                                info!(logger, "telemetry command stream closed by server");
                                break;
                            }
                            Err(e) => {
                                error!(logger, "telemetry response error: {:?}", e);
                            }
                        }
                    }
                    _ = &mut shutdown_rx => {
                        info!(logger, "receive shutdown signal, stop dealing with telemetry command");
                        break;
                    }
                }
            }
        });
        let _ = self.telemetry_tx.insert(tx);
        debug!(self.logger, "start session success");
        Ok(())
    }

    pub(crate) fn shutdown(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }

    pub(crate) fn is_started(&self) -> bool {
        self.shutdown_tx.is_some()
    }

    pub(crate) async fn update_settings(
        &mut self,
        settings: TelemetryCommand,
    ) -> Result<(), ClientError> {
        if let Some(tx) = self.telemetry_tx.as_ref() {
            tx.send(settings).await.map_err(|e| {
                ClientError::new(
                    ErrorKind::ChannelSend,
                    "failed to send telemetry command",
                    OPERATION_UPDATE_SETTINGS,
                )
                .set_source(e)
            })?;
        }
        Ok(())
    }
}

#[async_trait]
impl RPCClient for Session {
    async fn query_route(
        &mut self,
        request: QueryRouteRequest,
    ) -> Result<QueryRouteResponse, ClientError> {
        let request = self.sign(request);
        let response = self.stub.query_route(request).await.map_err(|e| {
            ClientError::new(
                ErrorKind::ClientInternal,
                "send rpc query_route failed",
                OPERATION_QUERY_ROUTE,
            )
            .set_source(e)
        })?;
        Ok(response.into_inner())
    }

    async fn heartbeat(
        &mut self,
        request: HeartbeatRequest,
    ) -> Result<HeartbeatResponse, ClientError> {
        let request = self.sign(request);
        let response = self.stub.heartbeat(request).await.map_err(|e| {
            ClientError::new(
                ErrorKind::ClientInternal,
                "send rpc heartbeat failed",
                OPERATION_HEARTBEAT,
            )
            .set_source(e)
        })?;
        Ok(response.into_inner())
    }

    async fn send_message(
        &mut self,
        request: SendMessageRequest,
    ) -> Result<SendMessageResponse, ClientError> {
        let request = self.sign(request);
        let response = self.stub.send_message(request).await.map_err(|e| {
            ClientError::new(
                ErrorKind::ClientInternal,
                "send rpc send_message failed",
                OPERATION_SEND_MESSAGE,
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
        let mut request = self.sign(request);
        request.set_timeout(*self.option.long_polling_timeout());
        let mut stream = self
            .stub
            .receive_message(request)
            .await
            .map_err(|e| {
                ClientError::new(
                    ErrorKind::ClientInternal,
                    "send rpc receive_message failed",
                    OPERATION_RECEIVE_MESSAGE,
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
                    OPERATION_RECEIVE_MESSAGE,
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
        let request = self.sign(request);
        let response = self.stub.ack_message(request).await.map_err(|e| {
            ClientError::new(
                ErrorKind::ClientInternal,
                "send rpc ack_message failed",
                OPERATION_ACK_MESSAGE,
            )
            .set_source(e)
        })?;
        Ok(response.into_inner())
    }

    async fn change_invisible_duration(
        &mut self,
        request: ChangeInvisibleDurationRequest,
    ) -> Result<ChangeInvisibleDurationResponse, ClientError> {
        let request = self.sign(request);
        let response = self
            .stub
            .change_invisible_duration(request)
            .await
            .map_err(|e| {
                ClientError::new(
                    ErrorKind::ClientInternal,
                    "send rpc change_invisible_duration failed",
                    OPERATION_CHANGE_INVISIBLE_DURATION,
                )
                .set_source(e)
            })?;
        Ok(response.into_inner())
    }

    async fn end_transaction(
        &mut self,
        request: EndTransactionRequest,
    ) -> Result<EndTransactionResponse, ClientError> {
        let request = self.sign(request);
        let response = self.stub.end_transaction(request).await.map_err(|e| {
            ClientError::new(
                ErrorKind::ClientInternal,
                "send rpc end_transaction failed",
                OPERATION_END_TRANSACTION,
            )
            .set_source(e)
        })?;
        Ok(response.into_inner())
    }

    async fn notify_shutdown(
        &mut self,
        request: NotifyClientTerminationRequest,
    ) -> Result<NotifyClientTerminationResponse, ClientError> {
        let request = self.sign(request);
        let response = self
            .stub
            .notify_client_termination(request)
            .await
            .map_err(|e| {
                ClientError::new(
                    ErrorKind::ClientInternal,
                    "send rpc notify_client_termination failed",
                    OPERATION_NOTIFY_CLIENT_TERMINATION,
                )
                .set_source(e)
            })?;
        Ok(response.into_inner())
    }

    async fn query_assignment(
        &mut self,
        request: QueryAssignmentRequest,
    ) -> Result<QueryAssignmentResponse, ClientError> {
        let request = self.sign(request);
        let response = self.stub.query_assignment(request).await.map_err(|e| {
            ClientError::new(
                ErrorKind::ClientInternal,
                "send rpc query_assignment failed",
                OPERATION_QUERY_ASSIGNMENT,
            )
            .set_source(e)
        })?;
        Ok(response.into_inner())
    }

    async fn forward_to_deadletter_queue(
        &mut self,
        request: ForwardMessageToDeadLetterQueueRequest,
    ) -> Result<ForwardMessageToDeadLetterQueueResponse, ClientError> {
        let request = self.sign(request);
        let response = self
            .stub
            .forward_message_to_dead_letter_queue(request)
            .await
            .map_err(|e| {
                ClientError::new(
                    ErrorKind::ClientInternal,
                    "send rpc forward_to_deadletter_queue failed",
                    OPERATION_FORWARD_TO_DEADLETTER_QUEUE,
                )
                .set_source(e)
            })?;
        Ok(response.into_inner())
    }
}

mock! {
    #[derive(Debug)]
    pub(crate) Session {
        pub(crate) fn shadow_session(&self) -> Self;
        pub(crate) async fn new(
            logger: &Logger,
            endpoints: &Endpoints,
            client_id: String,
            option: &ClientOption,
        ) -> Result<Self, ClientError>;
        pub(crate) fn peer(&self) -> &str;
        pub(crate) fn is_started(&self) -> bool;
        pub(crate) async fn update_settings(
            &mut self,
            settings: TelemetryCommand,
        ) -> Result<(), ClientError>;
        pub(crate) async fn start(
            &mut self,
            settings: TelemetryCommand,
            telemetry_command_tx: mpsc::Sender<Command>,
        ) -> Result<(), ClientError>;
        pub(crate) fn shutdown(&mut self);
    }

    #[async_trait]
    impl RPCClient for Session {
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
        async fn change_invisible_duration(
            &mut self,
            request: ChangeInvisibleDurationRequest,
        ) -> Result<ChangeInvisibleDurationResponse, ClientError>;
        async fn end_transaction(
            &mut self,
            request: EndTransactionRequest,
        ) -> Result<EndTransactionResponse, ClientError>;
        async fn notify_shutdown(
            &mut self,
            request: NotifyClientTerminationRequest,
        ) -> Result<NotifyClientTerminationResponse, ClientError>;
        async fn query_assignment(
            &mut self,
            request: QueryAssignmentRequest,
        ) -> Result<QueryAssignmentResponse, ClientError>;
        async fn forward_to_deadletter_queue(
            &mut self,
            request: ForwardMessageToDeadLetterQueueRequest,
        ) -> Result<ForwardMessageToDeadLetterQueueResponse, ClientError>;
    }

}

#[cfg(test)]
mod tests {
    use slog::debug;
    use wiremock_grpc::generate;

    use crate::conf::ProducerOption;
    use crate::log::terminal_logger;
    use crate::util::build_producer_settings;

    use super::*;

    generate!("apache.rocketmq.v2", RocketMQMockServer);

    #[tokio::test]
    async fn session_new() {
        let server = RocketMQMockServer::start_default().await;
        let logger = terminal_logger();
        let mut client_option = ClientOption::default();
        client_option.set_enable_tls(false);
        let session = Session::new(
            &logger,
            &Endpoints::from_url(&format!("localhost:{}", server.address().port())).unwrap(),
            "test_client".to_string(),
            &client_option,
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
        let mut client_option = ClientOption::default();
        client_option.set_enable_tls(false);
        let session = Session::new(
            &logger,
            &Endpoints::from_url(&format!("localhost:{}", server.address().port())).unwrap(),
            "test_client".to_string(),
            &client_option,
        )
        .await;
        debug!(logger, "session: {:?}", session);
        assert!(session.is_ok());

        let mut session = session.unwrap();

        let (tx, _) = mpsc::channel(16);
        let result = session
            .start(build_producer_settings(&ProducerOption::default()), tx)
            .await;
        assert!(result.is_ok());
        assert!(session.is_started());
    }
}
