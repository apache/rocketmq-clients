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
use std::clone::Clone;
use std::fmt::Debug;
use std::string::ToString;
use std::{collections::HashMap, sync::atomic::AtomicUsize, sync::Arc};

use mockall::automock;
use mockall_double::double;
use parking_lot::Mutex;
use prost_types::Duration;
use slog::{debug, error, info, o, warn, Logger};
use tokio::select;
use tokio::sync::{mpsc, oneshot};

use crate::conf::ClientOption;
use crate::error::{ClientError, ErrorKind};
use crate::model::common::{ClientType, Endpoints, Route, RouteStatus, SendReceipt};
use crate::model::message::{AckMessageEntry, MessageView};
use crate::model::transaction::{TransactionChecker, TransactionResolution};
use crate::pb;
use crate::pb::receive_message_response::Content;
use crate::pb::telemetry_command::Command::RecoverOrphanedTransactionCommand;
use crate::pb::{
    AckMessageRequest, AckMessageResultEntry, Code, EndTransactionRequest, FilterExpression,
    HeartbeatRequest, HeartbeatResponse, Message, MessageQueue, NotifyClientTerminationRequest,
    QueryRouteRequest, ReceiveMessageRequest, Resource, SendMessageRequest, Status,
    TelemetryCommand, TransactionSource,
};
#[double]
use crate::session::SessionManager;
use crate::session::{RPCClient, Session};

pub(crate) struct Client {
    logger: Logger,
    option: ClientOption,
    session_manager: Arc<SessionManager>,
    route_table: Mutex<HashMap<String /* topic */, RouteStatus>>,
    id: String,
    access_endpoints: Endpoints,
    settings: TelemetryCommand,
    transaction_checker: Option<Box<TransactionChecker>>,
    telemetry_command_tx: Option<mpsc::Sender<pb::telemetry_command::Command>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

lazy_static::lazy_static! {
    static ref CLIENT_ID_SEQUENCE: AtomicUsize = AtomicUsize::new(0);
}

const OPERATION_CLIENT_NEW: &str = "client.new";
const OPERATION_CLIENT_START: &str = "client.start";
const OPERATION_CLIENT_SHUTDOWN: &str = "client.shutdown";
const OPERATION_GET_SESSION: &str = "client.get_session";
const OPERATION_QUERY_ROUTE: &str = "client.query_route";
const OPERATION_HEARTBEAT: &str = "client.heartbeat";
const OPERATION_SEND_MESSAGE: &str = "client.send_message";
const OPERATION_RECEIVE_MESSAGE: &str = "client.receive_message";
const OPERATION_ACK_MESSAGE: &str = "client.ack_message";
const OPERATION_END_TRANSACTION: &str = "client.end_transaction";
const OPERATION_HANDLE_TELEMETRY_COMMAND: &str = "client.handle_telemetry_command";

impl Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("id", &self.id)
            .field("access_endpoints", &self.access_endpoints)
            .field("option", &self.option)
            .finish()
    }
}

#[automock]
impl Client {
    pub(crate) fn new(
        logger: &Logger,
        option: ClientOption,
        settings: TelemetryCommand,
    ) -> Result<Self, ClientError> {
        let id = Self::generate_client_id();
        let endpoints = Endpoints::from_url(option.access_url())
            .map_err(|e| e.with_operation(OPERATION_CLIENT_NEW))?;
        let session_manager = SessionManager::new(logger, id.clone(), &option);
        Ok(Client {
            logger: logger.new(o!("component" => "client")),
            option,
            session_manager: Arc::new(session_manager),
            route_table: Mutex::new(HashMap::new()),
            id,
            access_endpoints: endpoints,
            settings,
            transaction_checker: None,
            telemetry_command_tx: None,
            shutdown_tx: None,
        })
    }

    pub(crate) fn is_started(&self) -> bool {
        self.shutdown_tx.is_some()
    }

    pub(crate) fn has_transaction_checker(&self) -> bool {
        self.transaction_checker.is_some()
    }

    pub(crate) fn set_transaction_checker(&mut self, transaction_checker: Box<TransactionChecker>) {
        if self.is_started() {
            panic!("client {} is started, can not be modified", self.id)
        }
        self.transaction_checker = Some(transaction_checker);
    }

    pub(crate) async fn start(&mut self) -> Result<(), ClientError> {
        let logger = self.logger.clone();
        let session_manager = self.session_manager.clone();

        let group = self.option.group.clone();
        let namespace = self.option.namespace.to_string();
        let client_type = self.option.client_type.clone();

        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        self.shutdown_tx = Some(shutdown_tx);

        // send heartbeat and handle telemetry command
        let (telemetry_command_tx, mut telemetry_command_rx) = mpsc::channel(16);
        self.telemetry_command_tx = Some(telemetry_command_tx);
        let rpc_client = self
            .get_session()
            .await
            .map_err(|error| error.with_operation(OPERATION_CLIENT_START))?;
        let endpoints = self.access_endpoints.clone();
        let transaction_checker = self.transaction_checker.take();
        // give a placeholder
        if transaction_checker.is_some() {
            self.transaction_checker = Some(Box::new(|_, _| TransactionResolution::UNKNOWN));
        }

        tokio::spawn(async move {
            rpc_client.is_started();
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
            loop {
                select! {
                    _ = interval.tick() => {
                        let sessions = session_manager.get_all_sessions().await;
                        if sessions.is_err() {
                            error!(
                                logger,
                                "send heartbeat failed: failed to get sessions: {}",
                                sessions.unwrap_err()
                            );
                            continue;
                        }

                        for session in sessions.unwrap() {
                            let peer = session.peer().to_string();
                            let response = Self::heart_beat_inner(session, &group, &namespace, &client_type).await;
                            if response.is_err() {
                                error!(
                                    logger,
                                    "send heartbeat failed: failed to send heartbeat rpc: {}",
                                    response.unwrap_err()
                                );
                                continue;
                            }
                            let result =
                                Self::handle_response_status(response.unwrap().status, OPERATION_HEARTBEAT);
                            if result.is_err() {
                                error!(
                                    logger,
                                    "send heartbeat failed: server return error: {}",
                                    result.unwrap_err()
                                );
                                continue;
                            }
                            debug!(logger,"send heartbeat to server success, peer={}",peer);
                        }
                    },
                    command = telemetry_command_rx.recv() => {
                        if let Some(command) = command {
                            let result = Self::handle_telemetry_command(rpc_client.clone(), &transaction_checker, endpoints.clone(), command).await;
                            if let Err(error) = result {
                                error!(logger, "handle telemetry command failed: {:?}", error);
                            }
                        }
                    },
                    _ = &mut shutdown_rx => {
                        debug!(logger, "receive shutdown signal, stop heartbeat task and telemetry command handler");
                        break;
                    }
                }
            }
            info!(
                logger,
                "heartbeat task and telemetry command handler are stopped"
            );
        });
        Ok(())
    }

    fn check_started(&self, operation: &'static str) -> Result<(), ClientError> {
        if !self.is_started() {
            return Err(ClientError::new(
                ErrorKind::ClientIsNotRunning,
                "client is not started",
                operation,
            )
            .with_context("client_id", self.id.clone()));
        }
        Ok(())
    }

    pub(crate) async fn shutdown(mut self) -> Result<(), ClientError> {
        self.check_started(OPERATION_CLIENT_SHUTDOWN)?;
        let mut rpc_client = self.get_session().await?;
        self.telemetry_command_tx = None;
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        let group = self.option.group.as_ref().map(|group| Resource {
            name: group.to_string(),
            resource_namespace: self.option.namespace.to_string(),
        });
        let response = rpc_client.notify_shutdown(NotifyClientTerminationRequest { group });
        Self::handle_response_status(response.await?.status, OPERATION_CLIENT_SHUTDOWN)?;
        self.session_manager.shutdown().await;
        Ok(())
    }

    async fn handle_telemetry_command<T: RPCClient + 'static>(
        mut rpc_client: T,
        transaction_checker: &Option<Box<TransactionChecker>>,
        endpoints: Endpoints,
        command: pb::telemetry_command::Command,
    ) -> Result<(), ClientError> {
        return match command {
            RecoverOrphanedTransactionCommand(command) => {
                let transaction_id = command.transaction_id;
                let message = command.message.unwrap();
                let message_id = message
                    .system_properties
                    .as_ref()
                    .unwrap()
                    .message_id
                    .clone();
                let topic = message.topic.as_ref().unwrap().clone();
                if let Some(transaction_checker) = transaction_checker {
                    let resolution = transaction_checker(
                        transaction_id.clone(),
                        MessageView::from_pb_message(message, endpoints),
                    );

                    let response = rpc_client
                        .end_transaction(EndTransactionRequest {
                            topic: Some(topic),
                            message_id: message_id.to_string(),
                            transaction_id,
                            resolution: resolution as i32,
                            source: TransactionSource::SourceServerCheck as i32,
                            trace_context: "".to_string(),
                        })
                        .await?;
                    Self::handle_response_status(response.status, OPERATION_END_TRANSACTION)
                } else {
                    Err(ClientError::new(
                        ErrorKind::Config,
                        "failed to get transaction checker",
                        OPERATION_END_TRANSACTION,
                    ))
                }
            }
            _ => Err(ClientError::new(
                ErrorKind::Config,
                "receive telemetry command but there is no handler",
                OPERATION_HANDLE_TELEMETRY_COMMAND,
            )
            .with_context("command", format!("{:?}", command))),
        };
    }

    #[allow(dead_code)]
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

    pub(crate) async fn get_session(&self) -> Result<Session, ClientError> {
        self.check_started(OPERATION_GET_SESSION)?;
        let session = self
            .session_manager
            .get_or_create_session(
                &self.access_endpoints,
                self.settings.clone(),
                self.telemetry_command_tx.clone().unwrap(),
            )
            .await?;
        Ok(session)
    }

    async fn get_session_with_endpoints(
        &self,
        endpoints: &Endpoints,
    ) -> Result<Session, ClientError> {
        let session = self
            .session_manager
            .get_or_create_session(
                endpoints,
                self.settings.clone(),
                self.telemetry_command_tx.clone().unwrap(),
            )
            .await?;
        Ok(session)
    }

    pub(crate) fn handle_response_status(
        status: Option<Status>,
        operation: &'static str,
    ) -> Result<(), ClientError> {
        if status.is_none() {
            return Err(ClientError::new(
                ErrorKind::Server,
                "server do not return status, this may be a bug",
                operation,
            ));
        }

        let status = status.unwrap();
        let status_code = Code::from_i32(status.code).unwrap();
        if !status_code.eq(&Code::Ok) {
            return Err(
                ClientError::new(ErrorKind::Server, "server return an error", operation)
                    .with_context("code", status_code.as_str_name())
                    .with_context("message", status.message),
            );
        }
        Ok(())
    }

    pub(crate) fn topic_route_from_cache(&self, topic: &str) -> Option<Arc<Route>> {
        self.route_table.lock().get(topic).and_then(|route_status| {
            if let RouteStatus::Found(route) = route_status {
                // debug!(self.logger, "get route for topic={} from cache", topic);
                Some(Arc::clone(route))
            } else {
                None
            }
        })
    }

    #[allow(dead_code)]
    pub(crate) async fn topic_route(
        &self,
        topic: &str,
        lookup_cache: bool,
    ) -> Result<Arc<Route>, ClientError> {
        if lookup_cache {
            if let Some(route) = self.topic_route_from_cache(topic) {
                return Ok(route);
            }
        }
        let rpc_client = self.get_session().await?;
        self.topic_route_inner(rpc_client, topic).await
    }

    async fn query_topic_route<T: RPCClient + 'static>(
        &self,
        mut rpc_client: T,
        topic: &str,
    ) -> Result<Route, ClientError> {
        let request = QueryRouteRequest {
            topic: Some(Resource {
                name: topic.to_owned(),
                resource_namespace: self.option.namespace.to_string(),
            }),
            endpoints: Some(self.access_endpoints.inner().clone()),
        };

        let response = rpc_client.query_route(request).await?;
        Self::handle_response_status(response.status, OPERATION_QUERY_ROUTE)?;

        let route = Route {
            index: AtomicUsize::new(0),
            queue: response.message_queues,
        };
        Ok(route)
    }

    async fn topic_route_inner<T: RPCClient + 'static>(
        &self,
        rpc_client: T,
        topic: &str,
    ) -> Result<Arc<Route>, ClientError> {
        debug!(self.logger, "query route for topic={}", topic);
        let rx = match self
            .route_table
            .lock()
            .entry(topic.to_owned())
            .or_insert_with(|| RouteStatus::Querying(None))
        {
            RouteStatus::Found(_route) => None,
            RouteStatus::Querying(ref mut option) => {
                match option {
                    Some(vec) => {
                        // add self to waiting list
                        let (tx, rx) = oneshot::channel();
                        vec.push(tx);
                        Some(rx)
                    }
                    None => {
                        // there is no ongoing request, so we need to send a new request
                        let _ = option.insert(Vec::new());
                        None
                    }
                }
            }
        };

        // wait for inflight request
        if let Some(rx) = rx {
            return match rx.await {
                Ok(route) => route,
                Err(_e) => Err(ClientError::new(
                    ErrorKind::ChannelReceive,
                    "wait for inflight query topic route request failed",
                    OPERATION_QUERY_ROUTE,
                )),
            };
        }

        let result = self.query_topic_route(rpc_client, topic).await;

        // send result to all waiters
        if result.is_ok() {
            let route = result.unwrap();
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

            if let Some(RouteStatus::Querying(Some(mut v))) = prev {
                for item in v.drain(..) {
                    let _ = item.send(Ok(Arc::clone(&route)));
                }
            };
            Ok(route)
        } else {
            let err = result.unwrap_err();
            warn!(
                self.logger,
                "query route for topic={} failed: error={}", topic, err
            );
            let prev = self.route_table.lock().remove(topic);
            if let Some(RouteStatus::Querying(Some(mut v))) = prev {
                for item in v.drain(..) {
                    let _ = item.send(Err(ClientError::new(
                        ErrorKind::Server,
                        "query topic route failed",
                        OPERATION_QUERY_ROUTE,
                    )));
                }
            };
            Err(err)
        }
    }

    async fn heart_beat_inner<T: RPCClient + 'static>(
        mut rpc_client: T,
        group: &Option<String>,
        namespace: &str,
        client_type: &ClientType,
    ) -> Result<HeartbeatResponse, ClientError> {
        let group = group.as_ref().map(|group| Resource {
            name: group.to_string(),
            resource_namespace: namespace.to_string(),
        });
        let request = HeartbeatRequest {
            group,
            client_type: client_type.clone() as i32,
        };
        let response = rpc_client.heartbeat(request).await?;
        Ok(response)
    }

    #[allow(dead_code)]
    pub(crate) async fn send_message(
        &self,
        endpoints: &Endpoints,
        messages: Vec<Message>,
    ) -> Result<Vec<SendReceipt>, ClientError> {
        self.send_message_inner(
            self.get_session_with_endpoints(endpoints).await.unwrap(),
            messages,
        )
        .await
    }

    pub(crate) async fn send_message_inner<T: RPCClient + 'static>(
        &self,
        mut rpc_client: T,
        messages: Vec<Message>,
    ) -> Result<Vec<SendReceipt>, ClientError> {
        let request = SendMessageRequest { messages };
        let response = rpc_client.send_message(request).await?;
        Self::handle_response_status(response.status, OPERATION_SEND_MESSAGE)?;

        Ok(response
            .entries
            .iter()
            .map(SendReceipt::from_pb_send_result)
            .collect())
    }

    #[allow(dead_code)]
    pub(crate) async fn receive_message(
        &self,
        endpoints: &Endpoints,
        message_queue: MessageQueue,
        expression: FilterExpression,
        batch_size: i32,
        invisible_duration: Duration,
    ) -> Result<Vec<Message>, ClientError> {
        self.receive_message_inner(
            self.get_session_with_endpoints(endpoints).await.unwrap(),
            message_queue,
            expression,
            batch_size,
            invisible_duration,
        )
        .await
    }

    pub(crate) async fn receive_message_inner<T: RPCClient + 'static>(
        &self,
        mut rpc_client: T,
        message_queue: MessageQueue,
        expression: FilterExpression,
        batch_size: i32,
        invisible_duration: Duration,
    ) -> Result<Vec<Message>, ClientError> {
        let request = ReceiveMessageRequest {
            group: Some(Resource {
                name: self.option.group.as_ref().unwrap().to_string(),
                resource_namespace: self.option.namespace.to_string(),
            }),
            message_queue: Some(message_queue),
            filter_expression: Some(expression),
            batch_size,
            invisible_duration: Some(invisible_duration),
            auto_renew: false,
            long_polling_timeout: Some(
                Duration::try_from(*self.option.long_polling_timeout()).unwrap(),
            ),
        };
        let responses = rpc_client.receive_message(request).await?;

        let mut messages = Vec::with_capacity(batch_size as usize);
        for response in responses {
            match response.content.unwrap() {
                Content::Status(status) => {
                    if status.code() == Code::MessageNotFound {
                        return Ok(vec![]);
                    }
                    Self::handle_response_status(Some(status), OPERATION_RECEIVE_MESSAGE)?;
                }
                Content::Message(message) => {
                    messages.push(message);
                }
                Content::DeliveryTimestamp(_) => {}
            }
        }
        Ok(messages)
    }

    #[allow(dead_code)]
    pub(crate) async fn ack_message<T: AckMessageEntry + 'static>(
        &self,
        ack_entry: &T,
    ) -> Result<AckMessageResultEntry, ClientError> {
        let result = self
            .ack_message_inner(
                self.get_session_with_endpoints(ack_entry.endpoints())
                    .await
                    .unwrap(),
                ack_entry.topic(),
                vec![pb::AckMessageEntry {
                    message_id: ack_entry.message_id(),
                    receipt_handle: ack_entry.receipt_handle(),
                }],
            )
            .await?;
        Ok(result[0].clone())
    }

    pub(crate) async fn ack_message_inner<T: RPCClient + 'static>(
        &self,
        mut rpc_client: T,
        topic: String,
        entries: Vec<pb::AckMessageEntry>,
    ) -> Result<Vec<AckMessageResultEntry>, ClientError> {
        let request = AckMessageRequest {
            group: Some(Resource {
                name: self.option.group.as_ref().unwrap().to_string(),
                resource_namespace: self.option.namespace.to_string(),
            }),
            topic: Some(Resource {
                name: topic,
                resource_namespace: self.option.namespace.to_string(),
            }),
            entries,
        };
        let response = rpc_client.ack_message(request).await?;
        Self::handle_response_status(response.status, OPERATION_ACK_MESSAGE)?;
        Ok(response.entries)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread::sleep;
    use std::time::Duration;

    use lazy_static::lazy_static;

    use crate::client::Client;
    use crate::conf::ClientOption;
    use crate::error::{ClientError, ErrorKind};
    use crate::log::terminal_logger;
    use crate::model::common::{ClientType, Route};
    use crate::model::transaction::TransactionResolution;
    use crate::pb::receive_message_response::Content;
    use crate::pb::{
        AckMessageEntry, AckMessageResponse, Code, EndTransactionResponse, FilterExpression,
        HeartbeatResponse, Message, MessageQueue, QueryRouteResponse, ReceiveMessageResponse,
        Resource, SendMessageResponse, Status, SystemProperties, TelemetryCommand,
    };
    use crate::session;

    use super::*;

    lazy_static! {
        // The lock is used to prevent the mocking static function at same time during parallel testing.
        pub(crate) static ref MTX: Mutex<()> = Mutex::new(());
    }

    fn new_client_for_test() -> Client {
        Client {
            logger: terminal_logger(),
            option: ClientOption {
                group: Some("group".to_string()),
                ..Default::default()
            },
            session_manager: Arc::new(SessionManager::default()),
            route_table: Mutex::new(HashMap::new()),
            id: Client::generate_client_id(),
            access_endpoints: Endpoints::from_url("http://localhost:8081").unwrap(),
            settings: TelemetryCommand::default(),
            transaction_checker: None,
            telemetry_command_tx: None,
            shutdown_tx: None,
        }
    }

    fn new_client_with_session_manager(session_manager: SessionManager) -> Client {
        let (tx, _) = mpsc::channel(16);
        Client {
            logger: terminal_logger(),
            option: ClientOption::default(),
            session_manager: Arc::new(session_manager),
            route_table: Mutex::new(HashMap::new()),
            id: Client::generate_client_id(),
            access_endpoints: Endpoints::from_url("http://localhost:8081").unwrap(),
            settings: TelemetryCommand::default(),
            transaction_checker: None,
            telemetry_command_tx: Some(tx),
            shutdown_tx: None,
        }
    }

    #[test]
    fn client_id_sequence() {
        let v1 = CLIENT_ID_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let v2 = CLIENT_ID_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        assert!(v2 > v1, "Client ID sequence should be increasing");
    }

    #[test]
    fn client_new() -> Result<(), ClientError> {
        let ctx = crate::session::MockSessionManager::new_context();
        ctx.expect()
            .return_once(|_, _, _| SessionManager::default());
        Client::new(
            &terminal_logger(),
            ClientOption::default(),
            TelemetryCommand::default(),
        )?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn client_start() -> Result<(), ClientError> {
        let mut session_manager = SessionManager::default();
        session_manager
            .expect_get_all_sessions()
            .returning(|| Ok(vec![]));
        session_manager
            .expect_get_or_create_session()
            .returning(|_, _, _| Ok(Session::mock()));

        let mut client = new_client_with_session_manager(session_manager);
        client.start().await?;

        // TODO use countdown latch instead sleeping
        // wait for run
        tokio::time::sleep(Duration::from_secs(1)).await;
        Ok(())
    }

    #[tokio::test]
    async fn client_get_session() {
        let mut session_manager = SessionManager::default();
        session_manager
            .expect_get_or_create_session()
            .returning(|_, _, _| Ok(Session::mock()));

        let mut client = new_client_with_session_manager(session_manager);
        let _ = client.start().await;
        let result = client.get_session().await;
        assert!(result.is_ok());
        let result = client
            .get_session_with_endpoints(&Endpoints::from_url("localhost:8081").unwrap())
            .await;
        assert!(result.is_ok());
    }

    #[test]
    fn handle_response_status() {
        let result = Client::handle_response_status(None, "test");
        assert!(result.is_err(), "should return error when status is None");
        let result = result.unwrap_err();
        assert_eq!(result.kind, ErrorKind::Server);
        assert_eq!(
            result.message,
            "server do not return status, this may be a bug"
        );
        assert_eq!(result.operation, "test");

        let result = Client::handle_response_status(
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
                ("code", "BAD_REQUEST".to_string()),
                ("message", "test failed".to_string()),
            ]
        );

        let result = Client::handle_response_status(
            Some(Status {
                code: Code::Ok as i32,
                message: "test success".to_string(),
            }),
            "test success",
        );
        assert!(result.is_ok(), "should not return error when status is Ok");
    }

    pub(crate) fn new_topic_route_response() -> Result<QueryRouteResponse, ClientError> {
        Ok(QueryRouteResponse {
            status: Some(Status {
                code: Code::Ok as i32,
                message: "Success".to_string(),
            }),
            message_queues: vec![MessageQueue {
                topic: Some(Resource {
                    name: "DefaultCluster".to_string(),
                    resource_namespace: "default".to_string(),
                }),
                id: 0,
                permission: 0,
                broker: None,
                accept_message_types: vec![],
            }],
        })
    }

    #[tokio::test]
    async fn client_query_route_from_cache() {
        let client = new_client_for_test();
        client.route_table.lock().insert(
            "DefaultCluster".to_string(),
            RouteStatus::Found(Arc::new(Route {
                index: AtomicUsize::new(0),
                queue: vec![],
            })),
        );
        let option = client.topic_route_from_cache("DefaultCluster");
        assert!(option.is_some());
    }

    #[tokio::test]
    async fn client_query_route() {
        let client = new_client_for_test();

        let mut mock = session::MockRPCClient::new();
        mock.expect_query_route()
            .return_once(|_| Box::pin(futures::future::ready(new_topic_route_response())));

        let result = client.topic_route_inner(mock, "DefaultCluster").await;
        assert!(result.is_ok());

        let route = result.unwrap();
        assert!(!route.queue.is_empty());

        let topic = &route.queue[0].topic;
        assert!(topic.is_some());

        let topic = topic.clone().unwrap();
        assert_eq!(topic.name, "DefaultCluster");
        assert_eq!(topic.resource_namespace, "default");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn client_query_route_with_inflight_request() {
        let client = new_client_for_test();
        let client = Arc::new(client);

        let client_clone = client.clone();
        tokio::spawn(async move {
            let mut mock = session::MockRPCClient::new();
            mock.expect_query_route().return_once(|_| {
                sleep(Duration::from_millis(200));
                Box::pin(futures::future::ready(new_topic_route_response()))
            });

            let result = client_clone.topic_route_inner(mock, "DefaultCluster").await;
            assert!(result.is_ok());
        });

        let handle = tokio::spawn(async move {
            sleep(Duration::from_millis(100));
            let mock = session::MockRPCClient::new();
            let result = client.topic_route_inner(mock, "DefaultCluster").await;
            assert!(result.is_ok());
        });

        awaitility::at_most(Duration::from_secs(1)).until(|| handle.is_finished());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn client_query_route_with_failed_request() {
        let client = new_client_for_test();
        let client = Arc::new(client);

        let client_clone = client.clone();
        tokio::spawn(async move {
            let mut mock = session::MockRPCClient::new();
            mock.expect_query_route().return_once(|_| {
                sleep(Duration::from_millis(200));
                Box::pin(futures::future::ready(Err(ClientError::new(
                    ErrorKind::Server,
                    "server error",
                    "test",
                ))))
            });

            let result = client_clone.topic_route_inner(mock, "DefaultCluster").await;
            assert!(result.is_err());
        });

        let handle = tokio::spawn(async move {
            sleep(Duration::from_millis(100));
            let mock = session::MockRPCClient::new();
            let result = client.topic_route_inner(mock, "DefaultCluster").await;
            assert!(result.is_err());
        });

        awaitility::at_most(Duration::from_secs(1)).until(|| handle.is_finished());
    }

    #[tokio::test]
    async fn client_heartbeat() {
        let response = Ok(HeartbeatResponse {
            status: Some(Status {
                code: Code::Ok as i32,
                message: "Success".to_string(),
            }),
        });
        let mut mock = session::MockRPCClient::new();
        mock.expect_heartbeat()
            .return_once(|_| Box::pin(futures::future::ready(response)));

        let send_result =
            Client::heart_beat_inner(mock, &Some("group".to_string()), "", &ClientType::Producer)
                .await;
        assert!(send_result.is_ok());
    }

    #[tokio::test]
    async fn client_send_message() {
        let response = Ok(SendMessageResponse {
            status: Some(Status {
                code: Code::Ok as i32,
                message: "Success".to_string(),
            }),
            entries: vec![],
        });
        let mut mock = session::MockRPCClient::new();
        mock.expect_send_message()
            .return_once(|_| Box::pin(futures::future::ready(response)));

        let client = new_client_for_test();
        let send_result = client.send_message_inner(mock, vec![]).await;
        assert!(send_result.is_ok());

        let send_results = send_result.unwrap();
        assert_eq!(send_results.len(), 0);
    }

    #[tokio::test]
    async fn client_receive_message() {
        let response = Ok(vec![ReceiveMessageResponse {
            content: Some(Content::Message(Message::default())),
        }]);
        let mut mock = session::MockRPCClient::new();
        mock.expect_receive_message()
            .return_once(|_| Box::pin(futures::future::ready(response)));

        let client = new_client_for_test();
        let receive_result = client
            .receive_message_inner(
                mock,
                MessageQueue::default(),
                FilterExpression::default(),
                32,
                prost_types::Duration::default(),
            )
            .await;
        assert!(receive_result.is_ok());

        let messages = receive_result.unwrap();
        assert_eq!(messages.len(), 1);
    }

    #[tokio::test]
    async fn client_receive_message_failed() {
        let response = Ok(vec![ReceiveMessageResponse {
            content: Some(Content::Status(Status {
                code: Code::BadRequest as i32,
                message: "Unknown error".to_string(),
            })),
        }]);
        let mut mock = session::MockRPCClient::new();
        mock.expect_receive_message()
            .return_once(|_| Box::pin(futures::future::ready(response)));

        let client = new_client_for_test();
        let receive_result = client
            .receive_message_inner(
                mock,
                MessageQueue::default(),
                FilterExpression::default(),
                32,
                prost_types::Duration::default(),
            )
            .await;
        assert!(receive_result.is_err());

        let error = receive_result.unwrap_err();
        assert_eq!(error.kind, ErrorKind::Server);
        assert_eq!(error.message, "server return an error");
        assert_eq!(error.operation, "client.receive_message");
    }

    #[tokio::test]
    async fn client_ack_message() {
        let response = Ok(AckMessageResponse {
            status: Some(Status {
                code: Code::Ok as i32,
                message: "Success".to_string(),
            }),
            entries: vec![],
        });
        let mut mock = session::MockRPCClient::new();
        mock.expect_ack_message()
            .return_once(|_| Box::pin(futures::future::ready(response)));

        let client = new_client_for_test();
        let ack_entries: Vec<AckMessageEntry> = vec![];
        let ack_result = client
            .ack_message_inner(mock, "test_topic".to_string(), ack_entries)
            .await;
        assert!(ack_result.is_ok());
        assert_eq!(ack_result.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn client_ack_message_failed() {
        let response = Ok(AckMessageResponse {
            status: Some(Status {
                code: Code::BadRequest as i32,
                message: "Success".to_string(),
            }),
            entries: vec![],
        });
        let mut mock = session::MockRPCClient::new();
        mock.expect_ack_message()
            .return_once(|_| Box::pin(futures::future::ready(response)));

        let client = new_client_for_test();
        let ack_entries: Vec<AckMessageEntry> = vec![];
        let ack_result = client
            .ack_message_inner(mock, "test_topic".to_string(), ack_entries)
            .await;

        let error = ack_result.unwrap_err();
        assert_eq!(error.kind, ErrorKind::Server);
        assert_eq!(error.message, "server return an error");
        assert_eq!(error.operation, "client.ack_message");
    }

    #[tokio::test]
    async fn client_handle_telemetry_command() {
        let response = Ok(EndTransactionResponse {
            status: Some(Status {
                code: Code::Ok as i32,
                message: "".to_string(),
            }),
        });
        let mut mock = session::MockRPCClient::new();
        mock.expect_end_transaction()
            .return_once(|_| Box::pin(futures::future::ready(response)));
        let result = Client::handle_telemetry_command(
            mock,
            &Some(Box::new(|_, _| TransactionResolution::COMMIT)),
            Endpoints::from_url("localhost:8081").unwrap(),
            RecoverOrphanedTransactionCommand(pb::RecoverOrphanedTransactionCommand {
                message: Some(Message {
                    topic: Some(Resource::default()),
                    user_properties: Default::default(),
                    system_properties: Some(SystemProperties::default()),
                    body: vec![],
                }),
                transaction_id: "".to_string(),
            }),
        )
        .await;
        assert!(result.is_ok())
    }
}
