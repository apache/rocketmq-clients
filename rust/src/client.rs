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
use std::string::ToString;
use std::{collections::HashMap, sync::atomic::AtomicUsize, sync::Arc};

use mockall::automock;
use mockall_double::double;
use parking_lot::Mutex;
use prost_types::Duration;
use slog::{debug, error, info, o, warn, Logger};
use tokio::select;
use tokio::sync::oneshot;

use crate::conf::ClientOption;
use crate::error::{ClientError, ErrorKind};
use crate::model::common::{ClientType, Endpoints, Route, RouteStatus};
use crate::model::message::AckMessageEntry;
use crate::pb;
use crate::pb::receive_message_response::Content;
use crate::pb::{
    AckMessageRequest, AckMessageResultEntry, Code, FilterExpression, HeartbeatRequest,
    HeartbeatResponse, Message, MessageQueue, QueryRouteRequest, ReceiveMessageRequest, Resource,
    SendMessageRequest, SendResultEntry, Status, TelemetryCommand,
};
#[double]
use crate::session::SessionManager;
use crate::session::{RPCClient, Session};

#[derive(Debug)]
pub(crate) struct Client {
    logger: Logger,
    option: ClientOption,
    session_manager: Arc<SessionManager>,
    route_table: Mutex<HashMap<String /* topic */, RouteStatus>>,
    id: String,
    access_endpoints: Endpoints,
    settings: TelemetryCommand,
}

lazy_static::lazy_static! {
    static ref CLIENT_ID_SEQUENCE: AtomicUsize = AtomicUsize::new(0);
}

#[automock]
impl Client {
    const OPERATION_CLIENT_NEW: &'static str = "client.new";
    const OPERATION_QUERY_ROUTE: &'static str = "client.query_route";
    const OPERATION_HEARTBEAT: &'static str = "client.heartbeat";
    const OPERATION_SEND_MESSAGE: &'static str = "client.send_message";
    const OPERATION_RECEIVE_MESSAGE: &'static str = "client.receive_message";
    const OPERATION_ACK_MESSAGE: &'static str = "client.ack_message";

    pub(crate) fn new(
        logger: &Logger,
        option: ClientOption,
        settings: TelemetryCommand,
    ) -> Result<Self, ClientError> {
        let id = Self::generate_client_id();
        let endpoints = Endpoints::from_url(option.access_url())
            .map_err(|e| e.with_operation(Self::OPERATION_CLIENT_NEW))?;
        let session_manager = SessionManager::new(logger, id.clone(), &option);
        Ok(Client {
            logger: logger.new(o!("component" => "client")),
            option,
            session_manager: Arc::new(session_manager),
            route_table: Mutex::new(HashMap::new()),
            id,
            access_endpoints: endpoints,
            settings,
        })
    }

    pub(crate) fn start(&self) {
        let logger = self.logger.clone();
        let session_manager = self.session_manager.clone();

        let group = self.option.group.to_string();
        let namespace = self.option.namespace.to_string();
        let client_type = self.option.client_type.clone();

        tokio::spawn(async move {
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
                                Self::handle_response_status(response.unwrap().status, Self::OPERATION_HEARTBEAT);
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
                }
            }
        });
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

    async fn get_session(&self) -> Result<Session, ClientError> {
        let session = self
            .session_manager
            .get_or_create_session(&self.access_endpoints, self.settings.clone())
            .await?;
        Ok(session)
    }

    async fn get_session_with_endpoints(
        &self,
        endpoints: &Endpoints,
    ) -> Result<Session, ClientError> {
        let session = self
            .session_manager
            .get_or_create_session(endpoints, self.settings.clone())
            .await?;
        Ok(session)
    }

    fn handle_response_status(
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
                debug!(self.logger, "get route for topic={} from cache", topic);
                Some(Arc::clone(route))
            } else {
                None
            }
        })
    }

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
        self.topic_route_inner(self.get_session().await.unwrap(), topic)
            .await
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
        Self::handle_response_status(response.status, Self::OPERATION_QUERY_ROUTE)?;

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
                    Self::OPERATION_QUERY_ROUTE,
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
                        Self::OPERATION_QUERY_ROUTE,
                    )));
                }
            };
            Err(err)
        }
    }

    async fn heart_beat_inner<T: RPCClient + 'static>(
        mut rpc_client: T,
        group: &str,
        namespace: &str,
        client_type: &ClientType,
    ) -> Result<HeartbeatResponse, ClientError> {
        let request = HeartbeatRequest {
            group: Some(Resource {
                name: group.to_string(),
                resource_namespace: namespace.to_string(),
            }),
            client_type: client_type.clone() as i32,
        };
        let response = rpc_client.heartbeat(request).await?;
        Ok(response)
    }

    pub(crate) async fn send_message(
        &self,
        endpoints: &Endpoints,
        messages: Vec<Message>,
    ) -> Result<Vec<SendResultEntry>, ClientError> {
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
    ) -> Result<Vec<SendResultEntry>, ClientError> {
        let message_count = messages.len();
        let request = SendMessageRequest { messages };
        let response = rpc_client.send_message(request).await?;
        Self::handle_response_status(response.status, Self::OPERATION_SEND_MESSAGE)?;

        if response.entries.len() != message_count {
            error!(self.logger, "server do not return illegal send result, this may be a bug. except result count: {}, found: {}", response.entries.len(), message_count);
        }

        Ok(response.entries)
    }

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
                name: self.option.group.to_string(),
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
                    Self::handle_response_status(Some(status), Self::OPERATION_RECEIVE_MESSAGE)?;
                }
                Content::Message(message) => {
                    messages.push(message);
                }
                Content::DeliveryTimestamp(_) => {}
            }
        }
        Ok(messages)
    }

    pub(crate) async fn ack_message<T: AckMessageEntry + 'static>(
        &self,
        ack_entry: T,
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
                name: self.option.group.to_string(),
                resource_namespace: self.option.namespace.to_string(),
            }),
            topic: Some(Resource {
                name: topic,
                resource_namespace: self.option.namespace.to_string(),
            }),
            entries,
        };
        let response = rpc_client.ack_message(request).await?;
        Self::handle_response_status(response.status, Self::OPERATION_ACK_MESSAGE)?;
        Ok(response.entries)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread::sleep;
    use std::time::Duration;

    use crate::client::Client;
    use crate::conf::ClientOption;
    use crate::error::{ClientError, ErrorKind};
    use crate::log::terminal_logger;
    use crate::model::common::{ClientType, Route};
    use crate::pb::receive_message_response::Content;
    use crate::pb::{
        AckMessageEntry, AckMessageResponse, Code, FilterExpression, HeartbeatResponse, Message,
        MessageQueue, QueryRouteResponse, ReceiveMessageResponse, Resource, SendMessageResponse,
        Status, TelemetryCommand,
    };
    use crate::session;

    use super::*;

    fn new_client_for_test() -> Client {
        Client {
            logger: terminal_logger(),
            option: ClientOption::default(),
            session_manager: Arc::new(SessionManager::default()),
            route_table: Mutex::new(HashMap::new()),
            id: Client::generate_client_id(),
            access_endpoints: Endpoints::from_url("http://localhost:8081").unwrap(),
            settings: TelemetryCommand::default(),
        }
    }

    fn new_client_with_session_manager(session_manager: SessionManager) -> Client {
        Client {
            logger: terminal_logger(),
            option: ClientOption::default(),
            session_manager: Arc::new(session_manager),
            route_table: Mutex::new(HashMap::new()),
            id: Client::generate_client_id(),
            access_endpoints: Endpoints::from_url("http://localhost:8081").unwrap(),
            settings: TelemetryCommand::default(),
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

        let client = new_client_with_session_manager(session_manager);
        client.start();

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
            .returning(|_, _| Ok(Session::mock()));

        let client = new_client_with_session_manager(session_manager);
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

    fn new_topic_route_response() -> Result<QueryRouteResponse, ClientError> {
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

        let send_result = Client::heart_beat_inner(mock, "", "", &ClientType::Producer).await;
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
}
