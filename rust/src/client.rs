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

use parking_lot::Mutex;
use slog::{debug, error, info, o, warn, Logger};
use tokio::sync::oneshot;

use crate::conf::ClientOption;
use crate::error::{ClientError, ErrorKind};
use crate::model::common::{Endpoints, Route, RouteStatus};
use crate::pb::{
    Code, Message, QueryRouteRequest, Resource, SendMessageRequest, SendResultEntry, Status,
};
use crate::session::{RPCClient, Session, SessionManager};

#[derive(Debug)]
pub(crate) struct Client {
    logger: Logger,
    option: ClientOption,
    session_manager: SessionManager,
    route_table: Mutex<HashMap<String /* topic */, RouteStatus>>,
    id: String,
    access_endpoints: Endpoints,
}

lazy_static::lazy_static! {
    static ref CLIENT_ID_SEQUENCE: AtomicUsize = AtomicUsize::new(0);
}

impl Client {
    const OPERATION_CLIENT_NEW: &'static str = "client.new";
    const OPERATION_QUERY_ROUTE: &'static str = "client.query_route";
    const OPERATION_SEND_MESSAGE: &'static str = "client.send_message";

    pub(crate) fn new(logger: &Logger, option: ClientOption) -> Result<Self, ClientError> {
        let id = Self::generate_client_id();
        let endpoints = Endpoints::from_url(option.access_url())
            .map_err(|e| e.with_operation(Self::OPERATION_CLIENT_NEW))?;
        let session_manager = SessionManager::new(logger, id.clone(), &option);
        Ok(Client {
            logger: logger.new(o!("component" => "client")),
            option,
            session_manager,
            route_table: Mutex::new(HashMap::new()),
            id,
            access_endpoints: endpoints,
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

    async fn get_session(&self) -> Result<Session, ClientError> {
        self.session_manager
            .get_session(&self.access_endpoints)
            .await
    }

    async fn get_session_with_endpoints(
        &self,
        endpoints: &Endpoints,
    ) -> Result<Session, ClientError> {
        self.session_manager.get_session(endpoints).await
    }

    fn handle_response_status(
        &self,
        status: Option<Status>,
        operation: &'static str,
    ) -> Result<(), ClientError> {
        if status.is_none() {
            return Err(ClientError::new(
                ErrorKind::Server,
                "Server do not return status, this may be a bug.",
                operation,
            ));
        }

        let status = status.unwrap();
        let status_code = Code::from_i32(status.code).unwrap();
        if !status_code.eq(&Code::Ok) {
            return Err(
                ClientError::new(ErrorKind::Server, "Server return an error.", operation)
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

    async fn query_topic_route(
        &self,
        mut rpc_client: impl RPCClient,
        topic: &str,
    ) -> Result<Route, ClientError> {
        let request = QueryRouteRequest {
            topic: Some(Resource {
                name: topic.to_owned(),
                resource_namespace: self.option.name_space().to_string(),
            }),
            endpoints: Some(self.access_endpoints.inner().clone()),
        };

        let response = rpc_client.query_route(request).await?;
        self.handle_response_status(response.status, Self::OPERATION_QUERY_ROUTE)?;

        let route = Route {
            index: AtomicUsize::new(0),
            queue: response.message_queues,
        };
        Ok(route)
    }

    async fn topic_route_inner(
        &self,
        rpc_client: impl RPCClient,
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
                    "Wait for inflight query topic route request failed.",
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
                        "Query route failed.",
                        Self::OPERATION_QUERY_ROUTE,
                    )));
                }
            };
            Err(err)
        }
    }

    pub(crate) async fn send_message(
        &self,
        messages: Vec<Message>,
        endpoints: &Endpoints,
    ) -> Result<Vec<SendResultEntry>, ClientError> {
        self.send_message_inner(
            self.get_session_with_endpoints(endpoints).await.unwrap(),
            messages,
        )
        .await
    }

    pub(crate) async fn send_message_inner(
        &self,
        mut rpc_client: impl RPCClient,
        messages: Vec<Message>,
    ) -> Result<Vec<SendResultEntry>, ClientError> {
        let message_count = messages.len();
        let request = SendMessageRequest { messages };
        let response = rpc_client.send_message(request).await?;
        self.handle_response_status(response.status, Self::OPERATION_SEND_MESSAGE)?;

        if response.entries.len() != message_count {
            error!(self.logger, "server do not return illegal send result, this may be a bug. except result count: {}, found: {}", response.entries.len(), message_count);
        }

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
    use crate::model::common::Route;
    use crate::pb::{
        Code, MessageQueue, QueryRouteResponse, Resource, SendMessageResponse, Status,
    };
    use crate::session;

    use super::CLIENT_ID_SEQUENCE;

    #[test]
    fn client_id_sequence() {
        let v1 = CLIENT_ID_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let v2 = CLIENT_ID_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        assert!(v2 > v1, "Client ID sequence should be increasing");
    }

    #[test]
    fn handle_response_status() {
        let client = Client::new(&terminal_logger(), ClientOption::default()).unwrap();

        let result = client.handle_response_status(None, "test");
        assert!(result.is_err(), "should return error when status is None");
        let result = result.unwrap_err();
        assert_eq!(result.kind, ErrorKind::Server);
        assert_eq!(
            result.message,
            "Server do not return status, this may be a bug."
        );
        assert_eq!(result.operation, "test");

        let result = client.handle_response_status(
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
        assert_eq!(result.message, "Server return an error.");
        assert_eq!(result.operation, "test failed");
        assert_eq!(
            result.context,
            vec![
                ("code", "BAD_REQUEST".to_string()),
                ("message", "test failed".to_string())
            ]
        );

        let result = client.handle_response_status(
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
        let logger = terminal_logger();
        let client = Client::new(&logger, ClientOption::default()).unwrap();
        client.route_table.lock().insert(
            "DefaultCluster".to_string(),
            super::RouteStatus::Found(Arc::new(Route {
                index: AtomicUsize::new(0),
                queue: vec![],
            })),
        );
        let option = client.topic_route_from_cache("DefaultCluster");
        assert!(option.is_some());
    }

    #[tokio::test]
    async fn client_query_route() {
        let logger = terminal_logger();
        let client = Client::new(&logger, ClientOption::default()).unwrap();

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
        let logger = terminal_logger();
        let client = Client::new(&logger, ClientOption::default()).unwrap();
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
        let logger = terminal_logger();
        let client = Client::new(&logger, ClientOption::default()).unwrap();
        let client = Arc::new(client);

        let client_clone = client.clone();
        tokio::spawn(async move {
            let mut mock = session::MockRPCClient::new();
            mock.expect_query_route().return_once(|_| {
                sleep(Duration::from_millis(200));
                Box::pin(futures::future::ready(Err(ClientError::new(
                    ErrorKind::Server,
                    "Server error",
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

        let client = Client::new(&terminal_logger(), ClientOption::default()).unwrap();
        let send_result = client.send_message_inner(mock, vec![]).await;
        assert!(send_result.is_ok());

        let send_results = send_result.unwrap();
        assert_eq!(send_results.len(), 0);
    }
}
