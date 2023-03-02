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
use std::any::Any;
use prost_types::Timestamp;
use slog::{error, o, Logger};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::RecvError;
use tonic::{Request, Response};
use crate::client::{Route, RouteStatus};
use crate::error::ClientError;
use crate::pb::{Message, MessageType, ReceiveMessageRequest, ReceiveMessageResponse, Resource, SendMessageRequest, SendMessageResponse, Settings, SubscriptionEntry, SystemProperties, TelemetryCommand};
use crate::selector::{QueueSelect, QueueSelector};
use crate::{client, command, error, models, pb};
use models::message_id::UNIQ_ID_GENERATOR;
use crate::consumer_selector::{ConsumeQueueSelector, ConsumerQueueSelect};
use crate::models::message_view::MessageView;
use crate::pb::receive_message_response::Content;
use crate::pb::settings::PubSub::Subscription;
use crate::pb::telemetry_command::Command;
use tokio_stream::{self as stream, StreamExt};
use crate::pb::ClientType::SimpleConsumer;

struct Consumer {
    topic: String,
    group: String,
    client: client::Client,
    selector: ConsumeQueueSelector,
    logger: Logger,
}

impl Consumer {
    pub(crate) async fn sendClientTelemetry(&self) -> Result<TelemetryCommand, ClientError> {
        let client = Arc::new(&self.client);
        let client_weak = Arc::downgrade(&client);
        let client = match client_weak.upgrade() {
            Some(client) => client,
            None => {
                return Err(error::ClientError::ClientInternal);
            }
        };

        let guard = client.route_table.lock();
        let rxx = guard.get(self.topic.as_str()).unwrap().clone();
        let curr: Route;
        match rxx {
            RouteStatus::Found(route) => {
                curr = Route::new(route.message_queue_impls.clone());
            }
            _ => {
                return Err(error::ClientError::ClientInternal);
            }
        }

        let (tx1, rx1) = oneshot::channel();

        let message_queue = self.selector.select(
            curr.message_queue_impls,
        );

        let broker = message_queue.and_then(|mq| mq.broker.clone());
        let peer = broker.and_then(|br| br.endpoints);
        let mut address = peer.unwrap().addresses;
        let address_pop = address.pop().unwrap();
        let host = address_pop.host;
        let port = address_pop.port;
        let s = format!("http://{}:{}", host, port);

        let mut subscriptions = vec![];
        subscriptions.push(SubscriptionEntry {
            topic: Some(Resource {
                resource_namespace: "".to_string(),
                name: self.topic.clone(),
            }),
            expression: None,
        });
        let sett = TelemetryCommand {
            status: None,
            command: Some(Command::Settings(pb::Settings {
                client_type: Some(i32::from(SimpleConsumer)),
                access_point: None,
                backoff_policy: None,
                request_timeout: Some(prost_types::Duration::from(Duration::from_secs(10))),
                user_agent: None,
                metric: None,
                pub_sub: Some(Subscription(pb::Subscription {
                    group: Some(Resource {
                        resource_namespace: "".to_string(),
                        name: self.group.clone(),
                    }),
                    subscriptions: subscriptions,
                    fifo: None,
                    receive_batch_size: None,
                    long_polling_timeout: None,
                })),
            })),
        };
        let mut new_request = Request::new(stream::iter(vec![sett]));
        // copy metadata from request to new_request
        client.sign(new_request.metadata_mut());
        let telemetry = command::Command::SendClientTelemetry {
            peer: s,
            request: new_request,
            tx: tx1,
        };

        match self.client.fetch_session_manager().tx.send(telemetry).await {
            Ok(_) => {}
            Err(e) => {
                error!(self.logger, "Failed to submit request");
            }
        }

        match rx1.await {
            Err(e) => {
                error!(self.logger, "oneshot channel error. Cause: {:?}", e);
                Err(ClientError::ClientInternal)
            }
            Ok(result) => result.map(|_response| {
                let content = _response.into_inner();
                content
            }),
        }
    }
}

impl Consumer {
    pub async fn receive(
        &self,
        maxMessageNum: usize, invisibleDuration: Duration,
    ) -> Result<Vec<MessageView>, ClientError> {
        let client = Arc::new(&self.client);
        let client_weak = Arc::downgrade(&client);
        let client = match client_weak.upgrade() {
            Some(client) => client,
            None => {
                return Err(error::ClientError::ClientInternal);
            }
        };
        // get route table
        let guard = client.route_table.lock();
        let rxx = guard.get(self.topic.as_str()).unwrap().clone();
        let curr: Route;
        match rxx {
            RouteStatus::Found(route) => {
                curr = Route::new(route.message_queue_impls.clone());
            }
            _ => {
                return Err(error::ClientError::ClientInternal);
            }
        }

        let (tx1, rx1) = oneshot::channel();

        let message_queue = self.selector.select(
            curr.message_queue_impls,
        );


        let mut srequest = ReceiveMessageRequest {
            group: Some(Resource {
                resource_namespace: "".to_string(),
                name: self.group.clone(),
            }),
            message_queue: message_queue.clone(),
            filter_expression: None,
            batch_size: 1,
            invisible_duration: Some(prost_types::Duration::from(Duration::from_secs(2*60))),
            auto_renew: false,
        };
        let mut request = tonic::Request::new(srequest);
        client.sign(request.metadata_mut());
        request.set_timeout(Duration::from_secs(10));

        let broker = message_queue.and_then(|mq| mq.broker.clone());
        let peer = broker.and_then(|br| br.endpoints);
        let mut address = peer.unwrap().addresses;
        let address_pop = address.pop().unwrap();
        let host = address_pop.host;
        let port = address_pop.port;
        let s = format!("http://{}:{}", host, port);
        let command = command::Command::Receive {
            peer: s,
            request,
            tx: tx1,
        };

        match self.client.fetch_session_manager().tx.send(command).await {
            Ok(_) => {}
            Err(e) => {
                error!(self.logger, "Failed to submit request");
            }
        }

        match rx1.await {
            Err(e) => {
                error!(self.logger, "oneshot channel error. Cause: {:?}", e);
                Err(ClientError::ClientInternal)
            }
            Ok(result) => result.map(|_response| {
                let content = _response.into_inner();
                let mut result = Vec::new();
                for x in content.iter() {
                    result.push(MessageView {
                        body: x.body.clone(),
                        message_id: x.system_properties.clone().unwrap().message_id,
                    })
                }
                result
            }),
        }
    }
}

impl Consumer {
    pub async fn new<T>(
        logger: Logger,
        access_point: &str,
        topics: T,
    ) -> Result<Self, error::ClientError>
        where
            T: IntoIterator,
            T::Item: AsRef<str>,
    {
        let client_logger = logger.new(o!("component" => "client"));

        let client = client::Client::new(client_logger, access_point)?;

        for _topic in topics.into_iter() {
            client.query_route(_topic.as_ref(), false).await.unwrap();
        }
        let prodcuer_logger = logger.new(o!("component" => "consumer"));

        Ok(Consumer {
            topic: "TopicTest".to_string(),
            group: "xxx".to_string(),
            client: client,
            selector: ConsumeQueueSelector::default(),
            logger: prodcuer_logger,
        })
    }

    pub fn start(&mut self) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use slog::Drain;

    #[tokio::test]
    async fn test_consumer() {
        let drain = slog::Discard;
        let logger = Logger::root(drain, slog::o!());
        let access_point = "127.0.0.1:8081";
        let _consumer = Consumer::new(logger, access_point, vec!["TopicTest"])
            .await
            .unwrap();
        let tag = "TagA";
        let mut keys = Vec::new();
        keys.push(String::from("key1"));
        let message = models::message::MessageImpl::new("TopicTest", tag, keys, "hello world");

        let rs=_consumer.sendClientTelemetry().await;
        match rs {
            Ok(tc) => {
                println!("response: {:?}", tc);
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }

        match _consumer.receive(1, Duration::from_secs(2 * 60)).await {
            Ok(r) => {
                println!("response: {:?}", r);
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
    }
}
