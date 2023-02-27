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
use prost_types::Timestamp;
use slog::{error, o, Logger};
use std::sync::Arc;
use tokio::sync::oneshot;
use crate::client::{Route, RouteStatus};
use crate::error::ClientError;
use crate::pb::{
    Message, MessageType, Resource, SendMessageRequest, SendMessageResponse, SystemProperties,
};
use crate::selector::{QueueSelect, QueueSelector};
use crate::{client, command, error, models};
use models::message_id::UNIQ_ID_GENERATOR;

struct Producer {
    client: client::Client,
    selector: QueueSelector,
    logger: Logger,
}

impl Producer {
    pub async fn send(
        &self,
        message: &models::message::MessageImpl,
    ) -> Result<SendMessageResponse, ClientError> {
        let client = Arc::new(&self.client);
        let client_weak = Arc::downgrade(&client);
        let client = match client_weak.upgrade() {
            Some(client) => client,
            None => {
                return Err(error::ClientError::ClientInternal);
            }
        };

        let mut srequest = SendMessageRequest { messages: vec![] };

        let mut delivery_timestamp = None;

        if message.delivery_timestamp != 0 {
            delivery_timestamp = Option::from(Timestamp {
                seconds: message.delivery_timestamp,
                nanos: 0,
            })
        }

        srequest.messages.push(Message {
            topic: Some(Resource {
                resource_namespace: "".to_string(),
                name: message.topic.clone(),
            }),
            user_properties: Default::default(),
            system_properties: Option::from(SystemProperties {
                tag: Option::from(message.tags.clone()),
                keys: message.keys.clone(),
                message_id: UNIQ_ID_GENERATOR.lock().generate(),
                body_digest: None,
                body_encoding: 0,
                message_type: MessageType::Normal as i32,
                born_timestamp: None,
                born_host: "".to_string(),
                store_timestamp: None,
                store_host: "".to_string(),
                delivery_timestamp: delivery_timestamp,
                receipt_handle: None,
                queue_id: 0,
                queue_offset: None,
                invisible_duration: None,
                delivery_attempt: None,
                message_group: Option::from(message.message_group.clone()),
                trace_context: None,
                orphaned_transaction_recovery_duration: None,
            }),
            body: message.body.clone(),
        });

        let mut request = tonic::Request::new(srequest);
        client.sign(request.metadata_mut());

        // get route table
        let guard = client.route_table.lock();
        let rxx = guard.get(&message.topic).unwrap().clone();
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
            request.get_ref().messages.get(0).unwrap(),
            curr.message_queue_impls,
        );

        let broker = message_queue.and_then(|mq| mq.broker.clone());
        let peer = broker.and_then(|br| br.endpoints);
        let mut address = peer.unwrap().addresses;
        let address_pop = address.pop().unwrap();
        let host = address_pop.host;
        let port = address_pop.port;
        let s = format!("http://{}:{}", host, port);
        let command = command::Command::Send {
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
            Ok(result) => result.map(|_response| _response.into_inner()),
            Err(e) => {
                error!(self.logger, "oneshot channel error. Cause: {:?}", e);
                Err(ClientError::ClientInternal)
            }
        }
    }
}

impl Producer {
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
        let prodcuer_logger = logger.new(o!("component" => "producer"));

        Ok(Producer {
            client: client,
            selector: QueueSelector::default(),
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
    async fn test_producer() {
        let drain = slog::Discard;
        let logger = Logger::root(drain, slog::o!());
        let access_point = "127.0.0.1:8081";
        let _producer = Producer::new(logger, access_point, vec!["TopicTest"])
            .await
            .unwrap();
        let tag = "TagA";
        let mut keys = Vec::new();
        keys.push(String::from("key1"));
        let message = models::message::MessageImpl::new("TopicTest", tag, keys, "hello world");

        match _producer.send(&message).await {
            Ok(r) => {
                println!("response: {:?}", r);
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
    }
}
