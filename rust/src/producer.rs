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

//! Publish messages of various types to brokers.
//!
//! `Producer` is a thin wrapper of internal `Client` struct that shoulders the actual workloads.
//! Most of its methods take shared reference so that application developers may use it at will.

use crate::client::Client;
use crate::conf::{ClientOption, ProducerOption};
use crate::error::ClientError;
use crate::log;
use crate::pb::{Message, SendResultEntry};
use slog::{info, Logger};

/// `Producer` is the core struct, to which application developers should turn, when publishing messages to brokers.
///
/// `Producer` is `Send` and `Sync` by design, so that developers may get started easily.
#[derive(Debug)]
pub struct Producer {
    option: ProducerOption,
    logger: Logger,
    client: Client,
}

impl Producer {
    pub async fn new(
        option: ProducerOption,
        client_option: ClientOption,
    ) -> Result<Self, ClientError> {
        let logger = log::logger(&option);
        let client = Client::new(&logger, client_option)?;
        Ok(Producer {
            option,
            logger,
            client,
        })
    }

    pub async fn start(&self) -> Result<(), ClientError> {
        if let Some(topics) = self.option.topics() {
            for topic in topics {
                self.client.topic_route(topic, true).await?;
            }
        }
        info!(
            self.logger,
            "start producer success, client_id: {}",
            self.client.client_id()
        );
        Ok(())
    }

    pub async fn send(&self, message: Message) -> Result<SendResultEntry, ClientError> {
        self.client.send_message(message).await
    }
}

// #[cfg(test)]
// mod tests {
//     use crate::conf::{ClientOption, ProducerOption};
//     use crate::pb::{Message, Resource, SystemProperties};
//     use crate::producer::Producer;
//
//     #[tokio::test]
//     async fn test_producer_start() {
//         let mut producer_option = ProducerOption::default();
//         producer_option.set_topics(vec!["DefaultCluster".to_string()]);
//         let producer = Producer::new(producer_option, ClientOption::default())
//             .await
//             .unwrap();
//         producer.start().await.unwrap();
//     }
//
//     #[tokio::test]
//     async fn test_producer_send() {
//         let mut producer_option = ProducerOption::default();
//         producer_option.set_topics(vec!["DefaultCluster".to_string()]);
//         let producer = Producer::new(producer_option, ClientOption::default())
//             .await
//             .unwrap();
//         producer.start().await.unwrap();
//         let send_result = producer
//             .send(Message {
//                 topic: Some(Resource {
//                     resource_namespace: "".to_string(),
//                     name: "DefaultCluster".to_string(),
//                 }),
//                 user_properties: Default::default(),
//                 system_properties: Some(SystemProperties {
//                     message_id: "message_test_id".to_string(),
//                     ..SystemProperties::default()
//                 }),
//                 body: "Hello world".to_string().into_bytes(),
//             })
//             .await;
//         println!("{:?}", send_result);
//     }
// }
