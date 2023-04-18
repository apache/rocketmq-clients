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
use std::time::Duration;

use slog::{info, Logger};

use crate::client::Client;
use crate::conf::{ClientOption, SimpleConsumerOption};
use crate::error::{ClientError, ErrorKind};
use crate::model::common::{ClientType, FilterExpression};
use crate::model::message::{AckMessageEntry, MessageView};
use crate::util::{
    build_endpoints_by_message_queue, build_simple_consumer_settings, select_message_queue,
};
use crate::{log, pb};

#[derive(Debug)]
pub struct SimpleConsumer {
    option: SimpleConsumerOption,
    logger: Logger,
    client: Client,
}

impl SimpleConsumer {
    const OPERATION_START_SIMPLE_CONSUMER: &'static str = "simple_consumer.start";
    const OPERATION_RECEIVE_MESSAGE: &'static str = "simple_consumer.receive_message";

    pub fn new(
        option: SimpleConsumerOption,
        client_option: ClientOption,
    ) -> Result<Self, ClientError> {
        let client_option = ClientOption {
            client_type: ClientType::SimpleConsumer,
            group: option.consumer_group().to_string(),
            namespace: option.namespace().to_string(),
            ..client_option
        };
        let logger = log::logger(option.logging_format());
        let settings = build_simple_consumer_settings(&option, &client_option);
        let client = Client::new(&logger, client_option, settings)?;
        Ok(SimpleConsumer {
            option,
            logger,
            client,
        })
    }

    pub async fn start(&self) -> Result<(), ClientError> {
        if self.option.consumer_group().is_empty() {
            return Err(ClientError::new(
                ErrorKind::Config,
                "required option is missing: consumer group is empty",
                Self::OPERATION_START_SIMPLE_CONSUMER,
            ));
        }
        if let Some(topics) = self.option.topics() {
            for topic in topics {
                self.client.topic_route(topic, true).await?;
            }
        }
        self.client.start();
        info!(
            self.logger,
            "start simple consumer success, client_id: {}",
            self.client.client_id()
        );
        Ok(())
    }

    pub async fn receive(
        &self,
        topic: impl AsRef<str>,
        expression: &FilterExpression,
    ) -> Result<Vec<MessageView>, ClientError> {
        self.receive_with_batch_size(topic.as_ref(), expression, 32, Duration::from_secs(15))
            .await
    }

    pub async fn receive_with_batch_size(
        &self,
        topic: &str,
        expression: &FilterExpression,
        batch_size: i32,
        invisible_duration: Duration,
    ) -> Result<Vec<MessageView>, ClientError> {
        let route = self.client.topic_route(topic, true).await?;
        let message_queue = select_message_queue(route);
        let endpoints =
            build_endpoints_by_message_queue(&message_queue, Self::OPERATION_RECEIVE_MESSAGE)?;
        let messages = self
            .client
            .receive_message(
                &endpoints,
                message_queue,
                pb::FilterExpression {
                    r#type: expression.filter_type as i32,
                    expression: expression.expression.clone(),
                },
                batch_size,
                prost_types::Duration::try_from(invisible_duration).unwrap(),
            )
            .await?;
        Ok(messages
            .into_iter()
            .map(|message| MessageView::from_pb_message(message, endpoints.clone()))
            .collect())
    }

    pub async fn ack(&self, ack_entry: impl AckMessageEntry + 'static) -> Result<(), ClientError> {
        self.client.ack_message(ack_entry).await?;
        Ok(())
    }
}
