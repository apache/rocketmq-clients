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

use mockall_double::double;
use slog::{info, warn, Logger};
use tokio::select;
use tokio::sync::{mpsc, oneshot};

#[double]
use crate::client::Client;
use crate::conf::{ClientOption, SimpleConsumerOption};
use crate::error::{ClientError, ErrorKind};
use crate::model::common::{ClientType, FilterExpression};
use crate::model::message::{AckMessageEntry, MessageView};
use crate::util::{
    build_endpoints_by_message_queue, build_simple_consumer_settings, select_message_queue,
};
use crate::{log, pb};

/// [`SimpleConsumer`] is a lightweight consumer to consume messages from RocketMQ proxy.
///
/// If you want to fully control the message consumption operation by yourself,
/// the simple consumer should be your first consideration.
///
/// [`SimpleConsumer`] is a thin wrapper of internal client struct that shoulders the actual workloads.
/// Most of its methods take shared reference so that application developers may use it at will.
///
/// [`SimpleConsumer`] is `Send` and `Sync` by design, so that developers may get started easily.
#[derive(Debug)]
pub struct SimpleConsumer {
    option: SimpleConsumerOption,
    logger: Logger,
    client: Client,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl SimpleConsumer {
    const OPERATION_NEW_SIMPLE_CONSUMER: &'static str = "simple_consumer.new";
    const OPERATION_START_SIMPLE_CONSUMER: &'static str = "simple_consumer.start";
    const OPERATION_RECEIVE_MESSAGE: &'static str = "simple_consumer.receive_message";

    /// Create a new simple consumer instance
    pub fn new(
        option: SimpleConsumerOption,
        client_option: ClientOption,
    ) -> Result<Self, ClientError> {
        if option.consumer_group().is_empty() {
            return Err(ClientError::new(
                ErrorKind::Config,
                "required option is missing: consumer group is empty",
                Self::OPERATION_NEW_SIMPLE_CONSUMER,
            ));
        }

        let client_option = ClientOption {
            client_type: ClientType::SimpleConsumer,
            group: Some(option.consumer_group().to_string()),
            namespace: option.namespace().to_string(),
            ..client_option
        };
        let logger = log::logger(option.logging_format());
        let client = Client::new(
            &logger,
            client_option,
            build_simple_consumer_settings(&option),
        )?;
        Ok(SimpleConsumer {
            option,
            logger,
            client,
            shutdown_tx: None,
        })
    }

    /// Start the simple consumer
    pub async fn start(&mut self) -> Result<(), ClientError> {
        if self.option.consumer_group().is_empty() {
            return Err(ClientError::new(
                ErrorKind::Config,
                "required option is missing: consumer group is empty",
                Self::OPERATION_START_SIMPLE_CONSUMER,
            ));
        }
        let (telemetry_command_tx, mut telemetry_command_rx) = mpsc::channel(16);
        self.client.start(telemetry_command_tx).await?;
        if let Some(topics) = self.option.topics() {
            for topic in topics {
                self.client.topic_route(topic, true).await?;
            }
        }
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        self.shutdown_tx = Some(shutdown_tx);
        let logger = self.logger.clone();
        tokio::spawn(async move {
            loop {
                select! {
                    command = telemetry_command_rx.recv() => {
                        warn!(logger, "command {:?} cannot be handled in simple consumer.", command);
                    }

                    _ = &mut shutdown_rx => {
                       break;
                    }
                }
            }
        });
        info!(
            self.logger,
            "start simple consumer success, client_id: {}",
            self.client.client_id()
        );
        Ok(())
    }

    pub async fn shutdown(self) -> Result<(), ClientError> {
        if let Some(shutdown_tx) = self.shutdown_tx {
            let _ = shutdown_tx.send(());
        };
        self.client.shutdown().await
    }

    /// receive messages from the specified topic
    ///
    /// # Arguments
    ///
    /// * `topic` - the topic for receiving messages
    /// * `expression` - the subscription for the topic
    pub async fn receive(
        &self,
        topic: impl AsRef<str>,
        expression: &FilterExpression,
    ) -> Result<Vec<MessageView>, ClientError> {
        self.receive_with(topic.as_ref(), expression, 32, Duration::from_secs(15))
            .await
    }

    /// receive messages from the specified topic with batch size and invisible duration
    ///
    /// # Arguments
    ///
    /// * `topic` - the topic for receiving messages
    /// * `expression` - the subscription for the topic
    /// * `batch_size` - max message num of server returned
    /// * `invisible_duration` - set the invisible duration of messages that return from the server, these messages will not be visible to other consumers unless timeout
    pub async fn receive_with(
        &self,
        topic: impl AsRef<str>,
        expression: &FilterExpression,
        batch_size: i32,
        invisible_duration: Duration,
    ) -> Result<Vec<MessageView>, ClientError> {
        let route = self.client.topic_route(topic.as_ref(), true).await?;
        let message_queue = select_message_queue(route);
        let endpoints =
            build_endpoints_by_message_queue(&message_queue, Self::OPERATION_RECEIVE_MESSAGE)?;
        let messages = self
            .client
            .receive_message(
                &endpoints,
                message_queue,
                pb::FilterExpression {
                    r#type: expression.filter_type() as i32,
                    expression: expression.expression().to_string(),
                },
                batch_size,
                prost_types::Duration::try_from(invisible_duration).unwrap(),
            )
            .await?;
        Ok(messages
            .into_iter()
            .filter_map(|message| MessageView::from_pb_message(message, endpoints.clone()))
            .collect())
    }

    /// Ack the specified message
    ///
    /// It is important to acknowledge every consumed message, otherwise, they will be received again after the invisible duration
    ///
    /// # Arguments
    ///
    /// * `ack_entry` - special message view with handle want to ack
    pub async fn ack(
        &self,
        ack_entry: &(impl AckMessageEntry + 'static),
    ) -> Result<(), ClientError> {
        self.client.ack_message(ack_entry).await?;
        Ok(())
    }

    pub async fn change_invisible_duration(
        &self,
        ack_entry: &(impl AckMessageEntry + 'static),
        invisible_duration: Duration,
    ) -> Result<String, ClientError> {
        self.client
            .change_invisible_duration(
                ack_entry,
                prost_types::Duration::try_from(invisible_duration).unwrap(),
            )
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::log::terminal_logger;
    use crate::model::common::{FilterType, Route};
    use crate::pb::{
        AckMessageResultEntry, Broker, Message, MessageQueue, Resource, SystemProperties,
    };

    use super::*;

    #[tokio::test]
    async fn simple_consumer_start() -> Result<(), ClientError> {
        let _m = crate::client::tests::MTX.lock();

        let ctx = Client::new_context();
        ctx.expect().return_once(|_, _, _| {
            let mut client = Client::default();
            client.expect_topic_route().returning(|_, _| {
                Ok(Arc::new(Route {
                    index: Default::default(),
                    queue: vec![],
                }))
            });
            client.expect_start().returning(|_| Ok(()));
            client
                .expect_client_id()
                .return_const("fake_id".to_string());
            Ok(client)
        });
        let mut option = SimpleConsumerOption::default();
        option.set_consumer_group("test_group");
        option.set_topics(vec!["test_topic"]);
        SimpleConsumer::new(option, ClientOption::default())?
            .start()
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn simple_consumer_consume_message() -> Result<(), ClientError> {
        let mut client = Client::default();
        client.expect_topic_route().returning(|_, _| {
            Ok(Arc::new(Route {
                index: Default::default(),
                queue: vec![MessageQueue {
                    topic: Some(Resource {
                        name: "test_topic".to_string(),
                        ..Default::default()
                    }),
                    id: 0,
                    permission: 0,
                    broker: Some(Broker {
                        name: "".to_string(),
                        id: 0,
                        endpoints: Some(pb::Endpoints {
                            scheme: 0,
                            addresses: vec![],
                        }),
                    }),
                    accept_message_types: vec![],
                }],
            }))
        });
        client.expect_receive_message().returning(|_, _, _, _, _| {
            Ok(vec![Message {
                topic: Some(Resource {
                    name: "test_topic".to_string(),
                    ..Default::default()
                }),
                system_properties: Some(SystemProperties::default()),
                ..Default::default()
            }])
        });
        client
            .expect_ack_message()
            .returning(|_: &MessageView| Ok(AckMessageResultEntry::default()));
        let simple_consumer = SimpleConsumer {
            option: SimpleConsumerOption::default(),
            logger: terminal_logger(),
            client,
            shutdown_tx: None,
        };

        let messages = simple_consumer
            .receive(
                "test_topic",
                &FilterExpression::new(FilterType::Tag, "test_tag".to_string()),
            )
            .await?;
        assert_eq!(messages.len(), 1);
        simple_consumer
            .ack(&messages.into_iter().next().unwrap())
            .await?;
        Ok(())
    }
}
