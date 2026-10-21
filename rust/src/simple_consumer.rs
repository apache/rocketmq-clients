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
use std::time::Duration;

use mockall_double::double;
use tokio::select;
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn};

#[double]
use crate::client::Client;
use crate::conf::{ClientOption, SimpleConsumerOption};
use crate::error::{ClientError, ErrorKind};
use crate::model::common::{ClientType, FilterExpression};
use crate::model::message::{AckMessageEntry, MessageView};
use crate::pb::telemetry_command::Command;
use crate::session::SharedLongPollingTimeout;
use crate::util::{
    build_endpoints_by_message_queue, build_simple_consumer_settings, prune_lite_route,
    select_message_queue,
};

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
    client: Client,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl SimpleConsumer {
    const OPERATION_NEW_SIMPLE_CONSUMER: &'static str = "simple_consumer.new";
    const OPERATION_START_SIMPLE_CONSUMER: &'static str = "simple_consumer.start";
    const OPERATION_RECEIVE_MESSAGE: &'static str = "simple_consumer.receive_message";
    const OPERATION_ACK_MESSAGE: &'static str = "simple_consumer.ack_message";
    const OPERATION_CHANGE_INVISIBLE_DURATION: &'static str =
        "simple_consumer.change_invisible_duration";

    /// Convert the invisible duration to protobuf, rejecting values protobuf can not represent.
    fn to_pb_invisible_duration(
        invisible_duration: Duration,
        operation: &'static str,
    ) -> Result<prost_types::Duration, ClientError> {
        prost_types::Duration::try_from(invisible_duration).map_err(|_| {
            ClientError::new(
                ErrorKind::Config,
                "invisible duration is out of range",
                operation,
            )
            .with_context("invisible_duration", format!("{:?}", invisible_duration))
        })
    }

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
        let client = Client::new(client_option, build_simple_consumer_settings(&option))?;
        Ok(SimpleConsumer {
            option,
            client,
            shutdown_tx: None,
        })
    }

    /// Create a simple consumer instance on top of an already built client.
    ///
    /// Used by [`crate::lite_simple_consumer::LiteSimpleConsumer`], which has to pass a client
    /// whose settings carry the lite client type.
    pub(crate) fn new_with_client(
        client: Client,
        option: SimpleConsumerOption,
    ) -> Result<Self, ClientError> {
        if option.consumer_group().is_empty() {
            return Err(ClientError::new(
                ErrorKind::Config,
                "required option is missing: consumer group is empty",
                Self::OPERATION_NEW_SIMPLE_CONSUMER,
            ));
        }
        Ok(SimpleConsumer {
            option,
            client,
            shutdown_tx: None,
        })
    }

    /// Check whether the underlying client is started.
    pub(crate) fn check_started(&self, operation: &'static str) -> Result<(), ClientError> {
        self.client.check_started(operation)
    }

    /// Start the simple consumer, forwarding server telemetry commands to `telemetry_command_tx`.
    pub(crate) async fn start_with_telemetry(
        &mut self,
        telemetry_command_tx: mpsc::Sender<crate::pb::telemetry_command::Command>,
    ) -> Result<(), ClientError> {
        if self.option.consumer_group().is_empty() {
            return Err(ClientError::new(
                ErrorKind::Config,
                "required option is missing: consumer group is empty",
                Self::OPERATION_START_SIMPLE_CONSUMER,
            ));
        }
        self.client.start(telemetry_command_tx).await?;
        if let Some(topics) = self.option.topics() {
            for topic in topics {
                if let Err(error) = self.client.topic_route(topic, true).await {
                    // Reference Java: SimpleConsumerImpl#startUp shuts the client down when any
                    // startup step fails.
                    if let Err(shutdown_error) = self.client.shutdown_ref().await {
                        warn!(
                            "failed to shut down simple consumer after a failed start: {}",
                            shutdown_error
                        );
                    }
                    return Err(error);
                }
            }
        }
        info!(
            "start simple consumer success, client_id: {}",
            self.client.client_id()
        );
        Ok(())
    }

    /// Start the simple consumer
    pub async fn start(&mut self) -> Result<(), ClientError> {
        let (telemetry_command_tx, mut telemetry_command_rx) = mpsc::channel(16);
        self.start_with_telemetry(telemetry_command_tx).await?;
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        self.shutdown_tx = Some(shutdown_tx);
        let long_polling_timeout = self.client.long_polling_timeout_handle();
        tokio::spawn(async move {
            loop {
                select! {
                    command = telemetry_command_rx.recv() => {
                        match command {
                            Some(Command::Settings(settings)) => {
                                Self::apply_server_settings(&settings, &long_polling_timeout);
                            }
                            Some(command) => {
                                warn!("command {:?} cannot be handled in simple consumer.", command);
                            }
                            // The client was dropped; there is nothing left to handle.
                            None => break,
                        }
                    }

                    _ = &mut shutdown_rx => {
                       break;
                    }
                }
            }
        });
        info!(
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

    /// Apply the settings pushed by the server.
    ///
    /// The simple consumer only consumes the long-polling timeout for now: it drives both
    /// the gRPC deadline of a receive request and the `long_polling_timeout` field of the
    /// request body itself. Mirrors `SimpleSubscriptionSettings#sync` of the Java client.
    fn apply_server_settings(
        settings: &crate::pb::Settings,
        long_polling_timeout: &SharedLongPollingTimeout,
    ) {
        if let Some(crate::pb::settings::PubSub::Subscription(ref subscription)) = settings.pub_sub
        {
            if let Some(ref timeout) = subscription.long_polling_timeout {
                let seconds = timeout.seconds;
                let nanos = timeout.nanos;
                if seconds > 0 || nanos > 0 {
                    let timeout = Duration::new(seconds as u64, nanos as u32);
                    *long_polling_timeout.write() = timeout;
                    info!(
                        "simple consumer applied server-pushed long_polling_timeout: {:?}",
                        timeout
                    );
                }
            }
        }
    }

    /// Same as [`SimpleConsumer::shutdown`] but takes a mutable reference.
    pub(crate) async fn shutdown_ref(&mut self) -> Result<(), ClientError> {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        };
        self.client.shutdown_ref().await
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
        self.receive_internal(
            topic.as_ref(),
            expression,
            batch_size,
            invisible_duration,
            false,
        )
        .await
    }

    /// Receive messages of a lite consumer.
    ///
    /// The route is pruned to the first readable master queue because the server resolves the
    /// lite topic queues itself (reference Java: `LiteSimpleConsumerImpl`).
    pub(crate) async fn receive_lite(
        &self,
        topic: impl AsRef<str>,
        expression: &FilterExpression,
        batch_size: i32,
        invisible_duration: Duration,
    ) -> Result<Vec<MessageView>, ClientError> {
        self.receive_internal(
            topic.as_ref(),
            expression,
            batch_size,
            invisible_duration,
            true,
        )
        .await
    }

    async fn receive_internal(
        &self,
        topic: &str,
        expression: &FilterExpression,
        batch_size: i32,
        invisible_duration: Duration,
        lite: bool,
    ) -> Result<Vec<MessageView>, ClientError> {
        self.client.check_started(Self::OPERATION_RECEIVE_MESSAGE)?;
        // Reference Java: `SimpleConsumerImpl#receive0` rejects a non-positive maxMessageNum.
        if batch_size <= 0 {
            return Err(ClientError::new(
                ErrorKind::Config,
                "batch size must be greater than 0",
                Self::OPERATION_RECEIVE_MESSAGE,
            )
            .with_context("batch_size", batch_size.to_string()));
        }
        let invisible_duration =
            Self::to_pb_invisible_duration(invisible_duration, Self::OPERATION_RECEIVE_MESSAGE)?;
        let route = self.client.topic_route(topic, true).await?;
        let route = if lite {
            prune_lite_route(route, topic, Self::OPERATION_RECEIVE_MESSAGE)?
        } else {
            route
        };
        let message_queue = select_message_queue(route);
        let endpoints =
            build_endpoints_by_message_queue(&message_queue, Self::OPERATION_RECEIVE_MESSAGE)?;
        let messages = self
            .client
            .receive_message(
                &endpoints,
                message_queue,
                crate::pb::FilterExpression {
                    r#type: expression.filter_type() as i32,
                    expression: expression.expression().to_string(),
                },
                batch_size,
                invisible_duration,
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
        self.client.check_started(Self::OPERATION_ACK_MESSAGE)?;
        self.client.ack_message(ack_entry).await?;
        Ok(())
    }

    /// Change the invisible duration of the specified message
    ///
    /// The server issues a new receipt handle, which is returned. When `ack_entry` is a
    /// [`MessageView`], its receipt handle is refreshed as well, so the same view can be acked or
    /// changed again afterwards.
    ///
    /// # Arguments
    ///
    /// * `ack_entry` - special message view with handle want to change
    /// * `invisible_duration` - new invisible duration of the message, counted from now
    pub async fn change_invisible_duration(
        &self,
        ack_entry: &(impl AckMessageEntry + 'static),
        invisible_duration: Duration,
    ) -> Result<String, ClientError> {
        self.client
            .check_started(Self::OPERATION_CHANGE_INVISIBLE_DURATION)?;
        let invisible_duration = Self::to_pb_invisible_duration(
            invisible_duration,
            Self::OPERATION_CHANGE_INVISIBLE_DURATION,
        )?;
        let receipt_handle = self
            .client
            .change_invisible_duration(ack_entry, invisible_duration)
            .await?;
        // The server invalidates the old receipt handle. Refresh it in the message view, so that
        // a later ack or change of the same view takes effect.
        // Reference Java: `SimpleConsumerImpl#changeInvisibleDuration0`.
        if let Some(message_view) = (ack_entry as &dyn Any).downcast_ref::<MessageView>() {
            message_view.set_receipt_handle(receipt_handle.clone());
        }
        Ok(receipt_handle)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::model::common::{FilterType, Route};
    use crate::pb::{
        AckMessageResultEntry, Broker, Endpoints, Message, MessageQueue, Resource, SystemProperties,
    };

    use super::*;

    #[tokio::test]
    async fn simple_consumer_start() -> Result<(), ClientError> {
        let _m = crate::client::tests::MTX.lock();

        let ctx = Client::new_context();
        ctx.expect().return_once(|_, _| {
            let mut client = Client::default();
            client.expect_topic_route().returning(|_, _| {
                Ok(Arc::new(Route {
                    index: Default::default(),
                    queue: vec![],
                }))
            });
            client.expect_start().returning(|_| Ok(()));
            client
                .expect_long_polling_timeout_handle()
                .returning(|| Arc::new(parking_lot::RwLock::new(Duration::from_secs(40))));
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
        client.expect_check_started().returning(|_| Ok(()));
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
                        endpoints: Some(Endpoints {
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

    fn new_message_view(receipt_handle: &str) -> MessageView {
        MessageView::from_pb_message(
            Message {
                topic: Some(Resource {
                    name: "test_topic".to_string(),
                    ..Default::default()
                }),
                system_properties: Some(SystemProperties {
                    receipt_handle: Some(receipt_handle.to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            },
            crate::model::common::Endpoints::from_url("localhost:8081").unwrap(),
        )
        .unwrap()
    }

    fn new_simple_consumer(client: Client) -> SimpleConsumer {
        SimpleConsumer {
            option: SimpleConsumerOption::default(),
            client,
            shutdown_tx: None,
        }
    }

    fn new_simple_consumer_without_rpc() -> SimpleConsumer {
        let mut client = Client::default();
        client.expect_check_started().returning(|_| Ok(()));
        client.expect_topic_route().never();
        client.expect_receive_message().never();
        client
            .expect_change_invisible_duration::<MessageView>()
            .never();
        new_simple_consumer(client)
    }

    #[tokio::test]
    async fn receive_with_rejects_non_positive_batch_size() {
        let simple_consumer = new_simple_consumer_without_rpc();
        for batch_size in [0, -1, i32::MIN] {
            let result = simple_consumer
                .receive_with(
                    "test_topic",
                    &FilterExpression::new(FilterType::Tag, "*"),
                    batch_size,
                    Duration::from_secs(15),
                )
                .await;
            let error = result.expect_err("non-positive batch size must be rejected");
            assert_eq!(error.kind, ErrorKind::Config);
            assert_eq!(error.message, "batch size must be greater than 0");
        }
    }

    #[tokio::test]
    async fn receive_with_rejects_out_of_range_invisible_duration() {
        let simple_consumer = new_simple_consumer_without_rpc();
        let result = simple_consumer
            .receive_with(
                "test_topic",
                &FilterExpression::new(FilterType::Tag, "*"),
                32,
                Duration::MAX,
            )
            .await;
        let error = result.expect_err("out of range invisible duration must be rejected");
        assert_eq!(error.kind, ErrorKind::Config);
        assert_eq!(error.message, "invisible duration is out of range");
    }

    #[tokio::test]
    async fn change_invisible_duration_rejects_out_of_range_invisible_duration() {
        let simple_consumer = new_simple_consumer_without_rpc();
        let message = MessageView::from_pb_message(
            Message {
                topic: Some(Resource {
                    name: "test_topic".to_string(),
                    ..Default::default()
                }),
                system_properties: Some(SystemProperties {
                    receipt_handle: Some("receipt_handle".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            },
            crate::model::common::Endpoints::from_url("localhost:8081").unwrap(),
        )
        .unwrap();
        let result = simple_consumer
            .change_invisible_duration(&message, Duration::MAX)
            .await;
        let error = result.expect_err("out of range invisible duration must be rejected");
        assert_eq!(error.kind, ErrorKind::Config);
        assert_eq!(error.message, "invisible duration is out of range");
    }

    // Reference Java: SimpleConsumerImpl#changeInvisibleDuration0 refreshes the receipt handle of
    // the MessageView, so that a later ack or change of the same view uses the new handle.
    #[tokio::test]
    async fn change_invisible_duration_refreshes_receipt_handle() -> Result<(), ClientError> {
        let mut client = Client::default();
        client.expect_check_started().returning(|_| Ok(()));
        client
            .expect_change_invisible_duration()
            .times(1)
            .withf(|message: &MessageView, _| message.receipt_handle() == "handle-1")
            .returning(|_: &MessageView, _| Ok("handle-2".to_string()));
        client
            .expect_change_invisible_duration()
            .times(1)
            .withf(|message: &MessageView, _| message.receipt_handle() == "handle-2")
            .returning(|_: &MessageView, _| Ok("handle-3".to_string()));
        client
            .expect_ack_message()
            .times(1)
            .withf(|message: &MessageView| message.receipt_handle() == "handle-3")
            .returning(|_: &MessageView| Ok(AckMessageResultEntry::default()));
        let simple_consumer = new_simple_consumer(client);

        let message = new_message_view("handle-1");
        let handle = simple_consumer
            .change_invisible_duration(&message, Duration::from_secs(10))
            .await?;
        assert_eq!(handle, "handle-2");
        assert_eq!(message.receipt_handle(), "handle-2");
        simple_consumer
            .change_invisible_duration(&message, Duration::from_secs(10))
            .await?;
        assert_eq!(message.receipt_handle(), "handle-3");
        simple_consumer.ack(&message).await?;
        Ok(())
    }

    #[tokio::test]
    async fn failed_change_invisible_duration_keeps_receipt_handle() {
        let mut client = Client::default();
        client.expect_check_started().returning(|_| Ok(()));
        client
            .expect_change_invisible_duration()
            .times(1)
            .returning(|_: &MessageView, _| {
                Err(ClientError::new(
                    ErrorKind::Server,
                    "server error",
                    "change_invisible_duration",
                ))
            });
        let simple_consumer = new_simple_consumer(client);

        let message = new_message_view("handle-1");
        let result = simple_consumer
            .change_invisible_duration(&message, Duration::from_secs(10))
            .await;
        assert!(result.is_err());
        assert_eq!(message.receipt_handle(), "handle-1");
    }

    fn assert_not_running<T: std::fmt::Debug>(result: Result<T, ClientError>, operation: &str) {
        let error = result.unwrap_err();
        assert_eq!(error.kind, ErrorKind::ClientIsNotRunning, "{:?}", error);
        assert_eq!(error.operation, operation);
    }

    // Reference Java: SimpleConsumerImpl#receive0 / #ack0 / #changeInvisibleDuration0 fail with
    // IllegalStateException("Simple consumer is not running now") before any RPC.
    #[tokio::test]
    async fn operations_fail_when_not_started() {
        let mut client = Client::default();
        client.expect_check_started().returning(|operation| {
            Err(ClientError::new(
                ErrorKind::ClientIsNotRunning,
                "client is not started",
                operation,
            ))
        });
        client.expect_topic_route().never();
        client.expect_receive_message().never();
        client.expect_ack_message::<MessageView>().never();
        client
            .expect_change_invisible_duration::<MessageView>()
            .never();
        let simple_consumer = SimpleConsumer {
            option: SimpleConsumerOption::default(),
            client,
            shutdown_tx: None,
        };
        let expression = FilterExpression::new(FilterType::Tag, "*");
        let message = new_message_view("handle");

        assert_not_running(
            simple_consumer.receive("test_topic", &expression).await,
            "simple_consumer.receive_message",
        );
        assert_not_running(
            simple_consumer.ack(&message).await,
            "simple_consumer.ack_message",
        );
        assert_not_running(
            simple_consumer
                .change_invisible_duration(&message, Duration::from_secs(10))
                .await,
            "simple_consumer.change_invisible_duration",
        );
    }

    // Reference Java: SimpleConsumerImpl#startUp calls shutDown() when any startup step fails.
    #[tokio::test]
    async fn start_failure_shuts_down_client() {
        let _m = crate::client::tests::MTX.lock();

        let ctx = Client::new_context();
        ctx.expect().return_once(|_, _| {
            let mut client = Client::default();
            client.expect_start().times(1).returning(|_| Ok(()));
            client.expect_topic_route().times(1).returning(|_, _| {
                Err(ClientError::new(
                    ErrorKind::Server,
                    "topic not found",
                    "client.query_route",
                ))
            });
            client.expect_shutdown_ref().times(1).returning(|| Ok(()));
            client
                .expect_client_id()
                .return_const("fake_id".to_string());
            Ok(client)
        });
        let mut option = SimpleConsumerOption::default();
        option.set_consumer_group("test_group");
        option.set_topics(vec!["test_topic"]);
        let mut simple_consumer = SimpleConsumer::new(option, ClientOption::default()).unwrap();
        let result = simple_consumer.start().await;
        assert!(result.is_err());
        assert!(simple_consumer.shutdown_tx.is_none());
    }
}
