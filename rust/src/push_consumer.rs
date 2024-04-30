use crate::client::Client;
use crate::conf::{ClientOption, PushConsumerOption};
use crate::error::{ClientError, ErrorKind};
use crate::log;
use crate::model::common::{ClientType, ConsumeResult};
use crate::model::message::MessageView;
use parking_lot::RwLock;
use slog::Logger;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::RwLock as TokioRwLock;
use tokio::sync::{mpsc, oneshot};

const OPERATION_NEW_PUSH_CONSUMER: &str = "push_consumer.new";

pub type MessageListener = dyn Fn(MessageView) -> ConsumeResult;

pub struct PushConsumer {
    logger: Logger,
    client: Client<PushConsumerOption>,
    message_listener: Box<MessageListener>,
    option: Arc<RwLock<PushConsumerOption>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl PushConsumer {
    pub fn new(
        client_option: ClientOption,
        option: PushConsumerOption,
        message_listener: Box<MessageListener>,
    ) -> Result<Self, ClientError> {
        if option.consumer_group().is_empty() {
            return Err(ClientError::new(
                ErrorKind::Config,
                "consumer group is required.",
                OPERATION_NEW_PUSH_CONSUMER,
            ));
        }
        if option.subscription_expressions().is_empty() {
            return Err(ClientError::new(
                ErrorKind::Config,
                "subscription expressions is required.",
                OPERATION_NEW_PUSH_CONSUMER,
            ));
        }
        let client_option = ClientOption {
            client_type: ClientType::PushConsumer,
            ..client_option
        };
        let logger = log::logger(option.logging_format());
        let option = Arc::new(RwLock::new(option));
        let client =
            Client::<PushConsumerOption>::new(&logger, client_option, Arc::clone(&option))?;
        Ok(Self {
            logger,
            client,
            message_listener,
            option,
            shutdown_tx: None,
        })
    }

    pub async fn start(&mut self) -> Result<(), ClientError> {
        let (telemetry_command_tx, mut telemetry_command_rx) = mpsc::channel(16);
        self.client.start(telemetry_command_tx).await?;
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        self.shutdown_tx = Some(shutdown_tx);
        tokio::spawn(async move {
            let mut scan_assignments_timer = tokio::time::interval(Duration::from_secs(30));
            loop {
                select! {
                    command = telemetry_command_rx.recv() => {

                    }
                    _ = scan_assignments_timer.tick() => {

                    }
                    _ = &mut shutdown_rx => {
                        break;
                    }
                }
            }
        });
        Ok(())
    }

    pub async fn shutdown(mut self) -> Result<(), ClientError> {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
        self.client.shutdown().await?;
        Ok(())
    }

    async fn scan_assignments(option: Arc<RwLock<PushConsumerOption>>) {
        // TODO: Scan for assignments.
        let topics: Vec<String>;
        {
            topics = option
                .read()
                .subscription_expressions()
                .iter()
                .map(|(topic, _)| topic.to_string())
                .collect();
        }
    }
}
