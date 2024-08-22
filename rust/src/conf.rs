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

//! Configuration of RocketMQ rust client.

use std::collections::HashMap;
use std::time::Duration;

use crate::model::common::{ClientType, FilterExpression};
use crate::pb::{self, Resource};
#[allow(unused_imports)]
use crate::producer::Producer;
#[allow(unused_imports)]
use crate::simple_consumer::SimpleConsumer;

/// [`ClientOption`] is the configuration of internal client, which manages the connection and request with RocketMQ proxy.
#[derive(Debug, Clone)]
pub struct ClientOption {
    pub(crate) client_type: ClientType,
    pub(crate) group: Option<String>,
    pub(crate) namespace: String,
    pub(crate) access_url: String,
    pub(crate) enable_tls: bool,
    pub(crate) timeout: Duration,
    pub(crate) long_polling_timeout: Duration,
    pub(crate) access_key: Option<String>,
    pub(crate) secret_key: Option<String>,
}

impl Default for ClientOption {
    fn default() -> Self {
        ClientOption {
            client_type: ClientType::Producer,
            group: None,
            namespace: "".to_string(),
            access_url: "localhost:8081".to_string(),
            enable_tls: false,
            timeout: Duration::from_secs(3),
            long_polling_timeout: Duration::from_secs(40),
            access_key: None,
            secret_key: None,
        }
    }
}

impl ClientOption {
    /// Get the access url of RocketMQ proxy
    pub fn access_url(&self) -> &str {
        &self.access_url
    }
    /// Set the access url of RocketMQ proxy
    pub fn set_access_url(&mut self, access_url: impl Into<String>) {
        self.access_url = access_url.into();
    }

    /// Whether to enable tls
    pub fn enable_tls(&self) -> bool {
        self.enable_tls
    }
    /// Set whether to enable tls, default is true
    pub fn set_enable_tls(&mut self, enable_tls: bool) {
        self.enable_tls = enable_tls;
    }

    /// Get the timeout of connection and generic request
    pub fn timeout(&self) -> &Duration {
        &self.timeout
    }
    /// Set the timeout of connection and generic request, default is 3 seconds
    pub fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = timeout;
    }

    /// Get the await duration during long polling
    pub fn long_polling_timeout(&self) -> &Duration {
        &self.long_polling_timeout
    }
    /// Set the await duration during long polling, default is 40 seconds
    ///
    /// This option only affects receive requests, it means timeout for a receive request will be `long_polling_timeout` + `timeout`
    pub fn set_long_polling_timeout(&mut self, long_polling_timeout: Duration) {
        self.long_polling_timeout = long_polling_timeout;
    }

    /// Get the access key
    pub fn access_key(&self) -> Option<&String> {
        self.access_key.as_ref()
    }
    /// Set the access key
    pub fn set_access_key(&mut self, access_key: impl Into<String>) {
        self.access_key = Some(access_key.into());
    }

    /// Get the secret key
    pub fn secret_key(&self) -> Option<&String> {
        self.secret_key.as_ref()
    }
    /// Set the secret key
    pub fn set_secret_key(&mut self, secret_key: impl Into<String>) {
        self.secret_key = Some(secret_key.into());
    }

    pub fn get_namespace(&self) -> &str {
        &self.namespace
    }
    /// Set the namespace
    pub fn set_namespace(&mut self, namespace: impl Into<String>) {
        self.namespace = namespace.into();
    }
}

/// Log format for output.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum LoggingFormat {
    /// Print log in terminal
    Terminal,
    /// Print log in json file
    Json,
}

/// The configuration of [`Producer`].
#[derive(Debug, Clone)]
pub struct ProducerOption {
    logging_format: LoggingFormat,
    prefetch_route: bool,
    topics: Option<Vec<String>>,
    namespace: String,
    validate_message_type: bool,
    timeout: Duration,
}

impl Default for ProducerOption {
    fn default() -> Self {
        ProducerOption {
            logging_format: LoggingFormat::Terminal,
            prefetch_route: true,
            topics: None,
            namespace: "".to_string(),
            validate_message_type: true,
            timeout: Duration::from_secs(3),
        }
    }
}

impl ProducerOption {
    /// Get the logging format of producer
    pub fn logging_format(&self) -> &LoggingFormat {
        &self.logging_format
    }
    /// Set the logging format for producer
    pub fn set_logging_format(&mut self, logging_format: LoggingFormat) {
        self.logging_format = logging_format;
    }

    /// Whether to prefetch route info
    pub fn prefetch_route(&self) -> &bool {
        &self.prefetch_route
    }
    /// Set whether to prefetch route info, default is true
    pub fn set_prefetch_route(&mut self, prefetch_route: bool) {
        self.prefetch_route = prefetch_route;
    }

    /// Get which topic(s) to messages to
    pub fn topics(&self) -> &Option<Vec<String>> {
        &self.topics
    }
    /// Set which topic(s) to messages to, it will prefetch route info for these topics when the producer starts
    pub fn set_topics(&mut self, topics: Vec<impl Into<String>>) {
        self.topics = Some(topics.into_iter().map(|t| t.into()).collect());
    }

    // not expose to user for now
    pub(crate) fn namespace(&self) -> &str {
        &self.namespace
    }
    pub(crate) fn set_namespace(&mut self, name_space: impl Into<String>) {
        self.namespace = name_space.into();
    }

    /// Whether to validate message type
    pub fn validate_message_type(&self) -> bool {
        self.validate_message_type
    }
    /// Set whether to validate message type, default is true
    pub fn set_validate_message_type(&mut self, validate_message_type: bool) {
        self.validate_message_type = validate_message_type;
    }

    pub fn timeout(&self) -> &Duration {
        &self.timeout
    }
}

/// The configuration of [`SimpleConsumer`].
#[derive(Debug, Clone)]
pub struct SimpleConsumerOption {
    logging_format: LoggingFormat,
    consumer_group: String,
    prefetch_route: bool,
    topics: Option<Vec<String>>,
    namespace: String,
    timeout: Duration,
    long_polling_timeout: Duration,
}

impl Default for SimpleConsumerOption {
    fn default() -> Self {
        SimpleConsumerOption {
            logging_format: LoggingFormat::Terminal,
            consumer_group: "".to_string(),
            prefetch_route: true,
            topics: None,
            namespace: "".to_string(),
            timeout: Duration::from_secs(3),
            long_polling_timeout: Duration::from_secs(40),
        }
    }
}

impl SimpleConsumerOption {
    /// Set the logging format of simple consumer
    pub fn logging_format(&self) -> &LoggingFormat {
        &self.logging_format
    }
    /// set the logging format for simple consumer
    pub fn set_logging_format(&mut self, logging_format: LoggingFormat) {
        self.logging_format = logging_format;
    }

    /// Get the consumer group of simple consumer
    pub fn consumer_group(&self) -> &str {
        &self.consumer_group
    }
    /// Set the consumer group of simple consumer
    pub fn set_consumer_group(&mut self, consumer_group: impl Into<String>) {
        self.consumer_group = consumer_group.into();
    }

    /// Whether to prefetch route info
    pub fn prefetch_route(&self) -> &bool {
        &self.prefetch_route
    }
    /// Set whether to prefetch route info, default is true
    pub fn set_prefetch_route(&mut self, prefetch_route: bool) {
        self.prefetch_route = prefetch_route;
    }

    /// Set which topic(s) to receive messages
    pub fn topics(&self) -> &Option<Vec<String>> {
        &self.topics
    }
    /// Set which topic(s) to receive messages, it will prefetch route info for these topics when the simple consumer starts
    pub fn set_topics(&mut self, topics: Vec<impl Into<String>>) {
        self.topics = Some(topics.into_iter().map(|t| t.into()).collect());
    }

    // not expose to user for now
    pub(crate) fn namespace(&self) -> &str {
        &self.namespace
    }
    pub(crate) fn set_namespace(&mut self, name_space: impl Into<String>) {
        self.namespace = name_space.into();
    }

    pub fn timeout(&self) -> &Duration {
        &self.timeout
    }

    pub fn long_polling_timeout(&self) -> &Duration {
        &self.long_polling_timeout
    }
}

#[derive(Debug, Clone)]
pub struct PushConsumerOption {
    logging_format: LoggingFormat,
    consumer_group: String,
    namespace: String,
    timeout: Duration,
    long_polling_timeout: Duration,
    subscription_expressions: HashMap<String, FilterExpression>,
    fifo: bool,
    batch_size: i32,
    consumer_worker_count_each_queue: usize,
}

impl Default for PushConsumerOption {
    fn default() -> Self {
        Self {
            logging_format: LoggingFormat::Terminal,
            consumer_group: "".to_string(),
            namespace: "".to_string(),
            timeout: Duration::from_secs(3),
            long_polling_timeout: Duration::from_secs(40),
            subscription_expressions: HashMap::new(),
            fifo: false,
            batch_size: 32,
            consumer_worker_count_each_queue: 4,
        }
    }
}

impl PushConsumerOption {
    pub fn timeout(&self) -> &Duration {
        &self.timeout
    }

    pub fn consumer_group(&self) -> &str {
        &self.consumer_group
    }

    pub fn get_consumer_group_resource(&self) -> Resource {
        Resource {
            name: self.consumer_group.clone(),
            resource_namespace: self.namespace.clone(),
        }
    }

    pub fn set_consumer_group(&mut self, consumer_group: impl Into<String>) {
        self.consumer_group = consumer_group.into();
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn long_polling_timeout(&self) -> &Duration {
        &self.long_polling_timeout
    }

    pub fn subscription_expressions(&self) -> &HashMap<String, FilterExpression> {
        &self.subscription_expressions
    }

    pub fn subscribe(&mut self, topic: impl Into<String>, filter_expression: FilterExpression) {
        self.subscription_expressions
            .insert(topic.into(), filter_expression);
    }

    pub fn get_filter_expression(&self, topic: &str) -> Option<&FilterExpression> {
        self.subscription_expressions.get(topic)
    }

    pub fn fifo(&self) -> bool {
        self.fifo
    }

    pub(crate) fn set_fifo(&mut self, fifo: bool) {
        self.fifo = fifo;
    }

    pub fn logging_format(&self) -> &LoggingFormat {
        &self.logging_format
    }

    pub fn set_logging_format(&mut self, logging_format: LoggingFormat) {
        self.logging_format = logging_format;
    }

    pub fn batch_size(&self) -> i32 {
        self.batch_size
    }

    pub fn consumer_worker_count_each_queue(&self) -> usize {
        self.consumer_worker_count_each_queue
    }

    pub fn set_consumer_worker_count_each_queue(&mut self, count: usize) {
        self.consumer_worker_count_each_queue = count;
    }
}

#[derive(Debug, Clone)]
pub enum BackOffRetryPolicy {
    Exponential(ExponentialBackOffRetryPolicy),
    Customized(CustomizedBackOffRetryPolicy),
}

const INVALID_COMMAND: &str = "invalid command";

impl TryFrom<pb::telemetry_command::Command> for BackOffRetryPolicy {
    type Error = &'static str;

    fn try_from(value: pb::telemetry_command::Command) -> Result<Self, Self::Error> {
        if let pb::telemetry_command::Command::Settings(settings) = value {
            let pubsub = settings.pub_sub.ok_or(INVALID_COMMAND)?;
            if let pb::settings::PubSub::Subscription(_) = pubsub {
                let retry_policy = settings.backoff_policy.ok_or(INVALID_COMMAND)?;
                let strategy = retry_policy.strategy.ok_or(INVALID_COMMAND)?;
                return match strategy {
                    pb::retry_policy::Strategy::ExponentialBackoff(strategy) => {
                        Ok(BackOffRetryPolicy::Exponential(
                            ExponentialBackOffRetryPolicy::new(strategy, retry_policy.max_attempts),
                        ))
                    }
                    pb::retry_policy::Strategy::CustomizedBackoff(strategy) => {
                        Ok(BackOffRetryPolicy::Customized(
                            CustomizedBackOffRetryPolicy::new(strategy, retry_policy.max_attempts),
                        ))
                    }
                };
            }
        }
        Err(INVALID_COMMAND)
    }
}

impl BackOffRetryPolicy {
    pub fn get_next_attempt_delay(&self, attempts: i32) -> Duration {
        match self {
            BackOffRetryPolicy::Exponential(policy) => policy.get_next_attempt_delay(attempts),
            BackOffRetryPolicy::Customized(policy) => policy.get_next_attempt_delay(attempts),
        }
    }

    pub fn get_max_attempts(&self) -> i32 {
        match self {
            BackOffRetryPolicy::Exponential(policy) => policy.max_attempts(),
            BackOffRetryPolicy::Customized(policy) => policy.max_attempts(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ExponentialBackOffRetryPolicy {
    initial: Duration,
    max: Duration,
    multiplier: f32,
    max_attempts: i32,
}

impl ExponentialBackOffRetryPolicy {
    pub fn new(
        strategy: pb::ExponentialBackoff,
        max_attempts: i32,
    ) -> ExponentialBackOffRetryPolicy {
        let initial = strategy.initial.map_or(Duration::ZERO, |d| {
            Duration::new(d.seconds as u64, d.nanos as u32)
        });
        let max = strategy.max.map_or(Duration::ZERO, |d| {
            Duration::new(d.seconds as u64, d.nanos as u32)
        });
        let multiplier = strategy.multiplier;
        ExponentialBackOffRetryPolicy {
            initial,
            max,
            multiplier,
            max_attempts,
        }
    }

    pub(crate) fn get_next_attempt_delay(&self, attempts: i32) -> Duration {
        let delay_nanos = (self.initial.as_nanos() * self.multiplier.powi(attempts - 1) as u128)
            .min(self.max.as_nanos());
        Duration::from_nanos(delay_nanos as u64)
    }

    pub(crate) fn max_attempts(&self) -> i32 {
        self.max_attempts
    }
}

impl Default for ExponentialBackOffRetryPolicy {
    fn default() -> Self {
        Self {
            initial: Duration::ZERO,
            max: Duration::ZERO,
            multiplier: 1.0,
            max_attempts: 1,
        }
    }
}

#[derive(Clone, Debug)]
pub struct CustomizedBackOffRetryPolicy {
    next_list: Vec<Duration>,
    max_attempts: i32,
}

impl CustomizedBackOffRetryPolicy {
    pub fn new(strategy: pb::CustomizedBackoff, max_attempts: i32) -> CustomizedBackOffRetryPolicy {
        CustomizedBackOffRetryPolicy {
            next_list: strategy
                .next
                .iter()
                .map(|d| Duration::new(d.seconds as u64, d.nanos as u32))
                .collect(),
            max_attempts,
        }
    }

    pub(crate) fn get_next_attempt_delay(&self, attempts: i32) -> Duration {
        let mut index = attempts.min(self.next_list.len() as i32) - 1;
        if index < 0 {
            index = 0;
        }
        self.next_list[index as usize]
    }

    pub(crate) fn max_attempts(&self) -> i32 {
        self.max_attempts
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::model::common::FilterType;

    use super::*;

    #[test]
    fn conf_client_option() {
        let option = ClientOption::default();
        assert_eq!(option.access_url(), "localhost:8081");
        assert!(!option.enable_tls());
        assert_eq!(option.timeout(), &Duration::from_secs(3));
        assert_eq!(option.long_polling_timeout(), &Duration::from_secs(40));
    }

    #[test]
    fn conf_producer_option() {
        let option = ProducerOption::default();
        assert_eq!(option.logging_format(), &LoggingFormat::Terminal);
        assert!(option.prefetch_route());
        assert!(option.validate_message_type());
    }

    #[test]
    fn conf_simple_consumer_option() {
        let option = SimpleConsumerOption::default();
        assert_eq!(option.logging_format(), &LoggingFormat::Terminal);
        assert!(option.prefetch_route());
    }

    #[test]
    fn conf_push_consumer_option() {
        let mut option = PushConsumerOption::default();
        option.subscribe("topic", FilterExpression::new(FilterType::Tag, "*"));
        assert!(option.subscription_expressions().contains_key("topic"));
    }

    #[test]
    fn test_exponential_backoff() {
        let policy = ExponentialBackOffRetryPolicy::new(
            pb::ExponentialBackoff {
                initial: Some(prost_types::Duration::from_str("1s").unwrap()),
                max: Some(prost_types::Duration::from_str("60s").unwrap()),
                multiplier: 2.0,
            },
            1,
        );
        assert_eq!(Duration::from_secs(1), policy.get_next_attempt_delay(1));
        assert_eq!(Duration::from_secs(2), policy.get_next_attempt_delay(2));
        assert_eq!(Duration::from_secs(4), policy.get_next_attempt_delay(3));
        assert_eq!(Duration::from_secs(8), policy.get_next_attempt_delay(4));
        assert_eq!(Duration::from_secs(16), policy.get_next_attempt_delay(5));
        assert_eq!(Duration::from_secs(32), policy.get_next_attempt_delay(6));
        assert_eq!(Duration::from_secs(60), policy.get_next_attempt_delay(7));
        assert_eq!(Duration::from_secs(60), policy.get_next_attempt_delay(8));
    }

    #[test]
    fn test_customized_backoff() {
        let next: Vec<prost_types::Duration> = vec![
            prost_types::Duration::from_str("1s").unwrap(),
            prost_types::Duration::from_str("10s").unwrap(),
            prost_types::Duration::from_str("20s").unwrap(),
            prost_types::Duration::from_str("30s").unwrap(),
        ];
        let policy = CustomizedBackOffRetryPolicy::new(pb::CustomizedBackoff { next }, 1);
        assert_eq!(Duration::from_secs(1), policy.get_next_attempt_delay(1));
        assert_eq!(Duration::from_secs(10), policy.get_next_attempt_delay(2));
        assert_eq!(Duration::from_secs(20), policy.get_next_attempt_delay(3));
        assert_eq!(Duration::from_secs(30), policy.get_next_attempt_delay(4));
        assert_eq!(Duration::from_secs(30), policy.get_next_attempt_delay(5));
    }

    use pb::settings::PubSub;
    use pb::CustomizedBackoff;
    use pb::ExponentialBackoff;
    use pb::RetryPolicy;
    use pb::Settings;
    use pb::Subscription;

    #[test]
    fn test_parse_back_policy() -> Result<(), String> {
        let command = pb::telemetry_command::Command::VerifyMessageCommand(
            pb::VerifyMessageCommand::default(),
        );
        let result = BackOffRetryPolicy::try_from(command);
        assert!(result.is_err());

        let command2 = pb::telemetry_command::Command::Settings(pb::Settings::default());
        let result2 = BackOffRetryPolicy::try_from(command2);
        assert!(result2.is_err());

        let exponential_backoff_settings = pb::Settings {
            pub_sub: Some(PubSub::Subscription(Subscription::default())),
            client_type: Some(ClientType::PushConsumer as i32),
            backoff_policy: Some(RetryPolicy {
                max_attempts: 0,
                strategy: Some(pb::retry_policy::Strategy::ExponentialBackoff(
                    ExponentialBackoff {
                        initial: Some(prost_types::Duration::from_str("1s").unwrap()),
                        max: Some(prost_types::Duration::from_str("60s").unwrap()),
                        multiplier: 2.0,
                    },
                )),
            }),
            ..Settings::default()
        };
        let command3 = pb::telemetry_command::Command::Settings(exponential_backoff_settings);
        let result3 = BackOffRetryPolicy::try_from(command3);
        if let Ok(BackOffRetryPolicy::Exponential(_)) = result3 {
        } else {
            return Err("get_backoff_policy failed, expected ExponentialBackoff.".to_string());
        }

        let customized_backoff_settings = Settings {
            pub_sub: Some(PubSub::Subscription(Subscription::default())),
            client_type: Some(ClientType::PushConsumer as i32),
            backoff_policy: Some(RetryPolicy {
                max_attempts: 0,
                strategy: Some(pb::retry_policy::Strategy::CustomizedBackoff(
                    CustomizedBackoff {
                        next: vec![prost_types::Duration::from_str("1s").unwrap()],
                    },
                )),
            }),
            ..Settings::default()
        };
        let command4 = pb::telemetry_command::Command::Settings(customized_backoff_settings);
        let result4 = BackOffRetryPolicy::try_from(command4);
        if let Ok(BackOffRetryPolicy::Customized(_)) = result4 {
        } else {
            return Err("get_backoff_policy failed, expected CustomizedBackoff.".to_string());
        }
        Ok(())
    }
}
