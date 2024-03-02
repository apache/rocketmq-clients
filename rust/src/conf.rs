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

use std::time::Duration;

use crate::model::common::ClientType;
use crate::pb::TelemetryCommand;
#[allow(unused_imports)]
use crate::producer::Producer;
#[allow(unused_imports)]
use crate::simple_consumer::SimpleConsumer;
use crate::util::{build_producer_settings, build_simple_consumer_settings};

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

pub trait SettingsAware {
    fn build_telemetry_command(&self) -> TelemetryCommand;
}

impl SettingsAware for ProducerOption {
    fn build_telemetry_command(&self) -> TelemetryCommand {
        build_producer_settings(self)
    }
}

impl SettingsAware for SimpleConsumerOption {
    fn build_telemetry_command(&self) -> TelemetryCommand {
        build_simple_consumer_settings(self)
    }
}

#[cfg(test)]
mod tests {
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
}
