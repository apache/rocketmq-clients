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
use crate::model::common::ClientType;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct ClientOption {
    pub(crate) client_type: ClientType,
    pub(crate) group: String,
    pub(crate) namespace: String,
    pub(crate) access_url: String,
    pub(crate) enable_tls: bool,
    pub(crate) timeout: Duration,
    pub(crate) long_polling_timeout: Duration,
}

impl Default for ClientOption {
    fn default() -> Self {
        ClientOption {
            client_type: ClientType::Producer,
            group: "".to_string(),
            namespace: "".to_string(),
            access_url: "localhost:8081".to_string(),
            enable_tls: false,
            timeout: Duration::from_secs(10),
            long_polling_timeout: Duration::from_secs(40),
        }
    }
}

impl ClientOption {
    pub fn access_url(&self) -> &str {
        &self.access_url
    }
    pub fn set_access_url(&mut self, access_url: impl Into<String>) {
        self.access_url = access_url.into();
    }

    pub fn enable_tls(&self) -> bool {
        self.enable_tls
    }
    pub fn set_enable_tls(&mut self, enable_tls: bool) {
        self.enable_tls = enable_tls;
    }

    pub fn timeout(&self) -> &Duration {
        &self.timeout
    }
    pub fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = timeout;
    }

    pub fn long_polling_timeout(&self) -> &Duration {
        &self.long_polling_timeout
    }
    pub fn set_long_polling_timeout(&mut self, long_polling_timeout: Duration) {
        self.long_polling_timeout = long_polling_timeout;
    }
}

#[derive(Debug, Clone)]
pub enum LoggingFormat {
    Terminal,
    Json,
}

#[derive(Debug, Clone)]
pub struct ProducerOption {
    producer_group: String,
    logging_format: LoggingFormat,
    prefetch_route: bool,
    topics: Option<Vec<String>>,
    namespace: String,
    validate_message_type: bool,
}

impl Default for ProducerOption {
    fn default() -> Self {
        ProducerOption {
            producer_group: "".to_string(),
            logging_format: LoggingFormat::Terminal,
            prefetch_route: true,
            topics: None,
            namespace: "".to_string(),
            validate_message_type: true,
        }
    }
}

impl ProducerOption {
    pub fn producer_group(&self) -> &str {
        &self.producer_group
    }
    pub fn set_producer_group(&mut self, producer_group: impl Into<String>) {
        self.producer_group = producer_group.into();
    }

    pub fn logging_format(&self) -> &LoggingFormat {
        &self.logging_format
    }
    pub fn set_logging_format(&mut self, logging_format: LoggingFormat) {
        self.logging_format = logging_format;
    }

    pub fn prefetch_route(&self) -> &bool {
        &self.prefetch_route
    }
    pub fn set_prefetch_route(&mut self, prefetch_route: bool) {
        self.prefetch_route = prefetch_route;
    }

    pub fn topics(&self) -> &Option<Vec<String>> {
        &self.topics
    }
    pub fn set_topics(&mut self, topics: Vec<impl Into<String>>) {
        self.topics = Some(topics.into_iter().map(|t| t.into()).collect());
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }
    pub fn set_namespace(&mut self, name_space: impl Into<String>) {
        self.namespace = name_space.into();
    }

    pub fn validate_message_type(&self) -> bool {
        self.validate_message_type
    }
    pub fn set_validate_message_type(&mut self, validate_message_type: bool) {
        self.validate_message_type = validate_message_type;
    }
}

#[derive(Debug, Clone)]
pub struct SimpleConsumerOption {
    logging_format: LoggingFormat,
    consumer_group: String,
    prefetch_route: bool,
    topics: Option<Vec<String>>,
    namespace: String,
    await_duration: Duration,
}

impl Default for SimpleConsumerOption {
    fn default() -> Self {
        SimpleConsumerOption {
            logging_format: LoggingFormat::Terminal,
            consumer_group: "".to_string(),
            prefetch_route: true,
            topics: None,
            namespace: "".to_string(),
            await_duration: Duration::from_secs(1),
        }
    }
}

impl SimpleConsumerOption {
    pub fn logging_format(&self) -> &LoggingFormat {
        &self.logging_format
    }
    pub fn set_logging_format(&mut self, logging_format: LoggingFormat) {
        self.logging_format = logging_format;
    }

    pub fn consumer_group(&self) -> &str {
        &self.consumer_group
    }
    pub fn set_consumer_group(&mut self, consumer_group: impl Into<String>) {
        self.consumer_group = consumer_group.into();
    }

    pub fn prefetch_route(&self) -> &bool {
        &self.prefetch_route
    }
    pub fn set_prefetch_route(&mut self, prefetch_route: bool) {
        self.prefetch_route = prefetch_route;
    }

    pub fn topics(&self) -> &Option<Vec<String>> {
        &self.topics
    }
    pub fn set_topics(&mut self, topics: Vec<impl Into<String>>) {
        self.topics = Some(topics.into_iter().map(|t| t.into()).collect());
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }
    pub fn set_namespace(&mut self, name_space: impl Into<String>) {
        self.namespace = name_space.into();
    }

    pub fn await_duration(&self) -> &Duration {
        &self.await_duration
    }
    pub fn set_await_duration(&mut self, await_duration: Duration) {
        self.await_duration = await_duration;
    }
}
