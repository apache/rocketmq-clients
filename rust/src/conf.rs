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
#[derive(Debug, Clone)]
pub struct ClientOption {
    name_space: String,
    access_url: String,
    enable_tls: bool,
}

impl Default for ClientOption {
    fn default() -> Self {
        ClientOption {
            name_space: "".to_string(),
            access_url: "localhost:8081".to_string(),
            enable_tls: false,
        }
    }
}

impl ClientOption {
    pub fn name_space(&self) -> &str {
        &self.name_space
    }
    pub fn set_name_space(&mut self, name_space: String) {
        self.name_space = name_space;
    }

    pub fn access_url(&self) -> &str {
        &self.access_url
    }
    pub fn set_access_url(&mut self, access_url: String) {
        self.access_url = access_url;
    }

    pub fn enable_tls(&self) -> bool {
        self.enable_tls
    }
    pub fn set_enable_tls(&mut self, enable_tls: bool) {
        self.enable_tls = enable_tls;
    }
}

#[derive(Debug, Clone)]
pub enum LoggingFormat {
    Terminal,
    Json,
}

#[derive(Debug, Clone)]
pub struct ProducerOption {
    logging_format: LoggingFormat,
    prefetch_route: bool,
    topics: Option<Vec<String>>,
    namespace: String,
}

impl Default for ProducerOption {
    fn default() -> Self {
        ProducerOption {
            logging_format: LoggingFormat::Terminal,
            prefetch_route: true,
            topics: None,
            namespace: "".to_string(),
        }
    }
}

impl ProducerOption {
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
    pub fn set_topics(&mut self, topics: Vec<String>) {
        self.topics = Some(topics);
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }
    pub fn set_namespace(&mut self, name_space: String) {
        self.namespace = name_space;
    }
}
