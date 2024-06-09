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

//! # The Rust Implementation of Apache RocketMQ Client
//!
//! Here is the official rust client for [Apache RocketMQ](https://rocketmq.apache.org/)
//! providing async/await API powered by tokio runtime.
//!
//! Different from the [remoting-based client](https://github.com/apache/rocketmq/tree/develop/client),
//! the current implementation is based on separating architecture for computing and storage,
//! which is the more recommended way to access the RocketMQ service.
//!
//! Here are some preparations you may need to know: [RocketMQ Quick Start](https://rocketmq.apache.org/docs/quickStart/02quickstart).
//!
//! ## Examples
//!
//! Basic usage:
//!
//! ### Producer
//! ```rust,no_run
//! use rocketmq::conf::{ClientOption, ProducerOption};
//! use rocketmq::model::message::MessageBuilder;
//! use rocketmq::Producer;
//!
//! #[tokio::main]
//! async fn main() {
//!     // recommend to specify which topic(s) you would like to send message to
//!     // producer will prefetch topic route when start and failed fast if topic not exist
//!     let mut producer_option = ProducerOption::default();
//!     producer_option.set_topics(vec!["test_topic"]);
//!
//!     // set which rocketmq proxy to connect
//!     let mut client_option = ClientOption::default();
//!     client_option.set_access_url("localhost:8081");
//!
//!     // build and start producer
//!     let mut  producer = Producer::new(producer_option, client_option).unwrap();
//!     producer.start().await.unwrap();
//!
//!     // build message
//!     let message = MessageBuilder::builder()
//!         .set_topic("test_topic")
//!         .set_tag("test_tag")
//!         .set_body("hello world".as_bytes().to_vec())
//!         .build()
//!         .unwrap();
//!
//!     // send message to rocketmq proxy
//!     let result = producer.send(message).await;
//!     debug_assert!(result.is_ok(), "send message failed: {:?}", result);
//!     println!(
//!         "send message success, message_id={}",
//!         result.unwrap().message_id()
//!     );
//! }
//! ```
//!
//! ### Simple Consumer
//! ```rust,no_run
//! use rocketmq::conf::{ClientOption, SimpleConsumerOption};
//! use rocketmq::model::common::{FilterExpression, FilterType};
//! use rocketmq::SimpleConsumer;
//!
//! #[tokio::main]
//! async fn main() {
//!     // recommend to specify which topic(s) you would like to send message to
//!     // simple consumer will prefetch topic route when start and failed fast if topic not exist
//!     let mut consumer_option = SimpleConsumerOption::default();
//!     consumer_option.set_topics(vec!["test_topic"]);
//!     consumer_option.set_consumer_group("SimpleConsumerGroup");
//!
//!     // set which rocketmq proxy to connect
//!     let mut client_option = ClientOption::default();
//!     client_option.set_access_url("localhost:8081");
//!
//!     // build and start simple consumer
//!     let mut  consumer = SimpleConsumer::new(consumer_option, client_option).unwrap();
//!     consumer.start().await.unwrap();
//!
//!     // pop message from rocketmq proxy
//!     let receive_result = consumer
//!         .receive(
//!             "test_topic".to_string(),
//!             &FilterExpression::new(FilterType::Tag, "test_tag"),
//!         )
//!         .await;
//!     debug_assert!(
//!         receive_result.is_ok(),
//!         "receive message failed: {:?}",
//!         receive_result.unwrap_err()
//!     );
//!
//!     let messages = receive_result.unwrap();
//!     for message in messages {
//!         println!("receive message: {:?}", message);
//!         // ack message to rocketmq proxy
//!         let ack_result = consumer.ack(&message).await;
//!         debug_assert!(
//!             ack_result.is_ok(),
//!             "ack message failed: {:?}",
//!             ack_result.unwrap_err()
//!         );
//!     }
//! }
//! ```
//!

// Export structs that are part of crate API.
pub use producer::Producer;
pub use push_consumer::MessageListener;
pub use push_consumer::PushConsumer;
pub use simple_consumer::SimpleConsumer;

#[allow(dead_code)]
pub mod conf;
pub mod error;
#[allow(dead_code)]
mod log;

mod client;

#[rustfmt::skip]
#[allow(clippy::all)]
#[path = "pb/apache.rocketmq.v2.rs"]
mod pb;
mod session;

pub mod model;
mod util;

mod producer;
mod push_consumer;
mod simple_consumer;
