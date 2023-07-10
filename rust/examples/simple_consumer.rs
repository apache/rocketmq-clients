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
use rocketmq::conf::{ClientOption, SimpleConsumerOption};
use rocketmq::model::common::{FilterExpression, FilterType};
use rocketmq::SimpleConsumer;

#[tokio::main]
async fn main() {
    // recommend specifying which topic(s) you would like to send message to
    // simple consumer will prefetch topic route when starting and failed fast if topic does not exist
    let mut consumer_option = SimpleConsumerOption::default();
    consumer_option.set_topics(vec!["test_topic"]);
    consumer_option.set_consumer_group("SimpleConsumerGroup");

    // set which rocketmq proxy to connect
    let mut client_option = ClientOption::default();
    client_option.set_access_url("localhost:8081");
    client_option.set_enable_tls(false);

    // build and start simple consumer
    let mut consumer = SimpleConsumer::new(consumer_option, client_option).unwrap();
    consumer.start().await.unwrap();

    // pop message from rocketmq proxy
    let receive_result = consumer
        .receive(
            "test_topic".to_string(),
            &FilterExpression::new(FilterType::Tag, "test_tag"),
        )
        .await;
    debug_assert!(
        receive_result.is_ok(),
        "receive message failed: {:?}",
        receive_result.unwrap_err()
    );

    let messages = receive_result.unwrap();

    if messages.is_empty() {
        println!("no message received");
        return;
    }

    for message in messages {
        println!("receive message: {:?}", message);
        // ack message to rocketmq proxy
        let ack_result = consumer.ack(&message).await;
        debug_assert!(
            ack_result.is_ok(),
            "ack message failed: {:?}",
            ack_result.unwrap_err()
        );
    }

    // shutdown the simple consumer when you don't need it anymore.
    // you should shutdown it manually to gracefully stop and unregister from server
    let shutdown_result = consumer.shutdown().await;
    debug_assert!(
        shutdown_result.is_ok(),
        "simple consumer shutdown failed: {:?}",
        shutdown_result
    );
}
