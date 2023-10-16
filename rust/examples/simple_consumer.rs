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
#[cfg(feature = "example_change_invisible_duration")]
use std::time::Duration;

use rocketmq::conf::{ClientOption, SimpleConsumerOption};
use rocketmq::model::common::{FilterExpression, FilterType};
use rocketmq::SimpleConsumer;

#[tokio::main]
async fn main() {
    // It's recommended to specify the topics that applications will publish messages to
    // because the simple consumer will prefetch topic routes for them on start and fail fast in case they do not exist
    let mut consumer_option = SimpleConsumerOption::default();
    consumer_option.set_topics(vec!["test_topic"]);
    consumer_option.set_consumer_group("SimpleConsumerGroup");

    // set which rocketmq proxy to connect
    let mut client_option = ClientOption::default();
    client_option.set_access_url("localhost:8081");
    client_option.set_enable_tls(false);

    // build and start simple consumer
    let mut consumer = SimpleConsumer::new(consumer_option, client_option).unwrap();
    let start_result = consumer.start().await;
    if start_result.is_err() {
        eprintln!(
            "simple consumer start failed: {:?}",
            start_result.unwrap_err()
        );
        return;
    }

    // pop message from rocketmq proxy
    let receive_result = consumer
        .receive(
            "test_topic".to_string(),
            &FilterExpression::new(FilterType::Tag, "test_tag"),
        )
        .await;
    if receive_result.is_err() {
        eprintln!("receive message failed: {:?}", receive_result.unwrap_err());
        return;
    }

    let messages = receive_result.unwrap();

    if messages.is_empty() {
        println!("no message received");
        return;
    }

    for message in messages {
        println!("receive message: {:?}", message);

        // Do your business logic here
        // And then acknowledge the message to the RocketMQ proxy if everything is okay
        #[cfg(feature = "example_ack")]
        {
            println!("ack message {}", message.message_id());
            let ack_result = consumer.ack(&message).await;
            if ack_result.is_err() {
                eprintln!(
                    "ack message {} failed: {:?}",
                    message.message_id(),
                    ack_result.unwrap_err()
                );
            }
        }

        // Otherwise, you can retry this message later by changing the invisible duration
        #[cfg(feature = "example_change_invisible_duration")]
        {
            println!(
                "Delay next visible time of message {} by 10s",
                message.message_id()
            );
            let change_invisible_duration_result = consumer
                .change_invisible_duration(&message, Duration::from_secs(10))
                .await;
            if change_invisible_duration_result.is_err() {
                eprintln!(
                    "change message {} invisible duration failed: {:?}",
                    message.message_id(),
                    change_invisible_duration_result.unwrap_err()
                );
            }
        }
    }

    // shutdown the simple consumer when you don't need it anymore.
    // you should shutdown it manually to gracefully stop and unregister from server
    let shutdown_result = consumer.shutdown().await;
    if shutdown_result.is_err() {
        eprintln!(
            "simple consumer shutdown failed: {:?}",
            shutdown_result.unwrap_err()
        );
    }
}
