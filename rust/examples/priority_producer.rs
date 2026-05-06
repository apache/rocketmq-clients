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
use rocketmq::conf::{ClientOption, ProducerOption};
use rocketmq::model::message::MessageBuilder;
use rocketmq::Producer;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    // It's recommended to specify the topics that applications will publish messages to
    // because the producer will prefetch topic routes for them on start and fail fast in case they do not exist
    let mut producer_option = ProducerOption::default();
    producer_option.set_topics(vec!["priority_test"]);

    // set which rocketmq proxy to connect
    let mut client_option = ClientOption::default();
    client_option.set_access_url("localhost:8081");

    // build and start producer
    let mut producer = Producer::new(producer_option, client_option).unwrap();
    let start_result = producer.start().await;
    if start_result.is_err() {
        eprintln!("producer start failed: {:?}", start_result.unwrap_err());
        return;
    }

    // Build priority message using priority_message_builder
    let message = MessageBuilder::priority_message_builder(
        "priority_test",
        "This is a priority message".as_bytes().to_vec(),
        1, // priority level, higher value means higher priority
    )
    .set_tag("PriorityTag")
    .set_keys(vec!["priority-key"])
    .build()
    .unwrap();

    // send message to rocketmq proxy
    let send_result = producer.send(message).await;
    if send_result.is_err() {
        eprintln!("send message failed: {:?}", send_result.unwrap_err());
        return;
    }
    println!(
        "Send priority message success, message_id={}",
        send_result.unwrap().message_id()
    );

    // Alternatively, you can use builder pattern with set_priority
    let message2 = MessageBuilder::builder()
        .set_topic("priority_test")
        .set_body("Another priority message".as_bytes().to_vec())
        .set_tag("PriorityTag")
        .set_keys(vec!["priority-key-2"])
        .set_priority(5) // Set priority level
        .build()
        .unwrap();

    let send_result2 = producer.send(message2).await;
    if send_result2.is_err() {
        eprintln!("send message failed: {:?}", send_result2.unwrap_err());
        return;
    }
    println!(
        "Send priority message success, message_id={}",
        send_result2.unwrap().message_id()
    );

    // shutdown the producer when you don't need it anymore.
    // recommend shutdown manually to gracefully stop and unregister from server
    let shutdown_result = producer.shutdown().await;
    if shutdown_result.is_err() {
        eprintln!(
            "producer shutdown failed: {:?}",
            shutdown_result.unwrap_err()
        );
    }
}
