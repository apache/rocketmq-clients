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
use std::ops::Add;
use std::time::{Duration, SystemTime};

use rocketmq::conf::{ClientOption, ProducerOption};
use rocketmq::model::message::MessageBuilder;
use rocketmq::Producer;

#[tokio::main]
async fn main() {
    // It's recommended to specify the topics that applications will publish messages to
    // because the producer will prefetch topic routes for them on start and fail fast in case they do not exist
    let mut producer_option = ProducerOption::default();
    producer_option.set_topics(vec!["delay_test"]);

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

    // build message
    let message = MessageBuilder::delay_message_builder(
        "delay_test",
        "hello world".as_bytes().to_vec(),
        // deliver in 15 seconds
        SystemTime::now()
            .add(Duration::from_secs(15))
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64,
    )
    .build()
    .unwrap();

    // send message to rocketmq proxy
    let send_result = producer.send(message).await;
    if send_result.is_err() {
        eprintln!("send message failed: {:?}", send_result.unwrap_err());
        return;
    }
    println!(
        "send message success, message_id={}",
        send_result.unwrap().message_id()
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
