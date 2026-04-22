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
    tracing_subscriber::fmt::init();

    // It's recommended to specify the topics that applications will publish messages to
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

    // Build delay message
    let message = MessageBuilder::delay_message_builder(
        "delay_test",
        "This is a delay message that will be recalled"
            .as_bytes()
            .to_vec(),
        // deliver in 60 seconds
        SystemTime::now()
            .add(Duration::from_secs(60))
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64,
    )
    .set_tag("DelayTag")
    .set_keys(vec!["delay-key"])
    .build()
    .unwrap();

    // Send delay message to rocketmq proxy
    let send_result = producer.send(message).await;
    if send_result.is_err() {
        eprintln!("send message failed: {:?}", send_result.unwrap_err());
        return;
    }

    let receipt = send_result.unwrap();
    println!(
        "Send delay message success, message_id={}, recall_handle={:?}",
        receipt.message_id(),
        receipt.recall_handle()
    );

    // Attempt to recall the message if recall_handle is available
    if let Some(recall_handle) = receipt.recall_handle() {
        println!("Attempting to recall the message...");

        let recall_result = producer.recall_message("delay_test", recall_handle).await;

        match recall_result {
            Ok(recalled_message_id) => {
                println!(
                    "Successfully recalled message! Recalled message_id: {}",
                    recalled_message_id
                );
            }
            Err(e) => {
                eprintln!("Failed to recall message: {:?}", e);
                println!("Note: Recall operation requires server support.");
            }
        }
    } else {
        println!("No recall handle available. This message cannot be recalled.");
    }

    // shutdown the producer when you don't need it anymore.
    let shutdown_result = producer.shutdown().await;
    if shutdown_result.is_err() {
        eprintln!(
            "producer shutdown failed: {:?}",
            shutdown_result.unwrap_err()
        );
    }
}
