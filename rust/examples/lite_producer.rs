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

//! Example demonstrating how to send lite messages using RocketMQ Rust client.
//!
//! Lite topics provide reduced metadata and storage overhead compared to regular topics.
//! This example shows how to create and send messages to lite topics.

use rocketmq::conf::{ClientOption, ProducerOption};
use rocketmq::model::message::MessageBuilder;
use rocketmq::Producer;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize tracing for logging
    tracing_subscriber::fmt::init();

    // Configure client options
    let mut client_option = ClientOption::default();
    client_option.set_access_url("http://localhost:8080");

    // Configure producer options
    let mut producer_option = ProducerOption::default();
    producer_option.set_topics(vec!["parent_topic".to_string()]);

    // Create and start the producer
    let mut producer = Producer::new(producer_option, client_option)?;
    producer.start().await?;

    println!("Producer started successfully");

    // Define your message body
    let body = b"This is a lite message for Apache RocketMQ";

    // Build a lite message
    // Note: lite_topic cannot be used with message_group, delivery_timestamp, or priority
    let message = MessageBuilder::lite_message_builder(
        "parent_topic",           // Parent topic name
        body.to_vec(),            // Message body
        "lite-topic-1",           // Lite topic name
    )
    .set_keys(vec!["yourMessageKey-3ee439f945d7".to_string()])
    .build()?;

    println!("Sending lite message to lite-topic-1...");

    // Send the message
    match producer.send(message).await {
        Ok(receipt) => {
            println!(
                "Send message successfully, message_id={}",
                receipt.message_id()
            );
        }
        Err(e) => {
            eprintln!("Failed to send message: {:?}", e);
            // Handle quota exceeded error if needed
            // if e.kind() == ErrorKind::LiteTopicQuotaExceeded {
            //     eprintln!("Lite topic quota exceeded. Consider increasing the limit.");
            // }
        }
    }

    // Send multiple lite messages to different lite topics
    for i in 1..=3 {
        let lite_topic = format!("lite-topic-{}", i);
        let message_body = format!("Lite message {} content", i);

        let message = MessageBuilder::lite_message_builder(
            "parent_topic",
            message_body.into_bytes(),
            &lite_topic,
        )
        .build()?;

        match producer.send(message).await {
            Ok(receipt) => {
                println!(
                    "Sent message to {}, message_id={}",
                    lite_topic,
                    receipt.message_id()
                );
            }
            Err(e) => {
                eprintln!("Failed to send to {}: {:?}", lite_topic, e);
            }
        }
    }

    println!("\nAll messages sent successfully!");

    // Shutdown the producer when done
    producer.shutdown().await?;
    println!("Producer shutdown");

    Ok(())
}
