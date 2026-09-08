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

//! Example demonstrating how to consume lite messages using RocketMQ Rust client.
//!
//! LiteSimpleConsumer provides pull-style consumption of lite topics with reduced overhead.
//! This example shows how to subscribe to lite topics, receive messages, and ack them.

use rocketmq::conf::{ClientOption, SimpleConsumerOption};
use rocketmq::{LiteSimpleConsumer, OffsetOption, OffsetPolicy};
use std::error::Error;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize tracing for logging
    tracing_subscriber::fmt::init();

    #[cfg(not(test))]
    {
        // Configure client options
        let mut client_option = ClientOption::default();
        client_option.set_access_url("127.0.0.1:8080");

        // Configure simple consumer options
        let mut option = SimpleConsumerOption::default();
        option.set_consumer_group("yourConsumerGroup");

        // Create and start the LiteSimpleConsumer
        // Note: bind_topic is the parent topic that lite topics belong to.
        // It must match the parent topic used by the producer (see examples/lite_producer.rs).
        let mut consumer =
            LiteSimpleConsumer::new(client_option, option, "parent_topic".to_string())?;

        consumer.start().await?;
        println!("LiteSimpleConsumer started successfully");

        // Subscribe to lite topics.
        // `subscribe_lite()` defaults to consuming from the latest offset. Here we use
        // `subscribe_lite_with_offset()` with `OffsetPolicy::Min` to consume from the earliest
        // available offset, so that messages produced before this consumer subscribed are not missed.
        // These methods initiate network requests and perform quota verification, so they may fail;
        // it's important to check the result of each call.
        let from_beginning = OffsetOption::from_policy(OffsetPolicy::Min);
        match consumer
            .subscribe_lite_with_offset("lite-topic-1".to_string(), from_beginning.clone())
            .await
        {
            Ok(_) => println!("Subscribed to lite-topic-1"),
            Err(e) => eprintln!("Failed to subscribe to lite-topic-1: {:?}", e),
        }

        match consumer
            .subscribe_lite_with_offset("lite-topic-2".to_string(), from_beginning.clone())
            .await
        {
            Ok(_) => println!("Subscribed to lite-topic-2"),
            Err(e) => eprintln!("Failed to subscribe to lite-topic-2: {:?}", e),
        }

        // Get current subscribed lite topics
        let topics = consumer.get_lite_topic_set();
        println!("\nCurrently subscribed lite topics: {:?}", topics);

        // Receive messages from the bind (parent) topic.
        // In lite mode, messages are delivered from the aggregation topic, and each
        // message carries a `__LITE_TOPIC` property identifying its actual lite topic.
        let mut received = 0usize;
        for _ in 0..5 {
            match consumer.receive(16, Duration::from_secs(15)).await {
                Ok(messages) => {
                    if messages.is_empty() {
                        println!("No messages received in this round.");
                    }
                    for message in messages {
                        received += 1;
                        println!("Received message #{}", received);
                        println!("  Message ID: {}", message.message_id());
                        println!("  Topic: {}", message.topic());
                        if let Some(lite_topic) = message.lite_topic() {
                            println!("  Lite Topic: {}", lite_topic);
                        }
                        println!("  Body: {:?}", String::from_utf8_lossy(message.body()));

                        // Ack the message so it will not be delivered again.
                        if let Err(e) = consumer.ack(&message).await {
                            eprintln!("Failed to ack message {}: {:?}", message.message_id(), e);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Failed to receive messages: {:?}", e);
                }
            }
            tokio::time::sleep(Duration::from_secs(3)).await;
        }

        println!("\nReceived {} messages in total.", received);

        // Shutdown the consumer
        consumer.shutdown().await?;
        println!("LiteSimpleConsumer shutdown");
    }

    #[cfg(test)]
    {
        println!("This example is not available in test mode");
    }

    Ok(())
}
