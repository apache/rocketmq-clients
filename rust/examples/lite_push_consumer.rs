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
//! LitePushConsumer provides efficient consumption of lite topics with reduced overhead.
//! This example shows how to subscribe to lite topics and handle messages.

use rocketmq::conf::{ClientOption, PushConsumerOption};
use rocketmq::model::message::MessageView;
use rocketmq::{ConsumeResult, LitePushConsumer, LitePushConsumerTrait};
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize tracing for logging
    tracing_subscriber::fmt::init();

    #[cfg(not(test))]
    {
        // Configure client options
        let mut client_option = ClientOption::default();
        client_option.set_access_url("http://localhost:8080");

        // Configure push consumer options
        let mut option = PushConsumerOption::default();
        option.set_consumer_group("yourConsumerGroup");

        // Create message listener
        let message_listener = Box::new(|message: &MessageView| {
            println!("Received message: {:?}", message);
            println!("  Message ID: {}", message.message_id());
            println!("  Topic: {:?}", message.topic());
            if let Some(lite_topic) = message.lite_topic() {
                println!("  Lite Topic: {}", lite_topic);
            }
            println!("  Body: {:?}", String::from_utf8_lossy(message.body()));

            // Handle the received message and return consume result
            ConsumeResult::SUCCESS
        });

        // Create and start the LitePushConsumer
        // Note: bind_topic is the parent topic that lite topics belong to
        let mut consumer = LitePushConsumer::new(
            client_option,
            option,
            "yourParentTopic".to_string(),
            message_listener,
        )?;

        consumer.start().await?;
        println!("LitePushConsumer started successfully");

        // Subscribe to lite topics
        // The subscribe_lite() method initiates network requests and performs quota verification,
        // so it may fail. It's important to check the result of this call.
        //
        // Possible failure scenarios:
        // 1. Network request errors - can be retried
        // 2. Quota verification failures (LiteSubscriptionQuotaExceededException) - 
        //    evaluate whether quota is insufficient and unsubscribe unused topics
        
        match consumer.subscribe_lite("lite-topic-1".to_string()).await {
            Ok(_) => println!("Subscribed to lite-topic-1"),
            Err(e) => eprintln!("Failed to subscribe to lite-topic-1: {:?}", e),
        }

        match consumer.subscribe_lite("lite-topic-2".to_string()).await {
            Ok(_) => println!("Subscribed to lite-topic-2"),
            Err(e) => eprintln!("Failed to subscribe to lite-topic-2: {:?}", e),
        }

        match consumer.subscribe_lite("lite-topic-3".to_string()).await {
            Ok(_) => println!("Subscribed to lite-topic-3"),
            Err(e) => eprintln!("Failed to subscribe to lite-topic-3: {:?}", e),
        }

        // Optionally unsubscribe from a lite topic when no longer needed
        // This frees up quota resources
        // consumer.unsubscribe_lite("lite-topic-3".to_string()).await?;

        // Get current subscribed lite topics
        let topics = consumer.get_lite_topic_set();
        println!("\nCurrently subscribed lite topics: {:?}", topics);

        println!("\nConsumer is running. Press Ctrl+C to stop...");

        // Wait for shutdown signal
        tokio::signal::ctrl_c().await?;
        println!("\nReceived shutdown signal");

        // Shutdown the consumer
        consumer.shutdown().await?;
        println!("LitePushConsumer shutdown");
    }

    #[cfg(test)]
    {
        println!("This example is not available in test mode");
    }

    Ok(())
}
