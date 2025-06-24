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
use std::time::Duration;
use rocketmq::conf::{ClientOption, SimpleConsumerOption};
use rocketmq::model::common::{FilterExpression, FilterType};
use rocketmq::SimpleConsumer;

#[tokio::main]
async fn main() {
    // It's recommended to specify the topics that applications will publish messages to
    // because the simple consumer will prefetch topic routes for them on start and fail fast in case they do not exist
    let mut consumer_option = SimpleConsumerOption::default();
    consumer_option.set_topics(vec!["TestTopic"]);
    consumer_option.set_consumer_group("SimpleConsumerGroup");
    // set which rocketmq proxy to connect
    let mut client_option = ClientOption::default();
    client_option.set_access_url("localhost:8082");
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

    // Loop until a shutdown signal (like Ctrl+C) is received
    loop {
        let filter = FilterExpression::new(FilterType::Tag, "*");
        tokio::select! {
            // Branch to handle shutdown signal
            _ = tokio::signal::ctrl_c() => {
                println!("Received shutdown signal, exiting...");
                break; // Exit the loop to proceed to shutdown
            }
            // Branch to receive messages
            receive_result = consumer.receive(
                "TestTopic".to_string(),
                &filter,
            ) => {
                if let Err(e) = receive_result {
                    eprintln!("receive message failed: {:?}", e);
                    // Decide if you want to break the loop on error
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    continue;
                }

                let messages = receive_result.unwrap();
                if messages.is_empty() {
                    println!("no message received, waiting...");
                    // No need to sleep here because the `receive` call itself has a long-polling timeout.
                    // But adding a small sleep can be useful if you want to reduce polling frequency further.
                    // tokio::time::sleep(Duration::from_secs(1)).await;
                    continue; // Continue to the next loop iteration
                }

                println!("Received {} messages", messages.len());
                for message in messages {
                    println!("receive message body: {:?}", String::from_utf8_lossy(message.body()));
                    // Do your business logic here
                    // And then acknowledge the message to the RocketMQ proxy if everything is okay

                    // In a real application, you would typically either ack or change_invisible_duration, not both.
                    // For this example, let's pretend we are acknowledging.
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
            }
        }
    }

    // shutdown the simple consumer when you don't need it anymore.
    // you should shutdown it manually to gracefully stop and unregister from server
    println!("Shutting down consumer...");
    let shutdown_result = consumer.shutdown().await;
    if shutdown_result.is_err() {
        eprintln!(
            "simple consumer shutdown failed: {:?}",
            shutdown_result.unwrap_err()
        );
    } else {
        println!("Consumer shutdown successfully.");
    }
}
