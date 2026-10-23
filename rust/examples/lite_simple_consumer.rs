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

//! Example demonstrating how to pull and ack lite messages with a LiteSimpleConsumer.
//!
//! `LiteSimpleConsumer` is bound to one parent topic and dynamically (un)subscribes the lite
//! topics of that parent topic. Unlike `LitePushConsumer`, messages are pulled explicitly with
//! `receive(...)` and committed with `ack(...)`.
//!
//! Server prerequisites:
//! * broker started with `enableLmq=true` and `enableMultiDispatch=true`
//! * a parent topic created with `message.type=LITE`
//! * a consumer group created with `+lite.bind.topic=<parentTopic>`
//! * the proxy running in CLUSTER mode (LOCAL mode does not serve lite subscriptions)

use rocketmq::conf::{ClientOption, SimpleConsumerOption};
use rocketmq::model::offset_option::{OffsetOption, OffsetPolicy};
use rocketmq::{LiteSimpleConsumer, LiteSimpleConsumerTrait};
use std::env;
use std::error::Error;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();

    let access_url = env::var("ROCKETMQ_ENDPOINTS").unwrap_or("http://localhost:8081".to_string());
    let bind_topic =
        env::var("ROCKETMQ_LITE_PARENT_TOPIC").unwrap_or("lite-parent-topic".to_string());
    let consumer_group =
        env::var("ROCKETMQ_LITE_GROUP").unwrap_or("rust-lite-unittest-group".to_string());
    let lite_topic = env::var("ROCKETMQ_LITE_TOPIC").unwrap_or("lite-topic-1".to_string());

    let mut client_option = ClientOption::default();
    client_option.set_access_url(&access_url);

    let mut option = SimpleConsumerOption::default();
    option.set_consumer_group(&consumer_group);

    let mut consumer = LiteSimpleConsumer::new(client_option, option, bind_topic.clone())?;
    consumer.start().await?;
    println!("LiteSimpleConsumer started, bindTopic={}", bind_topic);

    // Subscribe a lite topic, starting from the earliest message.
    // The call performs a network request plus a quota check, so its result must be handled.
    consumer
        .subscribe_lite_with_offset(
            lite_topic.clone(),
            OffsetOption::from_policy(OffsetPolicy::Min),
        )
        .await?;
    println!("subscribed lite topic: {}", lite_topic);
    println!("lite topic set: {:?}", consumer.get_lite_topic_set());

    // Pull messages for at most 30 seconds.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while tokio::time::Instant::now() < deadline {
        let messages = consumer.receive(32, Duration::from_secs(15)).await?;
        if messages.is_empty() {
            continue;
        }
        for message in messages {
            println!(
                "received message: id={}, topic={}, lite_topic={:?}, body={}",
                message.message_id(),
                message.topic(),
                message.lite_topic(),
                String::from_utf8_lossy(message.body())
            );
            // Lite messages must be acked with their lite topic, which the client does
            // transparently from `MessageView::lite_topic()`.
            if let Err(e) = consumer.ack(&message).await {
                eprintln!("ack message {} failed: {:?}", message.message_id(), e);
                // Fall back to extending the invisible duration so that the message can be
                // consumed again later.
                consumer
                    .change_invisible_duration(&message, Duration::from_secs(10))
                    .await?;
            }
        }
    }

    // Unsubscribe when the lite topic is no longer needed, this releases the quota.
    consumer.unsubscribe_lite(lite_topic.clone()).await?;
    println!("unsubscribed lite topic: {}", lite_topic);

    consumer.shutdown().await?;
    println!("LiteSimpleConsumer shutdown");
    Ok(())
}
