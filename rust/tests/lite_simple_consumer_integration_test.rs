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

//! Real cluster integration tests for LiteSimpleConsumer.
//!
//! These tests need a running RocketMQ deployment:
//! * broker started with `enableLmq=true` and `enableMultiDispatch=true`
//! * a parent topic created with `message.type=LITE`
//! * a consumer group created with `+lite.bind.topic=<parentTopic>`
//! * a proxy running in CLUSTER mode
//!
//! They are skipped unless `ROCKETMQ_RUST_LITE_ENDPOINTS` is set, e.g.
//! ```shell
//! export ROCKETMQ_RUST_LITE_ENDPOINTS=127.0.0.1:8081
//! export ROCKETMQ_RUST_LITE_PARENT_TOPIC=lite-parent-topic
//! export ROCKETMQ_RUST_LITE_GROUP=rust-lite-unittest-group
//! cargo test --test lite_simple_consumer_integration_test
//! ```

#[cfg(test)]
mod lite_simple_consumer_integration_tests {
    use std::time::Duration;

    use rocketmq::conf::{ClientOption, ProducerOption, SimpleConsumerOption};
    use rocketmq::model::message::MessageBuilder;
    use rocketmq::model::offset_option::{OffsetOption, OffsetPolicy};
    use rocketmq::{LiteSimpleConsumer, LiteSimpleConsumerTrait, Producer};

    struct Config {
        endpoints: String,
        parent_topic: String,
        group: String,
    }

    impl Config {
        fn from_env() -> Option<Self> {
            Some(Self {
                endpoints: std::env::var("ROCKETMQ_RUST_LITE_ENDPOINTS").ok()?,
                parent_topic: std::env::var("ROCKETMQ_RUST_LITE_PARENT_TOPIC")
                    .unwrap_or("lite-parent-topic".to_string()),
                group: std::env::var("ROCKETMQ_RUST_LITE_GROUP")
                    .unwrap_or("rust-lite-unittest-group".to_string()),
            })
        }

        fn client_option(&self) -> ClientOption {
            let mut client_option = ClientOption::default();
            client_option.set_access_url(&self.endpoints);
            client_option
        }

        fn consumer_option(&self) -> SimpleConsumerOption {
            let mut option = SimpleConsumerOption::default();
            option.set_consumer_group(&self.group);
            option
        }
    }

    async fn send_lite_messages(config: &Config, lite_topic: &str, num: usize) -> Vec<String> {
        let mut producer_option = ProducerOption::default();
        producer_option.set_topics(vec![config.parent_topic.clone()]);
        let mut producer =
            Producer::new(producer_option, config.client_option()).expect("create producer");
        producer.start().await.expect("start producer");

        let mut message_ids = Vec::with_capacity(num);
        for i in 0..num {
            let message = MessageBuilder::lite_message_builder(
                config.parent_topic.clone(),
                format!("rust-lite-simple-consumer-{}", i).into_bytes(),
                lite_topic,
            )
            .build()
            .expect("build lite message");
            let receipt = producer.send(message).await.expect("send lite message");
            message_ids.push(receipt.message_id().to_string());
        }
        producer.shutdown().await.expect("shutdown producer");
        message_ids
    }

    /// Receive every message of a lite topic and ack it.
    #[tokio::test]
    async fn test_receive_and_ack() {
        let Some(config) = Config::from_env() else {
            println!("ROCKETMQ_RUST_LITE_ENDPOINTS is not set, skip the integration test");
            return;
        };

        let lite_topic = format!("rust-lite-ack-{}", chrono_like_nonce());
        let expected = send_lite_messages(&config, &lite_topic, 5).await;

        let mut consumer = LiteSimpleConsumer::new(
            config.client_option(),
            config.consumer_option(),
            config.parent_topic.clone(),
        )
        .expect("create lite simple consumer");
        consumer.start().await.expect("start lite simple consumer");
        consumer
            .subscribe_lite_with_offset(
                lite_topic.clone(),
                OffsetOption::from_policy(OffsetPolicy::Min),
            )
            .await
            .expect("subscribe lite topic");
        assert!(consumer.get_lite_topic_set().contains(&lite_topic));

        let mut received = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        while tokio::time::Instant::now() < deadline && received.len() < expected.len() {
            let messages = consumer
                .receive(32, Duration::from_secs(15))
                .await
                .expect("receive messages");
            for message in messages {
                // The lite topic must be present, it is required to ack the message.
                assert_eq!(message.lite_topic(), Some(lite_topic.as_str()));
                consumer.ack(&message).await.expect("ack message");
                received.push(message.message_id().to_string());
            }
        }

        for id in &expected {
            assert!(received.contains(id), "message {} was not received", id);
        }

        consumer
            .unsubscribe_lite(lite_topic.clone())
            .await
            .expect("unsubscribe lite topic");
        assert!(!consumer.get_lite_topic_set().contains(&lite_topic));
        consumer.shutdown().await.expect("shutdown consumer");
    }

    /// Messages sent after the subscription must be delivered too.
    #[tokio::test]
    async fn test_receive_after_subscribe() {
        let Some(config) = Config::from_env() else {
            println!("ROCKETMQ_RUST_LITE_ENDPOINTS is not set, skip the integration test");
            return;
        };

        let lite_topic = format!("rust-lite-live-{}", chrono_like_nonce());
        let mut consumer = LiteSimpleConsumer::new(
            config.client_option(),
            config.consumer_option(),
            config.parent_topic.clone(),
        )
        .expect("create lite simple consumer");
        consumer.start().await.expect("start lite simple consumer");
        consumer
            .subscribe_lite(lite_topic.clone())
            .await
            .expect("subscribe lite topic");

        let expected = send_lite_messages(&config, &lite_topic, 3).await;
        let mut received = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        while tokio::time::Instant::now() < deadline && received.len() < expected.len() {
            let messages = consumer
                .receive(32, Duration::from_secs(15))
                .await
                .expect("receive messages");
            for message in messages {
                consumer.ack(&message).await.expect("ack message");
                received.push(message.message_id().to_string());
            }
        }

        assert!(
            received.iter().any(|id| expected.contains(id)),
            "no message of the live lite topic was received"
        );

        consumer.shutdown().await.expect("shutdown consumer");
    }

    /// A stable nonce without extra dependencies.
    fn chrono_like_nonce() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    }
}
