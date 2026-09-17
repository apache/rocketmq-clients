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

//! Offline tests for LiteSimpleConsumer.
//!
//! These tests run without any RocketMQ deployment: they only assert the public API contract
//! (validation, accessors, life cycle checks) of `LiteSimpleConsumer`.

#[cfg(test)]
mod lite_simple_consumer_tests {
    use rocketmq::conf::{ClientOption, SimpleConsumerOption};
    use rocketmq::error::ErrorKind;
    use rocketmq::model::offset_option::{OffsetOption, OffsetPolicy};
    use rocketmq::{LiteSimpleConsumer, LiteSimpleConsumerTrait};

    fn client_option() -> ClientOption {
        let mut client_option = ClientOption::default();
        client_option.set_access_url("http://localhost:8080");
        client_option
    }

    fn consumer_option(consumer_group: &str) -> SimpleConsumerOption {
        let mut option = SimpleConsumerOption::default();
        option.set_consumer_group(consumer_group);
        option
    }

    #[test]
    fn test_new_requires_consumer_group() {
        let result = LiteSimpleConsumer::new(
            client_option(),
            consumer_option(""),
            "parent_topic".to_string(),
        );
        let err = result.err().expect("consumer group is mandatory");
        assert_eq!(*err.kind(), ErrorKind::Config);
        assert_eq!(err.operation(), "lite_simple_consumer.new");
    }

    #[test]
    fn test_new_requires_bind_topic() {
        let result =
            LiteSimpleConsumer::new(client_option(), consumer_option("group"), "".to_string());
        let err = result.err().expect("bind topic is mandatory");
        assert_eq!(*err.kind(), ErrorKind::Config);
        assert!(err.message().contains("bind topic"));
    }

    #[test]
    fn test_new_succeeds_and_exposes_metadata() {
        let consumer = LiteSimpleConsumer::new(
            client_option(),
            consumer_option("test_group"),
            "parent_topic".to_string(),
        )
        .expect("consumer should be created");
        assert_eq!(consumer.get_consumer_group(), "test_group");
        assert_eq!(consumer.get_bind_topic(), "parent_topic");
        assert!(consumer.get_lite_topic_set().is_empty());
    }

    #[tokio::test]
    async fn test_subscribe_lite_before_start_fails() {
        let consumer = LiteSimpleConsumer::new(
            client_option(),
            consumer_option("test_group"),
            "parent_topic".to_string(),
        )
        .expect("consumer should be created");

        let err = consumer
            .subscribe_lite("lite_topic".to_string())
            .await
            .err()
            .expect("subscribeLite requires a started consumer");
        assert_eq!(*err.kind(), ErrorKind::ClientIsNotRunning);

        let err = consumer
            .subscribe_lite_with_offset(
                "lite_topic".to_string(),
                OffsetOption::from_policy(OffsetPolicy::Min),
            )
            .await
            .err()
            .expect("subscribeLite requires a started consumer");
        assert_eq!(*err.kind(), ErrorKind::ClientIsNotRunning);

        let err = consumer
            .unsubscribe_lite("lite_topic".to_string())
            .await
            .err()
            .expect("unsubscribeLite requires a started consumer");
        assert_eq!(*err.kind(), ErrorKind::ClientIsNotRunning);
    }
}
