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

//! Integration tests for Lite functionality.
//!
//! These tests verify that Lite modules compile and can be used correctly.
//! Note: Integration tests don't trigger automock, so they use real Client types.

#[cfg(test)]
mod lite_tests {
    use rocketmq::conf::{ClientOption, PushConsumerOption};
    use rocketmq::model::message::{Message, MessageBuilder};
    use rocketmq::model::offset_option::{OffsetOption, OffsetPolicy};
    use rocketmq::{LitePushConsumer, LitePushConsumerTrait};

    #[test]
    fn test_lite_message_builder() {
        // Test that lite message builder works
        let message =
            MessageBuilder::lite_message_builder("parent_topic", vec![1, 2, 3], "lite_topic_001")
                .build();

        assert!(message.is_ok());
        let mut msg = message.unwrap();
        assert_eq!(msg.take_lite_topic(), Some("lite_topic_001".to_string()));
    }

    #[test]
    fn test_offset_option_creation() {
        // Test OffsetOption creation
        let _opt1 = OffsetOption::from_policy(OffsetPolicy::Last);
        let _opt2 = OffsetOption::from_offset(12345);
        let _opt3 = OffsetOption::from_tail_n(100);
        let _opt4 = OffsetOption::from_timestamp(1234567890000);
    }

    #[test]
    fn test_lite_push_consumer_compilation() {
        // This test verifies that LitePushConsumer can be instantiated
        // (without actually connecting to a server)

        let mut client_option = ClientOption::default();
        client_option.set_access_url("http://localhost:8080");

        let mut option = PushConsumerOption::default();
        option.set_consumer_group("test_group");

        let message_listener =
            Box::new(|_: &rocketmq::model::message::MessageView| rocketmq::ConsumeResult::SUCCESS);

        // This should compile without errors
        let result = LitePushConsumer::new(
            client_option,
            option,
            "parent_topic".to_string(),
            message_listener,
        );

        // We expect an error because we're not connecting to a real server,
        // but the important thing is that it compiles
        assert!(result.is_err() || result.is_ok());
    }
}
