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
use rocketmq::{ClientOption, MessageImpl, Producer, ProducerOption};

#[tokio::main]
async fn main() {
    // specify which topic(s) you would like to send message to
    // producer will prefetch topic route when start and failed fast if topic not exist
    let mut producer_option = ProducerOption::default();
    producer_option.set_topics(vec!["test_topic".to_string()]);

    // set which rocketmq proxy to connect
    let mut client_option = ClientOption::default();
    client_option.set_access_url("localhost:8081".to_string());

    // build and start producer
    let producer = Producer::new(producer_option, client_option).await.unwrap();
    producer.start().await.unwrap();

    // build message
    let message = MessageImpl::builder()
        .set_topic("test_topic".to_string())
        .set_tags("test_tag".to_string())
        .set_body("hello world".as_bytes().to_vec())
        .build()
        .unwrap();

    // send message to rocketmq proxy
    let result = producer.send_one(message).await;
    debug_assert!(result.is_ok(), "send message failed: {:?}", result);
    println!(
        "send message success, message_id={}",
        result.unwrap().message_id
    );
}
