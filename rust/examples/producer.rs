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
use rocketmq::conf::{ClientOption, ProducerOption};
use rocketmq::model::message::MessageBuilder;
use rocketmq::Producer;

#[tokio::main]
async fn main() {
    // recommend to specify which topic(s) you would like to send message to
    // producer will prefetch topic route when start and failed fast if topic not exist
    let mut producer_option = ProducerOption::default();
    producer_option.set_topics(vec!["test_topic"]);

    // set which rocketmq proxy to connect
    let mut client_option = ClientOption::default();
    client_option.set_access_url("localhost:8081");

    // build and start producer
    let producer = Producer::new(producer_option, client_option).unwrap();
    producer.start().await.unwrap();

    // build message
    let message = MessageBuilder::builder()
        .set_topic("test_topic")
        .set_tag("test_tag")
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
