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
use rocketmq::{
    conf::{ClientOption, PushConsumerOption},
    model::common::{ConsumeResult, FilterExpression, FilterType},
    MessageListener, PushConsumer,
};
use std::time::Duration;
use tokio::time;

#[tokio::main]
async fn main() {
    let mut client_option = ClientOption::default();
    client_option.set_access_url("localhost:8081");
    client_option.set_enable_tls(false);

    let mut option = PushConsumerOption::default();
    option.set_consumer_group("test");
    option.subscribe("test_topic", FilterExpression::new(FilterType::Tag, "*"));

    let callback: MessageListener = Box::new(|message| {
        println!("Receive message: {:?}", message);
        ConsumeResult::SUCCESS
    });

    let mut push_consumer = PushConsumer::new(client_option, option, callback).unwrap();
    let start_result = push_consumer.start().await;
    if start_result.is_err() {
        eprintln!(
            "push consumer start failed: {:?}",
            start_result.unwrap_err()
        );
        return;
    }

    time::sleep(Duration::from_secs(60)).await;

    let _ = push_consumer.shutdown().await;
}
