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

//! Live integration test: the telemetry stream must be re-established proactively after a
//! rolling restart of the proxy, instead of degrading until the next periodic settings
//! sync.
//!
//! Protocol:
//! 1. the test starts a producer (one message per second) and a simple consumer;
//! 2. an external driver touches the signal file, kills the proxy and restarts it; when the
//!    new proxy listens again, the driver writes the recovery timestamp (seconds since the
//!    UNIX epoch) into the proxy-up file;
//! 3. the test measures the client-side recovery: from the proxy being up again to the
//!    first successful receive. With a proactive reconnect this is bounded by the retry
//!    delay (about a second); the old behaviour degraded for 42-70 seconds because the
//!    client only noticed the dead stream at the next 30s settings sync and never
//!    re-registered.

use std::collections::HashSet;
use std::time::Duration;

use rocketmq::conf::{ClientOption, ProducerOption, SimpleConsumerOption};
use rocketmq::model::common::{FilterExpression, FilterType};
use rocketmq::model::message::MessageBuilder;
use rocketmq::{Producer, SimpleConsumer};

const TOPIC: &str = "rust-rolling-topic";
const GROUP: &str = "rust-rolling-group";
const SIGNAL_FILE: &str = "/tmp/rmq_rust_live/rolling_signal";
const PROXY_UP_FILE: &str = "/tmp/rmq_rust_live/proxy_up";
/// The proxy gRPC server is up and the stream must be re-established well within this
/// budget. The proactive reconnect retries every second; the remaining budget is the time
/// the restarted proxy itself needs to rebuild the topic routes from the broker heartbeats
/// (about 20s in practice), which no client can skip.
const CLIENT_RECOVERY_BUDGET: Duration = Duration::from_secs(30);
const MAX_TEST_DURATION: Duration = Duration::from_secs(180);

fn file_mtime(path: &str) -> Option<std::time::SystemTime> {
    std::fs::metadata(path).and_then(|m| m.modified()).ok()
}

#[tokio::test(flavor = "multi_thread")]
async fn simple_consumer_recovers_quickly_after_rolling_restart() {
    // This test needs a live cluster and an external driver that kills and restarts the
    // proxy; opt in with ROCKETMQ_ROLLING_IT=1 (see the module docs for the protocol).
    if std::env::var("ROCKETMQ_ROLLING_IT").as_deref() != Ok("1") {
        println!("skipping: set ROCKETMQ_ROLLING_IT=1 and follow the module docs to run");
        return;
    }
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    let _ = std::fs::remove_file(SIGNAL_FILE);
    let _ = std::fs::remove_file(PROXY_UP_FILE);

    let mut consumer_option = SimpleConsumerOption::default();
    consumer_option.set_topics(vec![TOPIC]);
    consumer_option.set_consumer_group(GROUP);

    let mut consumer_client_option = ClientOption::default();
    consumer_client_option.set_access_url("localhost:8081");
    consumer_client_option.set_enable_tls(false);

    let mut consumer = SimpleConsumer::new(consumer_option, consumer_client_option).unwrap();
    consumer.start().await.unwrap();

    let mut producer_option = ProducerOption::default();
    producer_option.set_topics(vec![TOPIC]);
    let mut producer_client_option = ClientOption::default();
    producer_client_option.set_access_url("localhost:8081");
    producer_client_option.set_enable_tls(false);
    let mut producer = Producer::new(producer_option, producer_client_option).unwrap();
    producer.start().await.unwrap();
    let producer = std::sync::Arc::new(producer);

    // Producer task: one message per second, retrying on failures. Send failures are
    // printed only a few times: when the broker disk is full every send fails and the log
    // would otherwise drown the timeline.
    let producer_for_task = std::sync::Arc::clone(&producer);
    let producer_handle = tokio::spawn(async move {
        let producer = producer_for_task;
        let start = std::time::Instant::now();
        let mut seq = 0u64;
        let mut send_failures = 0u64;
        while start.elapsed() < MAX_TEST_DURATION {
            let body = format!("rolling-{}", seq);
            let message = MessageBuilder::builder()
                .set_topic(TOPIC)
                .set_body(body.clone().into_bytes())
                .build()
                .unwrap();
            match producer.send(message).await {
                Ok(_) => {
                    seq += 1;
                }
                Err(e) => {
                    send_failures += 1;
                    if send_failures <= 3 {
                        println!("send failed (seq={}): {:?}", body, e);
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        (seq, send_failures)
    });

    // Consumer loop: every receive outcome is recorded; the first successful receive after
    // the signal marks the recovery.
    let t0 = std::time::Instant::now();
    let mut received: HashSet<String> = HashSet::new();
    let mut receive_failures = 0u64;
    let mut signal_seen = false;
    let mut recovery: Option<Duration> = None;

    while t0.elapsed() < MAX_TEST_DURATION {
        if !signal_seen && std::path::Path::new(SIGNAL_FILE).exists() {
            signal_seen = true;
            println!("signal file observed at +{:?}", t0.elapsed());
        }

        match consumer
            .receive(TOPIC, &FilterExpression::new(FilterType::Tag, "*"))
            .await
        {
            Ok(messages) => {
                for message in messages {
                    let body = String::from_utf8_lossy(message.body()).to_string();
                    if body.starts_with("rolling-") && received.insert(body.clone()) {
                        println!("received {} at +{:?}", body, t0.elapsed());
                    }
                }
                if signal_seen {
                    // The client can recover faster than the driver detects the proxy
                    // being up, so the marker file may not exist yet for a moment.
                    let proxy_up = loop {
                        if let Some(mtime) = file_mtime(PROXY_UP_FILE) {
                            break mtime;
                        }
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    };
                    recovery = Some(
                        std::time::SystemTime::now()
                            .duration_since(proxy_up)
                            .expect("proxy-up file must be in the past"),
                    );
                    println!(
                        "RECOVERED at +{:?}: first successful receive {:?} after the proxy came back up",
                        t0.elapsed(),
                        recovery.unwrap()
                    );
                    break;
                }
                // No sleep only during the warm-up phase; afterwards keep the loop calm.
                if !signal_seen {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
            Err(e) => {
                receive_failures += 1;
                if receive_failures <= 5 || receive_failures % 50 == 0 {
                    println!("receive failed at +{:?}: {:?}", t0.elapsed(), e);
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }

    let (produced, send_failures) = producer_handle.await.unwrap();
    if let Ok(producer) = std::sync::Arc::try_unwrap(producer) {
        let _ = producer.shutdown().await;
    }
    consumer.shutdown().await.unwrap();

    println!(
        "produced {} messages ({} send failures), received {} unique messages, {} receive failures",
        produced,
        send_failures,
        received.len(),
        receive_failures
    );

    let recovery = recovery.unwrap_or_else(|| {
        panic!(
            "the consumer never recovered within {:?}",
            MAX_TEST_DURATION
        );
    });
    println!("client-side recovery time: {:?}", recovery);
    assert!(
        recovery < CLIENT_RECOVERY_BUDGET,
        "the client must re-establish its telemetry stream within {:?} after the proxy came back, took {:?}",
        CLIENT_RECOVERY_BUDGET,
        recovery
    );
}
