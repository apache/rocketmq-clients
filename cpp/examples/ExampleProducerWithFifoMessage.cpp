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
#include <algorithm>
#include <atomic>
#include <iostream>
#include <random>
#include <string>
#include <system_error>

#include "gflags/gflags.h"
#include "rocketmq/Message.h"
#include "rocketmq/Producer.h"

using namespace ROCKETMQ_NAMESPACE;

const std::string& alphaNumeric() {
  static std::string alpha_numeric("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ");
  return alpha_numeric;
}

std::string randomString(std::string::size_type len) {
  std::string result;
  result.reserve(len);
  std::random_device rd;
  std::mt19937 generator(rd());
  std::string source(alphaNumeric());
  std::string::size_type generated = 0;
  while (generated < len) {
    std::shuffle(source.begin(), source.end(), generator);
    std::string::size_type delta = std::min({len - generated, source.length()});
    result.append(source.substr(0, delta));
    generated += delta;
  }
  return result;
}

DEFINE_string(topic, "FifoTopic", "Topic to which messages are published");
DEFINE_string(access_point, "127.0.0.1:8081", "Service access URL, provided by your service provider");
DEFINE_int32(message_body_size, 4096, "Message body size");
DEFINE_uint32(total, 256, "Number of sample messages to publish");
DEFINE_string(access_key, "", "Your access key ID");
DEFINE_string(access_secret, "", "Your access secret");
DEFINE_bool(tls, false, "Use HTTP2 with TLS/SSL");

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // Adjust log level for file/console sinks
  auto& logger = getLogger();
  logger.setConsoleLevel(Level::Info);
  logger.setLevel(Level::Info);
  logger.init();

  CredentialsProviderPtr credentials_provider;
  if (!FLAGS_access_key.empty() && !FLAGS_access_secret.empty()) {
    credentials_provider = std::make_shared<StaticCredentialsProvider>(FLAGS_access_key, FLAGS_access_secret);
  }

  // In most case, you don't need to create too many producers, singletion pattern is recommended.
  auto producer = Producer::newBuilder()
                      .withConfiguration(Configuration::newBuilder()
                                             .withEndpoints(FLAGS_access_point)
                                             .withCredentialsProvider(credentials_provider)
                                             .withSsl(FLAGS_tls)
                                             .build())
                      .withTopics({FLAGS_topic})
                      .build();

  std::atomic_bool stopped;
  std::atomic_long count(0);

  auto stats_lambda = [&] {
    std::cout << "Stats thread starts" << std::endl;
    while (!stopped.load(std::memory_order_relaxed)) {
      long cnt = count.load(std::memory_order_relaxed);
      while (!count.compare_exchange_weak(cnt, 0)) {
        cnt = count.load(std::memory_order_relaxed);
      }
      std::this_thread::sleep_for(std::chrono::seconds(1));
      std::cout << "QPS: " << cnt << std::endl;
    }
  };

  std::thread stats_thread(stats_lambda);

  std::string body = randomString(FLAGS_message_body_size);
  std::cout << "Message body size: " << body.length() << std::endl;

  try {
    for (std::size_t i = 0; i < FLAGS_total; ++i) {
      auto message = Message::newBuilder()
                         .withTopic(FLAGS_topic)
                         .withTag("TagA")
                         .withKeys({"Key-0"})
                         .withBody(body)
                         .withGroup("message-group" + std::to_string(i % 10))
                         .build();
      std::error_code ec;
      SendReceipt send_receipt = producer.send(std::move(message), ec);
      // std::cout << "Message-ID: " << send_receipt.message_id << std::endl;
      count++;
    }
  } catch (...) {
    std::cerr << "Ah...No!!!" << std::endl;
  }

  stopped.store(true, std::memory_order_relaxed);
  if (stats_thread.joinable()) {
    stats_thread.join();
  }

  return EXIT_SUCCESS;
}