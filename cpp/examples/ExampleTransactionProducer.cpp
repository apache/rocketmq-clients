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
#include "rocketmq/DefaultMQProducer.h"
#include <cstdlib>

using namespace ROCKETMQ_NAMESPACE;

int main(int argc, char* argv[]) {
  DefaultMQProducer producer("TestGroup");

  const char* topic = "cpp_sdk_standard";
  const char* name_server = "47.98.116.189:80";

  producer.setNamesrvAddr(name_server);
  producer.compressBodyThreshold(256);
  const char* resource_namespace = "MQ_INST_1080056302921134_BXuIbML7";
  producer.setRegion("cn-hangzhou-pre");
  producer.setResourceNamespace(resource_namespace);
  producer.setCredentialsProvider(std::make_shared<ConfigFileCredentialsProvider>());

  MQMessage message;
  message.setTopic(topic);
  message.setTags("TagA");
  message.setKey("Yuck! Why-plural?");
  message.setBody("ABC");

  producer.start();

  auto transaction = producer.prepare(message);

  transaction->commit();

  std::this_thread::sleep_for(std::chrono::minutes(30));

  producer.shutdown();

  return EXIT_SUCCESS;
}