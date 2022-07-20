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
#include "Tag.h"

ROCKETMQ_NAMESPACE_BEGIN

opencensus::tags::TagKey& Tag::topicTag() {
  static opencensus::tags::TagKey topic_tag = opencensus::tags::TagKey::Register("topic");
  return topic_tag;
}

opencensus::tags::TagKey& Tag::clientIdTag() {
  static opencensus::tags::TagKey client_id_tag = opencensus::tags::TagKey::Register("client_id");
  return client_id_tag;
}

opencensus::tags::TagKey& Tag::invocationStatusTag() {
  static opencensus::tags::TagKey invocation_status = opencensus::tags::TagKey::Register("invocation_status");
  return invocation_status;
}

opencensus::tags::TagKey& Tag::consumerGroupTag() {
  static opencensus::tags::TagKey consumer_group_tag = opencensus::tags::TagKey::Register("consumer_group");
  return consumer_group_tag;
}

ROCKETMQ_NAMESPACE_END
