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
#include "TlsHelper.h"
#include <gtest/gtest.h>
#include <string>

ROCKETMQ_NAMESPACE_BEGIN

TEST(TlsHelperTest, testSign) {
  const char* data = "some random data for test purpose only";
  const char* access_secret = "arbitrary-access-key";
  const std::string& signature = TlsHelper::sign(access_secret, data);
  const char* expect = "567868dc8e81f1e8095f88958edff1e07db4290e";
  EXPECT_STRCASEEQ(expect, signature.c_str());
}

ROCKETMQ_NAMESPACE_END