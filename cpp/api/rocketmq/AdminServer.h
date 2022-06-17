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
#pragma once

#include "State.h"

namespace rocketmq {
namespace admin {

  class AdminServer {
  public:
    virtual ~AdminServer() = default;

    virtual bool start() = 0;

    virtual bool stop() = 0;

    virtual int port() const = 0;
  };

  class AdminFacade {
  public:
    static AdminServer& getServer();
  };

} // namespace admin
} // namespace rocketmq