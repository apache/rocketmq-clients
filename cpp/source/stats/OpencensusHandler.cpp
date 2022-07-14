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
#include "OpencensusHandler.h"

#include "MetricBidiReactor.h"

ROCKETMQ_NAMESPACE_BEGIN

OpencensusHandler::OpencensusHandler(std::string endpoints, std::weak_ptr<Client> client)
    : exporter_(std::make_shared<OpencensusExporter>(std::move(endpoints), std::move(client))) {
}

void OpencensusHandler::ExportViewData(const MetricData& data) {
  exporter_->ExportViewData(data);
}

ROCKETMQ_NAMESPACE_END