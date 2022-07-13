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

#include "Client.h"
#include "grpcpp/grpcpp.h"
#include "opencensus/proto/agent/metrics/v1/metrics_service.grpc.pb.h"
#include "opencensus/stats/stats.h"

ROCKETMQ_NAMESPACE_BEGIN

class MetricBidiReactor;

using Stub = opencensus::proto::agent::metrics::v1::MetricsService::Stub;
using StubPtr = std::unique_ptr<Stub>;
using MetricData = std::vector<std::pair<opencensus::stats::ViewDescriptor, opencensus::stats::ViewData>>;
using ExportMetricsServiceRequest = opencensus::proto::agent::metrics::v1::ExportMetricsServiceRequest;

class OpencensusExporter : public opencensus::stats::StatsExporter::Handler,
                           public std::enable_shared_from_this<OpencensusExporter> {
public:
  OpencensusExporter(std::string endpoints, std::weak_ptr<Client> client);

  void ExportViewData(const MetricData& data) override;

  static void wrap(const MetricData& data, ExportMetricsServiceRequest& request);

  void resetStream();

  Stub* stub() {
    return stub_.get();
  }

private:
  std::weak_ptr<Client> client_;
  StubPtr stub_;
  std::unique_ptr<MetricBidiReactor> bidi_reactor_;
};

ROCKETMQ_NAMESPACE_END