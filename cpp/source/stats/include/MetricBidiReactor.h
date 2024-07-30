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

#include <list>

#include "Client.h"
#include "grpcpp/grpcpp.h"
#include "grpcpp/impl/codegen/client_callback.h"
#include "opencensus/proto/agent/metrics/v1/metrics_service.grpc.pb.h"

ROCKETMQ_NAMESPACE_BEGIN

class OpencensusExporter;

using ExportMetricsServiceRequest =
    opencensus::proto::agent::metrics::v1::ExportMetricsServiceRequest;
using ExportMetricsServiceResponse =
    opencensus::proto::agent::metrics::v1::ExportMetricsServiceResponse;

class MetricBidiReactor
    : public grpc::ClientBidiReactor<ExportMetricsServiceRequest,
                                     ExportMetricsServiceResponse> {
public:
  MetricBidiReactor(std::shared_ptr<Client> client,
                    std::shared_ptr<OpencensusExporter> exporter);

  /// Notifies the application that a StartRead operation completed.
  ///
  /// \param[in] ok Was it successful? If false, no new read/write operation
  ///               will succeed, and any further Start* should not be called.
  void OnReadDone(bool /*ok*/) override;

  /// Notifies the application that a StartWrite or StartWriteLast operation
  /// completed.
  ///
  /// \param[in] ok Was it successful? If false, no new read/write operation
  ///               will succeed, and any further Start* should not be called.
  void OnWriteDone(bool /*ok*/) override;

  /// Notifies the application that all operations associated with this RPC
  /// have completed and all Holds have been removed. OnDone provides the RPC
  /// status outcome for both successful and failed RPCs and will be called in
  /// all cases. If it is not called, it indicates an application-level problem
  /// (like failure to remove a hold).
  ///
  /// \param[in] s The status outcome of this RPC
  void OnDone(const grpc::Status & /*s*/) override;

  void write(ExportMetricsServiceRequest request) LOCKS_EXCLUDED(requests_mtx_);

private:
  std::weak_ptr<Client> client_;
  std::weak_ptr<OpencensusExporter> exporter_;
  grpc::ClientContext context_;

  /// Pending ExportMetricsServiceRequest items to write to server
  std::list<ExportMetricsServiceRequest> requests_ GUARDED_BY(requests_mtx_);
  absl::Mutex requests_mtx_;

  std::atomic_bool inflight_{false};
  std::atomic_bool read_{false};

  ExportMetricsServiceResponse response_;

  void tryWriteNext();

  void fireRead();
};

ROCKETMQ_NAMESPACE_END
