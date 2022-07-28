// Copyright 2019, OpenCensus Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef OPENCENSUS_EXPORTERS_TRACE_OCAGENT_OCAGENT_EXPORTER_H_
#define OPENCENSUS_EXPORTERS_TRACE_OCAGENT_OCAGENT_EXPORTER_H_

#include <string>

#include "absl/time/time.h"

#include "opencensus/proto/agent/trace/v1/trace_service.grpc.pb.h"
#include "opencensus/proto/agent/trace/v1/trace_service.pb.h"

namespace opencensus {
namespace exporters {
namespace trace {

struct OcAgentOptions {
  // The OcAgent address to use.
  std::string address;

  // The RPC deadline to use when exporting to OcAgent.
  absl::Duration rpc_deadline = absl::Seconds(5);

  // (optional) If not empty, set the service name to this.
  std::string service_name;

  // (optional) By default, the exporter connects to OcAgent using address. If
  // this stub is non-null, the exporter will use this stub to send
  // gRPC calls instead and ignore the address. Useful for testing.
  std::unique_ptr<
      opencensus::proto::agent::trace::v1::TraceService::StubInterface>
      trace_service_stub;
};

class OcAgentExporter {
 public:
  // Registers the exporter.
  static void Register(OcAgentOptions &&opts);

 private:
  OcAgentExporter() = delete;
};

}  // namespace trace
}  // namespace exporters
}  // namespace opencensus

#endif  // OPENCENSUS_EXPORTERS_TRACE_OCAGENT_OCAGENT_EXPORTER_H_
