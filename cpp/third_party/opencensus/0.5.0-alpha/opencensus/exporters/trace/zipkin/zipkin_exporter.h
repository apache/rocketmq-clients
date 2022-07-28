// Copyright 2018, OpenCensus Authors
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

#ifndef OPENCENSUS_EXPORTERS_TRACE_ZIPKIN_ZIPKIN_EXPORTER_H_
#define OPENCENSUS_EXPORTERS_TRACE_ZIPKIN_ZIPKIN_EXPORTER_H_

#include <string>

#include "absl/strings/string_view.h"
#include "absl/time/time.h"

namespace opencensus {
namespace exporters {
namespace trace {

struct ZipkinExporterOptions {
  explicit ZipkinExporterOptions(absl::string_view url) : url(url) {}

  // Uniform Resource Location for server that spans will be sent to.
  std::string url;
  // The proxy to use for the upcoming request.
  std::string proxy;
  // Tunnel through HTTP proxy
  bool http_proxy_tunnel = false;
  // The maximum number of redirects allowed. The default maximum redirect times
  // is 3.
  size_t max_redirect_times = 3;
  // The maximum timeout for TCP connect. The default connect timeout is 5
  // seconds.
  absl::Duration connect_timeout = absl::Seconds(5);
  // The maximum timeout for HTTP request. The default request timeout is 15
  // seconds.
  absl::Duration request_timeout = absl::Seconds(15);
  // Service name used by zipkin collector.
  std::string service_name;
  // Address family to be reported to zipkin collector.
  enum class AddressFamily : uint8_t { kIpv4, kIpv6 };
  AddressFamily af_type = AddressFamily::kIpv4;

  struct Service {
    Service() : af_type(AddressFamily::kIpv4) {}
    Service(const std::string& service_name, AddressFamily af_type)
        : service_name(service_name), af_type(af_type) {}

    std::string service_name;
    AddressFamily af_type;
    // IP address will be local address of machine. It will be populated
    // automatically.
    std::string ip_address;
  };
};

class ZipkinExporter {
 public:
  static void Register(const ZipkinExporterOptions& options);

 private:
  ZipkinExporter() = delete;
};

}  // namespace trace
}  // namespace exporters
}  // namespace opencensus

#endif  // OPENCENSUS_EXPORTERS_TRACE_ZIPKIN_ZIPKIN_EXPORTER_H_
