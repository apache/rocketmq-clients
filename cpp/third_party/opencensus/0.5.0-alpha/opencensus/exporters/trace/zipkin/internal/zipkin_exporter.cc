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

#include "opencensus/exporters/trace/zipkin/zipkin_exporter.h"

#include <arpa/inet.h>
#include <ifaddrs.h>
#include <iostream>

#include <curl/curl.h>
#undef RAPIDJSON_HAS_STDSTRING
#define RAPIDJSON_HAS_STDSTRING 1
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>
#include "absl/memory/memory.h"
#include "absl/strings/str_cat.h"
#include "opencensus/trace/exporter/attribute_value.h"
#include "opencensus/trace/exporter/span_exporter.h"

namespace opencensus {
namespace exporters {
namespace trace {

namespace {

constexpr char kZipkinLib[] = "zipkin/2.0";
constexpr char ipv4_loopback[] = "127.0.0.1";
constexpr char ipv6_loopback[] = "::1";

std::string SerializeMessageEvent(
    const ::opencensus::trace::exporter::MessageEvent& event) {
  return absl::StrCat(
      event.type() == ::opencensus::trace::exporter::MessageEvent::Type::SENT
          ? "SENT"
          : "RECEIVED",
      "/", event.id(), "/", event.compressed_size());
}

std::string AttributeValueToString(
    const ::opencensus::trace::exporter::AttributeValue& value) {
  switch (value.type()) {
    case ::opencensus::trace::AttributeValueRef::Type::kString:
      return value.string_value();
      break;
    case ::opencensus::trace::AttributeValueRef::Type::kBool:
      return value.bool_value() ? "true" : "false";
      break;
    case ::opencensus::trace::AttributeValueRef::Type::kInt:
      return std::to_string(value.int_value());
      break;
  }
  ABSL_ASSERT(false && "Unknown AttributeValue type");
  return "";
}

std::string SerializeAnnotation(
    const ::opencensus::trace::exporter::Annotation& annotation) {
  std::string annotation_str(annotation.description());
  if (!annotation.attributes().empty()) {
    absl::StrAppend(&annotation_str, " (");
    size_t count = 0;
    for (const auto& attribute : annotation.attributes()) {
      absl::StrAppend(&annotation_str, attribute.first, ":",
                      AttributeValueToString(attribute.second));

      if (++count < annotation.attributes().size()) {
        absl::StrAppend(&annotation_str, ", ");
      }
    }
    absl::StrAppend(&annotation_str, ")");
  }
  return annotation_str;
}

void SerializeJson(const ::opencensus::trace::exporter::SpanData& span,
                   const ZipkinExporterOptions::Service& service,
                   rapidjson::Writer<rapidjson::StringBuffer>* writer) {
  writer->StartObject();

  writer->Key("name");
  writer->String(std::string(span.name()));

  writer->Key("traceId");
  writer->String(span.context().trace_id().ToHex());

  if (span.parent_span_id().IsValid()) {
    writer->Key("parentId");
    writer->String(span.parent_span_id().ToHex());
  }

  writer->Key("id");
  writer->String(span.context().span_id().ToHex());

  // Write localEndpoint. OpenCensus does not support this by default.
  writer->Key("localEndpoint");
  writer->StartObject();
  writer->Key("serviceName");
  writer->String(service.service_name);
  if (service.af_type == ZipkinExporterOptions::AddressFamily::kIpv4) {
    writer->Key("ipv4");
  } else {
    writer->Key("ipv6");
  }
  writer->String(service.ip_address);
  writer->EndObject();

  if (!span.annotations().events().empty()) {
    writer->Key("annotations");
    writer->StartArray();
    for (const auto& annotation : span.annotations().events()) {
      writer->StartObject();
      writer->Key("timestamp");
      writer->Int64(absl::ToUnixMicros(annotation.timestamp()));
      writer->Key("value");
      writer->String(SerializeAnnotation(annotation.event()));
      writer->EndObject();
    }
    writer->EndArray(span.annotations().events().size());
  }

  if (!span.message_events().events().empty()) {
    writer->Key("annotations");
    writer->StartArray();
    for (const auto& event : span.message_events().events()) {
      writer->StartObject();
      writer->Key("timestamp");
      writer->Int64(absl::ToUnixMicros(event.timestamp()));
      writer->Key("value");
      writer->String(SerializeMessageEvent(event.event()));
      writer->EndObject();
    }
    writer->EndArray(span.message_events().events().size());
  }

  if (!span.attributes().empty()) {
    writer->Key("tags");
    writer->StartObject();
    for (const auto& attribute : span.attributes()) {
      writer->String(attribute.first);
      writer->String(AttributeValueToString(attribute.second));
    }
    writer->EndObject();
  }

  writer->Key("timestamp");
  writer->Int64(absl::ToUnixMicros(span.start_time()));

  writer->Key("duration");
  writer->Int64(absl::ToInt64Microseconds(span.end_time() - span.start_time()));

  writer->EndObject();
}

std::string EncodeJson(
    const std::vector<::opencensus::trace::exporter::SpanData>& spans,
    const ZipkinExporterOptions::Service& service) {
  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);

  writer.StartArray();
  for (const auto& span : spans) {
    SerializeJson(span, service, &writer);
  }
  writer.EndArray();
  return buffer.GetString();
}

std::string GetIpAddressHelper(ZipkinExporterOptions::AddressFamily af_type,
                               ifaddrs* ifaddr_list) {
  for (ifaddrs* if_address = ifaddr_list; if_address != nullptr;
       if_address = if_address->ifa_next) {
    if (if_address->ifa_addr == nullptr) continue;
    if (if_address->ifa_addr->sa_family == AF_INET) {
      if (af_type != ZipkinExporterOptions::AddressFamily::kIpv4) continue;
      char address[INET_ADDRSTRLEN];
      inet_ntop(AF_INET,
                &(reinterpret_cast<struct sockaddr_in*>(if_address->ifa_addr)
                      ->sin_addr),
                address, INET_ADDRSTRLEN);
      std::string ipv4_address(address);
      if (ipv4_address == ipv4_loopback) continue;
      return ipv4_address;
    } else if (if_address->ifa_addr->sa_family == AF_INET6) {
      if (af_type != ZipkinExporterOptions::AddressFamily::kIpv6) continue;
      char address[INET6_ADDRSTRLEN];
      inet_ntop(AF_INET6,
                &(reinterpret_cast<struct sockaddr_in6*>(if_address->ifa_addr)
                      ->sin6_addr),
                address, INET6_ADDRSTRLEN);
      std::string ipv6_address(address);
      if (ipv6_address == ipv6_loopback) continue;
      return ipv6_address;
    }
  }

  if (af_type == ZipkinExporterOptions::AddressFamily::kIpv4)
    return ipv4_loopback;
  else
    return ipv6_loopback;
}

std::string GetIpAddress(ZipkinExporterOptions::AddressFamily af_type) {
  ifaddrs* ifaddr;
  getifaddrs(&ifaddr);
  std::string out = GetIpAddressHelper(af_type, ifaddr);
  freeifaddrs(ifaddr);
  return out;
}

class CurlEnv {
 public:
  static CurlEnv* Get();

 private:
  CurlEnv() { curl_global_init(CURL_GLOBAL_DEFAULT); }
  ~CurlEnv() { curl_global_cleanup(); }
};

// static
CurlEnv* CurlEnv::Get() {
  static auto* const g_curl_env = new CurlEnv;
  return g_curl_env;
}

CURLcode CurlSendMessage(const uint8_t* data,
                         const ZipkinExporterOptions& options, size_t size,
                         const struct curl_slist* headers, CURL* curl,
                         char* err_msg) {
  CURLcode res;

  if ((res = curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers)) != CURLE_OK) {
    // Failed to set curl header.
    return res;
  }
  if ((res = curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, err_msg)) !=
      CURLE_OK) {
    // Failed to set curl error buffer.
    return res;
  }
  if ((res = curl_easy_setopt(curl, CURLOPT_URL, options.url.c_str())) !=
      CURLE_OK) {
    // Failed to set url.
    return res;
  }
  if ((res = curl_easy_setopt(curl, CURLOPT_USERAGENT, kZipkinLib)) !=
      CURLE_OK) {
    // Failed to set http user agent.
    return res;
  }
  if ((res = curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, size)) != CURLE_OK) {
    // Failed to set http body size.
    return res;
  }
  if ((res = curl_easy_setopt(curl, CURLOPT_POSTFIELDS, data)) != CURLE_OK) {
    // Failed to set http body data.
    return res;
  }
  if ((res = curl_easy_setopt(
           curl, CURLOPT_CONNECTTIMEOUT,
           absl::ToInt64Milliseconds(options.connect_timeout))) != CURLE_OK) {
    // Failed to set connect timeout.
    return res;
  }
  if ((res = curl_easy_setopt(
           curl, CURLOPT_TIMEOUT_MS,
           absl::ToInt64Milliseconds(options.request_timeout))) != CURLE_OK) {
    // Failed to set request timeout.
    return res;
  }
  if ((res = curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1)) != CURLE_OK) {
    // Failed to disable signals.
    return res;
  }

  if (options.proxy.empty()) {
    if ((res = curl_easy_setopt(curl, CURLOPT_PROXY, options.proxy.c_str())) !=
        CURLE_OK) {
      // Failed to set proxy.
      return res;
    }

    if (options.http_proxy_tunnel) {
      if ((res = curl_easy_setopt(curl, CURLOPT_PROXYTYPE, CURLPROXY_HTTP)) !=
          CURLE_OK) {
        // Failed to set HTTP proxy type.
        return res;
      }
      if ((res = curl_easy_setopt(curl, CURLOPT_HTTPPROXYTUNNEL, 1)) !=
          CURLE_OK) {
        // Failed to set HTTP proxy tunnel.
        return res;
      }
    }
  }

  if (options.max_redirect_times > 0) {
    if ((res = curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1)) != CURLE_OK) {
      // Failed to enable follow location.
      return res;
    }
    if ((res = curl_easy_setopt(curl, CURLOPT_MAXREDIRS,
                                options.max_redirect_times)) != CURLE_OK) {
      // Failed to set max redirect times.
      return res;
    }
  }

  // Sending HTTP request to url.
  res = curl_easy_perform(curl);

  return res;
}

class ZipkinExportHandler
    : public ::opencensus::trace::exporter::SpanExporter::Handler {
 public:
  explicit ZipkinExportHandler(const ZipkinExporterOptions& options)
      : options_(options) {}

  void Export(const std::vector<::opencensus::trace::exporter::SpanData>& spans)
      override;

  // Send HTTP message to zipkin endpoint using libcurl.
  void SendMessage(const std::string& msg, size_t size) const;

  ZipkinExporterOptions options_;
  ZipkinExporterOptions::Service service_;
};

void ZipkinExportHandler::SendMessage(const std::string& msg,
                                      size_t size) const {
  char err_msg[CURL_ERROR_SIZE] = {0};
  CURL* curl = curl_easy_init();
  struct curl_slist* headers = nullptr;

  if (!curl) {
    std::cerr << "ZipkinExporter: failed to create curl handle.\n";
    return;
  }

  // This is required for the server to recognize that it is a json encoded
  // message.
  headers = curl_slist_append(headers, "Content-Type: application/json");
  CURLcode res = CurlSendMessage(reinterpret_cast<const uint8_t*>(msg.data()),
                                 options_, size, headers, curl, err_msg);
  if (res != CURLE_OK) {
    std::cerr << "ZipkinExporter: curl error: " << curl_easy_strerror(res)
              << " (sending to \"" << options_.url << "\")\n";
  }

  curl_slist_free_all(headers);
  curl_easy_cleanup(curl);
}

void ZipkinExportHandler::Export(
    const std::vector<::opencensus::trace::exporter::SpanData>& spans) {
  if (!spans.empty()) {
    std::string msg = EncodeJson(spans, service_);
    SendMessage(msg, msg.size());
  }
}

}  // namespace

void ZipkinExporter::Register(const ZipkinExporterOptions& options) {
  // Initialize libcurl. This MUST only be done once per process.
  CurlEnv::Get();

  // Create new exporter.
  ZipkinExportHandler* handler = new ZipkinExportHandler(options);
  handler->service_.service_name = options.service_name;
  // Get IP address of current machine.
  handler->service_.af_type = options.af_type;
  handler->service_.ip_address = GetIpAddress(options.af_type);
  ::opencensus::trace::exporter::SpanExporter::RegisterHandler(
      absl::WrapUnique<::opencensus::trace::exporter::SpanExporter::Handler>(
          handler));
}

}  // namespace trace
}  // namespace exporters
}  // namespace opencensus
