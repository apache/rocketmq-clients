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

#include "opencensus/exporters/stats/stdout/stdout_exporter.h"

#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/memory/memory.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "absl/time/time.h"
#include "opencensus/stats/stats.h"

namespace opencensus {
namespace exporters {
namespace stats {

namespace {

// Functions to format data for different aggregation types.
std::string DataToString(double data) { return absl::StrCat(": ", data, "\n"); }
std::string DataToString(int64_t data) {
  return absl::StrCat(": ", data, "\n");
}
std::string DataToString(const opencensus::stats::Distribution& data) {
  std::string output = "\n";
  std::vector<std::string> lines = absl::StrSplit(data.DebugString(), '\n');
  // Add indent.
  for (const auto& line : lines) {
    absl::StrAppend(&output, "    ", line, "\n");
  }
  return output;
}

class Handler : public opencensus::stats::StatsExporter::Handler {
 public:
  explicit Handler(std::ostream* stream) : stream_(stream) {}

  void ExportViewData(
      const std::vector<std::pair<opencensus::stats::ViewDescriptor,
                                  opencensus::stats::ViewData>>& data) override;

 private:
  // Implements ExportViewData for supported data types.
  template <typename DataValueT>
  void ExportViewDataImpl(
      const opencensus::stats::ViewDescriptor& descriptor,
      const ::opencensus::stats::ViewData::DataMap<absl::Time>& start_times,
      absl::Time end_time,
      const opencensus::stats::ViewData::DataMap<DataValueT>& data);

  std::ostream* stream_;
};

void Handler::ExportViewData(
    const std::vector<std::pair<opencensus::stats::ViewDescriptor,
                                opencensus::stats::ViewData>>& data) {
  for (const auto& datum : data) {
    const auto& view_data = datum.second;
    switch (view_data.type()) {
      case opencensus::stats::ViewData::Type::kDouble:
        ExportViewDataImpl(datum.first, view_data.start_times(),
                           view_data.end_time(), view_data.double_data());
        break;
      case opencensus::stats::ViewData::Type::kInt64:
        ExportViewDataImpl(datum.first, view_data.start_times(),
                           view_data.end_time(), view_data.int_data());
        break;
      case opencensus::stats::ViewData::Type::kDistribution:
        ExportViewDataImpl(datum.first, view_data.start_times(),
                           view_data.end_time(), view_data.distribution_data());
        break;
    }
  }
  stream_->flush();
}

template <typename DataValueT>
void Handler::ExportViewDataImpl(
    const opencensus::stats::ViewDescriptor& descriptor,
    const ::opencensus::stats::ViewData::DataMap<absl::Time>& start_times,
    absl::Time end_time,
    const opencensus::stats::ViewData::DataMap<DataValueT>& data) {
  if (data.empty()) {
    *stream_ << absl::StrCat("No data for view \"", descriptor.name(), "\".\n");
    return;
  }
  // Build a string so we can write it in one shot to minimize crosstalk if
  // multiple threads write to stream_ simultaneously.
  std::string output =
      absl::StrCat("Data for view \"", descriptor.name(), "\":\n");

  for (const auto& row : data) {
    auto start_time = start_times.at(row.first);
    absl::StrAppend(&output, "Row data from ", absl::FormatTime(start_time),
                    " to ", absl::FormatTime(end_time), ":\n");
    absl::StrAppend(&output, "  ");
    for (int i = 0; i < descriptor.columns().size(); ++i) {
      absl::StrAppend(&output, descriptor.columns()[i].name(), "=",
                      row.first[i], " ");
    }
    absl::StrAppend(&output, DataToString(row.second));
  }
  absl::StrAppend(&output, "\n");
  *stream_ << output;
}

}  // namespace

// static
void StdoutExporter::Register(std::ostream* stream) {
  opencensus::stats::StatsExporter::RegisterPushHandler(
      absl::make_unique<Handler>(stream));
}

}  // namespace stats
}  // namespace exporters
}  // namespace opencensus
