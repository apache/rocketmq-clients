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

#include <mutex>
#include <string>

#include "opencensus/stats/stats.h"
#include "rocketmq/RocketMQ.h"

ROCKETMQ_NAMESPACE_BEGIN

class StdoutHandler : public opencensus::stats::StatsExporter::Handler {
public:
  void ExportViewData(
      const std::vector<std::pair<opencensus::stats::ViewDescriptor, opencensus::stats::ViewData>>& data) override;

private:
  template <typename T>
  void exportDatum(const opencensus::stats::ViewDescriptor& descriptor,
                   absl::Time start_time,
                   absl::Time end_time,
                   const opencensus::stats::ViewData::DataMap<T>& data) {
    if (data.empty()) {
      // std::cout << "No data for " << descriptor.name() << std::endl;
      return;
    }

    for (const auto& row : data) {
      for (std::size_t column = 0; column < descriptor.columns().size(); column++) {
        std::cout << descriptor.name() << "[" << descriptor.columns()[column].name() << "=" << row.first[column] << "]"
                  << dataToString(row.second) << std::endl;
      }
    }
  }

  std::mutex console_mtx_;

  void println(const std::string& line) {
    std::lock_guard<std::mutex> lk(console_mtx_);
    std::cout << line << std::endl;
  }

  // Functions to format data for different aggregation types.
  std::string dataToString(double data) {
    return absl::StrCat(": ", data, "\n");
  }
  std::string dataToString(int64_t data) {
    return absl::StrCat(": ", data, "\n");
  }
  std::string dataToString(const opencensus::stats::Distribution& data) {
    std::string output = "\n";
    std::vector<std::string> lines = absl::StrSplit(data.DebugString(), '\n');
    // Add indent.
    for (const auto& line : lines) {
      absl::StrAppend(&output, "    ", line, "\n");
    }
    return output;
  }
};

ROCKETMQ_NAMESPACE_END