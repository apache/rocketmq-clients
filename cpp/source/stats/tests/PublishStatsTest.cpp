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

#include <chrono>
#include <iostream>
#include <mutex>
#include <thread>

#include "PublishStats.h"
#include "gtest/gtest.h"

ROCKETMQ_NAMESPACE_BEGIN

class Handler : public opencensus::stats::StatsExporter::Handler {
public:
  void ExportViewData(
      const std::vector<std::pair<opencensus::stats::ViewDescriptor, opencensus::stats::ViewData>>& data) override {
    std::cout << "================================================================================" << std::endl;
    for (const auto& datum : data) {
      const auto& view_data = datum.second;
      const auto& descriptor = datum.first;
      auto start_times = view_data.start_times();
      auto columns = descriptor.columns();

      switch (view_data.type()) {
        case opencensus::stats::ViewData::Type::kInt64: {
          auto data_map = view_data.int_data();
          for (const auto& entry : data_map) {
            absl::Time time = start_times[entry.first];
            std::string line;
            line.append(absl::FormatTime(time)).append(" ");
            line.append(descriptor.name());
            line.append("{");
            for (std::size_t i = 0; i < columns.size(); i++) {
              line.append(columns[i].name()).append("=").append(entry.first[i]);
              if (i < columns.size() - 1) {
                line.append(", ");
              } else {
                line.append("} ==> ");
              }
            }
            line.append(std::to_string(entry.second));
            println(line);
          }
          break;
        }
        case opencensus::stats::ViewData::Type::kDouble: {
          exportDatum(datum.first, view_data.start_time(), view_data.end_time(), view_data.double_data());
          break;
        }
        case opencensus::stats::ViewData::Type::kDistribution: {
          for (const auto& entry : view_data.distribution_data()) {
            std::string line(descriptor.name());
            line.append("{");
            for (std::size_t i = 0; i < columns.size(); i++) {
              line.append(columns[i].name()).append("=").append(entry.first[i]);
              if (i < columns.size() - 1) {
                line.append(", ");
              } else {
                line.append("} ==> ");
              }
            }
            line.append(entry.second.DebugString());
            println(line);

            println(absl::StrJoin(entry.second.bucket_boundaries().lower_boundaries(), ","));
          }
          break;
        }
      }
    }
  }

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
                  << DataToString(row.second) << std::endl;
      }
    }
  }

  std::mutex console_mtx;
  void println(const std::string& line) {
    std::lock_guard<std::mutex> lk(console_mtx);
    std::cout << line << std::endl;
  }

  // Functions to format data for different aggregation types.
  std::string DataToString(double data) {
    return absl::StrCat(": ", data, "\n");
  }
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
};

TEST(StatsTest, testBasics) {
  std::string t1("T1");
  std::string t2("T2");
  PublishStats metrics;
  opencensus::stats::StatsExporter::SetInterval(absl::Seconds(5));
  opencensus::stats::StatsExporter::RegisterPushHandler(absl::make_unique<Handler>());
  std::atomic_bool stopped{false};
  auto generator = [&]() {
    while (!stopped) {
      for (std::size_t i = 0; i < 10; i++) {
        opencensus::stats::Record({{metrics.latency(), i * 10}},
                                  {{Tag::topicTag(), t1}, {Tag::clientIdTag(), "client-0"}});
      }
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  };
  std::thread feeder(generator);

  std::this_thread::sleep_for(std::chrono::seconds(10));
  stopped.store(true);
  if (feeder.joinable()) {
    feeder.join();
  }
}

ROCKETMQ_NAMESPACE_END
