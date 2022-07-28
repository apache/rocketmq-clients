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

#include "StdoutHandler.h"

#include "rocketmq/RocketMQ.h"

ROCKETMQ_NAMESPACE_BEGIN

void StdoutHandler::ExportViewData(
    const std::vector<std::pair<opencensus::stats::ViewDescriptor, opencensus::stats::ViewData>>& data) {
  for (const auto& datum : data) {
    const auto& view_data = datum.second;
    const auto& descriptor = datum.first;
    auto record_time = view_data.end_time();
    auto columns = descriptor.columns();

    switch (view_data.type()) {
      case opencensus::stats::ViewData::Type::kInt64: {
        auto data_map = view_data.int_data();
        for (const auto& entry : data_map) {
          absl::Time time = record_time;
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

ROCKETMQ_NAMESPACE_END