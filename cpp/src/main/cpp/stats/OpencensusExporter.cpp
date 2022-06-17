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

#include "OpencensusExporter.h"

#include "MetricBidiReactor.h"
#include "google/protobuf/util/time_util.h"

ROCKETMQ_NAMESPACE_BEGIN

namespace opencensus_proto = opencensus::proto::metrics::v1;

OpencensusExporter::OpencensusExporter(std::string endpoints, std::weak_ptr<Client> client) : client_(client) {
}

void OpencensusExporter::wrap(const MetricData& data, ExportMetricsServiceRequest& request) {
  auto metrics = request.mutable_metrics();

  for (const auto& entry : data) {
    const auto& view_descriptor = entry.first;

    auto metric = new opencensus::proto::metrics::v1::Metric();
    auto descriptor = metric->mutable_metric_descriptor();
    descriptor->set_name(view_descriptor.name());
    descriptor->set_description(view_descriptor.description());
    descriptor->set_unit(view_descriptor.measure_descriptor().units());
    switch (view_descriptor.aggregation().type()) {
      case opencensus::stats::Aggregation::Type::kCount: {
        descriptor->set_type(opencensus_proto::MetricDescriptor_Type::MetricDescriptor_Type_CUMULATIVE_INT64);
        break;
      }

      case opencensus::stats::Aggregation::Type::kSum: {
        descriptor->set_type(opencensus_proto::MetricDescriptor_Type::MetricDescriptor_Type_CUMULATIVE_INT64);
        break;
      }

      case opencensus::stats::Aggregation::Type::kLastValue: {
        descriptor->set_type(opencensus_proto::MetricDescriptor_Type::MetricDescriptor_Type_GAUGE_INT64);
        break;
      }

      case opencensus::stats::Aggregation::Type::kDistribution: {
        descriptor->set_type(opencensus_proto::MetricDescriptor_Type::MetricDescriptor_Type_GAUGE_DISTRIBUTION);
        break;
      }
    }

    auto label_keys = descriptor->mutable_label_keys();
    for (const auto& column : view_descriptor.columns()) {
      auto label_key = new opencensus::proto::metrics::v1::LabelKey;
      label_key->set_key(column.name());
      label_keys->AddAllocated(label_key);
    }

    auto time_series = metric->mutable_timeseries();
    const auto& view_data = entry.second;

    auto start_times = view_data.start_times();

    switch (view_data.type()) {
      case opencensus::stats::ViewData::Type::kInt64: {
        for (const auto& entry : view_data.int_data()) {
          auto time_series_element = new opencensus::proto::metrics::v1::TimeSeries();

          auto search = start_times.find(entry.first);
          if (search != start_times.end()) {
            absl::Time time = search->second;
            time_series_element->mutable_start_timestamp()->CopyFrom(
                google::protobuf::util::TimeUtil::TimeTToTimestamp(absl::ToTimeT(time)));
          }

          auto label_values = time_series_element->mutable_label_values();
          for (const auto& value : entry.first) {
            auto label_value = new opencensus::proto::metrics::v1::LabelValue;
            label_value->set_value(value);
            label_value->set_has_value(true);
            label_values->AddAllocated(label_value);
          }

          auto point = new opencensus::proto::metrics::v1::Point();
          point->set_int64_value(entry.second);
          time_series_element->mutable_points()->AddAllocated(point);

          time_series->AddAllocated(time_series_element);
        }
        break;
      }
      case opencensus::stats::ViewData::Type::kDouble: {
        for (const auto& entry : view_data.double_data()) {
          auto time_series_element = new opencensus::proto::metrics::v1::TimeSeries();
          auto search = start_times.find(entry.first);
          if (search != start_times.end()) {
            absl::Time time = search->second;
            time_series_element->mutable_start_timestamp()->CopyFrom(
                google::protobuf::util::TimeUtil::TimeTToTimestamp(absl::ToTimeT(time)));
          }

          auto label_values = time_series_element->mutable_label_values();
          for (const auto& value : entry.first) {
            auto label_value = new opencensus::proto::metrics::v1::LabelValue;
            label_value->set_value(value);
            label_value->set_has_value(true);
            label_values->AddAllocated(label_value);
          }

          auto point = new opencensus::proto::metrics::v1::Point();
          point->set_double_value(entry.second);
          time_series_element->mutable_points()->AddAllocated(point);

          time_series->AddAllocated(time_series_element);
        }
        break;
      }
      case opencensus::stats::ViewData::Type::kDistribution: {
        for (const auto& entry : view_data.distribution_data()) {
          auto time_series_element = new opencensus::proto::metrics::v1::TimeSeries();
          auto search = start_times.find(entry.first);
          if (search != start_times.end()) {
            absl::Time time = search->second;
            time_series_element->mutable_start_timestamp()->CopyFrom(
                google::protobuf::util::TimeUtil::TimeTToTimestamp(absl::ToTimeT(time)));
          }

          auto label_values = time_series_element->mutable_label_values();
          for (const auto& value : entry.first) {
            auto label_value = new opencensus::proto::metrics::v1::LabelValue;
            label_value->set_value(value);
            label_value->set_has_value(true);
            label_values->AddAllocated(label_value);
          }
          auto point = new opencensus::proto::metrics::v1::Point();
          auto distribution_value = new opencensus::proto::metrics::v1::DistributionValue;
          point->set_allocated_distribution_value(distribution_value);
          time_series_element->mutable_points()->AddAllocated(point);
          distribution_value->set_count(entry.second.count());
          distribution_value->set_sum_of_squared_deviation(entry.second.sum_of_squared_deviation());
          for (const auto& cnt : entry.second.bucket_counts()) {
            auto bucket = new opencensus::proto::metrics::v1::DistributionValue::Bucket();
            bucket->set_count(cnt);
            distribution_value->mutable_buckets()->AddAllocated(bucket);
          }

          auto bucket_options = distribution_value->mutable_bucket_options();

          for (const auto& boundary : entry.second.bucket_boundaries().lower_boundaries()) {
            bucket_options->mutable_explicit_()->mutable_bounds()->Add(boundary);
          }

          time_series->AddAllocated(time_series_element);
        }
        break;
      }
    }

    metrics->AddAllocated(metric);
  }
}

void OpencensusExporter::exportMetrics(
    const std::vector<std::pair<opencensus::stats::ViewDescriptor, opencensus::stats::ViewData>>& data) {
  opencensus::proto::agent::metrics::v1::ExportMetricsServiceRequest request;
  wrap(data, request);
  std::weak_ptr<OpencensusExporter> exporter{shared_from_this()};
  if (!bidi_reactor_) {
    bidi_reactor_ = absl::make_unique<MetricBidiReactor>(client_, exporter);
  }
  bidi_reactor_->write(request);
}

void OpencensusExporter::resetStream() {
  std::weak_ptr<OpencensusExporter> exporter{shared_from_this()};
  bidi_reactor_.reset(new MetricBidiReactor(client_, exporter));
}

ROCKETMQ_NAMESPACE_END
