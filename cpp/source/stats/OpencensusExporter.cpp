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

#include "ClientManager.h"
#include "MetricBidiReactor.h"
#include "google/protobuf/util/time_util.h"

ROCKETMQ_NAMESPACE_BEGIN

namespace opencensus_proto = opencensus::proto::metrics::v1;

OpencensusExporter::OpencensusExporter(std::string endpoints, std::weak_ptr<Client> client) : client_(client) {
  auto client_shared_ptr = client.lock();
  if (client_shared_ptr) {
    auto channel = client_shared_ptr->manager()->createChannel(endpoints);
    stub_ = opencensus::proto::agent::metrics::v1::MetricsService::NewStub(channel);
  } else {
    SPDLOG_ERROR("Failed to initialize OpencensusExporter. weak_ptr to Client is nullptr");
  }
}

void OpencensusExporter::wrap(const MetricData& data, ExportMetricsServiceRequest& request) {
  auto metrics = request.mutable_metrics();

  for (const auto& entry : data) {
    const auto& view_descriptor = entry.first;

    auto metric = absl::make_unique<opencensus::proto::metrics::v1::Metric>();
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
      auto label_key = absl::make_unique<opencensus::proto::metrics::v1::LabelKey>();
      label_key->set_key(column.name());
      label_keys->AddAllocated(label_key.release());
    }

    auto time_series = metric->mutable_timeseries();
    const auto& view_data = entry.second;

    // TODO: Opencensus provides end-timestamp of the statistics conducted whilst OpenTelemetry requires
    // start-timestamp. Let us ignore the difference for now.
    auto stats_time = google::protobuf::util::TimeUtil::TimeTToTimestamp(absl::ToTimeT(view_data.end_time()));
    switch (view_data.type()) {
      case opencensus::stats::ViewData::Type::kInt64: {
        for (const auto& entry : view_data.int_data()) {
          auto time_series_element = absl::make_unique<opencensus::proto::metrics::v1::TimeSeries>();
          time_series_element->mutable_start_timestamp()->CopyFrom(stats_time);
          auto label_values = time_series_element->mutable_label_values();
          for (const auto& value : entry.first) {
            auto label_value = absl::make_unique<opencensus::proto::metrics::v1::LabelValue>();
            label_value->set_value(value);
            label_value->set_has_value(true);
            label_values->AddAllocated(label_value.release());
          }
          auto point = absl::make_unique<opencensus::proto::metrics::v1::Point>();
          point->mutable_timestamp()->CopyFrom(stats_time);
          point->set_int64_value(entry.second);
          time_series_element->mutable_points()->AddAllocated(point.release());
          time_series->AddAllocated(time_series_element.release());
        }
        break;
      }
      case opencensus::stats::ViewData::Type::kDouble: {
        for (const auto& entry : view_data.double_data()) {
          auto time_series_element = absl::make_unique<opencensus::proto::metrics::v1::TimeSeries>();
          time_series_element->mutable_start_timestamp()->CopyFrom(stats_time);
          auto label_values = time_series_element->mutable_label_values();
          for (const auto& value : entry.first) {
            auto label_value = absl::make_unique<opencensus::proto::metrics::v1::LabelValue>();
            label_value->set_value(value);
            label_value->set_has_value(true);
            label_values->AddAllocated(label_value.release());
          }
          auto point = absl::make_unique<opencensus::proto::metrics::v1::Point>();
          point->mutable_timestamp()->CopyFrom(stats_time);
          point->set_double_value(entry.second);
          time_series_element->mutable_points()->AddAllocated(point.release());
          time_series->AddAllocated(time_series_element.release());
        }
        break;
      }
      case opencensus::stats::ViewData::Type::kDistribution: {
        for (const auto& entry : view_data.distribution_data()) {
          auto time_series_element = absl::make_unique<opencensus::proto::metrics::v1::TimeSeries>();
          time_series_element->mutable_start_timestamp()->CopyFrom(stats_time);
          auto label_values = time_series_element->mutable_label_values();
          for (const auto& value : entry.first) {
            auto label_value = absl::make_unique<opencensus::proto::metrics::v1::LabelValue>();
            label_value->set_value(value);
            label_value->set_has_value(true);
            label_values->AddAllocated(label_value.release());
          }
          auto point = absl::make_unique<opencensus::proto::metrics::v1::Point>();
          point->mutable_timestamp()->CopyFrom(stats_time);
          auto distribution_value = absl::make_unique<opencensus::proto::metrics::v1::DistributionValue>();
          distribution_value->set_count(entry.second.count());
          distribution_value->set_sum_of_squared_deviation(entry.second.sum_of_squared_deviation());
          distribution_value->set_sum(entry.second.count() * entry.second.mean());
          for (const auto& cnt : entry.second.bucket_counts()) {
            auto bucket = absl::make_unique<opencensus::proto::metrics::v1::DistributionValue::Bucket>();
            bucket->set_count(cnt);
            distribution_value->mutable_buckets()->AddAllocated(bucket.release());
          }
          auto bucket_options = distribution_value->mutable_bucket_options();
          for (const auto& boundary : entry.second.bucket_boundaries().lower_boundaries()) {
            bucket_options->mutable_explicit_()->mutable_bounds()->Add(boundary);
          }
          point->set_allocated_distribution_value(distribution_value.release());
          time_series_element->mutable_points()->AddAllocated(point.release());

          time_series->AddAllocated(time_series_element.release());
        }
        break;
      }
    }

    if (time_series->empty()) {
      continue;
    }

    metrics->AddAllocated(metric.release());
  }
}

void OpencensusExporter::ExportViewData(
    const std::vector<std::pair<opencensus::stats::ViewDescriptor, opencensus::stats::ViewData>>& data) {
  opencensus::proto::agent::metrics::v1::ExportMetricsServiceRequest request;
  wrap(data, request);
  if (!bidi_reactor_) {
    auto ptr = client_.lock();
    if (ptr) {
      bidi_reactor_ = absl::make_unique<MetricBidiReactor>(ptr, shared_from_this());
    } else {
      SPDLOG_INFO("did not create stream since the client is no longer available.");
    }
  }

  if (request.metrics_size() && bidi_reactor_) {
    SPDLOG_DEBUG("ExportMetricRequest: {}", request.DebugString());
    bidi_reactor_->write(request);
  } else {
    SPDLOG_DEBUG("ExportMetricRequest contains no valid metric");
  }
}

void OpencensusExporter::resetStream() {
  auto ptr = client_.lock();
  if (ptr) {
    bidi_reactor_.reset(new MetricBidiReactor(ptr, shared_from_this()));
  } else {
    SPDLOG_INFO("did not reset stream since the client is no longer available.");
  }
}

ROCKETMQ_NAMESPACE_END
