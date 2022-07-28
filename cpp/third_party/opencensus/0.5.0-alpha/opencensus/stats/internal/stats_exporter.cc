// Copyright 2017, OpenCensus Authors
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

#include "opencensus/stats/stats_exporter.h"
#include "opencensus/stats/internal/stats_exporter_impl.h"

#include <thread>
#include <utility>
#include <vector>

#include "absl/memory/memory.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "opencensus/stats/internal/aggregation_window.h"
#include "opencensus/stats/view_data.h"
#include "opencensus/stats/view_descriptor.h"

namespace opencensus {
namespace stats {

// static
StatsExporterImpl* StatsExporterImpl::Get() {
  static StatsExporterImpl* global_stats_exporter_impl =
      new StatsExporterImpl();
  return global_stats_exporter_impl;
}

void StatsExporterImpl::SetInterval(absl::Duration interval) {
  absl::MutexLock l(&mu_);
  export_interval_ = interval;
}

absl::Time StatsExporterImpl::GetNextExportTime() const {
  absl::MutexLock l(&mu_);
  return absl::Now() + export_interval_;
}

void StatsExporterImpl::AddView(const ViewDescriptor& view) {
  absl::MutexLock l(&mu_);
  views_[view.name()] = absl::make_unique<opencensus::stats::View>(view);
}

void StatsExporterImpl::RemoveView(absl::string_view name) {
  absl::MutexLock l(&mu_);
  views_.erase(std::string(name));
}

void StatsExporterImpl::RegisterPushHandler(
    std::unique_ptr<StatsExporter::Handler> handler) {
  absl::MutexLock l(&mu_);
  handlers_.push_back(std::move(handler));
  if (!thread_started_) {
    StartExportThread();
  }
}

std::vector<std::pair<ViewDescriptor, ViewData>>
StatsExporterImpl::GetViewData() {
  absl::ReaderMutexLock l(&mu_);
  std::vector<std::pair<ViewDescriptor, ViewData>> data;
  data.reserve(views_.size());
  for (const auto& view : views_) {
    data.emplace_back(view.second->descriptor(), view.second->GetData());
  }
  return data;
}

void StatsExporterImpl::Export() {
  absl::ReaderMutexLock l(&mu_);
  std::vector<std::pair<ViewDescriptor, ViewData>> data;
  data.reserve(views_.size());
  for (const auto& view : views_) {
    data.emplace_back(view.second->descriptor(), view.second->GetData());
  }
  for (auto& handler : handlers_) {
    handler->ExportViewData(data);
  }
}

void StatsExporterImpl::ClearHandlersForTesting() {
  absl::MutexLock l(&mu_);
  handlers_.clear();
}

void StatsExporterImpl::StartExportThread() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
  t_ = std::thread(&StatsExporterImpl::RunWorkerLoop, this);
  thread_started_ = true;
}

void StatsExporterImpl::RunWorkerLoop() {
  absl::Time next_export_time = GetNextExportTime();
  while (true) {
    // SleepFor() returns immediately when given a negative duration.
    absl::SleepFor(next_export_time - absl::Now());
    // In case the last export took longer than the export interval, we
    // calculate the next time from now.
    next_export_time = GetNextExportTime();
    Export();
  }
}

// StatsExporter
// -------------

// static
void StatsExporter::SetInterval(absl::Duration interval) {
  StatsExporterImpl::Get()->SetInterval(interval);
}

// static
void StatsExporter::RemoveView(absl::string_view name) {
  StatsExporterImpl::Get()->RemoveView(name);
}

// static
void StatsExporter::RegisterPushHandler(std::unique_ptr<Handler> handler) {
  StatsExporterImpl::Get()->RegisterPushHandler(std::move(handler));
}

// static
std::vector<std::pair<ViewDescriptor, ViewData>> StatsExporter::GetViewData() {
  return StatsExporterImpl::Get()->GetViewData();
}

// static
void StatsExporter::ExportForTesting() { StatsExporterImpl::Get()->Export(); }

// static
void StatsExporter::ClearHandlersForTesting() {
  StatsExporterImpl::Get()->ClearHandlersForTesting();
}

}  // namespace stats
}  // namespace opencensus
