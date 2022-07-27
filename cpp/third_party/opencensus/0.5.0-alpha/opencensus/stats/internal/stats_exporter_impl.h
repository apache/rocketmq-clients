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

#ifndef OPENCENSUS_STATS_INTERNAL_STATS_EXPORTER_IMPL_H_
#define OPENCENSUS_STATS_INTERNAL_STATS_EXPORTER_IMPL_H_

#include <thread>
#include <utility>
#include <vector>

#include "absl/synchronization/mutex.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "opencensus/stats/internal/aggregation_window.h"
#include "opencensus/stats/stats_exporter.h"
#include "opencensus/stats/view_data.h"
#include "opencensus/stats/view_descriptor.h"

namespace opencensus {
namespace stats {

class StatsExporterImpl {
 public:
  static StatsExporterImpl* Get();
  void SetInterval(absl::Duration interval);
  absl::Time GetNextExportTime() const;
  void AddView(const ViewDescriptor& view);
  void RemoveView(absl::string_view name);

  // Adds a handler, which cannot be subsequently removed (except by
  // ClearHandlersForTesting()). The background thread is started when the
  // first handler is registered.
  void RegisterPushHandler(std::unique_ptr<StatsExporter::Handler> handler);

  std::vector<std::pair<ViewDescriptor, ViewData>> GetViewData();
  void Export();
  void ClearHandlersForTesting();

 private:
  StatsExporterImpl() = default;

  void StartExportThread() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  // Loops forever, calling Export() every export_interval_.
  void RunWorkerLoop();

  mutable absl::Mutex mu_;
  absl::Duration export_interval_ ABSL_GUARDED_BY(mu_) = absl::Seconds(10);
  std::vector<std::unique_ptr<StatsExporter::Handler>> handlers_
      ABSL_GUARDED_BY(mu_);
  std::unordered_map<std::string, std::unique_ptr<View>> views_
      ABSL_GUARDED_BY(mu_);
  bool thread_started_ ABSL_GUARDED_BY(mu_) = false;
  std::thread t_ ABSL_GUARDED_BY(mu_);
};

}  // namespace stats
}  // namespace opencensus

#endif  // OPENCENSUS_STATS_INTERNAL_STATS_EXPORTER_IMPL_H_
