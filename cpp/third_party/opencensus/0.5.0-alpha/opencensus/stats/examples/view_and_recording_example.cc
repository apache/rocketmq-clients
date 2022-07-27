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

#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "opencensus/stats/stats.h"

#include "gtest/gtest.h"

namespace {

// The resource owner publicly defines their measure IDs.
constexpr char kFooUsageMeasureName[] = "example.com/Foo/FooUsage";

// The resource owner defines and registers a measure. A function exposing a
// function-local static is the recommended style, ensuring that the measure is
// only registered once.
opencensus::stats::MeasureDouble FooUsage() {
  static const opencensus::stats::MeasureDouble foo_usage =
      opencensus::stats::MeasureDouble::Register(kFooUsageMeasureName,
                                                 "Usage of foos.", "foos");
  return foo_usage;
}

// The resource owner publicly registers the tag keys used in their recording
// calls so that it is accessible to views.
opencensus::tags::TagKey FooIdTagKey() {
  static const auto foo_id_tag_key =
      opencensus::tags::TagKey::Register("foo_id");
  return foo_id_tag_key;
}

// Foo represents an arbitrary component that provides access to a resource and
// records the usage.
class Foo {
 public:
  Foo(absl::string_view id) : id_(id) {
    // Access the measure to ensure it is registered before Views on it are
    // created.
    FooUsage();
  }

  // A Foo API call that records the amount of usage and the id of the object
  // used.
  void Use(double quantity) {
    opencensus::stats::Record({{FooUsage(), quantity}}, {{FooIdTagKey(), id_}});
  }

 private:
  const std::string id_;
};

TEST(ViewAndRecordingExample, Sum) {
  // In this example, a foo must be created before any views using it.
  Foo foo("id_1");

  // The stats consumer creates a View on Foo Usage. Note that the ID is under
  // example.com/Bar--the hierarchical identifier in a View ID should be that of
  // the View's creator, not the Measure's owner.
  const auto view_descriptor =
      opencensus::stats::ViewDescriptor()
          .set_name("example.com/Bar/FooUsage-sum-cumulative-foo_id")
          .set_measure(kFooUsageMeasureName)
          .set_aggregation(opencensus::stats::Aggregation::Sum())
          .add_column(FooIdTagKey())
          .set_description(
              "Cumulative sum of example.com/Foo/FooUsage broken down by "
              "'foo_id'.");
  opencensus::stats::View view(view_descriptor);

  // Once the view is created, usage records under example.com/Bar/FooUsage.
  foo.Use(1);
  foo.Use(4);
  // Sleep to allow the data to propagate to views.
  absl::SleepFor(absl::Seconds(6));

  // The stats consumer can now query the recorded data.
  if (view.IsValid()) {
    const opencensus::stats::ViewData data = view.GetData();
    if (data.type() == opencensus::stats::ViewData::Type::kDouble) {
      std::cout << "Total usage under id_1 "
                << data.double_data().find({"id_1"})->second << "\n";
    }
  }
}

}  // namespace
