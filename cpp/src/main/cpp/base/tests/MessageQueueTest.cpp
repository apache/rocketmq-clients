#include <apache/rocketmq/v2/definition.pb.h>
#include <cstdint>

#include "absl/container/flat_hash_map.h"
#include "google/protobuf/map.h"
#include "gtest/gtest.h"

#include "Protocol.h"

ROCKETMQ_NAMESPACE_BEGIN

class MessageQueueTest : public testing::Test {
public:
  void SetUp() override {
  }

  void TearDown() override {
  }
};

ROCKETMQ_NAMESPACE_END
