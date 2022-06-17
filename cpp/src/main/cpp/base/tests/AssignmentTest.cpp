#include "gtest/gtest.h"
#include <algorithm>

#include "Protocol.h"

ROCKETMQ_NAMESPACE_BEGIN

class AssignmentTest : public testing::Test {
public:
  void SetUp() override {
  }

  void TearDown() override {
  }
};

TEST_F(AssignmentTest, testSort) {
  std::vector<rmq::Assignment> assignments;
  std::sort(assignments.begin(), assignments.end(),
            [](const rmq::Assignment& lhs, const rmq::Assignment& rhs) { return lhs < rhs; });
}

ROCKETMQ_NAMESPACE_END