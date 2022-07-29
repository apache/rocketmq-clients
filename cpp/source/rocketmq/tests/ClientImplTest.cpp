#include <cstddef>
#include <unordered_set>

#include "ClientImpl.h"
#include "gtest/gtest.h"
#include "rocketmq/RocketMQ.h"

ROCKETMQ_NAMESPACE_BEGIN

TEST(ClientImplTest, testClientId) {
  std::unordered_set<std::string> client_ids;
  for (std::size_t i = 0; i < 128; i++) {
    auto&& client_id = clientId();
    std::cout << client_id << std::endl;
    ASSERT_EQ(client_ids.find(client_id), client_ids.end());
    client_ids.insert(std::move(client_id));
  }
}

ROCKETMQ_NAMESPACE_END