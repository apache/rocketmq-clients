#include <chrono>
#include <cstddef>
#include <mutex>
#include <thread>

#include "CountdownLatch.h"
#include "gtest/gtest.h"
#include "rocketmq/RocketMQ.h"

ROCKETMQ_NAMESPACE_BEGIN

class CountdownLatchTest : public testing::Test {
public:
  void SetUp() override {
    countdown_latch_ = absl::make_unique<CountdownLatch>(permit_);
  }

  void TearDown() override {
  }

protected:
  const std::size_t permit_{2};
  std::unique_ptr<CountdownLatch> countdown_latch_;
  absl::Mutex mtx_;
};

TEST_F(CountdownLatchTest, testAwait) {
  int count = 0;

  auto lambda = [&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    {
      absl::MutexLock lk(&mtx_);
      count++;
    }
    countdown_latch_->countdown();
  };

  std::vector<std::thread> threads;
  for (std::size_t i = 0; i < permit_; i++) {
    auto t = std::thread(lambda);
    threads.push_back(std::move(t));
  }

  countdown_latch_->await();

  ASSERT_EQ(count, permit_);

  for (auto& thread : threads) {
    if (thread.joinable()) {
      thread.join();
    }
  }
}

ROCKETMQ_NAMESPACE_END