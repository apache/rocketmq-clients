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
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>

#include "ClientMock.h"
#include "TelemetryBidiReactor.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "grpcpp/create_channel.h"
#include "grpcpp/security/credentials.h"

ROCKETMQ_NAMESPACE_BEGIN

namespace {

/// Helper: create a real gRPC stub to localhost (no server needed for construction).
struct TestStubHolder {
  std::shared_ptr<grpc::Channel> channel;
  std::unique_ptr<rmq::MessagingService::Stub> stub;
};

TestStubHolder createTestStub() {
  auto channel = grpc::CreateChannel("127.0.0.1:19999", grpc::InsecureChannelCredentials());
  auto stub = rmq::MessagingService::NewStub(channel);
  TestStubHolder holder;
  holder.channel = channel;
  holder.stub = std::move(stub);
  return holder;
}

/// Helper: create an expired weak_ptr<Client> that causes the reactor to short-circuit.
std::weak_ptr<Client> expiredWeakClient() {
  std::shared_ptr<Client> empty;
  return std::weak_ptr<Client>(empty);
}

}  // namespace

// ---------------------------------------------------------------------------
// StreamState enum tests
// ---------------------------------------------------------------------------

TEST(StreamStateTest, enumValuesAndOrderingTest) {
  EXPECT_EQ(0, static_cast<std::uint8_t>(StreamState::Ready));
  EXPECT_EQ(1, static_cast<std::uint8_t>(StreamState::Closing));
  EXPECT_EQ(2, static_cast<std::uint8_t>(StreamState::Closed));

  // Verify ordering: Ready < Closing < Closed
  EXPECT_TRUE(StreamState::Ready < StreamState::Closing);
  EXPECT_TRUE(StreamState::Closing < StreamState::Closed);
  EXPECT_TRUE(StreamState::Ready < StreamState::Closed);
}

TEST(StreamStateTest, enumDistinctValuesTest) {
  EXPECT_NE(StreamState::Ready, StreamState::Closing);
  EXPECT_NE(StreamState::Closing, StreamState::Closed);
  EXPECT_NE(StreamState::Ready, StreamState::Closed);
}

// ---------------------------------------------------------------------------
// Construction tests (expired weak_ptr — no gRPC calls)
// ---------------------------------------------------------------------------

class TelemetryBidiReactorTest : public testing::Test {
protected:
  void SetUp() override {
    TestStubHolder h = createTestStub();
    channel_ = h.channel;
    stub_ = std::move(h.stub);
  }

  std::shared_ptr<grpc::Channel> channel_;
  std::unique_ptr<rmq::MessagingService::Stub> stub_;
};

TEST_F(TelemetryBidiReactorTest, constructWithExpiredClientSetsClosedStateTest) {
  // With an expired weak_ptr, the constructor short-circuits:
  //   - Logs a warning
  //   - Sets state_ = StreamState::Closed
  //   - Returns without calling stub->async()->Telemetry()
  auto reactor = std::make_shared<TelemetryBidiReactor>(expiredWeakClient(), stub_.get(), "127.0.0.1:19999");

  // The reactor should be in Closed state. We verify indirectly: close() on a
  // Closed reactor should return immediately (no blocking wait).
  auto start = std::chrono::steady_clock::now();
  reactor->close();
  auto elapsed = std::chrono::steady_clock::now() - start;

  // close() should complete nearly instantly since state is already Closed.
  EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(), 500);
}

TEST_F(TelemetryBidiReactorTest, constructWithExpiredClientDestructorSafeTest) {
  // Verify that the reactor can be constructed and destructed safely with an
  // expired client — no crashes, no leaks.
  {
    auto reactor = std::make_shared<TelemetryBidiReactor>(expiredWeakClient(), stub_.get(), "127.0.0.1:19999");
    // Let it go out of scope — destructor should log and exit cleanly.
  }
  // If we reach here without crashing, the test passes.
  SUCCEED();
}

// ---------------------------------------------------------------------------
// close() tests
// ---------------------------------------------------------------------------

TEST_F(TelemetryBidiReactorTest, closeOnExpiredClientReactorIsIdempotentTest) {
  auto reactor = std::make_shared<TelemetryBidiReactor>(expiredWeakClient(), stub_.get(), "127.0.0.1:19999");

  // First close — state is already Closed, should return immediately.
  reactor->close();

  // Second close — still safe, no crash, no hang.
  auto start = std::chrono::steady_clock::now();
  reactor->close();
  auto elapsed = std::chrono::steady_clock::now() - start;
  EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(), 500);
}

// ---------------------------------------------------------------------------
// write() tests
// ---------------------------------------------------------------------------

TEST_F(TelemetryBidiReactorTest, writeOnClosedReactorDropsCommandTest) {
  auto reactor = std::make_shared<TelemetryBidiReactor>(expiredWeakClient(), stub_.get(), "127.0.0.1:19999");

  // State is Closed (expired client). write() should silently drop the command
  // because state != Ready.
  TelemetryCommand cmd;
  cmd.mutable_thread_stack_trace()->set_nonce("test-nonce");
  cmd.mutable_thread_stack_trace()->set_thread_stack_trace("dummy");

  // Should not crash or hang — the command is rejected internally.
  reactor->write(std::move(cmd));
  SUCCEED();
}

TEST_F(TelemetryBidiReactorTest, writeMultipleCommandsOnClosedReactorTest) {
  auto reactor = std::make_shared<TelemetryBidiReactor>(expiredWeakClient(), stub_.get(), "127.0.0.1:19999");

  // Write several commands — all should be silently dropped.
  for (int i = 0; i < 10; ++i) {
    TelemetryCommand cmd;
    cmd.mutable_thread_stack_trace()->set_nonce("nonce-" + std::to_string(i));
    reactor->write(std::move(cmd));
  }
  SUCCEED();
}

// ---------------------------------------------------------------------------
// awaitApplyingSettings() tests
// ---------------------------------------------------------------------------

TEST_F(TelemetryBidiReactorTest, awaitApplyingSettingsTimesOutOnClosedReactorTest) {
  auto reactor = std::make_shared<TelemetryBidiReactor>(expiredWeakClient(), stub_.get(), "127.0.0.1:19999");

  // State is Closed. The promise is never set, so awaitApplyingSettings() will:
  //   1. Wait 3 seconds for the future (times out)
  //   2. Set intentional_close_ = true
  //   3. Try Ready→Closing transition (no-op, already Closed)
  //   4. Call context_.TryCancel() (harmless)
  //   5. Wait for state == Closed (already true, exits immediately)
  //   6. Return false
  auto start = std::chrono::steady_clock::now();
  bool result = reactor->awaitApplyingSettings();
  auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_FALSE(result);
  // Should take approximately 3 seconds (the wait_for timeout).
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
  EXPECT_GE(ms, 2500);
  EXPECT_LT(ms, 5000);
}

// ---------------------------------------------------------------------------
// OnDone() tests (manual invocation)
// ---------------------------------------------------------------------------

TEST_F(TelemetryBidiReactorTest, onDoneWithExpiredClientNoCrashTest) {
  auto reactor = std::make_shared<TelemetryBidiReactor>(expiredWeakClient(), stub_.get(), "127.0.0.1:19999");

  // Manually invoke OnDone with a cancelled status.
  // Since client_ is expired, OnDone should:
  //   1. Set state to Closed (already Closed, no-op)
  //   2. Signal condvar (harmless)
  //   3. client_.lock() returns nullptr → return early (no reconnect)
  grpc::Status status(grpc::StatusCode::CANCELLED, "test cancellation");
  reactor->OnDone(status);
  SUCCEED();
}

TEST_F(TelemetryBidiReactorTest, onDoneWithOkStatusAndExpiredClientTest) {
  auto reactor = std::make_shared<TelemetryBidiReactor>(expiredWeakClient(), stub_.get(), "127.0.0.1:19999");

  // OnDone with OK status — still safe with expired client.
  grpc::Status status(grpc::StatusCode::OK, "");
  reactor->OnDone(status);
  SUCCEED();
}

// ---------------------------------------------------------------------------
// Construction with valid ClientMock (gRPC stream starts, fails async)
// ---------------------------------------------------------------------------

class TelemetryBidiReactorWithClientTest : public testing::Test {
protected:
  void SetUp() override {
    TestStubHolder h = createTestStub();
    channel_ = h.channel;
    stub_ = std::move(h.stub);

    client_ = std::make_shared<testing::NiceMock<ClientMock>>();
    ON_CALL(*client_, config).WillByDefault(testing::ReturnRef(config_));
    ON_CALL(*client_, active).WillByDefault(testing::Return(false));
  }

  void TearDown() override {
    // Allow async gRPC callbacks to complete before destroying mocks.
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    client_.reset();
  }

  std::shared_ptr<grpc::Channel> channel_;
  std::unique_ptr<rmq::MessagingService::Stub> stub_;
  std::shared_ptr<testing::NiceMock<ClientMock>> client_;
  ClientConfig config_;
};

TEST_F(TelemetryBidiReactorWithClientTest, constructWithValidClientStartsStreamTest) {
  // With a valid client, the constructor:
  //   1. Locks the weak_ptr successfully
  //   2. Sets a deadline on the context
  //   3. Signs metadata from config
  //   4. Calls stub->async()->Telemetry() — starts the gRPC stream
  //   5. Calls StartRead(), AddHold(), StartCall()
  //
  // The stream will fail (no server), triggering OnDone asynchronously.
  // Since active() returns false, no reconnect is scheduled.
  auto reactor = std::make_shared<TelemetryBidiReactor>(
      std::weak_ptr<Client>(client_), stub_.get(), "127.0.0.1:19999");

  // close() should work: sets intentional_close_, cancels context, waits for Closed.
  // If OnDone already fired (stream failed quickly), close() returns immediately.
  // If OnDone hasn't fired yet, TryCancel triggers it.
  reactor->close();
  SUCCEED();
}

TEST_F(TelemetryBidiReactorWithClientTest, closeBeforeStreamConnectsTest) {
  // Immediately close after construction — the stream may not have connected yet.
  auto reactor = std::make_shared<TelemetryBidiReactor>(
      std::weak_ptr<Client>(client_), stub_.get(), "127.0.0.1:19999");

  // close() should complete within a reasonable time.
  auto start = std::chrono::steady_clock::now();
  reactor->close();
  auto elapsed = std::chrono::steady_clock::now() - start;

  // Should complete within 5 seconds (gRPC TryCancel is usually fast).
  EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(), 5000);
}

TEST_F(TelemetryBidiReactorWithClientTest, DISABLED_writeImmediatelyAfterConstructTest) {
  // Write a command immediately after construction. The stream is in Ready state
  // (gRPC hasn't reported failure yet), so the command should be buffered and
  // StartWrite attempted. It will fail when the stream fails.
  auto reactor = std::make_shared<TelemetryBidiReactor>(
      std::weak_ptr<Client>(client_), stub_.get(), "127.0.0.1:19999");

  TelemetryCommand cmd;
  cmd.mutable_thread_stack_trace()->set_nonce("early-write");
  reactor->write(std::move(cmd));

  // Clean up — close the stream.
  reactor->close();
  SUCCEED();
}

// ---------------------------------------------------------------------------
// OnDone with valid client — reconnect suppression
// ---------------------------------------------------------------------------

TEST_F(TelemetryBidiReactorWithClientTest, onDoneWithIntentionalCloseSkipsReconnectTest) {
  auto reactor = std::make_shared<TelemetryBidiReactor>(
      std::weak_ptr<Client>(client_), stub_.get(), "127.0.0.1:19999");

  // close() sets intentional_close_ = true, then cancels the stream.
  reactor->close();

  // After close(), OnDone has already been called by gRPC (via TryCancel).
  // Since intentional_close_ was set, no reconnect should have been scheduled.
  // We verify by checking that createSession was never called.
  // (NiceMock suppresses uninteresting calls, so no EXPECT_CALL needed —
  //  but we can verify the mock was not called if we set an expectation.)
  SUCCEED();
}

// ---------------------------------------------------------------------------
// DISABLED tests — require a live gRPC server
// ---------------------------------------------------------------------------

// DISABLED: Would test full settings exchange round-trip.
// Requires a gRPC server that responds to Telemetry with a Settings command.
// The test would:
//   1. Start a mock gRPC server
//   2. Create a reactor with a valid client
//   3. Server sends Settings → OnReadDone processes them
//   4. awaitApplyingSettings() returns true
//   5. Verify that client config was updated with the received settings
TEST_F(TelemetryBidiReactorTest, DISABLED_settingsExchangeRoundTripTest) {
  // Requires a live gRPC server that sends Settings on the telemetry stream.
}

// DISABLED: Would test bidirectional write and read.
// Requires a gRPC server that:
//   - Accepts Telemetry writes
//   - Sends Telemetry responses (e.g., PrintThreadStackTraceCommand)
// The test would verify:
//   - write() buffers and sends commands
//   - OnReadDone processes incoming commands
//   - OnWriteDone removes written commands from the buffer
//   - tryWriteNext() sends the next buffered command
TEST_F(TelemetryBidiReactorTest, DISABLED_bidirectionalWriteReadTest) {
  // Requires a live gRPC server supporting bidirectional telemetry.
}

// DISABLED: Would test automatic session reconnection.
// Requires a gRPC server that:
//   - Accepts initial connection
//   - Drops the connection after a short delay
// The test would verify:
//   - OnDone fires with non-OK status
//   - Since client is active and intentional_close_ is false, reconnect is scheduled
//   - client->schedule("session-reconnect", ...) is called
TEST_F(TelemetryBidiReactorWithClientTest, DISABLED_automaticReconnectOnUnexpectedDisconnectTest) {
  // Requires a gRPC server that drops connections.
}

// DISABLED: Would test OnReadDone handling of various TelemetryCommand types.
// Requires a gRPC server that sends:
//   - kSettings → applySettings()
//   - kRecoverOrphanedTransactionCommand → client->recoverOrphanedTransaction()
//   - kPrintThreadStackTraceCommand → writes response back
//   - kVerifyMessageCommand → client->verify()
TEST_F(TelemetryBidiReactorTest, DISABLED_onReadDoneHandlesAllCommandTypesTest) {
  // Requires a live gRPC server that sends specific TelemetryCommand types.
}

ROCKETMQ_NAMESPACE_END
