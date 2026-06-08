# C++ Client Bug Fixes: Correctness, Thread Safety & Memory Management

> Date: 2026-06-08

This document describes a set of bug fixes addressing correctness, thread safety,
and memory management issues found in the C++ client SDK.

## Summary

| # | Category | Issue | Severity | Files |
|---|----------|-------|----------|-------|
| 1 | Correctness | `onVerifyMessage` switch fall-through | Critical | PushConsumerImpl.cpp |
| 2 | Correctness | `onVerifyMessage` callback never invoked | Critical | PushConsumerImpl.cpp |
| 3 | Correctness | `schedule()` empty implementation | Critical | ConsumeMessageServiceImpl.cpp |
| 4 | Correctness | `std::remove_if` result discarded | Major | SimpleConsumerImpl.cpp |
| 5 | Thread Safety | Static `task_id` data race | Major | SchedulerImpl.cpp |
| 6 | Thread Safety | `state_` read without lock | Major | TelemetryBidiReactor.cpp |
| 7 | Thread Safety | Use-after-free in `asyncCallback` | Critical | RpcClientImpl.cpp |
| 8 | Memory | `const_cast` to move from const ref | Major | SendCallback.h, ProducerImpl.cpp, FifoProducerPartition.cpp/.h, SendContextTest.cpp |

## 1. `onVerifyMessage` switch fall-through

**File:** `source/rocketmq/PushConsumerImpl.cpp`

The switch statement in `onVerifyMessage` lacked `break` statements, causing the
`SUCCESS` case to fall through into `FAILURE`. Every message verification was
reported as failed to the server regardless of actual consumption result.

```cpp
// Before
switch (result) {
  case ConsumeResult::SUCCESS: {
    cmd.mutable_status()->set_code(rmq::Code::OK);
    cmd.mutable_status()->set_message("OK");
  }  // fall-through
  case ConsumeResult::FAILURE: {
    cmd.mutable_status()->set_code(rmq::Code::FAILED_TO_CONSUME_MESSAGE);
    cmd.mutable_status()->set_message("Consume message failed");
  }
}
```

**Fix:** Added `break;` to both cases.

## 2. `onVerifyMessage` callback never invoked

**File:** `source/rocketmq/PushConsumerImpl.cpp`

The function accepts a callback `std::function<void(TelemetryCommand)> cb` and
constructs a `TelemetryCommand cmd` with the verification result, but never
calls `cb(cmd)`. The verification result was silently discarded.

**Fix:** Added `cb(cmd);` at the end of the function, after all branches have
populated the command.

## 3. `schedule()` empty implementation

**File:** `source/rocketmq/ConsumeMessageServiceImpl.cpp`

The `schedule()` method had an empty body:

```cpp
void ConsumeMessageServiceImpl::schedule(
    std::shared_ptr<ConsumeTask> task, std::chrono::milliseconds delay) {
}
```

This method is called when a FIFO consumption attempt fails and needs to be
retried after a delay. With the empty body, failed consume tasks were silently
dropped, breaking FIFO retry semantics.

**Fix:** Implemented the method to schedule a delayed resubmission via
`SchedulerImpl`:

```cpp
void ConsumeMessageServiceImpl::schedule(
    std::shared_ptr<ConsumeTask> task, std::chrono::milliseconds delay) {
  auto consumer = consumer_.lock();
  if (!consumer) {
    return;
  }

  auto scheduler = consumer->manager()->getScheduler();
  if (!scheduler) {
    SPDLOG_WARN("Scheduler is not available, submitting task immediately");
    submit(task);
    return;
  }

  std::weak_ptr<ConsumeMessageServiceImpl> self(shared_from_this());
  scheduler->schedule(
      [self, task]() {
        auto svc = self.lock();
        if (!svc) {
          return;
        }
        svc->submit(task);
      },
      "consume-retry", delay, std::chrono::milliseconds(0));
}
```

Key design decisions:
- Uses `weak_ptr` for both the consumer and the service itself to avoid preventing
  shutdown during a pending retry.
- Falls back to immediate `submit()` if the scheduler is unavailable.
- Single-shot schedule (`interval = 0`), not periodic.

## 4. `std::remove_if` result discarded

**File:** `source/rocketmq/SimpleConsumerImpl.cpp`

`std::remove_if` does not erase elements from a container. It moves matching
elements to the end and returns an iterator to the new logical end. The return
value must be passed to `erase()` to actually remove them.

```cpp
// Before: elements moved but never erased
std::remove_if(assignments_.begin(), assignments_.end(),
    [&](const rmq::Assignment& e) { return e == item; });

// After: proper erase-remove idiom
assignments_.erase(
    std::remove_if(assignments_.begin(), assignments_.end(),
                   [&](const rmq::Assignment& e) { return e == item; }),
    assignments_.end());
```

Without this fix, queue assignment changes in `SimpleConsumer` were additive
only — removed assignments persisted in the vector, potentially causing
duplicate consumption.

## 5. Static `task_id` data race

**File:** `source/scheduler/SchedulerImpl.cpp`

The `schedule()` function used a function-local `static std::uint32_t task_id`
for generating unique task IDs. While `tasks_mtx_` (an instance-level mutex)
was held during the increment, the static variable is shared across all
`SchedulerImpl` instances. Concurrent calls from different instances constitute
a data race on the non-atomic static.

```cpp
// Before: static variable protected by instance-level lock
static std::uint32_t task_id = 0;
// ...
absl::MutexLock lk(&tasks_mtx_);
id = ++task_id;

// After: atomic increment, then take lock only for the map insertion
static std::atomic<std::uint32_t> task_id{0};
std::uint32_t id = ++task_id;
{
  absl::MutexLock lk(&tasks_mtx_);
  tasks_.insert({id, task});
}
```

## 6. `state_` read without lock

**File:** `source/client/TelemetryBidiReactor.cpp`

`tryWriteNext()` read `state_` while holding `writes_mtx_`, but `state_` is
modified under `state_mtx_` (a different mutex). Reading a variable protected by
mutex A while holding only mutex B is a data race.

The function also contained redundant logic: after checking `writes_.empty()`
and returning if true, it immediately checked `!writes_.empty()` again, and
re-checked `state_` a second time inside that block.

**Fix:** Restructured to acquire `state_mtx_` first for the state check (then
release it), then acquire `writes_mtx_` for the write operation. Removed the
redundant second emptiness check and second state check.

```cpp
void TelemetryBidiReactor::tryWriteNext() {
  {
    absl::MutexLock state_lk(&state_mtx_);
    if (StreamState::Ready != state_) {
      return;
    }
  }

  absl::MutexLock lk(&writes_mtx_);
  if (writes_.empty()) {
    return;
  }

  AddHold();
  StartWrite(&(writes_.front()));
}
```

Note: There is a TOCTOU window between releasing `state_mtx_` and calling
`StartWrite`. This is acceptable because gRPC's `StartWrite` is safe to call
on a closing stream — it will simply fail, and the error is handled in
`OnWriteDone`.

## 7. Use-after-free in `asyncCallback`

**File:** `source/client/RpcClientImpl.cpp`

When `ClientManager` has already destructed (`manager` is null), the code
deleted `invocation_context` but did not return. Execution continued to the
lambda capture (capturing the now-dangling pointer) and the `manager->submit()`
call (null pointer dereference).

```cpp
// Before
if (!manager) {
  SPDLOG_WARN("ClientManager has destructed. Response ignored");
  delete invocation_context;
}
// continues to use invocation_context and manager...

// After
if (!manager) {
  SPDLOG_WARN("ClientManager has destructed. Response ignored");
  delete invocation_context;
  return;
}
```

This also resolves the related `InvocationContext` dual-deletion concern: with
the early return, the deleted pointer is never captured by the lambda, so the
`onCompletion()` → `delete this` path cannot be reached for an already-freed
object.

## 8. `const_cast` to move from const reference

**Files:** `include/rocketmq/SendCallback.h`, `source/rocketmq/ProducerImpl.cpp`,
`source/rocketmq/FifoProducerPartition.cpp`, `source/rocketmq/include/FifoProducerPartition.h`,
`source/rocketmq/tests/SendContextTest.cpp`

`SendCallback` was defined as:

```cpp
using SendCallback = std::function<void(const std::error_code&, const SendReceipt&)>;
```

The synchronous `ProducerImpl::send()` needed to move fields out of the receipt
for efficiency, so it used `const_cast` to strip the const qualifier — undefined
behavior if the object is truly const.

**Fix:** Changed the callback signature from `const SendReceipt&` to
`SendReceipt&`. The receipt is always a mutable temporary created by the send
path, so there is no semantic reason for it to be const. Updated all call sites:

- `ProducerImpl::send()` — removed `const_cast`, moves directly from `receipt`
- `FifoProducerPartition::onComplete()` — parameter changed to `SendReceipt&`
- `FifoProducerPartition::trySend()` — lambda signature updated
- `SendContextTest` — test lambda updated

## Verification

- **CMake build:** All targets compiled successfully (library, tests, examples)
- **CMake tests:** 24/24 passed
- **Bazel build:** All library and test targets compiled successfully (3387 actions)
- **Bazel tests:** 25/25 passed
- **Runtime validation:** Producer (sync + async), PushConsumer, and SimpleConsumer
  examples tested against a live RocketMQ 5.x instance — message send, receive,
  ack, and invisible duration change all work correctly

---

# C++ 客户端缺陷修复：正确性、线程安全与内存管理

> 日期：2026-06-08

本文档记录了 C++ 客户端 SDK 中一组正确性、线程安全和内存管理缺陷的修复内容。

## 概览

| # | 分类 | 问题 | 严重程度 | 涉及文件 |
|---|------|------|----------|----------|
| 1 | 正确性 | `onVerifyMessage` switch 穿透 | 严重 | PushConsumerImpl.cpp |
| 2 | 正确性 | `onVerifyMessage` 回调未调用 | 严重 | PushConsumerImpl.cpp |
| 3 | 正确性 | `schedule()` 方法体为空 | 严重 | ConsumeMessageServiceImpl.cpp |
| 4 | 正确性 | `std::remove_if` 返回值被丢弃 | 重要 | SimpleConsumerImpl.cpp |
| 5 | 线程安全 | 静态 `task_id` 数据竞争 | 重要 | SchedulerImpl.cpp |
| 6 | 线程安全 | `state_` 无锁读取 | 重要 | TelemetryBidiReactor.cpp |
| 7 | 线程安全 | `asyncCallback` 中 use-after-free | 严重 | RpcClientImpl.cpp |
| 8 | 内存管理 | 通过 `const_cast` 从 const 引用 move | 重要 | SendCallback.h、ProducerImpl.cpp、FifoProducerPartition.cpp/.h、SendContextTest.cpp |

## 1. `onVerifyMessage` switch 穿透

**文件：** `source/rocketmq/PushConsumerImpl.cpp`

`onVerifyMessage` 中的 switch 语句缺少 `break`，导致 `SUCCESS` 分支穿透到
`FAILURE` 分支。无论消费结果如何，所有消息验证均被报告为失败。

```cpp
// 修复前
switch (result) {
  case ConsumeResult::SUCCESS: {
    cmd.mutable_status()->set_code(rmq::Code::OK);
    cmd.mutable_status()->set_message("OK");
  }  // 穿透到 FAILURE
  case ConsumeResult::FAILURE: {
    cmd.mutable_status()->set_code(rmq::Code::FAILED_TO_CONSUME_MESSAGE);
    cmd.mutable_status()->set_message("Consume message failed");
  }
}
```

**修复：** 为两个 case 添加 `break;`。

## 2. `onVerifyMessage` 回调未调用

**文件：** `source/rocketmq/PushConsumerImpl.cpp`

该函数接受回调参数 `std::function<void(TelemetryCommand)> cb`，内部构造了
`TelemetryCommand cmd` 并填充验证结果，但从未调用 `cb(cmd)`。验证结果被静默丢弃，
Server 端的消息验证机制完全失效。

**修复：** 在函数末尾所有分支完成 cmd 填充后，添加 `cb(cmd);`。

## 3. `schedule()` 方法体为空

**文件：** `source/rocketmq/ConsumeMessageServiceImpl.cpp`

`schedule()` 方法体为空：

```cpp
void ConsumeMessageServiceImpl::schedule(
    std::shared_ptr<ConsumeTask> task, std::chrono::milliseconds delay) {
}
```

该方法在 FIFO 消费失败后被调用，用于延迟重试。方法体为空意味着失败的消费任务被静默
丢弃，FIFO 重试语义完全失效。

**修复：** 实现延迟重新提交逻辑，通过 `SchedulerImpl` 调度延迟回调：

```cpp
void ConsumeMessageServiceImpl::schedule(
    std::shared_ptr<ConsumeTask> task, std::chrono::milliseconds delay) {
  auto consumer = consumer_.lock();
  if (!consumer) {
    return;
  }

  auto scheduler = consumer->manager()->getScheduler();
  if (!scheduler) {
    SPDLOG_WARN("Scheduler is not available, submitting task immediately");
    submit(task);
    return;
  }

  std::weak_ptr<ConsumeMessageServiceImpl> self(shared_from_this());
  scheduler->schedule(
      [self, task]() {
        auto svc = self.lock();
        if (!svc) {
          return;
        }
        svc->submit(task);
      },
      "consume-retry", delay, std::chrono::milliseconds(0));
}
```

关键设计考量：
- 对 consumer 和 service 自身均使用 `weak_ptr`，避免在待重试期间阻止正常关闭。
- 调度器不可用时降级为立即 `submit()`。
- 一次性调度（`interval = 0`），非周期性。

## 4. `std::remove_if` 返回值被丢弃

**文件：** `source/rocketmq/SimpleConsumerImpl.cpp`

`std::remove_if` 不会从容器中删除元素。它将匹配元素移至末尾并返回新的逻辑结尾
迭代器，必须将该返回值传给 `erase()` 才能真正删除。

```cpp
// 修复前：元素被移动但未删除
std::remove_if(assignments_.begin(), assignments_.end(),
    [&](const rmq::Assignment& e) { return e == item; });

// 修复后：标准 erase-remove 惯用法
assignments_.erase(
    std::remove_if(assignments_.begin(), assignments_.end(),
                   [&](const rmq::Assignment& e) { return e == item; }),
    assignments_.end());
```

修复前，`SimpleConsumer` 的队列分配变更只增不减——被移除的 assignment 仍留在
vector 中，可能导致重复消费。

## 5. 静态 `task_id` 数据竞争

**文件：** `source/scheduler/SchedulerImpl.cpp`

`schedule()` 使用函数级 `static std::uint32_t task_id` 生成唯一任务 ID。虽然
递增时持有 `tasks_mtx_`（实例级互斥锁），但该静态变量跨所有 `SchedulerImpl` 实例
共享。不同实例的并发调用构成对非原子静态变量的数据竞争。

```cpp
// 修复前：静态变量由实例级锁保护
static std::uint32_t task_id = 0;
// ...
absl::MutexLock lk(&tasks_mtx_);
id = ++task_id;

// 修复后：原子递增，仅在 map 插入时加锁
static std::atomic<std::uint32_t> task_id{0};
std::uint32_t id = ++task_id;
{
  absl::MutexLock lk(&tasks_mtx_);
  tasks_.insert({id, task});
}
```

## 6. `state_` 无锁读取

**文件：** `source/client/TelemetryBidiReactor.cpp`

`tryWriteNext()` 在持有 `writes_mtx_` 时读取 `state_`，但 `state_` 由
`state_mtx_`（另一把锁）保护。持有互斥锁 A 读取由互斥锁 B 保护的变量构成数据竞争。

原函数还包含冗余逻辑：检查 `writes_.empty()` 并在为空时返回后，紧接着又检查
`!writes_.empty()`，并在该分支内再次检查 `state_`。

**修复：** 重构为先获取 `state_mtx_` 检查状态（然后释放），再获取 `writes_mtx_`
执行写操作。移除冗余的二次空检查和二次状态检查。

```cpp
void TelemetryBidiReactor::tryWriteNext() {
  {
    absl::MutexLock state_lk(&state_mtx_);
    if (StreamState::Ready != state_) {
      return;
    }
  }

  absl::MutexLock lk(&writes_mtx_);
  if (writes_.empty()) {
    return;
  }

  AddHold();
  StartWrite(&(writes_.front()));
}
```

说明：释放 `state_mtx_` 到调用 `StartWrite` 之间存在 TOCTOU 窗口。这是可接受的，
因为 gRPC 的 `StartWrite` 在流关闭时调用是安全的——只是会失败，错误在 `OnWriteDone`
中处理。

## 7. `asyncCallback` 中 use-after-free

**文件：** `source/client/RpcClientImpl.cpp`

当 `ClientManager` 已析构（`manager` 为 null）时，代码删除了 `invocation_context`
但未返回。执行继续到 lambda 捕获（捕获了已悬空的指针）和 `manager->submit()` 调用
（空指针解引用）。

```cpp
// 修复前
if (!manager) {
  SPDLOG_WARN("ClientManager has destructed. Response ignored");
  delete invocation_context;
}
// 继续使用 invocation_context 和 manager...

// 修复后
if (!manager) {
  SPDLOG_WARN("ClientManager has destructed. Response ignored");
  delete invocation_context;
  return;
}
```

此修复同时解决了 `InvocationContext` 双重删除的关联问题：有了提前返回，已删除的指针
不会被 lambda 捕获，因此 `onCompletion()` → `delete this` 路径不可能在已释放的
对象上触发。

## 8. 通过 `const_cast` 从 const 引用 move

**文件：** `include/rocketmq/SendCallback.h`、`source/rocketmq/ProducerImpl.cpp`、
`source/rocketmq/FifoProducerPartition.cpp`、`source/rocketmq/include/FifoProducerPartition.h`、
`source/rocketmq/tests/SendContextTest.cpp`

`SendCallback` 原定义为：

```cpp
using SendCallback = std::function<void(const std::error_code&, const SendReceipt&)>;
```

同步 `ProducerImpl::send()` 需要从 receipt 中 move 字段以提高效率，因此使用
`const_cast` 去除 const 限定符——如果对象确实是 const 的，这是未定义行为。

**修复：** 将回调签名从 `const SendReceipt&` 改为 `SendReceipt&`。Receipt 始终是
发送路径中创建的可变临时对象，语义上没有理由要求 const。更新了所有调用点：

- `ProducerImpl::send()` — 移除 `const_cast`，直接从 `receipt` move
- `FifoProducerPartition::onComplete()` — 参数改为 `SendReceipt&`
- `FifoProducerPartition::trySend()` — lambda 签名更新
- `SendContextTest` — 测试 lambda 更新

## 验证

- **CMake 构建：** 所有目标编译成功（库、测试、示例）
- **CMake 测试：** 24/24 通过
- **Bazel 构建：** 所有库和测试目标编译成功（3387 个 action）
- **Bazel 测试：** 25/25 通过
- **运行时验证：** Producer（同步 + 异步）、PushConsumer、SimpleConsumer 示例在
  RocketMQ 5.x 实例上测试通过——消息发送、接收、ACK、不可见时间变更均工作正常
