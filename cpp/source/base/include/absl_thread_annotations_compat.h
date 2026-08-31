/**
 * @file absl_thread_annotations_compat.h
 * @brief absl 旧版线程注解宏 → 新版 ABSL_ 前缀宏兼容层（RocketMQ vcpkg 编译 patch）
 *
 * RocketMQ cpp-5.1.1 源码使用旧版 absl 宏（LOCKS_EXCLUDED/GUARDED_BY 等），
 * absl lts_20230718+ 已改名为 ABSL_LOCKS_EXCLUDED/ABSL_GUARDED_BY。
 * 本头文件在 include absl/base/thread_annotations.h 之后引入，补齐旧名宏。
 */
#pragma once

#include "absl/base/thread_annotations.h"

#ifndef LOCKS_EXCLUDED
#define LOCKS_EXCLUDED(...) ABSL_LOCKS_EXCLUDED(__VA_ARGS__)
#endif

#ifndef GUARDED_BY
#define GUARDED_BY(x) ABSL_GUARDED_BY(x)
#endif

#ifndef PT_GUARDED_BY
#define PT_GUARDED_BY(x) ABSL_PT_GUARDED_BY(x)
#endif

#ifndef LOCKS_SHARED
#define LOCKS_SHARED ABSL_LOCKS_SHARED
#endif

#ifndef EXCLUSIVE_LOCKS_REQUIRED
#define EXCLUSIVE_LOCKS_REQUIRED(...) ABSL_EXCLUSIVE_LOCKS_REQUIRED(__VA_ARGS__)
#endif

#ifndef SHARED_LOCKS_REQUIRED
#define SHARED_LOCKS_REQUIRED(...) ABSL_SHARED_LOCKS_REQUIRED(__VA_ARGS__)
#endif

#ifndef ACQUIRED_BEFORE
#define ACQUIRED_BEFORE(...) ABSL_ACQUIRED_BEFORE(__VA_ARGS__)
#endif

#ifndef ACQUIRED_AFTER
#define ACQUIRED_AFTER(...) ABSL_ACQUIRED_AFTER(__VA_ARGS__)
#endif
