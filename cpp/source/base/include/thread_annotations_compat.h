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
#pragma once

// Compatibility header for thread safety annotation macros.
//
// Newer versions of abseil (>= 20230125) renamed the thread safety annotation
// macros with an ABSL_ prefix (e.g. GUARDED_BY -> ABSL_GUARDED_BY).
// This header maps the legacy names used throughout the codebase to the new
// ABSL_-prefixed names, so existing code continues to compile without changes.
//
// This file is force-included into every translation unit via the compiler
// -include option (configured in CMakeLists.txt / BUILD.bazel).

#include "absl/base/thread_annotations.h"

// ABSL_GUARDED_BY
#ifndef GUARDED_BY
#define GUARDED_BY(x) ABSL_GUARDED_BY(x)
#endif

// ABSL_PT_GUARDED_BY
#ifndef PT_GUARDED_BY
#define PT_GUARDED_BY(x) ABSL_PT_GUARDED_BY(x)
#endif

// ABSL_LOCKS_EXCLUDED
#ifndef LOCKS_EXCLUDED
#define LOCKS_EXCLUDED(...) ABSL_LOCKS_EXCLUDED(__VA_ARGS__)
#endif

// ABSL_EXCLUSIVE_LOCKS_REQUIRED
#ifndef EXCLUSIVE_LOCKS_REQUIRED
#define EXCLUSIVE_LOCKS_REQUIRED(...) ABSL_EXCLUSIVE_LOCKS_REQUIRED(__VA_ARGS__)
#endif

// ABSL_SHARED_LOCKS_REQUIRED
#ifndef SHARED_LOCKS_REQUIRED
#define SHARED_LOCKS_REQUIRED(...) ABSL_SHARED_LOCKS_REQUIRED(__VA_ARGS__)
#endif

// ABSL_LOCK_RETURNED
#ifndef LOCK_RETURNED
#define LOCK_RETURNED(x) ABSL_LOCK_RETURNED(x)
#endif

// ABSL_LOCKS_SHARED
#ifndef LOCKS_SHARED
#define LOCKS_SHARED ABSL_LOCKS_SHARED
#endif

// ABSL_LOCKS_EXCLUSIVE
#ifndef LOCKS_EXCLUSIVE
#define LOCKS_EXCLUSIVE ABSL_LOCKS_EXCLUSIVE
#endif

// ABSL_ACQUIRED_BEFORE
#ifndef ACQUIRED_BEFORE
#define ACQUIRED_BEFORE(...) ABSL_ACQUIRED_BEFORE(__VA_ARGS__)
#endif

// ABSL_ACQUIRED_AFTER
#ifndef ACQUIRED_AFTER
#define ACQUIRED_AFTER(...) ABSL_ACQUIRED_AFTER(__VA_ARGS__)
#endif

// ABSL_NO_THREAD_SAFETY_ANALYSIS
#ifndef NO_THREAD_SAFETY_ANALYSIS
#define NO_THREAD_SAFETY_ANALYSIS ABSL_NO_THREAD_SAFETY_ANALYSIS
#endif

// ABSL_ASSERT_EXCLUSIVE_LOCK
#ifndef ASSERT_EXCLUSIVE_LOCK
#define ASSERT_EXCLUSIVE_LOCK(...) ABSL_ASSERT_EXCLUSIVE_LOCK(__VA_ARGS__)
#endif

// ABSL_ASSERT_SHARED_LOCK
#ifndef ASSERT_SHARED_LOCK
#define ASSERT_SHARED_LOCK(...) ABSL_ASSERT_SHARED_LOCK(__VA_ARGS__)
#endif

// ABSL_EXCLUSIVE_LOCK_FUNCTION
#ifndef EXCLUSIVE_LOCK_FUNCTION
#define EXCLUSIVE_LOCK_FUNCTION(...) ABSL_EXCLUSIVE_LOCK_FUNCTION(__VA_ARGS__)
#endif

// ABSL_SHARED_LOCK_FUNCTION
#ifndef SHARED_LOCK_FUNCTION
#define SHARED_LOCK_FUNCTION(...) ABSL_SHARED_LOCK_FUNCTION(__VA_ARGS__)
#endif

// ABSL_UNLOCK_FUNCTION
#ifndef UNLOCK_FUNCTION
#define UNLOCK_FUNCTION(...) ABSL_UNLOCK_FUNCTION(__VA_ARGS__)
#endif

// ABSL_EXCLUSIVE_TRYLOCK_FUNCTION
#ifndef EXCLUSIVE_TRYLOCK_FUNCTION
#define EXCLUSIVE_TRYLOCK_FUNCTION(...) ABSL_EXCLUSIVE_TRYLOCK_FUNCTION(__VA_ARGS__)
#endif

// ABSL_SHARED_TRYLOCK_FUNCTION
#ifndef SHARED_TRYLOCK_FUNCTION
#define SHARED_TRYLOCK_FUNCTION(...) ABSL_SHARED_TRYLOCK_FUNCTION(__VA_ARGS__)
#endif

// ABSL_SCOPED_LOCKABLE
#ifndef SCOPED_LOCKABLE
#define SCOPED_LOCKABLE ABSL_SCOPED_LOCKABLE
#endif
