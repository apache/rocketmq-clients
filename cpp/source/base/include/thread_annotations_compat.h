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

// Self-contained thread safety annotation macros.
//
// Defines ABSL_* macros directly via Clang __attribute__ (no dependency on
// absl/base/thread_annotations.h), then maps legacy names used throughout
// the codebase to the ABSL_-prefixed equivalents.
//
// This file is force-included into every translation unit via the compiler
// -include option (configured in CMakeLists.txt).

// ---------------------------------------------------------------------------
// Step 1: Define ABSL_* macros using Clang thread-safety attributes.
//         Non-Clang compilers get empty expansions.
// ---------------------------------------------------------------------------
#if defined(__clang__) && (!defined(SWIG))

#define ABSL_GUARDED_BY(x)                 __attribute__((guarded_by(x)))
#define ABSL_PT_GUARDED_BY(x)              __attribute__((pt_guarded_by(x)))
#define ABSL_LOCKS_EXCLUDED(...)            __attribute__((locks_excluded(__VA_ARGS__)))
#define ABSL_EXCLUSIVE_LOCKS_REQUIRED(...)  __attribute__((exclusive_locks_required(__VA_ARGS__)))
#define ABSL_SHARED_LOCKS_REQUIRED(...)     __attribute__((shared_locks_required(__VA_ARGS__)))
#define ABSL_LOCK_RETURNED(x)              __attribute__((lock_returned(x)))
#define ABSL_LOCKS_SHARED                  __attribute__((shared_lock_function))
#define ABSL_LOCKS_EXCLUSIVE               __attribute__((exclusive_lock_function))
#define ABSL_ACQUIRED_BEFORE(...)           __attribute__((acquired_before(__VA_ARGS__)))
#define ABSL_ACQUIRED_AFTER(...)            __attribute__((acquired_after(__VA_ARGS__)))
#define ABSL_NO_THREAD_SAFETY_ANALYSIS     __attribute__((no_thread_safety_analysis))

#define ABSL_ASSERT_EXCLUSIVE_LOCK(...)     __attribute__((assert_exclusive_lock(__VA_ARGS__)))
#define ABSL_ASSERT_SHARED_LOCK(...)        __attribute__((assert_shared_lock(__VA_ARGS__)))
#define ABSL_EXCLUSIVE_LOCK_FUNCTION(...)   __attribute__((exclusive_lock_function(__VA_ARGS__)))
#define ABSL_SHARED_LOCK_FUNCTION(...)      __attribute__((shared_lock_function(__VA_ARGS__)))
#define ABSL_UNLOCK_FUNCTION(...)           __attribute__((unlock_function(__VA_ARGS__)))
#define ABSL_EXCLUSIVE_TRYLOCK_FUNCTION(...) __attribute__((exclusive_trylock_function(__VA_ARGS__)))
#define ABSL_SHARED_TRYLOCK_FUNCTION(...)   __attribute__((shared_trylock_function(__VA_ARGS__)))
#define ABSL_SCOPED_LOCKABLE               __attribute__((scoped_lockable))

#else  // Non-Clang: all annotations expand to nothing.

#define ABSL_GUARDED_BY(x)
#define ABSL_PT_GUARDED_BY(x)
#define ABSL_LOCKS_EXCLUDED(...)
#define ABSL_EXCLUSIVE_LOCKS_REQUIRED(...)
#define ABSL_SHARED_LOCKS_REQUIRED(...)
#define ABSL_LOCK_RETURNED(x)
#define ABSL_LOCKS_SHARED
#define ABSL_LOCKS_EXCLUSIVE
#define ABSL_ACQUIRED_BEFORE(...)
#define ABSL_ACQUIRED_AFTER(...)
#define ABSL_NO_THREAD_SAFETY_ANALYSIS

#define ABSL_ASSERT_EXCLUSIVE_LOCK(...)
#define ABSL_ASSERT_SHARED_LOCK(...)
#define ABSL_EXCLUSIVE_LOCK_FUNCTION(...)
#define ABSL_SHARED_LOCK_FUNCTION(...)
#define ABSL_UNLOCK_FUNCTION(...)
#define ABSL_EXCLUSIVE_TRYLOCK_FUNCTION(...)
#define ABSL_SHARED_TRYLOCK_FUNCTION(...)
#define ABSL_SCOPED_LOCKABLE

#endif  // __clang__

// ---------------------------------------------------------------------------
// Step 2: Map legacy names (used in the codebase) to ABSL_-prefixed macros.
// ---------------------------------------------------------------------------
#ifndef GUARDED_BY
#define GUARDED_BY(x)                  ABSL_GUARDED_BY(x)
#endif

#ifndef PT_GUARDED_BY
#define PT_GUARDED_BY(x)               ABSL_PT_GUARDED_BY(x)
#endif

#ifndef LOCKS_EXCLUDED
#define LOCKS_EXCLUDED(...)             ABSL_LOCKS_EXCLUDED(__VA_ARGS__)
#endif

#ifndef EXCLUSIVE_LOCKS_REQUIRED
#define EXCLUSIVE_LOCKS_REQUIRED(...)   ABSL_EXCLUSIVE_LOCKS_REQUIRED(__VA_ARGS__)
#endif

#ifndef SHARED_LOCKS_REQUIRED
#define SHARED_LOCKS_REQUIRED(...)      ABSL_SHARED_LOCKS_REQUIRED(__VA_ARGS__)
#endif

#ifndef LOCK_RETURNED
#define LOCK_RETURNED(x)               ABSL_LOCK_RETURNED(x)
#endif

#ifndef LOCKS_SHARED
#define LOCKS_SHARED                   ABSL_LOCKS_SHARED
#endif

#ifndef LOCKS_EXCLUSIVE
#define LOCKS_EXCLUSIVE                ABSL_LOCKS_EXCLUSIVE
#endif

#ifndef ACQUIRED_BEFORE
#define ACQUIRED_BEFORE(...)            ABSL_ACQUIRED_BEFORE(__VA_ARGS__)
#endif

#ifndef ACQUIRED_AFTER
#define ACQUIRED_AFTER(...)             ABSL_ACQUIRED_AFTER(__VA_ARGS__)
#endif

#ifndef NO_THREAD_SAFETY_ANALYSIS
#define NO_THREAD_SAFETY_ANALYSIS      ABSL_NO_THREAD_SAFETY_ANALYSIS
#endif

#ifndef ASSERT_EXCLUSIVE_LOCK
#define ASSERT_EXCLUSIVE_LOCK(...)      ABSL_ASSERT_EXCLUSIVE_LOCK(__VA_ARGS__)
#endif

#ifndef ASSERT_SHARED_LOCK
#define ASSERT_SHARED_LOCK(...)         ABSL_ASSERT_SHARED_LOCK(__VA_ARGS__)
#endif

#ifndef EXCLUSIVE_LOCK_FUNCTION
#define EXCLUSIVE_LOCK_FUNCTION(...)    ABSL_EXCLUSIVE_LOCK_FUNCTION(__VA_ARGS__)
#endif

#ifndef SHARED_LOCK_FUNCTION
#define SHARED_LOCK_FUNCTION(...)       ABSL_SHARED_LOCK_FUNCTION(__VA_ARGS__)
#endif

#ifndef UNLOCK_FUNCTION
#define UNLOCK_FUNCTION(...)            ABSL_UNLOCK_FUNCTION(__VA_ARGS__)
#endif

#ifndef EXCLUSIVE_TRYLOCK_FUNCTION
#define EXCLUSIVE_TRYLOCK_FUNCTION(...) ABSL_EXCLUSIVE_TRYLOCK_FUNCTION(__VA_ARGS__)
#endif

#ifndef SHARED_TRYLOCK_FUNCTION
#define SHARED_TRYLOCK_FUNCTION(...)    ABSL_SHARED_TRYLOCK_FUNCTION(__VA_ARGS__)
#endif

#ifndef SCOPED_LOCKABLE
#define SCOPED_LOCKABLE                ABSL_SCOPED_LOCKABLE
#endif
