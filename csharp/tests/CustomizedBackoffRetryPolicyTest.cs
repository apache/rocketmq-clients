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

using System;
using System.Collections.Generic;
using System.Linq;
using Apache.Rocketmq.V2;
using Google.Protobuf.WellKnownTypes;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.Rocketmq;

namespace tests
{
    [TestClass]
    public class CustomizedBackoffRetryPolicyTest
    {
        [TestMethod]
        public void TestConstructWithValidDurationsAndMaxAttempts()
        {
            var durations = new List<TimeSpan> { TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2) };
            var maxAttempts = 3;
            var policy = new CustomizedBackoffRetryPolicy(durations, maxAttempts);

            Assert.AreEqual(maxAttempts, policy.GetMaxAttempts());
            CollectionAssert.AreEqual(durations, policy.GetDurations());
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestConstructWithEmptyDurations()
        {
            new CustomizedBackoffRetryPolicy(new List<TimeSpan>(), 3);
        }

        [TestMethod]
        public void TestGetNextAttemptDelayWithValidAttempts()
        {
            var durations = new List<TimeSpan> { TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(5) };
            var policy = new CustomizedBackoffRetryPolicy(durations, 5);

            Assert.AreEqual(TimeSpan.FromSeconds(1), policy.GetNextAttemptDelay(1));
            Assert.AreEqual(TimeSpan.FromSeconds(3), policy.GetNextAttemptDelay(2));
            Assert.AreEqual(TimeSpan.FromSeconds(5), policy.GetNextAttemptDelay(3));
            Assert.AreEqual(TimeSpan.FromSeconds(5), policy.GetNextAttemptDelay(4)); // Should inherit the last duration
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestGetNextAttemptDelayWithInvalidAttempt()
        {
            var policy = new CustomizedBackoffRetryPolicy(new List<TimeSpan> { TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2) }, 3);
            policy.GetNextAttemptDelay(0);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestGetNextAttemptDelayWithNegativeAttempt()
        {
            var policy = new CustomizedBackoffRetryPolicy(new List<TimeSpan> { TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2) }, 3);
            policy.GetNextAttemptDelay(-1);
        }

        [TestMethod]
        public void TestFromProtobufWithValidRetryPolicy()
        {
            var protoDurations = new List<Duration>
            {
                Duration.FromTimeSpan(TimeSpan.FromSeconds(1)),
                Duration.FromTimeSpan(TimeSpan.FromSeconds(2))
            };
            var protoRetryPolicy = new RetryPolicy
            {
                MaxAttempts = 3,
                CustomizedBackoff = new CustomizedBackoff { Next = { protoDurations } },
            };
            var policy = CustomizedBackoffRetryPolicy.FromProtobuf(protoRetryPolicy);

            Assert.AreEqual(3, policy.GetMaxAttempts());
            Assert.AreEqual(protoDurations.Count, policy.GetDurations().Count);
            CollectionAssert.AreEqual(protoDurations.Select(d => d.ToTimeSpan()).ToList(), policy.GetDurations());
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestFromProtobufWithInvalidRetryPolicy()
        {
            var retryPolicy = new RetryPolicy
            {
                MaxAttempts = 3,
                ExponentialBackoff = new ExponentialBackoff
                {
                    Initial = Duration.FromTimeSpan(TimeSpan.FromSeconds(1)),
                    Max = Duration.FromTimeSpan(TimeSpan.FromSeconds(1)),
                    Multiplier = 1.0f
                }
            };
            CustomizedBackoffRetryPolicy.FromProtobuf(retryPolicy);
        }

        [TestMethod]
        public void ToProtobuf_ShouldReturnCorrectProtobuf()
        {
            var durations = new List<TimeSpan> { TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2) };
            var maxAttempts = 3;
            var policy = new CustomizedBackoffRetryPolicy(durations, maxAttempts);
            var proto = policy.ToProtobuf();

            Assert.AreEqual(maxAttempts, proto.MaxAttempts);
            CollectionAssert.AreEqual(durations, proto.CustomizedBackoff.Next.Select(d => d.ToTimeSpan()).ToList());
        }

        [TestMethod]
        public void TestInheritBackoffWithValidCustomizedBackoffPolicy()
        {
            var originalDurations = new List<TimeSpan> { TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(3) };
            var newDurations = new List<Duration>
            {
                Duration.FromTimeSpan(TimeSpan.FromSeconds(2)),
                Duration.FromTimeSpan(TimeSpan.FromSeconds(4))
            };
            var backoff = new CustomizedBackoff { Next = { newDurations } };
            var retryPolicy = new RetryPolicy
            {
                MaxAttempts = 5,
                CustomizedBackoff = backoff,
            };
            var policy = new CustomizedBackoffRetryPolicy(originalDurations, 5);
            var inheritedPolicy = policy.InheritBackoff(retryPolicy);
            Assert.IsTrue(inheritedPolicy is CustomizedBackoffRetryPolicy);
            var customizedBackoffRetryPolicy = (CustomizedBackoffRetryPolicy)inheritedPolicy;
            Assert.AreEqual(policy.GetMaxAttempts(), inheritedPolicy.GetMaxAttempts());
            var inheritedDurations = customizedBackoffRetryPolicy.GetDurations();
            Assert.AreEqual(newDurations.Count, inheritedDurations.Count);
            for (var i = 0; i < newDurations.Count; i++)
            {
                Assert.AreEqual(newDurations[i].ToTimeSpan(), inheritedDurations[i]);
            }
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void TestInheritBackoffWithInvalidPolicy()
        {
            var policy = new CustomizedBackoffRetryPolicy(new List<TimeSpan>
            {
                TimeSpan.FromSeconds(3),
                TimeSpan.FromSeconds(2),
                TimeSpan.FromSeconds(1)
            }, 3);
            var retryPolicy = new RetryPolicy
            {
                ExponentialBackoff = new ExponentialBackoff()
            };
            policy.InheritBackoff(retryPolicy);
        }
    }
}