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
using Apache.Rocketmq.V2;
using Google.Protobuf.WellKnownTypes;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.Rocketmq;

namespace tests
{
    [TestClass]
    public class ExponentialBackoffRetryPolicyTest
    {
        private TimeSpan initialBackoff = TimeSpan.FromMilliseconds(5);
        private TimeSpan maxBackoff = TimeSpan.FromSeconds(1);
        private float backoffMultiplier = 5;
        private int maxAttempts = 3;

        [TestMethod]
        public void TestNextAttemptDelayForImmediatelyRetryPolicy()
        {
            var retryPolicy = ExponentialBackoffRetryPolicy.ImmediatelyRetryPolicy(3);
            for (int i = 1; i <= 4; i++)
            {
                Assert.AreEqual(TimeSpan.Zero, retryPolicy.GetNextAttemptDelay(i));
            }
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestGetNextAttemptDelayWithIllegalAttempt()
        {
            var retryPolicy = new ExponentialBackoffRetryPolicy(maxAttempts, initialBackoff, maxBackoff, backoffMultiplier);
            retryPolicy.GetNextAttemptDelay(0);
        }

        [TestMethod]
        public void TestGetNextAttemptDelay()
        {
            var retryPolicy = new ExponentialBackoffRetryPolicy(maxAttempts, initialBackoff, maxBackoff, backoffMultiplier);
            Assert.AreEqual(TimeSpan.FromMilliseconds(5), retryPolicy.GetNextAttemptDelay(1));
            Assert.AreEqual(TimeSpan.FromMilliseconds(25), retryPolicy.GetNextAttemptDelay(2));
            Assert.AreEqual(TimeSpan.FromMilliseconds(125), retryPolicy.GetNextAttemptDelay(3));
            Assert.AreEqual(TimeSpan.FromMilliseconds(625), retryPolicy.GetNextAttemptDelay(4));
            Assert.AreEqual(TimeSpan.FromSeconds(1), retryPolicy.GetNextAttemptDelay(5));
        }

        [TestMethod]
        public void TestFromProtobuf()
        {
            var initialBackoffProto = Duration.FromTimeSpan(initialBackoff);
            var maxBackoffProto = Duration.FromTimeSpan(maxBackoff);

            var exponentialBackoff = new ExponentialBackoff
            {
                Initial = initialBackoffProto,
                Max = maxBackoffProto,
                Multiplier = backoffMultiplier
            };
            var retryPolicyProto = new RetryPolicy
            {
                MaxAttempts = maxAttempts,
                ExponentialBackoff = exponentialBackoff
            };

            var policy = ExponentialBackoffRetryPolicy.FromProtobuf(retryPolicyProto);

            Assert.AreEqual(maxAttempts, policy.GetMaxAttempts());
            Assert.AreEqual(initialBackoff, policy.InitialBackoff);
            Assert.AreEqual(maxBackoff, policy.MaxBackoff);
            Assert.AreEqual(backoffMultiplier, policy.BackoffMultiplier);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestFromProtobufWithoutExponentialBackoff()
        {
            var customizedBackoff = new CustomizedBackoff();
            var retryPolicyProto = new RetryPolicy
            {
                MaxAttempts = maxAttempts,
                CustomizedBackoff = customizedBackoff
            };
            ExponentialBackoffRetryPolicy.FromProtobuf(retryPolicyProto);
        }

        [TestMethod]
        public void TestToProtobuf()
        {
            var retryPolicy = new ExponentialBackoffRetryPolicy(maxAttempts, initialBackoff, maxBackoff, backoffMultiplier);
            var retryPolicyProto = retryPolicy.ToProtobuf();

            Assert.IsNotNull(retryPolicyProto.ExponentialBackoff);

            var exponentialBackoff = retryPolicyProto.ExponentialBackoff;
            var initialBackoffProto = Duration.FromTimeSpan(initialBackoff);
            var maxBackoffProto = Duration.FromTimeSpan(maxBackoff);

            Assert.AreEqual(exponentialBackoff.Initial, initialBackoffProto);
            Assert.AreEqual(exponentialBackoff.Max, maxBackoffProto);
            Assert.AreEqual(exponentialBackoff.Multiplier, backoffMultiplier);
            Assert.AreEqual(retryPolicyProto.MaxAttempts, maxAttempts);
        }

        [TestMethod]
        public void TestInheritBackoff()
        {
            var retryPolicy = new ExponentialBackoffRetryPolicy(maxAttempts, initialBackoff, maxBackoff, backoffMultiplier);

            var initialBackoffProto = TimeSpan.FromMilliseconds(10);
            var maxBackoffProto = TimeSpan.FromSeconds(3);
            double backoffMultiplierProto = 10;

            var exponentialBackoff = new ExponentialBackoff
            {
                Initial = Duration.FromTimeSpan(initialBackoffProto),
                Max = Duration.FromTimeSpan(maxBackoffProto),
                Multiplier = (float)backoffMultiplierProto
            };
            var retryPolicyProto = new RetryPolicy
            {
                ExponentialBackoff = exponentialBackoff
            };

            var inheritedRetryPolicy = retryPolicy.InheritBackoff(retryPolicyProto);

            Assert.IsInstanceOfType(inheritedRetryPolicy, typeof(ExponentialBackoffRetryPolicy));

            var exponentialBackoffRetryPolicy = (ExponentialBackoffRetryPolicy)inheritedRetryPolicy;

            Assert.AreEqual(initialBackoffProto, exponentialBackoffRetryPolicy.InitialBackoff);
            Assert.AreEqual(maxBackoffProto, exponentialBackoffRetryPolicy.MaxBackoff);
            Assert.AreEqual(backoffMultiplierProto, exponentialBackoffRetryPolicy.BackoffMultiplier);
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void TestInheritBackoffWithoutExponentialBackoff()
        {
            var customizedBackoff = new CustomizedBackoff();
            var retryPolicyProto = new RetryPolicy
            {
                MaxAttempts = maxAttempts,
                CustomizedBackoff = customizedBackoff
            };

            var exponentialBackoffRetryPolicy = new ExponentialBackoffRetryPolicy(maxAttempts, initialBackoff, maxBackoff, backoffMultiplier);
            exponentialBackoffRetryPolicy.InheritBackoff(retryPolicyProto);
        }
    }

}