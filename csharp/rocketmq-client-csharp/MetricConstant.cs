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

using OpenTelemetry.Metrics;

namespace Org.Apache.Rocketmq
{
    public class MetricConstant
    {
        // Metric Name
        public const string SendCostTimeMetricName = "rocketmq_send_cost_time";
        public const string DeliveryLatencyMetricName = "rocketmq_delivery_latency";
        public const string AwaitTimeMetricName = "rocketmq_await_time";
        public const string ProcessTimeMetricName = "rocketmq_process_time";

        // Metric Label Name
        public const string Topic = "topic";
        public const string ClientId = "client_id";
        public const string ConsumerGroup = "consumer_group";
        public const string InvocationStatus = "invocation_status";

        // Metric Label Value
        public const string Success = "success";
        public const string Failure = "failure";

        public readonly ExplicitBucketHistogramConfiguration SendCostTimeBucket;
        public readonly ExplicitBucketHistogramConfiguration DeliveryLatencyBucket;
        public readonly ExplicitBucketHistogramConfiguration AwaitTimeBucket;
        public readonly ExplicitBucketHistogramConfiguration ProcessTimeBucket;

        public static readonly MetricConstant Instance = new MetricConstant();

        private MetricConstant()
        {
            SendCostTimeBucket = new ExplicitBucketHistogramConfiguration
            {
                Boundaries = new[] { 1.0, 5.0, 10.0, 20.0, 50.0, 200.0, 500.0 }
            };

            DeliveryLatencyBucket = new ExplicitBucketHistogramConfiguration
            {
                Boundaries = new[] { 1.0, 5.0, 10.0, 20.0, 50.0, 200.0, 500.0 }
            };

            AwaitTimeBucket = new ExplicitBucketHistogramConfiguration
            {
                Boundaries = new[] { 1.0, 5.0, 20.0, 100.0, 1000.0, 5 * 1000.0, 10 * 1000.0 }
            };

            ProcessTimeBucket = new ExplicitBucketHistogramConfiguration
            {
                Boundaries = new[] { 1.0, 5.0, 10.0, 100.0, 1000.0, 10 * 1000.0, 60 * 1000.0 }
            };
        }
    }
}