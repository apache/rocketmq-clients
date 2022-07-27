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
using System.Threading.Tasks;
using System.Threading;

namespace examples
{

    class Foo
    {
        public int bar = 1;
    }
    class Program
    {

        static void RT(Action action, int seconds, CancellationToken token)
        {
            if (null == action)
            {
                return;
            }

            Task.Run(async () =>
            {
                while (!token.IsCancellationRequested)
                {
                    action();
                    await Task.Delay(TimeSpan.FromSeconds(seconds), token);
                }
            });
        }

        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");

            string accessKey = "key";
            string accessSecret = "secret";
            var credentials = new Org.Apache.Rocketmq.StaticCredentialsProvider(accessKey, accessSecret).getCredentials();
            bool expired = credentials.expired();

            int workerThreads;
            int completionPortThreads;
            ThreadPool.GetMaxThreads(out workerThreads, out completionPortThreads);
            Console.WriteLine($"Max: workerThread={workerThreads}, completionPortThreads={completionPortThreads}");
            ThreadPool.GetMinThreads(out workerThreads, out completionPortThreads);
            Console.WriteLine($"Min: workerThread={workerThreads}, completionPortThreads={completionPortThreads}");

            ThreadPool.QueueUserWorkItem((Object stateInfo) =>
            {
                Console.WriteLine("From ThreadPool");
                if (stateInfo is Foo)
                {
                    Console.WriteLine("Foo: bar=" + (stateInfo as Foo).bar);
                }
            }, new Foo());

            var cts = new CancellationTokenSource();
            RT(() =>
            {
                Console.WriteLine("Hello Again" + Thread.CurrentThread.Name);
            }, 1, cts.Token);
            cts.CancelAfter(3000);
            Console.ReadKey();
        }
    }
}
