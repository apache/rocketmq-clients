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

using System.Threading;
using Grpc.Core;
using Proto = Apache.Rocketmq.V2;

namespace Org.Apache.Rocketmq
{
    public interface IClient
    {
        CancellationTokenSource TelemetryCts();

        ClientConfig GetClientConfig();

        Proto.Settings GetSettings();

        /// <summary>
        /// Get the identifier of current client. 
        /// </summary>
        /// <returns>Client identifier.</returns>
        string GetClientId();

        /// <summary>
        /// This method will be triggered when client settings is received from remote endpoints.
        /// </summary>
        /// <param name="endpoints"></param>
        /// <param name="settings"></param>
        void OnSettingsCommand(Endpoints endpoints, Proto.Settings settings);

        /// <summary>
        /// This method will be triggered when orphaned transaction need to be recovered.
        /// </summary>
        /// <param name="endpoints">Remote endpoints.</param>
        /// <param name="command">Command of orphaned transaction recovery.</param>
        void OnRecoverOrphanedTransactionCommand(Endpoints endpoints, Proto.RecoverOrphanedTransactionCommand command);

        /// <summary>
        /// This method will be triggered when message verification command is received.
        /// </summary>
        /// <param name="endpoints">Remote endpoints.</param>
        /// <param name="command">Command of message verification.</param>
        void OnVerifyMessageCommand(Endpoints endpoints, Proto.VerifyMessageCommand command);

        /// <summary>
        /// This method will be triggered when thread stack trace command is received.
        /// </summary>
        /// <param name="endpoints">Remote endpoints.</param>
        /// <param name="command">Command of printing thread stack trace.</param>
        void OnPrintThreadStackTraceCommand(Endpoints endpoints, Proto.PrintThreadStackTraceCommand command);

        Metadata Sign();
    }
}