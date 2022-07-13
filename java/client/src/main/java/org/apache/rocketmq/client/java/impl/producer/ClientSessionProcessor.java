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

package org.apache.rocketmq.client.java.impl.producer;

import apache.rocketmq.v2.PrintThreadStackTraceCommand;
import apache.rocketmq.v2.RecoverOrphanedTransactionCommand;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.TelemetryCommand;
import apache.rocketmq.v2.VerifyMessageCommand;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.stub.StreamObserver;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.java.route.Endpoints;

public interface ClientSessionProcessor {
    ListenableFuture<Void> register();

    String clientId();

    TelemetryCommand getSettingsCommand();

    StreamObserver<TelemetryCommand> telemetry(Endpoints endpoints, StreamObserver<TelemetryCommand> observer)
        throws ClientException;

    void onSettingsCommand(Endpoints endpoints, Settings settings);

    void onRecoverOrphanedTransactionCommand(Endpoints endpoints, RecoverOrphanedTransactionCommand command);

    void onVerifyMessageCommand(Endpoints endpoints, VerifyMessageCommand command);

    void onPrintThreadStackTraceCommand(Endpoints endpoints, PrintThreadStackTraceCommand command);
}
