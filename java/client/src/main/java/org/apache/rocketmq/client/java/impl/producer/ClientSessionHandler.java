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
import java.util.concurrent.ScheduledExecutorService;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.java.route.Endpoints;

public interface ClientSessionHandler {
    /**
     * Returns {@code true} if this handler is running.
     */
    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    boolean isRunning();

    /**
     * Returns the shared scheduler.
     *
     * @return shared scheduler.
     */
    ScheduledExecutorService getScheduler();

    /**
     * Returns {@code true} if the endpoints is deprecated.
     */
    boolean isEndpointsDeprecated(Endpoints endpoints);

    /**
     * Await the settings to be synchronized with the server.
     */
    ListenableFuture<Void> awaitSettingSynchronized();

    /**
     * Indicates the client identifier.
     *
     * @return client identifier.
     */
    String clientId();

    /**
     * Get the settings of client.
     *
     * @return settings command.
     */
    TelemetryCommand settingsCommand();

    /**
     * Establish telemetry session stream to server.
     *
     * @param endpoints request endpoints.
     * @param observer  response observer.
     * @return request observer.
     * @throws ClientException if failed to establish telemetry session stream.
     */
    StreamObserver<TelemetryCommand> telemetry(Endpoints endpoints, StreamObserver<TelemetryCommand> observer)
        throws ClientException;

    /**
     * Event processor for {@link VerifyMessageCommand}.
     */
    void onSettingsCommand(Endpoints endpoints, Settings settings);

    /**
     * Event processor for {@link RecoverOrphanedTransactionCommand}.
     */
    void onRecoverOrphanedTransactionCommand(Endpoints endpoints, RecoverOrphanedTransactionCommand command);

    /**
     * Event processor for {@link PrintThreadStackTraceCommand}.
     */
    void onVerifyMessageCommand(Endpoints endpoints, VerifyMessageCommand command);

    /**
     * Event processor for {@link TelemetryCommand}.
     */
    void onPrintThreadStackTraceCommand(Endpoints endpoints, PrintThreadStackTraceCommand command);
}
