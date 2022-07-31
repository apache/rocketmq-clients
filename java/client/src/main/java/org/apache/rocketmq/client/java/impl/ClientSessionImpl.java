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

package org.apache.rocketmq.client.java.impl;

import apache.rocketmq.v2.PrintThreadStackTraceCommand;
import apache.rocketmq.v2.RecoverOrphanedTransactionCommand;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.TelemetryCommand;
import apache.rocketmq.v2.VerifyMessageCommand;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.java.impl.producer.ClientSessionHandler;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Telemetry session is constructed before first communication between client and remote route endpoints.
 */
public class ClientSessionImpl implements StreamObserver<TelemetryCommand> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientSessionImpl.class);
    private static final Duration REQUEST_OBSERVER_RENEW_BACKOFF_DELAY = Duration.ofSeconds(1);

    private final ClientSessionHandler sessionHandler;
    private final Endpoints endpoints;
    private volatile StreamObserver<TelemetryCommand> requestObserver;

    protected ClientSessionImpl(ClientSessionHandler sessionHandler, Endpoints endpoints) throws ClientException {
        this.sessionHandler = sessionHandler;
        this.endpoints = endpoints;
        this.requestObserver = sessionHandler.telemetry(endpoints, this);
    }

    private void renewRequestObserver() {
        try {
            if (sessionHandler.isEndpointsDeprecated(endpoints)) {
                LOGGER.info("Endpoints is deprecated, no longer to renew requestObserver, endpoints={}", endpoints);
                return;
            }
            this.requestObserver = sessionHandler.telemetry(endpoints, this);
        } catch (Throwable t) {
            LOGGER.error("Failed to renew requestObserver, attempt to renew later, endpoints={}, delay={}", endpoints,
                REQUEST_OBSERVER_RENEW_BACKOFF_DELAY, t);
            sessionHandler.getScheduler().schedule(this::renewRequestObserver,
                REQUEST_OBSERVER_RENEW_BACKOFF_DELAY.toNanos(), TimeUnit.NANOSECONDS);
            return;
        }
        syncSettings();
    }

    protected ListenableFuture<Void> syncSettingsSafely() {
        try {
            this.syncSettings();
            return sessionHandler.awaitSettingSynchronized();
        } catch (Throwable t) {
            return Futures.immediateFailedFuture(t);
        }
    }

    private void syncSettings() {
        final TelemetryCommand settings = sessionHandler.settingsCommand();
        fireWrite(settings);
    }

    /**
     * Release telemetry session.
     */
    public void release() {
        if (null == requestObserver) {
            return;
        }
        try {
            requestObserver.onCompleted();
        } catch (Throwable ignore) {
            // Ignore exception on purpose.
        }
    }

    void fireWrite(TelemetryCommand command) {
        if (null == requestObserver) {
            LOGGER.error("Request observer does not exist, ignore current command, endpoints={}, command={}",
                endpoints, command);
            return;
        }
        requestObserver.onNext(command);
    }

    @Override
    public void onNext(TelemetryCommand command) {
        final String clientId = sessionHandler.clientId();
        try {
            switch (command.getCommandCase()) {
                case SETTINGS: {
                    final Settings settings = command.getSettings();
                    LOGGER.info("Receive settings from remote, endpoints={}, clientId={}", endpoints, clientId);
                    sessionHandler.onSettingsCommand(endpoints, settings);
                    break;
                }
                case RECOVER_ORPHANED_TRANSACTION_COMMAND: {
                    final RecoverOrphanedTransactionCommand recoverOrphanedTransactionCommand =
                        command.getRecoverOrphanedTransactionCommand();
                    LOGGER.info("Receive orphaned transaction recovery command from remote, endpoints={}, "
                        + "clientId={}", endpoints, clientId);
                    sessionHandler.onRecoverOrphanedTransactionCommand(endpoints, recoverOrphanedTransactionCommand);
                    break;
                }
                case VERIFY_MESSAGE_COMMAND: {
                    final VerifyMessageCommand verifyMessageCommand = command.getVerifyMessageCommand();
                    LOGGER.info("Receive message verification command from remote, endpoints={}, clientId={}",
                        endpoints, clientId);
                    sessionHandler.onVerifyMessageCommand(endpoints, verifyMessageCommand);
                    break;
                }
                case PRINT_THREAD_STACK_TRACE_COMMAND: {
                    final PrintThreadStackTraceCommand printThreadStackTraceCommand =
                        command.getPrintThreadStackTraceCommand();
                    LOGGER.info("Receive thread stack print command from remote, endpoints={}, clientId={}",
                        endpoints, clientId);
                    sessionHandler.onPrintThreadStackTraceCommand(endpoints, printThreadStackTraceCommand);
                    break;
                }
                default:
                    LOGGER.warn("Receive unrecognized command from remote, endpoints={}, command={}, clientId={}",
                        endpoints, command, clientId);
            }
        } catch (Throwable t) {
            LOGGER.error("[Bug] unexpected exception raised while receiving command from remote, command={}, "
                + "clientId={}", command, clientId, t);
        }
    }

    @Override
    public void onError(Throwable throwable) {
        LOGGER.error("Exception raised from stream response observer, clientId={}, endpoints={}",
            sessionHandler.clientId(), endpoints, throwable);
        release();
        if (!sessionHandler.isRunning()) {
            return;
        }
        sessionHandler.getScheduler().schedule(this::renewRequestObserver, 3, TimeUnit.SECONDS);
    }

    @Override
    public void onCompleted() {
        release();
        if (!sessionHandler.isRunning()) {
            return;
        }
        sessionHandler.getScheduler().schedule(this::renewRequestObserver, 3, TimeUnit.SECONDS);
    }
}
