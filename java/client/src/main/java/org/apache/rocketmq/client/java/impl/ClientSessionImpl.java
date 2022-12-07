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
import com.google.common.util.concurrent.SettableFuture;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.java.impl.producer.ClientSessionHandler;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Telemetry session is constructed before first communication between client and remote route endpoints.
 */
public class ClientSessionImpl implements StreamObserver<TelemetryCommand> {
    static final Duration REQUEST_OBSERVER_RENEW_BACKOFF_DELAY = Duration.ofSeconds(1);
    private static final Logger log = LoggerFactory.getLogger(ClientSessionImpl.class);
    private static final Duration SETTINGS_INITIALIZATION_TIMEOUT = Duration.ofSeconds(3);

    private final ClientSessionHandler sessionHandler;
    private final Endpoints endpoints;
    private final SettableFuture<Settings> future;
    private volatile StreamObserver<TelemetryCommand> requestObserver;

    @SuppressWarnings("UnstableApiUsage")
    protected ClientSessionImpl(ClientSessionHandler sessionHandler, Duration tolerance, Endpoints endpoints)
        throws ClientException {
        this.sessionHandler = sessionHandler;
        this.endpoints = endpoints;
        this.future = SettableFuture.create();
        Futures.withTimeout(future, SETTINGS_INITIALIZATION_TIMEOUT.plus(tolerance).toMillis(),
            TimeUnit.MILLISECONDS, sessionHandler.getScheduler());
        this.requestObserver = sessionHandler.telemetry(endpoints, this);
    }

    private void renewRequestObserver() {
        final ClientId clientId = sessionHandler.getClientId();
        try {
            if (sessionHandler.isEndpointsDeprecated(endpoints)) {
                log.info("Endpoints is deprecated, no longer to renew requestObserver, endpoints={}, clientId={}",
                    endpoints, clientId);
                sessionHandler.removeClientSession(endpoints, this);
                return;
            }
            log.info("Try to renew requestObserver, endpoints={}, clientId={}", endpoints, clientId);
            this.requestObserver = sessionHandler.telemetry(endpoints, this);
        } catch (Throwable t) {
            log.error("Failed to renew requestObserver, attempt to renew later, endpoints={}, delay={}, clientId={}",
                endpoints, REQUEST_OBSERVER_RENEW_BACKOFF_DELAY, clientId, t);
            sessionHandler.getScheduler().schedule(this::renewRequestObserver,
                REQUEST_OBSERVER_RENEW_BACKOFF_DELAY.toNanos(), TimeUnit.NANOSECONDS);
            return;
        }
        log.info("Sync setting to remote after requestObserver is renewed, endpoints={}, clientId={}", endpoints,
            clientId);
        syncSettings0();
    }

    protected ListenableFuture<Settings> syncSettings() {
        this.syncSettings0();
        return future;
    }

    private void syncSettings0() {
        final TelemetryCommand settings = sessionHandler.settingsCommand();
        write(settings);
    }

    /**
     * Release telemetry session.
     */
    public void release() {
        final ClientId clientId = sessionHandler.getClientId();
        if (null == requestObserver) {
            log.error("[Bug] request observer does not exist, no need to release, endpoints={}, clientId={}",
                endpoints, clientId);
            return;
        }
        log.info("Begin to release client session, endpoints={}, clientId={}", endpoints, clientId);
        try {
            requestObserver.onCompleted();
        } catch (Throwable ignore) {
            // Ignore exception on purpose.
        }
    }

    void write(TelemetryCommand command) {
        if (null == requestObserver) {
            log.error("[Bug] Request observer does not exist, ignore current command, endpoints={}, command={}, "
                + "clientId={}", endpoints, command, sessionHandler.getClientId());
            return;
        }
        requestObserver.onNext(command);
    }

    @Override
    public void onNext(TelemetryCommand command) {
        final ClientId clientId = sessionHandler.getClientId();
        try {
            switch (command.getCommandCase()) {
                case SETTINGS: {
                    final Settings settings = command.getSettings();
                    log.info("Receive settings from remote, endpoints={}, clientId={}", endpoints, clientId);
                    sessionHandler.onSettingsCommand(endpoints, settings);
                    if (future.set(settings)) {
                        log.info("Init settings successfully, endpoints={}, clientId={}", endpoints, clientId);
                    }
                    break;
                }
                case RECOVER_ORPHANED_TRANSACTION_COMMAND: {
                    final RecoverOrphanedTransactionCommand recoverOrphanedTransactionCommand =
                        command.getRecoverOrphanedTransactionCommand();
                    log.info("Receive orphaned transaction recovery command from remote, endpoints={}, "
                        + "clientId={}", endpoints, clientId);
                    sessionHandler.onRecoverOrphanedTransactionCommand(endpoints, recoverOrphanedTransactionCommand);
                    break;
                }
                case VERIFY_MESSAGE_COMMAND: {
                    final VerifyMessageCommand verifyMessageCommand = command.getVerifyMessageCommand();
                    log.info("Receive message verification command from remote, endpoints={}, clientId={}",
                        endpoints, clientId);
                    sessionHandler.onVerifyMessageCommand(endpoints, verifyMessageCommand);
                    break;
                }
                case PRINT_THREAD_STACK_TRACE_COMMAND: {
                    final PrintThreadStackTraceCommand printThreadStackTraceCommand =
                        command.getPrintThreadStackTraceCommand();
                    log.info("Receive thread stack print command from remote, endpoints={}, clientId={}",
                        endpoints, clientId);
                    sessionHandler.onPrintThreadStackTraceCommand(endpoints, printThreadStackTraceCommand);
                    break;
                }
                default:
                    log.warn("Receive unrecognized command from remote, endpoints={}, command={}, clientId={}",
                        endpoints, command, clientId);
            }
        } catch (Throwable t) {
            log.error("[Bug] unexpected exception raised while receiving command from remote, command={}, "
                + "clientId={}", command, clientId, t);
        }
    }

    @Override
    public void onError(Throwable throwable) {
        final ClientId clientId = sessionHandler.getClientId();
        log.error("Exception raised from stream response observer, clientId={}, endpoints={}", clientId, endpoints,
            throwable);
        release();
        if (!sessionHandler.isRunning()) {
            log.info("Session handler is not running, forgive to renew request observer, clientId={}, "
                + "endpoints={}", clientId, endpoints);
            return;
        }
        sessionHandler.getScheduler().schedule(this::renewRequestObserver,
            REQUEST_OBSERVER_RENEW_BACKOFF_DELAY.toNanos(), TimeUnit.NANOSECONDS);
    }

    @Override
    public void onCompleted() {
        final ClientId clientId = sessionHandler.getClientId();
        log.info("Receive completion for stream response observer, clientId={}, endpoints={}", clientId, endpoints);
        release();
        if (!sessionHandler.isRunning()) {
            log.info("Session handler is not running, forgive to renew request observer, clientId={}, "
                + "endpoints={}", clientId, endpoints);
            return;
        }
        sessionHandler.getScheduler().schedule(this::renewRequestObserver,
            REQUEST_OBSERVER_RENEW_BACKOFF_DELAY.toNanos(), TimeUnit.NANOSECONDS);
    }
}
