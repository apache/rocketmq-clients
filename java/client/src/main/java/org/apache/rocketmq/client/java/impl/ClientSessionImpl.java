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
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.java.impl.producer.ClientSessionProcessor;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Telemetry session is constructed before first communication between client and remote route endpoints.
 */
@SuppressWarnings({"UnstableApiUsage", "NullableProblems"})
public class ClientSessionImpl implements StreamObserver<TelemetryCommand> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientSessionImpl.class);

    private final ClientSessionProcessor processor;
    private final Endpoints endpoints;
    private final ReadWriteLock observerLock;
    private StreamObserver<TelemetryCommand> requestObserver = null;

    protected ClientSessionImpl(ClientSessionProcessor processor, Endpoints endpoints) {
        this.processor = processor;
        this.endpoints = endpoints;
        this.observerLock = new ReentrantReadWriteLock();
    }

    protected ListenableFuture<ClientSessionImpl> register() {
        ListenableFuture<ClientSessionImpl> future;
        try {
            final TelemetryCommand command = processor.getSettingsCommand();
            this.publish(command);
            future = Futures.transform(processor.register(), input -> this, MoreExecutors.directExecutor());
        } catch (Throwable t) {
            future = Futures.immediateFailedFuture(t);
        }
        final String clientId = processor.clientId();
        Futures.addCallback(future, new FutureCallback<ClientSessionImpl>() {
            @Override
            public void onSuccess(ClientSessionImpl session) {
                LOGGER.info("Register client session successfully, endpoints={}, clientId={}", endpoints, clientId);
            }

            @Override
            public void onFailure(Throwable t) {
                LOGGER.error("Failed to register client session, endpoints={}, clientId={}", endpoints, clientId, t);
                release();
            }
        }, MoreExecutors.directExecutor());
        return future;
    }

    /**
     * Release telemetry session.
     */
    public void release() {
        this.observerLock.writeLock().lock();
        try {
            if (null != requestObserver) {
                try {
                    requestObserver.onCompleted();
                } catch (Throwable ignore) {
                    // Ignore exception on purpose.
                }
                requestObserver = null;
            }
        } finally {
            this.observerLock.writeLock().unlock();
        }
    }

    /**
     * Telemeter command to remote.
     *
     * @param command appointed command to telemeter
     */
    public void publish(TelemetryCommand command) throws ClientException {
        this.observerLock.readLock().lock();
        try {
            if (null != requestObserver) {
                requestObserver.onNext(command);
                return;
            }
        } finally {
            this.observerLock.readLock().unlock();
        }
        this.observerLock.writeLock().lock();
        try {
            if (null == requestObserver) {
                this.requestObserver = processor.telemetry(endpoints, this);
            }
            requestObserver.onNext(command);
        } finally {
            this.observerLock.writeLock().unlock();
        }
    }

    @Override
    public void onNext(TelemetryCommand command) {
        try {
            switch (command.getCommandCase()) {
                case SETTINGS: {
                    final Settings settings = command.getSettings();
                    LOGGER.info("Receive settings from remote, endpoints={}, clientId={}", endpoints,
                        processor.clientId());
                    processor.onSettingsCommand(endpoints, settings);
                    break;
                }
                case RECOVER_ORPHANED_TRANSACTION_COMMAND: {
                    final RecoverOrphanedTransactionCommand recoverOrphanedTransactionCommand =
                        command.getRecoverOrphanedTransactionCommand();
                    LOGGER.info("Receive orphaned transaction recovery command from remote, endpoints={}, "
                        + "clientId={}", endpoints, processor.clientId());
                    processor.onRecoverOrphanedTransactionCommand(endpoints, recoverOrphanedTransactionCommand);
                    break;
                }
                case VERIFY_MESSAGE_COMMAND: {
                    final VerifyMessageCommand verifyMessageCommand = command.getVerifyMessageCommand();
                    LOGGER.info("Receive message verification command from remote, endpoints={}, clientId={}",
                        endpoints, processor.clientId());
                    processor.onVerifyMessageCommand(endpoints, verifyMessageCommand);
                    break;
                }
                case PRINT_THREAD_STACK_TRACE_COMMAND: {
                    final PrintThreadStackTraceCommand printThreadStackTraceCommand =
                        command.getPrintThreadStackTraceCommand();
                    LOGGER.info("Receive thread stack print command from remote, endpoints={}, clientId={}",
                        endpoints, processor.clientId());
                    processor.onPrintThreadStackTraceCommand(endpoints, printThreadStackTraceCommand);
                    break;
                }
                default:
                    LOGGER.warn("Receive unrecognized command from remote, endpoints={}, command={}, clientId={}",
                        endpoints, command, processor.clientId());
            }
        } catch (Throwable t) {
            LOGGER.error("[Bug] unexpected exception raised while receiving command from remote, command={}, "
                + "clientId={}", command, processor.clientId(), t);
        }
    }

    @Override
    public void onError(Throwable throwable) {
        LOGGER.error("Exception raised from stream response observer, clientId={}, endpoints={}",
            processor.clientId(), endpoints, throwable);
        this.release();
    }

    @Override
    public void onCompleted() {
        this.release();
    }
}
