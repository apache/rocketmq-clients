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

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

import apache.rocketmq.v2.PrintThreadStackTraceCommand;
import apache.rocketmq.v2.RecoverOrphanedTransactionCommand;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.TelemetryCommand;
import apache.rocketmq.v2.VerifyMessageCommand;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.java.impl.producer.ClientSessionHandler;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.awaitility.Durations;
import org.junit.Test;
import org.mockito.Mockito;

public class ClientSessionImplTest extends TestBase {

    @SuppressWarnings("unchecked")
    @Test
    public void syncSettings() throws ClientException, ExecutionException, InterruptedException {
        final Endpoints endpoints = fakeEndpoints();
        final ClientSessionHandler sessionHandler = Mockito.mock(ClientSessionHandler.class);
        Mockito.when(sessionHandler.getScheduler()).thenReturn(new ScheduledThreadPoolExecutor(1));
        final StreamObserver<TelemetryCommand> requestObserver = Mockito.mock(StreamObserver.class);
        Mockito.doReturn(requestObserver).when(sessionHandler).telemetry(any(Endpoints.class),
            any(StreamObserver.class));
        final ClientSessionImpl clientSession = new ClientSessionImpl(sessionHandler, Duration.ofSeconds(3), endpoints);
        Mockito.doNothing().when(requestObserver).onNext(any(TelemetryCommand.class));
        Mockito.doReturn(FAKE_CLIENT_ID).when(sessionHandler).getClientId();
        Mockito.doNothing().when(sessionHandler).onSettingsCommand(any(Endpoints.class), any(Settings.class));
        final ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 60, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>());
        final Settings settings = Settings.newBuilder().build();
        TelemetryCommand settingsCommand = TelemetryCommand.newBuilder().setSettings(settings).build();
        executor.submit(() -> clientSession.onNext(settingsCommand));
        clientSession.syncSettings().get();
        Mockito.verify(sessionHandler, times(1)).onSettingsCommand(eq(endpoints), eq(settings));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testOnNextWithRecoverOrphanedTransactionCommand() throws ClientException {
        final Endpoints endpoints = fakeEndpoints();
        final ClientSessionHandler sessionHandler = Mockito.mock(ClientSessionHandler.class);
        Mockito.when(sessionHandler.getScheduler()).thenReturn(new ScheduledThreadPoolExecutor(1));
        final StreamObserver<TelemetryCommand> requestObserver = Mockito.mock(StreamObserver.class);
        Mockito.doReturn(requestObserver).when(sessionHandler).telemetry(any(Endpoints.class),
            any(StreamObserver.class));
        final ClientSessionImpl clientSession = new ClientSessionImpl(sessionHandler, Duration.ofSeconds(3), endpoints);
        Mockito.doReturn(FAKE_CLIENT_ID).when(sessionHandler).getClientId();
        Mockito.doNothing().when(sessionHandler).onRecoverOrphanedTransactionCommand(any(Endpoints.class),
            any(RecoverOrphanedTransactionCommand.class));
        RecoverOrphanedTransactionCommand command0 = RecoverOrphanedTransactionCommand.newBuilder().build();
        TelemetryCommand command = TelemetryCommand.newBuilder()
            .setRecoverOrphanedTransactionCommand(command0).build();
        clientSession.onNext(command);
        Mockito.verify(sessionHandler, times(1)).onRecoverOrphanedTransactionCommand(eq(endpoints), eq(command0));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testOnNextWithVerifyMessageCommand() throws ClientException {
        final Endpoints endpoints = fakeEndpoints();
        final ClientSessionHandler sessionHandler = Mockito.mock(ClientSessionHandler.class);
        Mockito.when(sessionHandler.getScheduler()).thenReturn(new ScheduledThreadPoolExecutor(1));
        final StreamObserver<TelemetryCommand> requestObserver = Mockito.mock(StreamObserver.class);
        Mockito.doReturn(requestObserver).when(sessionHandler).telemetry(any(Endpoints.class),
            any(StreamObserver.class));
        final ClientSessionImpl clientSession = new ClientSessionImpl(sessionHandler, Duration.ofSeconds(3), endpoints);
        Mockito.doReturn(FAKE_CLIENT_ID).when(sessionHandler).getClientId();
        Mockito.doNothing().when(sessionHandler).onVerifyMessageCommand(any(Endpoints.class),
            any(VerifyMessageCommand.class));
        VerifyMessageCommand command0 = VerifyMessageCommand.newBuilder().build();
        TelemetryCommand command = TelemetryCommand.newBuilder()
            .setVerifyMessageCommand(command0).build();
        clientSession.onNext(command);
        Mockito.verify(sessionHandler, times(1)).onVerifyMessageCommand(eq(endpoints), eq(command0));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testOnNextWithPrintThreadStackTraceCommand() throws ClientException {
        final Endpoints endpoints = fakeEndpoints();
        final ClientSessionHandler sessionHandler = Mockito.mock(ClientSessionHandler.class);
        Mockito.when(sessionHandler.getScheduler()).thenReturn(new ScheduledThreadPoolExecutor(1));
        final StreamObserver<TelemetryCommand> requestObserver = Mockito.mock(StreamObserver.class);
        Mockito.doReturn(requestObserver).when(sessionHandler).telemetry(any(Endpoints.class),
            any(StreamObserver.class));
        final ClientSessionImpl clientSession = new ClientSessionImpl(sessionHandler, Duration.ofSeconds(3), endpoints);
        Mockito.doReturn(FAKE_CLIENT_ID).when(sessionHandler).getClientId();
        Mockito.doNothing().when(sessionHandler).onPrintThreadStackTraceCommand(any(Endpoints.class),
            any(PrintThreadStackTraceCommand.class));
        PrintThreadStackTraceCommand command0 = PrintThreadStackTraceCommand.newBuilder().build();
        TelemetryCommand command = TelemetryCommand.newBuilder()
            .setPrintThreadStackTraceCommand(command0).build();
        clientSession.onNext(command);
        Mockito.verify(sessionHandler, times(1)).onPrintThreadStackTraceCommand(eq(endpoints), eq(command0));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testOnNextWithUnrecognizedCommand() throws ClientException {
        final Endpoints endpoints = fakeEndpoints();
        final ClientSessionHandler sessionHandler = Mockito.mock(ClientSessionHandler.class);
        Mockito.when(sessionHandler.getScheduler()).thenReturn(new ScheduledThreadPoolExecutor(1));
        final StreamObserver<TelemetryCommand> requestObserver = Mockito.mock(StreamObserver.class);
        Mockito.doReturn(requestObserver).when(sessionHandler).telemetry(any(Endpoints.class),
            any(StreamObserver.class));
        final ClientSessionImpl clientSession = new ClientSessionImpl(sessionHandler, Duration.ofSeconds(3), endpoints);
        Mockito.doReturn(FAKE_CLIENT_ID).when(sessionHandler).getClientId();
        Mockito.doNothing().when(sessionHandler).onSettingsCommand(any(Endpoints.class), any(Settings.class));
        Mockito.doNothing().when(sessionHandler).onRecoverOrphanedTransactionCommand(any(Endpoints.class),
            any(RecoverOrphanedTransactionCommand.class));
        Mockito.doNothing().when(sessionHandler).onVerifyMessageCommand(any(Endpoints.class),
            any(VerifyMessageCommand.class));
        Mockito.doNothing().when(sessionHandler).onPrintThreadStackTraceCommand(any(Endpoints.class),
            any(PrintThreadStackTraceCommand.class));
        TelemetryCommand command = TelemetryCommand.newBuilder().build();
        clientSession.onNext(command);
        Mockito.verify(sessionHandler, never()).onSettingsCommand(any(Endpoints.class), any(Settings.class));
        Mockito.verify(sessionHandler, never()).onRecoverOrphanedTransactionCommand(any(Endpoints.class),
            any(RecoverOrphanedTransactionCommand.class));
        Mockito.verify(sessionHandler, never()).onVerifyMessageCommand(any(Endpoints.class),
            any(VerifyMessageCommand.class));
        Mockito.verify(sessionHandler, never()).onPrintThreadStackTraceCommand(any(Endpoints.class),
            any(PrintThreadStackTraceCommand.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testOnError() throws ClientException {
        final Endpoints endpoints = fakeEndpoints();
        final ClientSessionHandler sessionHandler = Mockito.mock(ClientSessionHandler.class);
        Mockito.when(sessionHandler.getScheduler()).thenReturn(new ScheduledThreadPoolExecutor(1));
        final StreamObserver<TelemetryCommand> requestObserver = Mockito.mock(StreamObserver.class);
        Mockito.doReturn(requestObserver).when(sessionHandler).telemetry(any(Endpoints.class),
            any(StreamObserver.class));
        Mockito.doNothing().when(requestObserver).onCompleted();
        final ClientSessionImpl clientSession = new ClientSessionImpl(sessionHandler, Duration.ofSeconds(3), endpoints);
        Mockito.doReturn(FAKE_CLIENT_ID).when(sessionHandler).getClientId();
        Mockito.doReturn(true).when(sessionHandler).isRunning();
        Mockito.doReturn(SCHEDULER).when(sessionHandler).getScheduler();
        final Exception e = new Exception();
        clientSession.onError(e);
        Mockito.verify(sessionHandler, times(1)).isRunning();
        Mockito.verify(requestObserver, times(1)).onCompleted();
        Mockito.verify(sessionHandler, times(2)).getScheduler();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testOnErrorWithSessionHandlerIsNotRunning() throws ClientException {
        final Endpoints endpoints = fakeEndpoints();
        final ClientSessionHandler sessionHandler = Mockito.mock(ClientSessionHandler.class);
        Mockito.when(sessionHandler.getScheduler()).thenReturn(new ScheduledThreadPoolExecutor(1));
        final StreamObserver<TelemetryCommand> requestObserver = Mockito.mock(StreamObserver.class);
        Mockito.doReturn(requestObserver).when(sessionHandler).telemetry(any(Endpoints.class),
            any(StreamObserver.class));
        Mockito.doNothing().when(requestObserver).onCompleted();
        final ClientSessionImpl clientSession = new ClientSessionImpl(sessionHandler, Duration.ofSeconds(3), endpoints);
        Mockito.doReturn(FAKE_CLIENT_ID).when(sessionHandler).getClientId();
        Mockito.doReturn(false).when(sessionHandler).isRunning();
        final Exception e = new Exception();
        clientSession.onError(e);
        Mockito.verify(sessionHandler, times(1)).isRunning();
        Mockito.verify(requestObserver, times(1)).onCompleted();
        Mockito.verify(sessionHandler, times(1)).getScheduler();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testOnCompleted() throws ClientException {
        final Endpoints endpoints = fakeEndpoints();
        final ClientSessionHandler sessionHandler = Mockito.mock(ClientSessionHandler.class);
        Mockito.when(sessionHandler.getScheduler()).thenReturn(new ScheduledThreadPoolExecutor(1));
        final StreamObserver<TelemetryCommand> requestObserver = Mockito.mock(StreamObserver.class);
        Mockito.doReturn(requestObserver).when(sessionHandler).telemetry(any(Endpoints.class),
            any(StreamObserver.class));
        Mockito.doNothing().when(requestObserver).onCompleted();
        final ClientSessionImpl clientSession = new ClientSessionImpl(sessionHandler, Duration.ofSeconds(3), endpoints);
        Mockito.doReturn(FAKE_CLIENT_ID).when(sessionHandler).getClientId();
        Mockito.doReturn(true).when(sessionHandler).isRunning();
        Mockito.doReturn(SCHEDULER).when(sessionHandler).getScheduler();
        Mockito.doReturn(false).when(sessionHandler).isEndpointsDeprecated(endpoints);
        clientSession.onCompleted();
        Mockito.verify(sessionHandler, times(1)).isRunning();
        Mockito.verify(requestObserver, times(1)).onCompleted();
        Mockito.verify(sessionHandler, times(2)).getScheduler();
        await().atMost(ClientSessionImpl.REQUEST_OBSERVER_RENEW_BACKOFF_DELAY.plus(Durations.ONE_SECOND))
            .untilAsserted(() -> {
                Mockito.verify(sessionHandler, times(1)).isEndpointsDeprecated(eq(endpoints));
                Mockito.verify(sessionHandler, times(2)).telemetry(eq(endpoints), eq(clientSession));
            });
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testOnCompletedWithSessionHandlerIsNotRunning() throws ClientException {
        final Endpoints endpoints = fakeEndpoints();
        final ClientSessionHandler sessionHandler = Mockito.mock(ClientSessionHandler.class);
        Mockito.when(sessionHandler.getScheduler()).thenReturn(new ScheduledThreadPoolExecutor(1));
        final StreamObserver<TelemetryCommand> requestObserver = Mockito.mock(StreamObserver.class);
        Mockito.doReturn(requestObserver).when(sessionHandler).telemetry(any(Endpoints.class),
            any(StreamObserver.class));
        Mockito.doNothing().when(requestObserver).onCompleted();
        final ClientSessionImpl clientSession = new ClientSessionImpl(sessionHandler, Duration.ofSeconds(3), endpoints);
        Mockito.doReturn(FAKE_CLIENT_ID).when(sessionHandler).getClientId();
        Mockito.doReturn(false).when(sessionHandler).isRunning();
        clientSession.onCompleted();
        Mockito.verify(sessionHandler, times(1)).isRunning();
        Mockito.verify(requestObserver, times(1)).onCompleted();
        Mockito.verify(sessionHandler, times(1)).getScheduler();
    }
}