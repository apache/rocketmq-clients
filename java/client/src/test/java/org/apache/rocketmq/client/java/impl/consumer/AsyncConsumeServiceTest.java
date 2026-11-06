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

package org.apache.rocketmq.client.java.impl.consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.rocketmq.client.apis.consumer.AsyncMessageListener;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.java.hook.MessageInterceptor;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

@RunWith(Parameterized.class)
public class AsyncConsumeServiceTest extends TestBase {
    private final String executorMode;
    private final ClientId clientId = new ClientId();
    private final MessageInterceptor interceptor = Mockito.mock(MessageInterceptor.class);
    private ExecutorService consumptionExecutor;
    private ExecutorService drainExecutor;
    private ScheduledExecutorService scheduler;

    public AsyncConsumeServiceTest(String executorMode) {
        this.executorMode = executorMode;
    }

    @Parameterized.Parameters(name = "executor={0}")
    public static Collection<Object[]> executorModes() {
        final List<Object[]> modes = new ArrayList<>();
        modes.add(new Object[] {"platform-single"});
        modes.add(new Object[] {"platform-pool"});
        try {
            // Keep the tests Java 8 compatible while exercising virtual threads on Java 21+.
            Executors.class.getMethod("newVirtualThreadPerTaskExecutor");
            modes.add(new Object[] {"virtual"});
        } catch (NoSuchMethodException ignored) {
            // Virtual threads are unavailable on this runtime.
        }
        return modes;
    }

    @Before
    public void setUp() throws Exception {
        if ("virtual".equals(executorMode)) {
            final Method factory = Executors.class.getMethod("newVirtualThreadPerTaskExecutor");
            consumptionExecutor = (ExecutorService) factory.invoke(null);
        } else if ("platform-pool".equals(executorMode)) {
            consumptionExecutor = Executors.newFixedThreadPool(4);
        } else {
            consumptionExecutor = Executors.newSingleThreadExecutor();
        }
        drainExecutor = Executors.newSingleThreadExecutor();
        scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    @After
    public void tearDown() throws InterruptedException {
        for (ExecutorService executor : Arrays.asList(consumptionExecutor, drainExecutor, scheduler)) {
            if (executor != null) {
                executor.shutdownNow();
                assertTrue("Test executor did not terminate", executor.awaitTermination(5, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    public void testStageCompletionControlsResult() throws Exception {
        final CompletableFuture<ConsumeResult> completion = new CompletableFuture<>();
        final CountDownLatch invoked = new CountDownLatch(1);
        final ConsumeService service = service(message -> {
            invoked.countDown();
            return completion;
        }, 1);
        final ListenableFuture<ConsumeResult> result = service.consume(fakeMessageViewImpl());

        assertTrue(invoked.await(5, TimeUnit.SECONDS));
        assertFalse(result.isDone());
        completion.complete(ConsumeResult.SUCCESS);
        assertEquals(ConsumeResult.SUCCESS, result.get(5, TimeUnit.SECONDS));
    }

    @Test
    public void testConcurrencyLimitCoversUncompletedStages() throws Exception {
        final CompletableFuture<ConsumeResult> firstCompletion = new CompletableFuture<>();
        final CompletableFuture<ConsumeResult> secondCompletion = new CompletableFuture<>();
        final MessageViewImpl first = fakeMessageViewImpl();
        final MessageViewImpl second = fakeMessageViewImpl();
        final LinkedBlockingQueue<MessageView> invocations = new LinkedBlockingQueue<>();
        final ConsumeService service = service(message -> {
            invocations.add(message);
            return message == first ? firstCompletion : secondCompletion;
        }, 1);
        final ListenableFuture<ConsumeResult> firstResult = service.consume(first);
        final ListenableFuture<ConsumeResult> secondResult = service.consume(second);

        assertSame(first, invocations.poll(5, TimeUnit.SECONDS));
        assertNull("A pending stage must retain the concurrency slot", invocations.poll(100, TimeUnit.MILLISECONDS));
        assertFalse(secondResult.isDone());
        firstCompletion.complete(ConsumeResult.SUCCESS);
        assertEquals(ConsumeResult.SUCCESS, firstResult.get(5, TimeUnit.SECONDS));
        assertSame(second, invocations.poll(5, TimeUnit.SECONDS));
        secondCompletion.complete(ConsumeResult.SUCCESS);
        assertEquals(ConsumeResult.SUCCESS, secondResult.get(5, TimeUnit.SECONDS));
    }

    @Test
    public void testIncreasingConcurrencyStartsQueuedStage() throws Exception {
        final MessageViewImpl first = fakeMessageViewImpl();
        final MessageViewImpl second = fakeMessageViewImpl();
        final CompletableFuture<ConsumeResult> firstCompletion = new CompletableFuture<>();
        final CompletableFuture<ConsumeResult> secondCompletion = new CompletableFuture<>();
        final LinkedBlockingQueue<MessageView> invocations = new LinkedBlockingQueue<>();
        final ConsumeService service = service(message -> {
            invocations.add(message);
            return message == first ? firstCompletion : secondCompletion;
        }, 1);
        final ListenableFuture<ConsumeResult> firstResult = service.consume(first);
        final ListenableFuture<ConsumeResult> secondResult = service.consume(second);
        assertSame(first, invocations.poll(5, TimeUnit.SECONDS));
        assertNull(invocations.poll(100, TimeUnit.MILLISECONDS));

        service.updateConcurrency(2);

        assertSame(second, invocations.poll(5, TimeUnit.SECONDS));
        assertFalse("Increasing concurrency must not complete existing processing", firstResult.isDone());
        assertFalse(secondResult.isDone());
        firstCompletion.complete(ConsumeResult.SUCCESS);
        secondCompletion.complete(ConsumeResult.SUCCESS);
        assertEquals(ConsumeResult.SUCCESS, firstResult.get(5, TimeUnit.SECONDS));
        assertEquals(ConsumeResult.SUCCESS, secondResult.get(5, TimeUnit.SECONDS));
        awaitDrain(service).get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testDecreasingConcurrencyLetsExistingStagesFinish() throws Exception {
        final MessageViewImpl first = fakeMessageViewImpl();
        final MessageViewImpl second = fakeMessageViewImpl();
        final MessageViewImpl third = fakeMessageViewImpl();
        final CompletableFuture<ConsumeResult> firstCompletion = new CompletableFuture<>();
        final CompletableFuture<ConsumeResult> secondCompletion = new CompletableFuture<>();
        final CompletableFuture<ConsumeResult> thirdCompletion = new CompletableFuture<>();
        final LinkedBlockingQueue<MessageView> invocations = new LinkedBlockingQueue<>();
        final ConsumeService service = service(message -> {
            invocations.add(message);
            if (message == first) {
                return firstCompletion;
            }
            return message == second ? secondCompletion : thirdCompletion;
        }, 2);
        final ListenableFuture<ConsumeResult> firstResult = service.consume(first);
        final ListenableFuture<ConsumeResult> secondResult = service.consume(second);
        final MessageView firstInvocation = invocations.poll(5, TimeUnit.SECONDS);
        final MessageView secondInvocation = invocations.poll(5, TimeUnit.SECONDS);
        assertTrue(firstInvocation == first || firstInvocation == second);
        assertTrue(secondInvocation == first || secondInvocation == second);
        assertTrue(firstInvocation != secondInvocation);

        service.updateConcurrency(1);
        final ListenableFuture<ConsumeResult> thirdResult = service.consume(third);
        assertFalse(firstResult.isDone());
        assertFalse(secondResult.isDone());
        assertNull(invocations.poll(100, TimeUnit.MILLISECONDS));
        firstCompletion.complete(ConsumeResult.SUCCESS);
        assertEquals(ConsumeResult.SUCCESS, firstResult.get(5, TimeUnit.SECONDS));
        assertFalse(secondResult.isDone());
        assertNull("One active stage still occupies the reduced limit", invocations.poll(100, TimeUnit.MILLISECONDS));
        secondCompletion.complete(ConsumeResult.SUCCESS);
        assertEquals(ConsumeResult.SUCCESS, secondResult.get(5, TimeUnit.SECONDS));
        assertSame(third, invocations.poll(5, TimeUnit.SECONDS));
        thirdCompletion.complete(ConsumeResult.SUCCESS);
        assertEquals(ConsumeResult.SUCCESS, thirdResult.get(5, TimeUnit.SECONDS));
        awaitDrain(service).get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testRejectedConsumptionCompletesAsFailure() throws Exception {
        final AsyncMessageListener listener = Mockito.mock(AsyncMessageListener.class);
        final ConsumeService service = service(listener, 1);
        consumptionExecutor.shutdown();

        assertEquals(ConsumeResult.FAILURE, service.consume(fakeMessageViewImpl()).get(5, TimeUnit.SECONDS));
        verify(listener, never()).consumeAsync(any());
        awaitDrain(service).get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testDelayedStageStartsOnlyAfterScheduledTask() throws Exception {
        final CompletableFuture<ConsumeResult> completion = new CompletableFuture<>();
        final AsyncMessageListener listener = Mockito.mock(AsyncMessageListener.class);
        final ScheduledExecutorService controlledScheduler = Mockito.mock(ScheduledExecutorService.class);
        Mockito.when(listener.consumeAsync(any())).thenReturn(completion);
        Mockito.when(controlledScheduler.schedule(any(Runnable.class), eq(7L), eq(TimeUnit.NANOSECONDS)))
            .thenReturn(Mockito.mock(ScheduledFuture.class));
        final ConsumeService service = service(listener, controlledScheduler, 1);
        final ListenableFuture<ConsumeResult> result = service.consume(fakeMessageViewImpl(), Duration.ofNanos(7));
        final ArgumentCaptor<Runnable> scheduledTask = ArgumentCaptor.forClass(Runnable.class);
        verify(controlledScheduler).schedule(scheduledTask.capture(), eq(7L), eq(TimeUnit.NANOSECONDS));
        verify(listener, never()).consumeAsync(any());
        assertFalse(result.isDone());

        scheduledTask.getValue().run();
        verify(listener, timeout(5000)).consumeAsync(any());
        assertFalse(result.isDone());
        completion.complete(ConsumeResult.SUCCESS);
        assertEquals(ConsumeResult.SUCCESS, result.get(5, TimeUnit.SECONDS));
    }

    @Test
    public void testRejectedDelayedConsumptionCompletesAsFailure() throws Exception {
        final ConsumeService service = service(message -> CompletableFuture.completedFuture(ConsumeResult.SUCCESS), 1);
        scheduler.shutdown();

        assertEquals(ConsumeResult.FAILURE,
            service.consume(fakeMessageViewImpl(), Duration.ofSeconds(1)).get(5, TimeUnit.SECONDS));
        awaitDrain(service).get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testAwaitConsumptionWaitsForPendingStage() throws Exception {
        final CompletableFuture<ConsumeResult> completion = new CompletableFuture<>();
        final CountDownLatch invoked = new CountDownLatch(1);
        final ConsumeService service = service(message -> {
            invoked.countDown();
            return completion;
        }, 1);
        service.consume(fakeMessageViewImpl());
        assertTrue(invoked.await(5, TimeUnit.SECONDS));
        final Future<?> drained = awaitDrain(service);

        assertStillPending(drained);
        completion.complete(ConsumeResult.SUCCESS);
        drained.get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testStandardDoesNotAcknowledgeBeforeStageCompletion() throws Exception {
        final CompletableFuture<ConsumeResult> completion = new CompletableFuture<>();
        final CountDownLatch invoked = new CountDownLatch(1);
        final SettableFuture<Void> acknowledgment = SettableFuture.create();
        final ProcessQueue processQueue = Mockito.mock(ProcessQueue.class);
        final MessageViewImpl message = fakeMessageViewImpl();
        Mockito.when(processQueue.eraseMessageAsync(message, ConsumeResult.SUCCESS)).thenReturn(acknowledgment);
        final StandardConsumeService service = new StandardConsumeService(clientId, FAKE_CONSUMER_GROUP_0,
            view -> {
                invoked.countDown();
                return completion;
            }, consumptionExecutor, interceptor, scheduler, 1);
        service.consume(processQueue, Collections.singletonList(message));
        assertTrue(invoked.await(5, TimeUnit.SECONDS));
        verify(processQueue, never()).eraseMessageAsync(any(), any());
        final Future<?> drained = awaitDrain(service);
        assertStillPending(drained);

        completion.complete(ConsumeResult.SUCCESS);
        verify(processQueue, timeout(5000)).eraseMessageAsync(message, ConsumeResult.SUCCESS);
        assertStillPending(drained);
        acknowledgment.set(null);
        drained.get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testStandardExceptionalStageTriggersFailureSettlement() throws Exception {
        final CompletableFuture<ConsumeResult> completion = new CompletableFuture<>();
        final CountDownLatch invoked = new CountDownLatch(1);
        final ProcessQueue processQueue = Mockito.mock(ProcessQueue.class);
        final MessageViewImpl message = fakeMessageViewImpl();
        Mockito.when(processQueue.eraseMessageAsync(message, ConsumeResult.FAILURE))
            .thenReturn(Futures.immediateFuture(null));
        final StandardConsumeService service = new StandardConsumeService(clientId, FAKE_CONSUMER_GROUP_0,
            view -> {
                invoked.countDown();
                return completion;
            }, consumptionExecutor, interceptor, scheduler, 1);
        service.consume(processQueue, Collections.singletonList(message));
        assertTrue(invoked.await(5, TimeUnit.SECONDS));
        completion.completeExceptionally(new IllegalStateException("Processing failed"));

        verify(processQueue, timeout(5000)).eraseMessageAsync(message, ConsumeResult.FAILURE);
        verify(processQueue, never()).eraseMessageAsync(message, ConsumeResult.SUCCESS);
        awaitDrain(service).get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testStandardRejectionTriggersFailureSettlement() throws Exception {
        final ProcessQueue processQueue = Mockito.mock(ProcessQueue.class);
        final MessageViewImpl message = fakeMessageViewImpl();
        Mockito.when(processQueue.eraseMessageAsync(message, ConsumeResult.FAILURE))
            .thenReturn(Futures.immediateFuture(null));
        final StandardConsumeService service = new StandardConsumeService(clientId, FAKE_CONSUMER_GROUP_0,
            view -> CompletableFuture.completedFuture(ConsumeResult.SUCCESS), consumptionExecutor, interceptor,
            scheduler, 1);
        consumptionExecutor.shutdown();
        service.consume(processQueue, Collections.singletonList(message));

        verify(processQueue, timeout(5000)).eraseMessageAsync(message, ConsumeResult.FAILURE);
        awaitDrain(service).get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testStandardFailedAcknowledgmentDoesNotStrandShutdown() throws Exception {
        final ProcessQueue processQueue = Mockito.mock(ProcessQueue.class);
        final MessageViewImpl message = fakeMessageViewImpl();
        final SettableFuture<Void> acknowledgment = SettableFuture.create();
        Mockito.when(processQueue.eraseMessageAsync(message, ConsumeResult.SUCCESS)).thenReturn(acknowledgment);
        final StandardConsumeService service = new StandardConsumeService(clientId, FAKE_CONSUMER_GROUP_0,
            view -> CompletableFuture.completedFuture(ConsumeResult.SUCCESS), consumptionExecutor, interceptor,
            scheduler, 1);
        service.consume(processQueue, Collections.singletonList(message));
        verify(processQueue, timeout(5000)).eraseMessageAsync(message, ConsumeResult.SUCCESS);
        final Future<?> drained = awaitDrain(service);
        assertStillPending(drained);
        acknowledgment.setException(new IllegalStateException("Acknowledgment failed"));

        drained.get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testFifoWaitsForStageAndAcknowledgmentBeforeNextMessage() throws Exception {
        final CompletableFuture<ConsumeResult> firstCompletion = new CompletableFuture<>();
        final CompletableFuture<ConsumeResult> secondCompletion = new CompletableFuture<>();
        final SettableFuture<Void> firstAcknowledgment = SettableFuture.create();
        final SettableFuture<Void> secondAcknowledgment = SettableFuture.create();
        final MessageViewImpl first = fakeMessageViewImpl();
        final MessageViewImpl second = fakeMessageViewImpl();
        final ProcessQueue processQueue = Mockito.mock(ProcessQueue.class);
        Mockito.when(processQueue.eraseFifoMessage(first, ConsumeResult.SUCCESS)).thenReturn(firstAcknowledgment);
        Mockito.when(processQueue.eraseFifoMessage(second, ConsumeResult.SUCCESS)).thenReturn(secondAcknowledgment);
        final LinkedBlockingQueue<MessageView> invocations = new LinkedBlockingQueue<>();
        final FifoConsumeService service = new FifoConsumeService(clientId, FAKE_CONSUMER_GROUP_0, message -> {
            invocations.add(message);
            return message == first ? firstCompletion : secondCompletion;
        }, consumptionExecutor, interceptor, scheduler, 2, false);
        service.consume(processQueue, Arrays.asList(first, second));
        assertSame(first, invocations.poll(5, TimeUnit.SECONDS));
        assertNull(invocations.poll(100, TimeUnit.MILLISECONDS));
        final Future<?> drained = awaitDrain(service);
        assertStillPending(drained);

        firstCompletion.complete(ConsumeResult.SUCCESS);
        verify(processQueue, timeout(5000)).eraseFifoMessage(first, ConsumeResult.SUCCESS);
        assertNull("FIFO must also wait for acknowledgment", invocations.poll(100, TimeUnit.MILLISECONDS));
        firstAcknowledgment.set(null);
        assertSame(second, invocations.poll(5, TimeUnit.SECONDS));
        assertStillPending(drained);
        secondCompletion.complete(ConsumeResult.SUCCESS);
        verify(processQueue, timeout(5000)).eraseFifoMessage(second, ConsumeResult.SUCCESS);
        assertStillPending(drained);
        secondAcknowledgment.set(null);
        drained.get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testFifoAcceleratorStartsIndependentMessageGroups() throws Exception {
        final MessageViewImpl first = Mockito.spy(fakeMessageViewImpl());
        final MessageViewImpl second = Mockito.spy(fakeMessageViewImpl());
        Mockito.when(first.getMessageGroup()).thenReturn(Optional.of("first-group"));
        Mockito.when(second.getMessageGroup()).thenReturn(Optional.of("second-group"));
        final CompletableFuture<ConsumeResult> firstCompletion = new CompletableFuture<>();
        final CompletableFuture<ConsumeResult> secondCompletion = new CompletableFuture<>();
        final ProcessQueue processQueue = Mockito.mock(ProcessQueue.class);
        Mockito.when(processQueue.eraseFifoMessage(any(), any())).thenReturn(Futures.immediateFuture(null));
        final LinkedBlockingQueue<MessageView> invocations = new LinkedBlockingQueue<>();
        final FifoConsumeService service = new FifoConsumeService(clientId, FAKE_CONSUMER_GROUP_0, message -> {
            invocations.add(message);
            return message == first ? firstCompletion : secondCompletion;
        }, consumptionExecutor, interceptor, scheduler, 2, true);
        service.consume(processQueue, Arrays.asList(first, second));
        final MessageView firstInvocation = invocations.poll(5, TimeUnit.SECONDS);
        final MessageView secondInvocation = invocations.poll(5, TimeUnit.SECONDS);

        assertTrue(firstInvocation == first || firstInvocation == second);
        assertTrue(secondInvocation == first || secondInvocation == second);
        assertTrue(firstInvocation != secondInvocation);
        firstCompletion.complete(ConsumeResult.SUCCESS);
        secondCompletion.complete(ConsumeResult.SUCCESS);
        awaitDrain(service).get(5, TimeUnit.SECONDS);
    }

    private ConsumeService service(AsyncMessageListener listener, int maxConcurrentConsumptions) {
        return service(listener, scheduler, maxConcurrentConsumptions);
    }

    private ConsumeService service(AsyncMessageListener listener, ScheduledExecutorService scheduledExecutor,
        int maxConcurrentConsumptions) {
        return new ConsumeService(clientId, FAKE_CONSUMER_GROUP_0, listener, consumptionExecutor, interceptor,
            scheduledExecutor, maxConcurrentConsumptions) {
            @Override
            public void consume(ProcessQueue processQueue, List<MessageViewImpl> messages) {
            }
        };
    }

    private Future<?> awaitDrain(ConsumeService service) {
        return drainExecutor.submit(() -> {
            service.awaitConsumption();
            return null;
        });
    }

    private void assertStillPending(Future<?> future) throws Exception {
        try {
            future.get(100, TimeUnit.MILLISECONDS);
            fail("Consumption drain completed while asynchronous work was pending");
        } catch (TimeoutException expected) {
            // The drain must remain blocked until processing and settlement finish.
        }
    }
}
