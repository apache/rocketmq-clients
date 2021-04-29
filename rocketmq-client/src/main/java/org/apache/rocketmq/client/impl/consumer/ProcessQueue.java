package org.apache.rocketmq.client.impl.consumer;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.client.consumer.filter.ExpressionType;
import org.apache.rocketmq.client.consumer.filter.FilterExpression;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.ClientInstance;
import org.apache.rocketmq.client.message.MessageConst;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.client.misc.MixAll;
import org.apache.rocketmq.proto.AckMessageRequest;
import org.apache.rocketmq.proto.ChangeInvisibleTimeRequest;
import org.apache.rocketmq.proto.PopMessageRequest;

@Slf4j
public class ProcessQueue {
  private static final long LONG_POLLING_TIMEOUT_MILLIS =
      MixAll.DEFAULT_LONG_POLLING_TIMEOUT_MILLIS;
  private static final long MAX_CACHED_MESSAGES_COUNT_PER_MESSAGE_QUEUE =
      MixAll.DEFAULT_MAX_CACHED_MESSAGES_COUNT_PER_MESSAGE_QUEUE;
  private static final long MAX_CACHED_MESSAGES_SIZE_PER_MESSAGE_QUEUE =
      MixAll.DEFAULT_MAX_CACHED_MESSAGES_SIZE_PER_MESSAGE_QUEUE;
  private static final long MAX_POP_MESSAGE_INTERVAL_MILLIS =
      MixAll.DEFAULT_MAX_POP_MESSAGE_INTERVAL_MILLIS;

  private static final long POP_TIME_DELAY_TIME_MILLIS_WHEN_FLOW_CONTROL = 3000L;

  @Setter @Getter private volatile boolean dropped;
  @Getter private final MessageQueue messageQueue;
  private final FilterExpression filterExpression;

  @Getter private final DefaultMQPushConsumerImpl consumerImpl;

  private final TreeMap<Long, MessageExt> cachedMessages;
  private final ReentrantReadWriteLock cachedMessagesLock;
  private final AtomicLong cachedMsgCount;
  private final AtomicLong cachedMsgSize;

  private volatile long lastPopTimestamp;
  private volatile long lastThrottledTimestamp;

  private final AtomicLong termId;

  public ProcessQueue(
      DefaultMQPushConsumerImpl consumerImpl,
      MessageQueue messageQueue,
      FilterExpression filterExpression) {
    this.consumerImpl = consumerImpl;
    this.messageQueue = messageQueue;
    this.filterExpression = filterExpression;
    this.dropped = false;

    this.cachedMessages = new TreeMap<Long, MessageExt>();
    this.cachedMessagesLock = new ReentrantReadWriteLock();

    this.cachedMsgCount = new AtomicLong(0L);
    this.cachedMsgSize = new AtomicLong(0L);

    this.lastPopTimestamp = -1L;
    this.lastThrottledTimestamp = -1L;

    this.termId = new AtomicLong(1);
  }

  public boolean isPopExpired() {
    final long popDuration = System.currentTimeMillis() - lastPopTimestamp;
    if (popDuration > MAX_POP_MESSAGE_INTERVAL_MILLIS && lastPopTimestamp >= 0) {
      log.warn(
          "ProcessQueue is expired, duration from last pop={}ms, lastPopTimestamp={}, currentTimestamp={}, mq={}",
          popDuration,
          lastPopTimestamp,
          System.currentTimeMillis(),
          messageQueue.simpleName());
      return true;
    }

    final long throttledDuration = System.currentTimeMillis() - lastThrottledTimestamp;
    if (throttledDuration > MAX_POP_MESSAGE_INTERVAL_MILLIS && lastThrottledTimestamp >= 0) {
      log.warn(
          "ProcessQueue is expired, duration from last throttle={}ms, lastPopTimestamp={}, currentTimestamp={}, mq={}",
          throttledDuration,
          lastThrottledTimestamp,
          System.currentTimeMillis(),
          messageQueue.simpleName());
      return true;
    }

    return false;
  }

  public void cacheMessages(List<MessageExt> messageList) {
    cachedMessagesLock.writeLock().lock();
    try {
      for (MessageExt message : messageList) {
        if (null == cachedMessages.put(message.getQueueOffset(), message)) {
          cachedMsgCount.incrementAndGet();
          cachedMsgSize.addAndGet(null == message.getBody() ? 0 : message.getBody().length);
        }
      }
    } finally {
      cachedMessagesLock.writeLock().unlock();
    }
  }

  public List<MessageExt> getCachedMessages() {
    cachedMessagesLock.readLock().lock();
    try {
      return new ArrayList<MessageExt>(cachedMessages.values());
    } finally {
      cachedMessagesLock.readLock().unlock();
    }
  }

  public void removeCachedMessages(List<MessageExt> messageExtList) {
    cachedMessagesLock.writeLock().lock();
    try {
      for (MessageExt messageExt : messageExtList) {
        final MessageExt removed = cachedMessages.remove(messageExt.getQueueOffset());
        if (null != removed) {
          cachedMsgCount.decrementAndGet();
          cachedMsgSize.addAndGet(null == removed.getBody() ? 0 : -removed.getBody().length);
        }
      }
    } finally {
      cachedMessagesLock.writeLock().unlock();
    }
  }

  private void handlePopResult(PopResult popResult) {
    final PopStatus popStatus = popResult.getPopStatus();
    final List<MessageExt> msgFoundList = popResult.getMsgFoundList();

    switch (popStatus) {
      case FOUND:
        cacheMessages(msgFoundList);

        consumerImpl.popTimes.getAndAdd(1);
        consumerImpl.popMsgCount.getAndAdd(msgFoundList.size());

        consumerImpl.getConsumeService().dispatch(this);
      case NO_NEW_MSG:
      case POLLING_FULL:
      case POLLING_NOT_FOUND:
      case SERVICE_UNSTABLE:
        log.debug(
            "Pop message from target={} with status={}, mq={}, message count={}",
            popResult.getTarget(),
            popStatus,
            messageQueue.simpleName(),
            msgFoundList.size());
        prepareNextPop(popResult.getTermId(), popResult.getTarget());
        break;
      default:
        log.warn(
            "Pop message from target={} with unknown status={}, mq={}, message count={}",
            popResult.getTarget(),
            popStatus,
            messageQueue.simpleName(),
            msgFoundList.size());
        prepareNextPop(popResult.getTermId(), popResult.getTarget());
    }
  }

  private void prepareNextPop(long currentTermId, String target) {
    if (!leaseNextTerm(currentTermId, target)) {
      log.debug("No need to prepare for next pop, mq={}", messageQueue.simpleName());
      return;
    }
    if (this.isDropped()) {
      log.debug("Process queue has been dropped, mq={}.", messageQueue.simpleName());
      return;
    }
    if (this.isPopThrottled()) {
      log.warn(
          "Process queue flow control is triggered, would pop message later, mq={}.",
          messageQueue.simpleName());

      lastThrottledTimestamp = System.currentTimeMillis();

      popMessageLater();
      return;
    }
    popMessage();
  }

  private void popMessageLater() {
    final ScheduledExecutorService scheduler = consumerImpl.getClientInstance().getScheduler();
    scheduler.schedule(
        new Runnable() {
          @Override
          public void run() {
            popMessage();
          }
        },
        POP_TIME_DELAY_TIME_MILLIS_WHEN_FLOW_CONTROL,
        TimeUnit.MILLISECONDS);
  }

  private boolean isPopThrottled() {
    final long actualCachedMsgCount = cachedMsgCount.get();
    if (MAX_CACHED_MESSAGES_COUNT_PER_MESSAGE_QUEUE <= actualCachedMsgCount) {
      log.warn(
          "Process queue cached message count exceeds the threshold, max count={}, cached count={}, mq={}",
          MAX_CACHED_MESSAGES_COUNT_PER_MESSAGE_QUEUE,
          actualCachedMsgCount,
          messageQueue.simpleName());
      return true;
    }
    final long actualCachedMsgSize = cachedMsgSize.get();
    if (MAX_CACHED_MESSAGES_SIZE_PER_MESSAGE_QUEUE <= actualCachedMsgSize) {
      log.warn(
          "Process queue cached message size exceeds the threshold, max size={}, cached size={}, mq={}",
          MAX_CACHED_MESSAGES_SIZE_PER_MESSAGE_QUEUE,
          actualCachedMsgSize,
          messageQueue.simpleName());
      return true;
    }
    return false;
  }

  public void ackMessage(MessageExt messageExt) throws MQClientException {
    if (messageExt.isExpired(10)) {
      log.warn(
          "Message is already expired, skip ACK, topic={}, msgId={}, decodedTimestamp={}, currentTimestamp={}, expiredTimestamp={}",
          messageExt.getTopic(),
          messageExt.getMsgId(),
          messageExt.getDecodedTimestamp(),
          System.currentTimeMillis(),
          messageExt.getExpiredTimestamp());
      return;
    }
    final AckMessageRequest request = wrapAckMessageRequest(messageExt);
    final ClientInstance clientInstance = consumerImpl.getClientInstance();
    final String target = messageExt.getProperty(MessageConst.PROPERTY_ACK_HOST_ADDRESS);

    if (consumerImpl.getDefaultMQPushConsumer().isAckMessageAsync()) {
      clientInstance.ackMessageAsync(target, request);
      return;
    }
    clientInstance.ackMessage(target, request);
  }

  public void negativeAckMessage(MessageExt messageExt) throws MQClientException {
    final ChangeInvisibleTimeRequest request = wrapChangeInvisibleTimeRequest(messageExt);
    final ClientInstance clientInstance = consumerImpl.getClientInstance();
    final String target = messageExt.getProperty(MessageConst.PROPERTY_ACK_HOST_ADDRESS);
    clientInstance.changeInvisibleTime(target, request);
  }

  public void popMessage() {
    try {
      final ClientInstance clientInstance = consumerImpl.getClientInstance();
      final String target =
          clientInstance.resolveTarget(messageQueue.getTopic(), messageQueue.getBrokerName());
      final PopMessageRequest request = wrapPopMessageRequest();

      lastPopTimestamp = System.currentTimeMillis();

      final ListenableFuture<PopResult> future =
          clientInstance.popMessageAsync(
              target, request, LONG_POLLING_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
      Futures.addCallback(
          future,
          new FutureCallback<PopResult>() {
            @Override
            public void onSuccess(PopResult popResult) {
              ProcessQueue.this.handlePopResult(popResult);
            }

            @Override
            public void onFailure(Throwable t) {
              log.error(
                  "Unexpected error while popping message, would pop message later, mq={}",
                  messageQueue,
                  t);
              popMessageLater();
            }
          },
          MoreExecutors.directExecutor());
    } catch (Throwable t) {
      log.error(
          "Exception raised while popping message, would pop message later, mq={}.",
          messageQueue,
          t);
      popMessageLater();
    }
  }

  private boolean leaseNextTerm(long currentTermId, String target) {
    if (0 >= currentTermId) {
      log.warn("Version of target is too old, mq={}, target={}", messageQueue.simpleName(), target);
    }
    if (currentTermId < termId.get()) {
      return false;
    }
    final boolean acquired = termId.compareAndSet(currentTermId, currentTermId + 1);
    if (acquired) {
      log.debug("Lease acquired, mq={}, new termId={}", messageQueue.simpleName(), termId.get());
    }
    return acquired;
  }

  private AckMessageRequest wrapAckMessageRequest(MessageExt messageExt) {
    return AckMessageRequest.newBuilder()
        .setConsumerGroup(consumerImpl.getDefaultMQPushConsumer().getConsumerGroup())
        .setTopic(messageExt.getTopic())
        .setQueueId(messageExt.getQueueId())
        .setOffset(messageExt.getQueueOffset())
        .setExtraInfo(messageExt.getProperty(MessageConst.PROPERTY_POP_CK))
        .build();
  }

  private ChangeInvisibleTimeRequest wrapChangeInvisibleTimeRequest(MessageExt messageExt) {
    return ChangeInvisibleTimeRequest.newBuilder()
        .setConsumerGroup(consumerImpl.getDefaultMQPushConsumer().getConsumerGroup())
        .setTopic(messageExt.getTopic())
        .setQueueId(messageExt.getQueueId())
        .setOffset(messageExt.getQueueOffset())
        .setExtraInfo(messageExt.getProperty(MessageConst.PROPERTY_POP_CK))
        .setInvisibleTime(MixAll.DEFAULT_INVISIBLE_TIME_MILLIS)
        .build();
  }

  private PopMessageRequest wrapPopMessageRequest() {
    final PopMessageRequest.Builder builder =
        PopMessageRequest.newBuilder()
            .setConsumerGroup(consumerImpl.getDefaultMQPushConsumer().getConsumerGroup())
            .setTopic(messageQueue.getTopic())
            .setBrokerName(messageQueue.getBrokerName())
            .setQueueId(messageQueue.getQueueId())
            .setMaxMessageNumber(MixAll.DEFAULT_MAX_MESSAGE_NUMBER_PRE_BATCH)
            .setInvisibleTime(MixAll.DEFAULT_INVISIBLE_TIME_MILLIS)
            .setPollTime(MixAll.DEFAULT_POLL_TIME_MILLIS)
            .setBornTimestamp(System.currentTimeMillis())
            .setExpression(filterExpression.getExpression());

    final ExpressionType expressionType = filterExpression.getExpressionType();
    switch (expressionType) {
      case SQL92:
        builder.setExpressionType(ExpressionType.SQL92.getType());
        break;
      case TAG:
        builder.setExpressionType(ExpressionType.TAG.getType());
        break;
      default:
        log.error(
            "Unknown filter expression type={}, expression string={}",
            expressionType,
            filterExpression.getExpression());
    }

    switch (consumerImpl.getDefaultMQPushConsumer().getConsumeFromWhere()) {
      case CONSUME_FROM_LAST_OFFSET:
        builder.setInitialMode(PopMessageRequest.ConsumeInitialMode.MAX);
        break;
      case CONSUME_FROM_FIRST_OFFSET:
        builder.setInitialMode(PopMessageRequest.ConsumeInitialMode.MIN);
        break;
      default:
        log.warn("Unknown initial consume mode, fall back to max mode.");
        builder.setInitialMode(PopMessageRequest.ConsumeInitialMode.MAX);
    }

    builder.setOrder(false);
    builder.setTermId(termId.get());
    return builder.build();
  }

  public long getCachedMsgCount() {
    return cachedMsgCount.get();
  }

  public long getCachedMsgSize() {
    return cachedMsgSize.get();
  }
}
