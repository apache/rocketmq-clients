# Workflow

This document elaborates the unified workflow of client. The specific implementations maybe differ from language to language, but they follow the same workflow.

## Startup

Different from previous clients, the new version adds some preparations during the startup. One benefit of this change is to catch more obvious errors or exceptions earlier. These perparations include:

1. Try to fetch route data of topics.
2. Try to get settings from the server, which could do hot-update about these settings. we call this process **server-client telemetry**.

Failure of any preparation will result in the failure of client startup.

<div align="center">
<img src="./artwork/client_startup_process.png" width="80%">
</div>

In details, the **server-client telemetry** provides a channel to upload the local settings and to overwrite the client settings.

## Periodic Task

The client performs same tasks periodically.

* Update topic route data and cache it. The subsequent request could get route from cache directly.
* Send heartbeat to keep alive.
* Send **server-client telemetry** request. Client settings may be overwritten by telemetry response.

<div align="center">
<img src="./artwork/client_periodic_task.png" width="60%">
</div>

## Message Flow in Producer

The workflow to publish a single message of NORMAL type. The message publishing of other types and publishing of batch messages is similar to it. Some special cases will be explained later.

<div align="center">
<img src="./artwork/message_publishing_in_producer.png" width="70%">
</div>

The publishing procedure is as follows:

1. Check if topic route is cached before or not.
2. If topic route is not cached, then try to fetch it from server, otherwise go to step 4.
3. Return failure and end the current process if topic route is failed to fetch, otherwise cache the topic route and go to the next step.
4. Select writable candicate message queues from topic route to publish message.
5. Return failure and end the current process if the type of message queue is not matched with message type.
6. Attempt to publish message.
7. Return success and end the current process if message is published successfully.
8. Catch the error information of failed message publishing.
9. Return failure and end the current process if the attempt times is run out, otherwirse deecide to retry or not according to the error type.
10. Return failure and end the current process if there is no need to retry, otherwise go to the next step.
11. Isolate the current endpoint for publishing.
12. Rotate to next message queue to publish message, and go to step 6.

> **Note**: The failure of message publishing will isolate the endpoint, this makes the endpoint will not be selected for load balancing as much as possible. The periodic heartbeat to the isolate endpoint will check health status about it and remove it from the isolate endpoints if no more exception is detected.

For TRANSACTIONAL messages, the publishing will not be retried if failure is encountered. The ordering of FIFO message is based on the assumption that messages which have the same `message group` will be put into the same message queue, thus the message queue to publish is not disturbed by the isolated endpoint([SipHash](https://en.wikipedia.org/wiki/SipHash) algorithm is used to calculate the message queue index for FIFO message publishing).

About the message publishing retry policy when failure is encountered, in order to ensure the timeliness of message publishing, the next attempt would be executed immediately in most case if flow control does not encountered. Actually the server would deliver the explicit retry policy flow control for each producer while publishing flow control occurs.

<div align="center">
<img src="./artwork/fifo_message_group_message_queue_mapping.png" width="50%">
</div>

## Message Flow in Push Consumer

### Message Receiving in Push Consumer

<div align="center">
<img src="./artwork/message_receiving_in_push_consumer.png" width="70%">
</div>

The receiving procedure is as follows:

1. Fetch the latest queue assignment from server.
2. If flow control occurs during message receiving, consumer will retry after 20 milliseconds, otherwise go to step3.
3. Cache message and trigger the consumption(Once the lifecycle of message is over, it will removed from cache immediately).
4. Check if the cache is full. If the cache is full, the consumer will attempt to receive the message after 1 second; otherwise, it will retry immediately.

### Message Consumption in Push Consumer(Non-FIFO)

<div align="center">
<img src="./artwork/message_consumption_in_push_consumer_non_FIFO.png" width="50%">
</div>

In push consumer, we provide retry policies for acknowledging messages and changing the invisible time to ensure the reliability of message processing.

When calling `ackMessage` or `changeInvisibleDuration` methods, if receiving a non-OK response code, we will log the error and retry later. We will retry the method with an increasing attempt value until it succeeds. If receiving an `INVALID_RECEIPT_HANDLE` response code, we will not retry and directly return the exception to the caller.

### Message Consumption in Push Consumer(FIFO)

<div align="center">
<img src="./artwork/message_consumption_in_push_consumer_FIFO.png" width="60%">
</div>

## Message Flow in Simple Consumer

<div align="center">
<img src="./artwork/message_receiving_and_process_in_simple_consumer.png" width="50%">
</div>
