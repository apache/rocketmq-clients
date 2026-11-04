<?php
/**
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
namespace Tests\Mocks;

use Apache\Rocketmq\V2\Status;
use Apache\Rocketmq\V2\Code;
use Apache\Rocketmq\V2\SendMessageResponse;
use Apache\Rocketmq\V2\AckMessageResponse;
use Apache\Rocketmq\V2\ChangeInvisibleDurationResponse;
use Apache\Rocketmq\V2\HeartbeatResponse;
use Apache\Rocketmq\V2\ForwardMessageToDeadLetterQueueResponse;
use Apache\Rocketmq\V2\SendResultEntry;
use Apache\Rocketmq\V2\AckMessageResultEntry;
use Apache\Rocketmq\V2\QueryRouteResponse;
use Apache\Rocketmq\V2\ReceiveMessageResponse;
use Apache\Rocketmq\V2\EndTransactionResponse;
use Apache\Rocketmq\V2\QueryAssignmentResponse;
use Apache\Rocketmq\V2\MessageQueue;
use Apache\Rocketmq\V2\Assignment;
use Apache\Rocketmq\V2\Message;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\Broker;

/**
 * gRPC Mock Factory Class
 * 
 * Provides unified gRPC response object creation methods for unit and integration tests
 */
class GrpcMocks
{
    /**
     * Create a successful Status  object
     *
     * @param string $message Optional success message
     * @return Status
     */
    public static function successStatus(string $message = 'OK'): Status
    {
        $status = new Status();
        $status->setCode(Code::OK);
        $status->setMessage($message);
        
        return $status;
    }

    /**
     * Create a failed Status  object
     *
     * @param int $code Error code
     * @param string $message Error message
     * @return Status
     */
    public static function errorStatus(int $code = Code::INTERNAL_ERROR, string $message = 'Internal Error'): Status
    {
        $status = new Status();
        $status->setCode($code);
        $status->setMessage($message);
        
        return $status;
    }

    /**
     * Create SendMessageResponse Mock  object (success)
     *
     * @param array $entries Array of send result entries
     * @param string $message Success message
     * @return SendMessageResponse
     */
    public static function mockSendMessageSuccess(array $entries = [], string $message = 'OK'): SendMessageResponse
    {
        $response = new SendMessageResponse();
        $response->setStatus(self::successStatus($message));
        
        if (!empty($entries)) {
            $response->setEntries($entries);
        }
        
        return $response;
    }

    /**
     * Create SendMessageResponse Mock  object (failure)
     *
     * @param int $code Error code
     * @param string $message Error message
     * @return SendMessageResponse
     */
    public static function mockSendMessageError(int $code = Code::INTERNAL_ERROR, string $message = 'Failed to send message'): SendMessageResponse
    {
        $response = new SendMessageResponse();
        $response->setStatus(self::errorStatus($code, $message));
        $response->setEntries([]);
        
        return $response;
    }

    /**
     * Create a single SendResultEntry
     *
     * @param string $messageId Message ID
     * @param string $transactionId Transaction ID (optional)
     * @param int $code Status code
     * @param string $errorMessage Error message (optional)
     * @return SendResultEntry
     */
    public static function mockSendResultEntry(
        string $messageId,
        string $transactionId = '',
        int $code = Code::OK,
        string $errorMessage = ''
    ): SendResultEntry {
        $entry = new SendResultEntry();
        $entry->setMessageId($messageId);
        
        if (!empty($transactionId)) {
            $entry->setTransactionId($transactionId);
        }
        
        if ($code !== Code::OK) {
            $status = self::errorStatus($code, $errorMessage ?: 'Send failed');
            $entry->setStatus($status);
        }
        
        return $entry;
    }

    /**
     * Create AckMessageResponse Mock  object (success)
     *
     * @param array $entries Array of ACK result entries
     * @param string $message Success message
     * @return AckMessageResponse
     */
    public static function mockAckMessageSuccess(array $entries = [], string $message = 'OK'): AckMessageResponse
    {
        $response = new AckMessageResponse();
        $response->setStatus(self::successStatus($message));
        
        if (!empty($entries)) {
            $response->setEntries($entries);
        }
        
        return $response;
    }

    /**
     * Create AckMessageResponse Mock  object (failure)
     *
     * @param int $code Error code
     * @param string $message Error message
     * @return AckMessageResponse
     */
    public static function mockAckMessageError(int $code = Code::INVALID_RECEIPT_HANDLE, string $message = 'Failed to ack message'): AckMessageResponse
    {
        $response = new AckMessageResponse();
        $response->setStatus(self::errorStatus($code, $message));
        $response->setEntries([]);
        
        return $response;
    }

    /**
     * Create a single AckMessageResultEntry
     *
     * @param string $receiptHandle Receipt handle
     * @param int $code Status code
     * @param string $errorMessage Error message (optional)
     * @return AckMessageResultEntry
     */
    public static function mockAckMessageResultEntry(
        string $receiptHandle,
        int $code = Code::OK,
        string $errorMessage = ''
    ): AckMessageResultEntry {
        $entry = new AckMessageResultEntry();
        $entry->setReceiptHandle($receiptHandle);
        
        if ($code !== Code::OK) {
            $status = self::errorStatus($code, $errorMessage ?: 'Ack failed');
            $entry->setStatus($status);
        }
        
        return $entry;
    }

    /**
     * Create ChangeInvisibleDurationResponse Mock  object (success)
     *
     * @param string $receiptHandle newReceipt handle
     * @param string $message Success message
     * @return ChangeInvisibleDurationResponse
     */
    public static function mockChangeInvisibleDurationSuccess(string $receiptHandle = '', string $message = 'OK'): ChangeInvisibleDurationResponse
    {
        $response = new ChangeInvisibleDurationResponse();
        $response->setStatus(self::successStatus($message));
        
        if (!empty($receiptHandle)) {
            $response->setReceiptHandle($receiptHandle);
        }
        
        return $response;
    }

    /**
     * Create ChangeInvisibleDurationResponse Mock  object (failure)
     *
     * @param int $code Error code
     * @param string $message Error message
     * @return ChangeInvisibleDurationResponse
     */
    public static function mockChangeInvisibleDurationError(int $code = Code::INVALID_RECEIPT_HANDLE, string $message = 'Failed to change invisible duration'): ChangeInvisibleDurationResponse
    {
        $response = new ChangeInvisibleDurationResponse();
        $response->setStatus(self::errorStatus($code, $message));
        
        return $response;
    }

    /**
     * Create HeartbeatResponse Mock  object (success)
     *
     * @param string $message Success message
     * @return HeartbeatResponse
     */
    public static function mockHeartbeatSuccess(string $message = 'OK'): HeartbeatResponse
    {
        $response = new HeartbeatResponse();
        $response->setStatus(self::successStatus($message));
        
        return $response;
    }

    /**
     * Create HeartbeatResponse Mock  object (failure)
     *
     * @param int $code Error code
     * @param string $message Error message
     * @return HeartbeatResponse
     */
    public static function mockHeartbeatError(int $code = Code::UNRECOGNIZED_CLIENT_TYPE, string $message = 'Heartbeat failed'): HeartbeatResponse
    {
        $response = new HeartbeatResponse();
        $response->setStatus(self::errorStatus($code, $message));
        
        return $response;
    }

    /**
     * Create ForwardMessageToDeadLetterQueueResponse Mock  object (success)
     *
     * @param string $message Success message
     * @return ForwardMessageToDeadLetterQueueResponse
     */
    public static function mockForwardToDlqSuccess(string $message = 'OK'): ForwardMessageToDeadLetterQueueResponse
    {
        $response = new ForwardMessageToDeadLetterQueueResponse();
        $response->setStatus(self::successStatus($message));
        
        return $response;
    }

    /**
     * Create ForwardMessageToDeadLetterQueueResponse Mock  object (failure)
     *
     * @param int $code Error code
     * @param string $message Error message
     * @return ForwardMessageToDeadLetterQueueResponse
     */
    public static function mockForwardToDlqError(int $code = Code::MESSAGE_NOT_FOUND, string $message = 'Failed to forward to DLQ'): ForwardMessageToDeadLetterQueueResponse
    {
        $response = new ForwardMessageToDeadLetterQueueResponse();
        $response->setStatus(self::errorStatus($code, $message));
        
        return $response;
    }

    /**
     * Batch create successful SendResultEntry entries
     *
     * @param int $count Number of entries to create
     * @param string $messageIdPrefix Message ID prefix
     * @return array
     */
    public static function mockMultipleSendResultEntries(int $count = 3, string $messageIdPrefix = 'msg-'): array
    {
        $entries = [];
        for ($i = 1; $i <= $count; $i++) {
            $entries[] = self::mockSendResultEntry($messageIdPrefix . $i);
        }
        
        return $entries;
    }

    /**
     * Batch create successful AckMessageResultEntry entries
     *
     * @param int $count Number of entries to create
     * @param string $receiptHandlePrefix Receipt handleprefix
     * @return array
     */
    public static function mockMultipleAckResultEntries(int $count = 3, string $receiptHandlePrefix = 'receipt-'): array
    {
        $entries = [];
        for ($i = 1; $i <= $count; $i++) {
            $entries[] = self::mockAckMessageResultEntry($receiptHandlePrefix . $i);
        }
        
        return $entries;
    }

    /**
     * Create QueryRouteResponse Mock  object (success)
     *
     * @param array $messageQueues Array of MessageQueue objects
     * @param string $message Success message
     * @return QueryRouteResponse
     */
    public static function mockQueryRouteSuccess(array $messageQueues = [], string $message = 'OK'): QueryRouteResponse
    {
        $response = new QueryRouteResponse();
        $response->setStatus(self::successStatus($message));

        if (!empty($messageQueues)) {
            $response->setMessageQueues($messageQueues);
        }

        return $response;
    }

    /**
     * Create QueryRouteResponse Mock  object (failure)
     *
     * @param int $code Error code
     * @param string $message Error message
     * @return QueryRouteResponse
     */
    public static function mockQueryRouteError(int $code = Code::NOT_FOUND, string $message = 'Topic not found'): QueryRouteResponse
    {
        $response = new QueryRouteResponse();
        $response->setStatus(self::errorStatus($code, $message));

        return $response;
    }

    /**
     * Create a single MessageQueue  object
     *
     * @param string $topicName Topic name
     * @param int $queueId Queue ID
     * @param string $brokerName Broker name
     * @param string $brokerEndpoint Broker endpoint address
     * @return MessageQueue
     */
    public static function mockMessageQueue(
        string $topicName,
        int $queueId = 0,
        string $brokerName = 'broker-0',
        string $brokerEndpoint = '127.0.0.1:8080'
    ): MessageQueue {
        $topic = new Resource();
        $topic->setName($topicName);

        $broker = new Broker();
        $broker->setName($brokerName);

        $mq = new MessageQueue();
        $mq->setTopic($topic);
        $mq->setId($queueId);
        $mq->setBroker($broker);

        return $mq;
    }

    /**
     * Create a ReceiveMessageResponse Mock object (success, with message)
     *
     * NOTE: ReceiveMessageResponse uses oneof, status and message are mutually exclusive.
     * The actual broker sends multiple responses: first status, then message, then delivery_timestamp.
     * This mock returns a response containing the message.
     *
     * @param Message|null $message Protobuf Message object
     * @return ReceiveMessageResponse
     */
    public static function mockReceiveMessageSuccess(?Message $message = null): ReceiveMessageResponse
    {
        $response = new ReceiveMessageResponse();

        if ($message !== null) {
            // oneof: setting message clears status
            $response->setMessage($message);
        } else {
            $response->setStatus(self::successStatus());
        }

        return $response;
    }

    /**
     * Create a ReceiveMessageResponse Mock object (no message)
     *
     * @return ReceiveMessageResponse
     */
    public static function mockReceiveMessageEmpty(): ReceiveMessageResponse
    {
        $response = new ReceiveMessageResponse();
        $response->setStatus(self::successStatus());

        return $response;
    }

    /**
     * Create ReceiveMessageResponse Mock  object (failure)
     *
     * @param int $code Error code
     * @param string $message Error message
     * @return ReceiveMessageResponse
     */
    public static function mockReceiveMessageError(int $code = Code::INTERNAL_ERROR, string $message = 'Receive failed'): ReceiveMessageResponse
    {
        $response = new ReceiveMessageResponse();
        $response->setStatus(self::errorStatus($code, $message));

        return $response;
    }

    /**
     * Create a Protobuf Message object
     *
     * @param string $topicName Topic name
     * @param string $body Message body
     * @param string $messageId Message ID
     * @return Message
     */
    public static function mockProtobufMessage(string $topicName, string $body, string $messageId = ''): Message
    {
        $topic = new Resource();
        $topic->setName($topicName);

        $message = new Message();
        $message->setTopic($topic);
        $message->setBody($body);

        return $message;
    }

    /**
     * Create EndTransactionResponse Mock  object (success)
     *
     * @param string $message Success message
     * @return EndTransactionResponse
     */
    public static function mockEndTransactionSuccess(string $message = 'OK'): EndTransactionResponse
    {
        $response = new EndTransactionResponse();
        $response->setStatus(self::successStatus($message));

        return $response;
    }

    /**
     * Create EndTransactionResponse Mock  object (failure)
     *
     * @param int $code Error code
     * @param string $message Error message
     * @return EndTransactionResponse
     */
    public static function mockEndTransactionError(int $code = Code::INVALID_TRANSACTION_ID, string $message = 'Transaction failed'): EndTransactionResponse
    {
        $response = new EndTransactionResponse();
        $response->setStatus(self::errorStatus($code, $message));

        return $response;
    }

    /**
     * Create QueryAssignmentResponse Mock  object (success)
     *
     * @param array $assignments Array of Assignment objects
     * @param string $message Success message
     * @return QueryAssignmentResponse
     */
    public static function mockQueryAssignmentSuccess(array $assignments = [], string $message = 'OK'): QueryAssignmentResponse
    {
        $response = new QueryAssignmentResponse();
        $response->setStatus(self::successStatus($message));

        if (!empty($assignments)) {
            $response->setAssignments($assignments);
        }

        return $response;
    }

    /**
     * Create QueryAssignmentResponse Mock  object (failure)
     *
     * @param int $code Error code
     * @param string $message Error message
     * @return QueryAssignmentResponse
     */
    public static function mockQueryAssignmentError(int $code = Code::NOT_FOUND, string $message = 'Assignment not found'): QueryAssignmentResponse
    {
        $response = new QueryAssignmentResponse();
        $response->setStatus(self::errorStatus($code, $message));

        return $response;
    }

    /**
     * Create a single Assignment  object
     *
     * @param string $topicName Topic name
     * @param int $queueId Queue ID
     * @param string $brokerName Broker name
     * @return Assignment
     */
    public static function mockAssignment(string $topicName, int $queueId = 0, string $brokerName = 'broker-0'): Assignment
    {
        $mq = self::mockMessageQueue($topicName, $queueId, $brokerName);

        $assignment = new Assignment();
        $assignment->setMessageQueue($mq);

        return $assignment;
    }
}
