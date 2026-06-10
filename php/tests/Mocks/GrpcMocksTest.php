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

use PHPUnit\Framework\TestCase;
use Apache\Rocketmq\V2\Code;
use Apache\Rocketmq\V2\QueryRouteResponse;
use Apache\Rocketmq\V2\ReceiveMessageResponse;
use Apache\Rocketmq\V2\EndTransactionResponse;
use Apache\Rocketmq\V2\QueryAssignmentResponse;

/**
 * Unit tests for GrpcMocks factory class
 */
class GrpcMocksTest extends TestCase
{
    /**
     * Test creating a successful Status object
     */
    public function testSuccessStatus(): void
    {
        $status = GrpcMocks::successStatus();
        
        $this->assertEquals(Code::OK, $status->getCode());
        $this->assertEquals('OK', $status->getMessage());
    }

    /**
     * Test creating successful Status with custom message
     */
    public function testSuccessStatusWithCustomMessage(): void
    {
        $status = GrpcMocks::successStatus('Operation successful');
        
        $this->assertEquals(Code::OK, $status->getCode());
        $this->assertEquals('Operation successful', $status->getMessage());
    }

    /**
     * Test creating a failed Status object
     */
    public function testErrorStatus(): void
    {
        $status = GrpcMocks::errorStatus(Code::INTERNAL_ERROR, 'Server error');
        
        $this->assertEquals(Code::INTERNAL_ERROR, $status->getCode());
        $this->assertEquals('Server error', $status->getMessage());
    }

    /**
     * Test creating a successful SendMessageResponse
     */
    public function testMockSendMessageSuccess(): void
    {
        $response = GrpcMocks::mockSendMessageSuccess();
        
        $this->assertTrue($response->hasStatus());
        $this->assertEquals(Code::OK, $response->getStatus()->getCode());
        $this->assertEmpty($response->getEntries());
    }

    /**
     * Test creating successful with entries SendMessageResponse
     */
    public function testMockSendMessageSuccessWithEntries(): void
    {
        $entries = [
            GrpcMocks::mockSendResultEntry('msg-001'),
            GrpcMocks::mockSendResultEntry('msg-002', 'txn-001'),
        ];
        
        $response = GrpcMocks::mockSendMessageSuccess($entries);
        
        $this->assertEquals(Code::OK, $response->getStatus()->getCode());
        $this->assertCount(2, $response->getEntries());
        $this->assertEquals('msg-001', $response->getEntries()[0]->getMessageId());
    }

    /**
     * Test creating a failed SendMessageResponse
     */
    public function testMockSendMessageError(): void
    {
        $response = GrpcMocks::mockSendMessageError(
            Code::MESSAGE_BODY_TOO_LARGE,
            'Message too large'
        );
        
        $this->assertEquals(Code::MESSAGE_BODY_TOO_LARGE, $response->getStatus()->getCode());
        $this->assertEquals('Message too large', $response->getStatus()->getMessage());
        $this->assertEmpty($response->getEntries());
    }

    /**
     * Test creating SendResultEntry
     */
    public function testMockSendResultEntry(): void
    {
        $entry = GrpcMocks::mockSendResultEntry('msg-123', 'txn-456');
        
        $this->assertEquals('msg-123', $entry->getMessageId());
        $this->assertEquals('txn-456', $entry->getTransactionId());
    }

    /**
     * Test creating with error SendResultEntry
     */
    public function testMockSendResultEntryWithError(): void
    {
        $entry = GrpcMocks::mockSendResultEntry(
            'msg-failed',
            '',
            Code::INTERNAL_ERROR,
            'Send failed'
        );
        
        $this->assertEquals('msg-failed', $entry->getMessageId());
        $this->assertTrue($entry->hasStatus());
        $this->assertEquals(Code::INTERNAL_ERROR, $entry->getStatus()->getCode());
        $this->assertEquals('Send failed', $entry->getStatus()->getMessage());
    }

    /**
     * Test creating a successful AckMessageResponse
     */
    public function testMockAckMessageSuccess(): void
    {
        $response = GrpcMocks::mockAckMessageSuccess();
        
        $this->assertTrue($response->hasStatus());
        $this->assertEquals(Code::OK, $response->getStatus()->getCode());
    }

    /**
     * Test creating successful with entries AckMessageResponse
     */
    public function testMockAckMessageSuccessWithEntries(): void
    {
        $entries = [
            GrpcMocks::mockAckMessageResultEntry('receipt-001'),
            GrpcMocks::mockAckMessageResultEntry('receipt-002'),
        ];
        
        $response = GrpcMocks::mockAckMessageSuccess($entries);
        
        $this->assertCount(2, $response->getEntries());
        $this->assertEquals('receipt-001', $response->getEntries()[0]->getReceiptHandle());
    }

    /**
     * Test creating a failed AckMessageResponse
     */
    public function testMockAckMessageError(): void
    {
        $response = GrpcMocks::mockAckMessageError(
            Code::INVALID_RECEIPT_HANDLE,
            'Invalid receipt'
        );
        
        $this->assertEquals(Code::INVALID_RECEIPT_HANDLE, $response->getStatus()->getCode());
    }

    /**
     * Test creating AckMessageResultEntry
     */
    public function testMockAckMessageResultEntry(): void
    {
        $entry = GrpcMocks::mockAckMessageResultEntry('receipt-handle-xyz');
        
        $this->assertEquals('receipt-handle-xyz', $entry->getReceiptHandle());
    }

    /**
     * Test creating with error AckMessageResultEntry
     */
    public function testMockAckMessageResultEntryWithError(): void
    {
        $entry = GrpcMocks::mockAckMessageResultEntry(
            'receipt-handle',
            Code::INVALID_RECEIPT_HANDLE,
            'Receipt expired'
        );
        
        $this->assertTrue($entry->hasStatus());
        $this->assertEquals(Code::INVALID_RECEIPT_HANDLE, $entry->getStatus()->getCode());
        $this->assertEquals('Receipt expired', $entry->getStatus()->getMessage());
    }

    /**
     * Test creating a successful ChangeInvisibleDurationResponse
     */
    public function testMockChangeInvisibleDurationSuccess(): void
    {
        $response = GrpcMocks::mockChangeInvisibleDurationSuccess('new-receipt-handle');
        
        $this->assertEquals(Code::OK, $response->getStatus()->getCode());
        $this->assertEquals('new-receipt-handle', $response->getReceiptHandle());
    }

    /**
     * Test creating a failed ChangeInvisibleDurationResponse
     */
    public function testMockChangeInvisibleDurationError(): void
    {
        $response = GrpcMocks::mockChangeInvisibleDurationError(
            Code::INVALID_RECEIPT_HANDLE,
            'Cannot change duration'
        );
        
        $this->assertEquals(Code::INVALID_RECEIPT_HANDLE, $response->getStatus()->getCode());
    }

    /**
     * Test creating a successful HeartbeatResponse
     */
    public function testMockHeartbeatSuccess(): void
    {
        $response = GrpcMocks::mockHeartbeatSuccess();
        
        $this->assertTrue($response->hasStatus());
        $this->assertEquals(Code::OK, $response->getStatus()->getCode());
    }

    /**
     * Test creating a failed HeartbeatResponse
     */
    public function testMockHeartbeatError(): void
    {
        $response = GrpcMocks::mockHeartbeatError(
            Code::UNRECOGNIZED_CLIENT_TYPE,
            'Unknown client'
        );
        
        $this->assertEquals(Code::UNRECOGNIZED_CLIENT_TYPE, $response->getStatus()->getCode());
    }

    /**
     * Test creating a successful ForwardMessageToDeadLetterQueueResponse
     */
    public function testMockForwardToDlqSuccess(): void
    {
        $response = GrpcMocks::mockForwardToDlqSuccess();
        
        $this->assertTrue($response->hasStatus());
        $this->assertEquals(Code::OK, $response->getStatus()->getCode());
    }

    /**
     * Test creating a failed ForwardMessageToDeadLetterQueueResponse
     */
    public function testMockForwardToDlqError(): void
    {
        $response = GrpcMocks::mockForwardToDlqError(
            Code::MESSAGE_NOT_FOUND,
            'Message not found'
        );
        
        $this->assertEquals(Code::MESSAGE_NOT_FOUND, $response->getStatus()->getCode());
    }

    /**
     * Test batch creating SendResultEntry
     */
    public function testMockMultipleSendResultEntries(): void
    {
        $entries = GrpcMocks::mockMultipleSendResultEntries(5, 'batch-');
        
        $this->assertCount(5, $entries);
        $this->assertEquals('batch-1', $entries[0]->getMessageId());
        $this->assertEquals('batch-5', $entries[4]->getMessageId());
    }

    /**
     * Test batch creating AckMessageResultEntry
     */
    public function testMockMultipleAckResultEntries(): void
    {
        $entries = GrpcMocks::mockMultipleAckResultEntries(3, 'rcpt-');
        
        $this->assertCount(3, $entries);
        $this->assertEquals('rcpt-1', $entries[0]->getReceiptHandle());
        $this->assertEquals('rcpt-3', $entries[2]->getReceiptHandle());
    }

    /**
     * Test defaultError code
     */
    public function testDefaultErrorCodes(): void
    {
        $sendResponse = GrpcMocks::mockSendMessageError();
        $this->assertEquals(Code::INTERNAL_ERROR, $sendResponse->getStatus()->getCode());
        
        $ackResponse = GrpcMocks::mockAckMessageError();
        $this->assertEquals(Code::INVALID_RECEIPT_HANDLE, $ackResponse->getStatus()->getCode());
        
        $heartbeatResponse = GrpcMocks::mockHeartbeatError();
        $this->assertEquals(Code::UNRECOGNIZED_CLIENT_TYPE, $heartbeatResponse->getStatus()->getCode());
        
        $dlqResponse = GrpcMocks::mockForwardToDlqError();
        $this->assertEquals(Code::MESSAGE_NOT_FOUND, $dlqResponse->getStatus()->getCode());
    }

    // ==================== QueryRouteResponse Tests ====================

    /**
     * Test creating a successful QueryRouteResponse
     */
    public function testMockQueryRouteSuccess(): void
    {
        $response = GrpcMocks::mockQueryRouteSuccess();

        $this->assertTrue($response->hasStatus());
        $this->assertEquals(Code::OK, $response->getStatus()->getCode());
        $this->assertEmpty(iterator_to_array($response->getMessageQueues()));
    }

    /**
     * Test creating a successful QueryRouteResponse with MessageQueue
     */
    public function testMockQueryRouteSuccessWithQueues(): void
    {
        $queues = [
            GrpcMocks::mockMessageQueue('test-topic', 0, 'broker-0', '127.0.0.1:8080'),
            GrpcMocks::mockMessageQueue('test-topic', 1, 'broker-1', '127.0.0.1:8081'),
        ];

        $response = GrpcMocks::mockQueryRouteSuccess($queues);

        $this->assertEquals(Code::OK, $response->getStatus()->getCode());
        $mqList = iterator_to_array($response->getMessageQueues());
        $this->assertCount(2, $mqList);
        /** @var \Apache\Rocketmq\V2\MessageQueue $mq0 */
        $mq0 = $mqList[0];
        /** @var \Apache\Rocketmq\V2\MessageQueue $mq1 */
        $mq1 = $mqList[1];
        $this->assertEquals(0, $mq0->getId());
        $this->assertEquals(1, $mq1->getId());
    }

    /**
     * Test creating a failed QueryRouteResponse
     */
    public function testMockQueryRouteError(): void
    {
        $response = GrpcMocks::mockQueryRouteError(Code::NOT_FOUND, 'Topic not found');

        $this->assertEquals(Code::NOT_FOUND, $response->getStatus()->getCode());
        $this->assertEquals('Topic not found', $response->getStatus()->getMessage());
    }

    /**
     * Test creating a MessageQueue object
     */
    public function testMockMessageQueue(): void
    {
        $mq = GrpcMocks::mockMessageQueue('my-topic', 3, 'broker-x', '10.0.0.1:8080');

        $this->assertTrue($mq->hasTopic());
        $this->assertEquals('my-topic', $mq->getTopic()->getName());
        $this->assertEquals(3, $mq->getId());
        $this->assertTrue($mq->hasBroker());
        $this->assertEquals('broker-x', $mq->getBroker()->getName());
    }

    // ==================== ReceiveMessageResponse Tests ====================

    /**
     * Test creating a successful ReceiveMessageResponse (with message)
     * NOTE: oneof field, setting message clears status
     */
    public function testMockReceiveMessageSuccessWithMessage(): void
    {
        $msg = GrpcMocks::mockProtobufMessage('test-topic', 'hello world');
        $response = GrpcMocks::mockReceiveMessageSuccess($msg);

        // oneof: hasStatus=false when hasMessage=true
        $this->assertTrue($response->hasMessage());
        $this->assertFalse($response->hasStatus());
        $this->assertEquals('hello world', $response->getMessage()->getBody());
    }

    /**
     * Test creating a successful ReceiveMessageResponse (no message, only status)
     */
    public function testMockReceiveMessageEmpty(): void
    {
        $response = GrpcMocks::mockReceiveMessageEmpty();

        $this->assertTrue($response->hasStatus());
        $this->assertEquals(Code::OK, $response->getStatus()->getCode());
        $this->assertFalse($response->hasMessage());
    }

    /**
     * Test creating a failed ReceiveMessageResponse
     */
    public function testMockReceiveMessageError(): void
    {
        $response = GrpcMocks::mockReceiveMessageError(Code::INTERNAL_ERROR, 'Broker unavailable');

        $this->assertEquals(Code::INTERNAL_ERROR, $response->getStatus()->getCode());
        $this->assertEquals('Broker unavailable', $response->getStatus()->getMessage());
    }

    /**
     * Test creating Protobuf Message object
     */
    public function testMockProtobufMessage(): void
    {
        $msg = GrpcMocks::mockProtobufMessage('order-topic', 'order data');

        $this->assertTrue($msg->hasTopic());
        $this->assertEquals('order-topic', $msg->getTopic()->getName());
        $this->assertEquals('order data', $msg->getBody());
    }

    // ==================== EndTransactionResponse Tests ====================

    /**
     * Test creating a successful EndTransactionResponse
     */
    public function testMockEndTransactionSuccess(): void
    {
        $response = GrpcMocks::mockEndTransactionSuccess();

        $this->assertTrue($response->hasStatus());
        $this->assertEquals(Code::OK, $response->getStatus()->getCode());
        $this->assertEquals('OK', $response->getStatus()->getMessage());
    }

    /**
     * Test creating a failed EndTransactionResponse
     */
    public function testMockEndTransactionError(): void
    {
        $response = GrpcMocks::mockEndTransactionError(Code::INVALID_TRANSACTION_ID, 'Invalid transaction');

        $this->assertEquals(Code::INVALID_TRANSACTION_ID, $response->getStatus()->getCode());
        $this->assertEquals('Invalid transaction', $response->getStatus()->getMessage());
    }

    /**
     * Test default EndTransaction Error code
     */
    public function testMockEndTransactionDefaultErrorCode(): void
    {
        $response = GrpcMocks::mockEndTransactionError();

        $this->assertEquals(Code::INVALID_TRANSACTION_ID, $response->getStatus()->getCode());
    }

    // ==================== QueryAssignmentResponse Tests ====================

    /**
     * Test creating a successful QueryAssignmentResponse
     */
    public function testMockQueryAssignmentSuccess(): void
    {
        $response = GrpcMocks::mockQueryAssignmentSuccess();

        $this->assertTrue($response->hasStatus());
        $this->assertEquals(Code::OK, $response->getStatus()->getCode());
        $this->assertEmpty(iterator_to_array($response->getAssignments()));
    }

    /**
     * Test creating successful QueryAssignmentResponse with Assignments
     */
    public function testMockQueryAssignmentSuccessWithAssignments(): void
    {
        $assignments = [
            GrpcMocks::mockAssignment('test-topic', 0, 'broker-0'),
            GrpcMocks::mockAssignment('test-topic', 1, 'broker-1'),
        ];

        $response = GrpcMocks::mockQueryAssignmentSuccess($assignments);

        $this->assertEquals(Code::OK, $response->getStatus()->getCode());
        $list = iterator_to_array($response->getAssignments());
        $this->assertCount(2, $list);
        /** @var \Apache\Rocketmq\V2\Assignment $a0 */
        $a0 = $list[0];
        /** @var \Apache\Rocketmq\V2\Assignment $a1 */
        $a1 = $list[1];
        $this->assertTrue($a0->hasMessageQueue());
        $this->assertEquals(0, $a0->getMessageQueue()->getId());
        $this->assertEquals(1, $a1->getMessageQueue()->getId());
    }

    /**
     * Test creating a failed QueryAssignmentResponse
     */
    public function testMockQueryAssignmentError(): void
    {
        $response = GrpcMocks::mockQueryAssignmentError(Code::NOT_FOUND, 'No assignment');

        $this->assertEquals(Code::NOT_FOUND, $response->getStatus()->getCode());
        $this->assertEquals('No assignment', $response->getStatus()->getMessage());
    }

    /**
     * Test creating an Assignment object
     */
    public function testMockAssignment(): void
    {
        $assignment = GrpcMocks::mockAssignment('my-topic', 5, 'broker-y');

        $this->assertTrue($assignment->hasMessageQueue());
        $mq = $assignment->getMessageQueue();
        $this->assertEquals('my-topic', $mq->getTopic()->getName());
        $this->assertEquals(5, $mq->getId());
        $this->assertEquals('broker-y', $mq->getBroker()->getName());
    }
}
