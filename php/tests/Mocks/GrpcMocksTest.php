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

/**
 * GrpcMocks 工厂类的单元测试
 */
class GrpcMocksTest extends TestCase
{
    /**
     * 测试创建成功的 Status 对象
     */
    public function testSuccessStatus(): void
    {
        $status = GrpcMocks::successStatus();
        
        $this->assertEquals(Code::OK, $status->getCode());
        $this->assertEquals('OK', $status->getMessage());
    }

    /**
     * 测试创建带自定义消息的成功 Status
     */
    public function testSuccessStatusWithCustomMessage(): void
    {
        $status = GrpcMocks::successStatus('Operation successful');
        
        $this->assertEquals(Code::OK, $status->getCode());
        $this->assertEquals('Operation successful', $status->getMessage());
    }

    /**
     * 测试创建失败的 Status 对象
     */
    public function testErrorStatus(): void
    {
        $status = GrpcMocks::errorStatus(Code::INTERNAL_ERROR, 'Server error');
        
        $this->assertEquals(Code::INTERNAL_ERROR, $status->getCode());
        $this->assertEquals('Server error', $status->getMessage());
    }

    /**
     * 测试创建成功的 SendMessageResponse
     */
    public function testMockSendMessageSuccess(): void
    {
        $response = GrpcMocks::mockSendMessageSuccess();
        
        $this->assertTrue($response->hasStatus());
        $this->assertEquals(Code::OK, $response->getStatus()->getCode());
        $this->assertEmpty($response->getEntries());
    }

    /**
     * 测试创建带条目的成功 SendMessageResponse
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
     * 测试创建失败的 SendMessageResponse
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
     * 测试创建 SendResultEntry
     */
    public function testMockSendResultEntry(): void
    {
        $entry = GrpcMocks::mockSendResultEntry('msg-123', 'txn-456');
        
        $this->assertEquals('msg-123', $entry->getMessageId());
        $this->assertEquals('txn-456', $entry->getTransactionId());
    }

    /**
     * 测试创建带错误的 SendResultEntry
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
     * 测试创建成功的 AckMessageResponse
     */
    public function testMockAckMessageSuccess(): void
    {
        $response = GrpcMocks::mockAckMessageSuccess();
        
        $this->assertTrue($response->hasStatus());
        $this->assertEquals(Code::OK, $response->getStatus()->getCode());
    }

    /**
     * 测试创建带条目的成功 AckMessageResponse
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
     * 测试创建失败的 AckMessageResponse
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
     * 测试创建 AckMessageResultEntry
     */
    public function testMockAckMessageResultEntry(): void
    {
        $entry = GrpcMocks::mockAckMessageResultEntry('receipt-handle-xyz');
        
        $this->assertEquals('receipt-handle-xyz', $entry->getReceiptHandle());
    }

    /**
     * 测试创建带错误的 AckMessageResultEntry
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
     * 测试创建成功的 ChangeInvisibleDurationResponse
     */
    public function testMockChangeInvisibleDurationSuccess(): void
    {
        $response = GrpcMocks::mockChangeInvisibleDurationSuccess('new-receipt-handle');
        
        $this->assertEquals(Code::OK, $response->getStatus()->getCode());
        $this->assertEquals('new-receipt-handle', $response->getReceiptHandle());
    }

    /**
     * 测试创建失败的 ChangeInvisibleDurationResponse
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
     * 测试创建成功的 HeartbeatResponse
     */
    public function testMockHeartbeatSuccess(): void
    {
        $response = GrpcMocks::mockHeartbeatSuccess();
        
        $this->assertTrue($response->hasStatus());
        $this->assertEquals(Code::OK, $response->getStatus()->getCode());
    }

    /**
     * 测试创建失败的 HeartbeatResponse
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
     * 测试创建成功的 ForwardMessageToDeadLetterQueueResponse
     */
    public function testMockForwardToDlqSuccess(): void
    {
        $response = GrpcMocks::mockForwardToDlqSuccess();
        
        $this->assertTrue($response->hasStatus());
        $this->assertEquals(Code::OK, $response->getStatus()->getCode());
    }

    /**
     * 测试创建失败的 ForwardMessageToDeadLetterQueueResponse
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
     * 测试批量创建 SendResultEntry
     */
    public function testMockMultipleSendResultEntries(): void
    {
        $entries = GrpcMocks::mockMultipleSendResultEntries(5, 'batch-');
        
        $this->assertCount(5, $entries);
        $this->assertEquals('batch-1', $entries[0]->getMessageId());
        $this->assertEquals('batch-5', $entries[4]->getMessageId());
    }

    /**
     * 测试批量创建 AckMessageResultEntry
     */
    public function testMockMultipleAckResultEntries(): void
    {
        $entries = GrpcMocks::mockMultipleAckResultEntries(3, 'rcpt-');
        
        $this->assertCount(3, $entries);
        $this->assertEquals('rcpt-1', $entries[0]->getReceiptHandle());
        $this->assertEquals('rcpt-3', $entries[2]->getReceiptHandle());
    }

    /**
     * 测试默认错误代码
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
}
