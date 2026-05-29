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

/**
 * gRPC Mock 工厂类
 * 
 * 提供统一的 gRPC 响应对象创建方法，用于单元测试和集成测试
 */
class GrpcMocks
{
    /**
     * 创建成功的 Status 对象
     *
     * @param string $message 可选的成功消息
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
     * 创建失败的 Status 对象
     *
     * @param int $code 错误代码
     * @param string $message 错误消息
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
     * 创建 SendMessageResponse Mock 对象（成功）
     *
     * @param array $entries 发送结果条目数组
     * @param string $message 成功消息
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
     * 创建 SendMessageResponse Mock 对象（失败）
     *
     * @param int $code 错误代码
     * @param string $message 错误消息
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
     * 创建单个 SendResultEntry
     *
     * @param string $messageId 消息 ID
     * @param string $transactionId 事务 ID（可选）
     * @param int $code 状态码
     * @param string $errorMessage 错误消息（可选）
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
     * 创建 AckMessageResponse Mock 对象（成功）
     *
     * @param array $entries ACK 结果条目数组
     * @param string $message 成功消息
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
     * 创建 AckMessageResponse Mock 对象（失败）
     *
     * @param int $code 错误代码
     * @param string $message 错误消息
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
     * 创建单个 AckMessageResultEntry
     *
     * @param string $receiptHandle 接收句柄
     * @param int $code 状态码
     * @param string $errorMessage 错误消息（可选）
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
     * 创建 ChangeInvisibleDurationResponse Mock 对象（成功）
     *
     * @param string $receiptHandle 新的接收句柄
     * @param string $message 成功消息
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
     * 创建 ChangeInvisibleDurationResponse Mock 对象（失败）
     *
     * @param int $code 错误代码
     * @param string $message 错误消息
     * @return ChangeInvisibleDurationResponse
     */
    public static function mockChangeInvisibleDurationError(int $code = Code::INVALID_RECEIPT_HANDLE, string $message = 'Failed to change invisible duration'): ChangeInvisibleDurationResponse
    {
        $response = new ChangeInvisibleDurationResponse();
        $response->setStatus(self::errorStatus($code, $message));
        
        return $response;
    }

    /**
     * 创建 HeartbeatResponse Mock 对象（成功）
     *
     * @param string $message 成功消息
     * @return HeartbeatResponse
     */
    public static function mockHeartbeatSuccess(string $message = 'OK'): HeartbeatResponse
    {
        $response = new HeartbeatResponse();
        $response->setStatus(self::successStatus($message));
        
        return $response;
    }

    /**
     * 创建 HeartbeatResponse Mock 对象（失败）
     *
     * @param int $code 错误代码
     * @param string $message 错误消息
     * @return HeartbeatResponse
     */
    public static function mockHeartbeatError(int $code = Code::UNRECOGNIZED_CLIENT_TYPE, string $message = 'Heartbeat failed'): HeartbeatResponse
    {
        $response = new HeartbeatResponse();
        $response->setStatus(self::errorStatus($code, $message));
        
        return $response;
    }

    /**
     * 创建 ForwardMessageToDeadLetterQueueResponse Mock 对象（成功）
     *
     * @param string $message 成功消息
     * @return ForwardMessageToDeadLetterQueueResponse
     */
    public static function mockForwardToDlqSuccess(string $message = 'OK'): ForwardMessageToDeadLetterQueueResponse
    {
        $response = new ForwardMessageToDeadLetterQueueResponse();
        $response->setStatus(self::successStatus($message));
        
        return $response;
    }

    /**
     * 创建 ForwardMessageToDeadLetterQueueResponse Mock 对象（失败）
     *
     * @param int $code 错误代码
     * @param string $message 错误消息
     * @return ForwardMessageToDeadLetterQueueResponse
     */
    public static function mockForwardToDlqError(int $code = Code::MESSAGE_NOT_FOUND, string $message = 'Failed to forward to DLQ'): ForwardMessageToDeadLetterQueueResponse
    {
        $response = new ForwardMessageToDeadLetterQueueResponse();
        $response->setStatus(self::errorStatus($code, $message));
        
        return $response;
    }

    /**
     * 批量创建成功的 SendResultEntry
     *
     * @param int $count 需要创建的条目数量
     * @param string $messageIdPrefix 消息 ID 前缀
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
     * 批量创建成功的 AckMessageResultEntry
     *
     * @param int $count 需要创建的条目数量
     * @param string $receiptHandlePrefix 接收句柄前缀
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
}
