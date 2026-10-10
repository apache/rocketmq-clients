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

namespace Apache\Rocketmq\Test;

use PHPUnit\Framework\TestCase;
require_once __DIR__ . '/../autoload.php';

require_once __DIR__ . '/../grpc/Apache/Rocketmq/V2/Code.php';

use Apache\Rocketmq\V2\Code;

/**
 * Tests for gRPC status code groupings and mappings.
 * Mirrors Java's StatusCheckerTest.
 *
 * The PHP client doesn't have a dedicated StatusChecker class like Java,
 * but these tests verify the Code enum constants are correct and
 * document how they should be grouped into exception categories.
 */
class StatusCheckerTest extends TestCase
{
    /**
     * Mirrors Java: testOK - success codes.
     */
    public function testSuccessCodes()
    {
        $this->assertEquals(20000, Code::OK, "OK should be 20000");
        $this->assertEquals(30000, Code::MULTIPLE_RESULTS, "MULTIPLE_RESULTS should be 30000");
    }

    /**
     * Mirrors Java: testBadRequest - all 400xx codes that map to BadRequestException.
     */
    public function testBadRequestCodes()
    {
        $badRequestCodes = [
            Code::BAD_REQUEST,
            Code::ILLEGAL_ACCESS_POINT,
            Code::ILLEGAL_TOPIC,
            Code::ILLEGAL_CONSUMER_GROUP,
            Code::ILLEGAL_LITE_TOPIC,
            Code::ILLEGAL_MESSAGE_TAG,
            Code::ILLEGAL_MESSAGE_KEY,
            Code::ILLEGAL_MESSAGE_GROUP,
            Code::ILLEGAL_MESSAGE_PROPERTY_KEY,
            Code::INVALID_TRANSACTION_ID,
            Code::ILLEGAL_MESSAGE_ID,
            Code::ILLEGAL_FILTER_EXPRESSION,
            Code::ILLEGAL_INVISIBLE_TIME,
            Code::ILLEGAL_DELIVERY_TIME,
            Code::INVALID_RECEIPT_HANDLE,
            Code::MESSAGE_PROPERTY_CONFLICT_WITH_TYPE,
            Code::UNRECOGNIZED_CLIENT_TYPE,
            Code::MESSAGE_CORRUPTED,
            Code::CLIENT_ID_REQUIRED,
            Code::ILLEGAL_POLLING_TIME,
            Code::ILLEGAL_OFFSET,
        ];

        foreach ($badRequestCodes as $code) {
            $isBadRequest = ($code >= 40000 && $code < 40100);
            $this->assertTrue(
                $isBadRequest,
                "Code {$code} (" . Code::name($code) . ") should be in BadRequest range (40000-40099)"
            );
        }
    }

    /**
     * Mirrors Java: testUnauthorized.
     */
    public function testUnauthorizedCode()
    {
        $this->assertEquals(40100, Code::UNAUTHORIZED, "UNAUTHORIZED should be 40100");
    }

    /**
     * Mirrors Java: testPaymentRequired.
     */
    public function testPaymentRequiredCode()
    {
        $this->assertEquals(40200, Code::PAYMENT_REQUIRED, "PAYMENT_REQUIRED should be 40200");
    }

    /**
     * Mirrors Java: testForbidden.
     */
    public function testForbiddenCode()
    {
        $this->assertEquals(40300, Code::FORBIDDEN, "FORBIDDEN should be 40300");
    }

    /**
     * Mirrors Java: testNotFound - all 404xx codes.
     */
    public function testNotFoundCodes()
    {
        $this->assertEquals(40400, Code::NOT_FOUND, "NOT_FOUND should be 40400");
        $this->assertEquals(40401, Code::MESSAGE_NOT_FOUND, "MESSAGE_NOT_FOUND should be 40401");
        $this->assertEquals(40402, Code::TOPIC_NOT_FOUND, "TOPIC_NOT_FOUND should be 40402");
        $this->assertEquals(40403, Code::CONSUMER_GROUP_NOT_FOUND, "CONSUMER_GROUP_NOT_FOUND should be 40403");
        $this->assertEquals(40404, Code::OFFSET_NOT_FOUND, "OFFSET_NOT_FOUND should be 40404");
    }

    /**
     * Mirrors Java: testPayloadTooLarge - all 413xx codes.
     */
    public function testPayloadTooLargeCodes()
    {
        $this->assertEquals(41300, Code::PAYLOAD_TOO_LARGE, "PAYLOAD_TOO_LARGE should be 41300");
        $this->assertEquals(41301, Code::MESSAGE_BODY_TOO_LARGE, "MESSAGE_BODY_TOO_LARGE should be 41301");
        $this->assertEquals(41302, Code::MESSAGE_BODY_EMPTY, "MESSAGE_BODY_EMPTY should be 41302");
    }

    /**
     * Mirrors Java: testTooManyRequests - all 429xx codes.
     */
    public function testTooManyRequestsCodes()
    {
        $this->assertEquals(42900, Code::TOO_MANY_REQUESTS, "TOO_MANY_REQUESTS should be 42900");
        $this->assertEquals(42901, Code::LITE_TOPIC_QUOTA_EXCEEDED, "LITE_TOPIC_QUOTA_EXCEEDED should be 42901");
        $this->assertEquals(42902, Code::LITE_SUBSCRIPTION_QUOTA_EXCEEDED, "LITE_SUBSCRIPTION_QUOTA_EXCEEDED should be 42902");
    }

    /**
     * Mirrors Java: testRequestHeaderFieldsTooLarge.
     */
    public function testRequestHeaderTooLargeCodes()
    {
        $this->assertEquals(43100, Code::REQUEST_HEADER_FIELDS_TOO_LARGE, "REQUEST_HEADER_FIELDS_TOO_LARGE should be 43100");
        $this->assertEquals(43101, Code::MESSAGE_PROPERTIES_TOO_LARGE, "MESSAGE_PROPERTIES_TOO_LARGE should be 43101");
    }

    /**
     * Mirrors Java: testInternalError - all 500xx codes.
     */
    public function testInternalErrorCodes()
    {
        $this->assertEquals(50000, Code::INTERNAL_ERROR, "INTERNAL_ERROR should be 50000");
        $this->assertEquals(50001, Code::INTERNAL_SERVER_ERROR, "INTERNAL_SERVER_ERROR should be 50001");
        $this->assertEquals(50002, Code::HA_NOT_AVAILABLE, "HA_NOT_AVAILABLE should be 50002");
    }

    /**
     * Mirrors Java: testProxyTimeout - all 504xx codes.
     */
    public function testProxyTimeoutCodes()
    {
        $this->assertEquals(50400, Code::PROXY_TIMEOUT, "PROXY_TIMEOUT should be 50400");
        $this->assertEquals(50401, Code::MASTER_PERSISTENCE_TIMEOUT, "MASTER_PERSISTENCE_TIMEOUT should be 50401");
        $this->assertEquals(50402, Code::SLAVE_PERSISTENCE_TIMEOUT, "SLAVE_PERSISTENCE_TIMEOUT should be 50402");
    }

    /**
     * Mirrors Java: testUnsupported - all 505xx codes.
     */
    public function testUnsupportedCodes()
    {
        $this->assertEquals(50500, Code::UNSUPPORTED, "UNSUPPORTED should be 50500");
        $this->assertEquals(50501, Code::VERSION_UNSUPPORTED, "VERSION_UNSUPPORTED should be 50501");
        $this->assertEquals(50502, Code::VERIFY_FIFO_MESSAGE_UNSUPPORTED, "VERIFY_FIFO_MESSAGE_UNSUPPORTED should be 50502");
    }

    /**
     * Tests code name() round-trip for all defined codes.
     */
    public function testAllCodeNamesAreValid()
    {
        $allCodes = [
            Code::CODE_UNSPECIFIED,
            Code::OK,
            Code::MULTIPLE_RESULTS,
            Code::BAD_REQUEST,
            Code::ILLEGAL_ACCESS_POINT,
            Code::ILLEGAL_TOPIC,
            Code::UNAUTHORIZED,
            Code::PAYMENT_REQUIRED,
            Code::FORBIDDEN,
            Code::NOT_FOUND,
            Code::PAYLOAD_TOO_LARGE,
            Code::TOO_MANY_REQUESTS,
            Code::REQUEST_HEADER_FIELDS_TOO_LARGE,
            Code::INTERNAL_ERROR,
            Code::PROXY_TIMEOUT,
            Code::UNSUPPORTED,
            Code::ILLEGAL_OFFSET,
            Code::ILLEGAL_LITE_TOPIC,
            Code::PRECONDITION_FAILED,
            Code::REQUEST_TIMEOUT,
            Code::NOT_IMPLEMENTED,
            Code::FAILED_TO_CONSUME_MESSAGE,
        ];

        foreach ($allCodes as $code) {
            $name = Code::name($code);
            $this->assertTrue(
                is_string($name) && strlen($name) > 0,
                "Code {$code} should have a non-empty name"
            );
        }
    }

    /**
     * Tests that invalid code value throws exception.
     */
    public function testInvalidCodeNameThrows()
    {
        $this->expectException(\UnexpectedValueException::class);
        Code::name(99999);
    }

    /**
     * Tests status code categorization helper.
     * Mirrors Java's StatusChecker exception mapping logic.
     */
    public function testStatusCodeCategoryHelper()
    {
        $tests = [
            [Code::OK, 'success'],
            [Code::MULTIPLE_RESULTS, 'success'],
            [Code::BAD_REQUEST, 'badRequest'],
            [Code::ILLEGAL_TOPIC, 'badRequest'],
            [Code::UNAUTHORIZED, 'unauthorized'],
            [Code::PAYMENT_REQUIRED, 'paymentRequired'],
            [Code::FORBIDDEN, 'forbidden'],
            [Code::NOT_FOUND, 'notFound'],
            [Code::MESSAGE_NOT_FOUND, 'notFound'],
            [Code::PAYLOAD_TOO_LARGE, 'payloadTooLarge'],
            [Code::TOO_MANY_REQUESTS, 'tooManyRequests'],
            [Code::REQUEST_HEADER_FIELDS_TOO_LARGE, 'requestHeaderTooLarge'],
            [Code::INTERNAL_ERROR, 'internalError'],
            [Code::INTERNAL_SERVER_ERROR, 'internalError'],
            [Code::PROXY_TIMEOUT, 'proxyTimeout'],
            [Code::UNSUPPORTED, 'unsupported'],
            [Code::VERSION_UNSUPPORTED, 'unsupported'],
        ];

        foreach ($tests as [$code, $expectedCategory]) {
            $actualCategory = self::getStatusCodeCategory($code);
            $this->assertEquals(
                $expectedCategory,
                $actualCategory,
                "Code " . Code::name($code) . " ({$code}) should be categorized as {$expectedCategory}"
            );
        }
    }

    /**
     * Helper to categorize status codes into exception groups.
     * Mirrors Java's StatusChecker.check() logic.
     */
    private static function getStatusCodeCategory($code)
    {
        switch ($code) {
            case Code::OK:
            case Code::MULTIPLE_RESULTS:
                return 'success';
            case Code::UNAUTHORIZED:
                return 'unauthorized';
            case Code::PAYMENT_REQUIRED:
                return 'paymentRequired';
            case Code::FORBIDDEN:
                return 'forbidden';
            case Code::NOT_FOUND:
            case Code::MESSAGE_NOT_FOUND:
            case Code::TOPIC_NOT_FOUND:
            case Code::CONSUMER_GROUP_NOT_FOUND:
            case Code::OFFSET_NOT_FOUND:
                return 'notFound';
            case Code::PAYLOAD_TOO_LARGE:
            case Code::MESSAGE_BODY_TOO_LARGE:
            case Code::MESSAGE_BODY_EMPTY:
                return 'payloadTooLarge';
            case Code::TOO_MANY_REQUESTS:
            case Code::LITE_TOPIC_QUOTA_EXCEEDED:
            case Code::LITE_SUBSCRIPTION_QUOTA_EXCEEDED:
                return 'tooManyRequests';
            case Code::REQUEST_HEADER_FIELDS_TOO_LARGE:
            case Code::MESSAGE_PROPERTIES_TOO_LARGE:
                return 'requestHeaderTooLarge';
            case Code::INTERNAL_ERROR:
            case Code::INTERNAL_SERVER_ERROR:
            case Code::HA_NOT_AVAILABLE:
                return 'internalError';
            case Code::PROXY_TIMEOUT:
            case Code::MASTER_PERSISTENCE_TIMEOUT:
            case Code::SLAVE_PERSISTENCE_TIMEOUT:
                return 'proxyTimeout';
            case Code::UNSUPPORTED:
            case Code::VERSION_UNSUPPORTED:
            case Code::VERIFY_FIFO_MESSAGE_UNSUPPORTED:
                return 'unsupported';
            default:
                if ($code >= 40000 && $code < 40100) {
                    return 'badRequest';
                }
                return 'unknown';
        }
    }
}

