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

require_once __DIR__ . '/TestRunner.php';
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
class StatusCheckerTest
{
    /**
     * Mirrors Java: testOK - success codes.
     */
    public function testSuccessCodes()
    {
        TestRunner::assertEqualsWithMessage(20000, Code::OK, "OK should be 20000");
        TestRunner::assertEqualsWithMessage(30000, Code::MULTIPLE_RESULTS, "MULTIPLE_RESULTS should be 30000");
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
            TestRunner::assertTrue(
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
        TestRunner::assertEqualsWithMessage(40100, Code::UNAUTHORIZED, "UNAUTHORIZED should be 40100");
    }

    /**
     * Mirrors Java: testPaymentRequired.
     */
    public function testPaymentRequiredCode()
    {
        TestRunner::assertEqualsWithMessage(40200, Code::PAYMENT_REQUIRED, "PAYMENT_REQUIRED should be 40200");
    }

    /**
     * Mirrors Java: testForbidden.
     */
    public function testForbiddenCode()
    {
        TestRunner::assertEqualsWithMessage(40300, Code::FORBIDDEN, "FORBIDDEN should be 40300");
    }

    /**
     * Mirrors Java: testNotFound - all 404xx codes.
     */
    public function testNotFoundCodes()
    {
        TestRunner::assertEqualsWithMessage(40400, Code::NOT_FOUND, "NOT_FOUND should be 40400");
        TestRunner::assertEqualsWithMessage(40401, Code::MESSAGE_NOT_FOUND, "MESSAGE_NOT_FOUND should be 40401");
        TestRunner::assertEqualsWithMessage(40402, Code::TOPIC_NOT_FOUND, "TOPIC_NOT_FOUND should be 40402");
        TestRunner::assertEqualsWithMessage(40403, Code::CONSUMER_GROUP_NOT_FOUND, "CONSUMER_GROUP_NOT_FOUND should be 40403");
        TestRunner::assertEqualsWithMessage(40404, Code::OFFSET_NOT_FOUND, "OFFSET_NOT_FOUND should be 40404");
    }

    /**
     * Mirrors Java: testPayloadTooLarge - all 413xx codes.
     */
    public function testPayloadTooLargeCodes()
    {
        TestRunner::assertEqualsWithMessage(41300, Code::PAYLOAD_TOO_LARGE, "PAYLOAD_TOO_LARGE should be 41300");
        TestRunner::assertEqualsWithMessage(41301, Code::MESSAGE_BODY_TOO_LARGE, "MESSAGE_BODY_TOO_LARGE should be 41301");
        TestRunner::assertEqualsWithMessage(41302, Code::MESSAGE_BODY_EMPTY, "MESSAGE_BODY_EMPTY should be 41302");
    }

    /**
     * Mirrors Java: testTooManyRequests - all 429xx codes.
     */
    public function testTooManyRequestsCodes()
    {
        TestRunner::assertEqualsWithMessage(42900, Code::TOO_MANY_REQUESTS, "TOO_MANY_REQUESTS should be 42900");
        TestRunner::assertEqualsWithMessage(42901, Code::LITE_TOPIC_QUOTA_EXCEEDED, "LITE_TOPIC_QUOTA_EXCEEDED should be 42901");
        TestRunner::assertEqualsWithMessage(42902, Code::LITE_SUBSCRIPTION_QUOTA_EXCEEDED, "LITE_SUBSCRIPTION_QUOTA_EXCEEDED should be 42902");
    }

    /**
     * Mirrors Java: testRequestHeaderFieldsTooLarge.
     */
    public function testRequestHeaderTooLargeCodes()
    {
        TestRunner::assertEqualsWithMessage(43100, Code::REQUEST_HEADER_FIELDS_TOO_LARGE, "REQUEST_HEADER_FIELDS_TOO_LARGE should be 43100");
        TestRunner::assertEqualsWithMessage(43101, Code::MESSAGE_PROPERTIES_TOO_LARGE, "MESSAGE_PROPERTIES_TOO_LARGE should be 43101");
    }

    /**
     * Mirrors Java: testInternalError - all 500xx codes.
     */
    public function testInternalErrorCodes()
    {
        TestRunner::assertEqualsWithMessage(50000, Code::INTERNAL_ERROR, "INTERNAL_ERROR should be 50000");
        TestRunner::assertEqualsWithMessage(50001, Code::INTERNAL_SERVER_ERROR, "INTERNAL_SERVER_ERROR should be 50001");
        TestRunner::assertEqualsWithMessage(50002, Code::HA_NOT_AVAILABLE, "HA_NOT_AVAILABLE should be 50002");
    }

    /**
     * Mirrors Java: testProxyTimeout - all 504xx codes.
     */
    public function testProxyTimeoutCodes()
    {
        TestRunner::assertEqualsWithMessage(50400, Code::PROXY_TIMEOUT, "PROXY_TIMEOUT should be 50400");
        TestRunner::assertEqualsWithMessage(50401, Code::MASTER_PERSISTENCE_TIMEOUT, "MASTER_PERSISTENCE_TIMEOUT should be 50401");
        TestRunner::assertEqualsWithMessage(50402, Code::SLAVE_PERSISTENCE_TIMEOUT, "SLAVE_PERSISTENCE_TIMEOUT should be 50402");
    }

    /**
     * Mirrors Java: testUnsupported - all 505xx codes.
     */
    public function testUnsupportedCodes()
    {
        TestRunner::assertEqualsWithMessage(50500, Code::UNSUPPORTED, "UNSUPPORTED should be 50500");
        TestRunner::assertEqualsWithMessage(50501, Code::VERSION_UNSUPPORTED, "VERSION_UNSUPPORTED should be 50501");
        TestRunner::assertEqualsWithMessage(50502, Code::VERIFY_FIFO_MESSAGE_UNSUPPORTED, "VERIFY_FIFO_MESSAGE_UNSUPPORTED should be 50502");
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
            TestRunner::assertTrue(
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
        TestRunner::assertThrows(\UnexpectedValueException::class, function() {
            Code::name(99999);
        }, "name() with invalid code should throw");
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
            TestRunner::assertEqualsWithMessage(
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

echo "=== StatusCheckerTest ===\n";
$test = new StatusCheckerTest();
$test->testSuccessCodes();
echo "  [OK] testSuccessCodes\n";
$test->testBadRequestCodes();
echo "  [OK] testBadRequestCodes\n";
$test->testUnauthorizedCode();
echo "  [OK] testUnauthorizedCode\n";
$test->testPaymentRequiredCode();
echo "  [OK] testPaymentRequiredCode\n";
$test->testForbiddenCode();
echo "  [OK] testForbiddenCode\n";
$test->testNotFoundCodes();
echo "  [OK] testNotFoundCodes\n";
$test->testPayloadTooLargeCodes();
echo "  [OK] testPayloadTooLargeCodes\n";
$test->testTooManyRequestsCodes();
echo "  [OK] testTooManyRequestsCodes\n";
$test->testRequestHeaderTooLargeCodes();
echo "  [OK] testRequestHeaderTooLargeCodes\n";
$test->testInternalErrorCodes();
echo "  [OK] testInternalErrorCodes\n";
$test->testProxyTimeoutCodes();
echo "  [OK] testProxyTimeoutCodes\n";
$test->testUnsupportedCodes();
echo "  [OK] testUnsupportedCodes\n";
$test->testAllCodeNamesAreValid();
echo "  [OK] testAllCodeNamesAreValid\n";
$test->testInvalidCodeNameThrows();
echo "  [OK] testInvalidCodeNameThrows\n";
$test->testStatusCodeCategoryHelper();
echo "  [OK] testStatusCodeCategoryHelper\n";
