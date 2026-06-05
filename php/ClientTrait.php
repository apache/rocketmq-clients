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

namespace Apache\Rocketmq;

use Apache\Rocketmq\V2\Address;
use Apache\Rocketmq\V2\AddressScheme;
use Apache\Rocketmq\V2\Endpoints;

/**
 * ClientTrait - Shared methods extracted from duplicated code across client classes.
 *
 * Eliminates duplication of:
 * - buildMetadata() (appeared in 7 classes)
 * - parseEndpoints() (appeared in 3 classes)
 * - extractReceiptHandle(), extractMessageId(), extractTopic() (appeared in ConsumeService, SimpleConsumer)
 * - extractLiteTopic() (for lite consumer support)
 *
 * Consuming classes MUST implement:
 * - protected function getCredentials(): ?SessionCredentials
 * - protected function getClientIdValue(): string
 * - protected function getNamespaceValue(): string
 */
trait ClientTrait
{
    /**
     * Build metadata for gRPC calls using Signature class.
     *
     * @param int|null $timeoutMs Optional timeout in milliseconds (will be converted to microseconds for gRPC deadline)
     * @return array
     */
    protected function buildMetadata(?int $timeoutMs = null): array
    {
        $metadata = Signature::sign(
            $this->getCredentials(),
            $this->getClientIdValue(),
            ClientConstants::LANGUAGE,
            ClientConstants::CLIENT_VERSION,
            $this->getNamespaceValue(),
            'v2'
        );
        
        // Set gRPC deadline if timeout is provided
        if ($timeoutMs !== null && $timeoutMs > 0) {
            // Convert milliseconds to microseconds for gRPC deadline
            $timeoutUs = $timeoutMs * 1000;
            $metadata['grpc-timeout'] = [$timeoutUs . 'u']; // microseconds format, array-wrapped for gRPC
        }
        
        return $metadata;
    }

    /**
     * Parse endpoints string into protobuf Endpoints object.
     *
     * @param string $endpoints e.g. "127.0.0.1:8080" or "example.com:8080"
     * @return Endpoints
     */
    protected function parseEndpoints(string $endpoints): Endpoints
    {
        $cleaned = match (true) {
            str_starts_with($endpoints, 'https://') => substr($endpoints, 8),
            str_starts_with($endpoints, 'http://') => substr($endpoints, 7),
            default => $endpoints,
        };

        $lastColon = strrpos($cleaned, ':');
        if ($lastColon !== false) {
            $host = substr($cleaned, 0, $lastColon);
            $port = (int)substr($cleaned, $lastColon + 1);
        } else {
            $host = $cleaned;
            $port = 80;
        }

        $scheme = filter_var($host, FILTER_VALIDATE_IP, FILTER_FLAG_IPV4) !== false
            ? AddressScheme::IPv4
            : AddressScheme::DOMAIN_NAME;

        $address = new Address();
        $address->setHost($host);
        $address->setPort($port);

        $endpointsObj = new Endpoints();
        $endpointsObj->setScheme($scheme);
        $endpointsObj->setAddresses([$address]);

        return $endpointsObj;
    }

    /**
     * Extract receipt handle from a message view.
     *
     * @param MessageViewInterface|object $messageView
     * @return string|null
     */
    protected function extractReceiptHandle($messageView): ?string
    {
        if ($messageView instanceof MessageViewInterface) {
            $sysProps = $messageView->getSystemProperties();
            return $sysProps?->getReceiptHandle();
        }
        return null;
    }

    /**
     * Extract message ID from a message view.
     *
     * @param object $messageView
     * @return string|null
     */
    protected function extractMessageId($messageView): ?string
    {
        if ($messageView instanceof MessageViewInterface) {
            return $messageView->getMessageId();
        }
        return null;
    }

    /**
     * Extract topic name from a message view.
     *
     * @param MessageViewInterface|object $messageView
     * @return string|null
     */
    protected function extractTopic($messageView): ?string
    {
        if ($messageView instanceof MessageViewInterface) {
            $topic = $messageView->getTopic();
            return $topic !== '' ? $topic : null;
        }
        return null;
    }

    /**
     * Get call options for gRPC calls.
     *
     * @param int|null $overrideTimeout Optional timeout to override default (in microseconds)
     * @return array Array with 'timeout' key for gRPC call options
     */
    protected function getCallOptions(?int $overrideTimeout = null): array
    {
        return ['timeout' => $overrideTimeout ?? ClientConstants::GRPC_DEFAULT_TIMEOUT];
    }

    /**
     * Get operation-specific timeout from constants.
     *
     * @param string $operation Operation name (e.g., 'SEND_MESSAGE', 'ACK_MESSAGE')
     * @return int Timeout in microseconds
     */
    protected function getOperationTimeout(string $operation): int
    {
        return match ($operation) {
            'SEND_MESSAGE' => ClientConstants::GRPC_SEND_MESSAGE_TIMEOUT,
            'ACK_MESSAGE' => ClientConstants::GRPC_ACK_MESSAGE_TIMEOUT,
            'QUERY_ROUTE' => ClientConstants::GRPC_QUERY_ROUTE_TIMEOUT,
            'HEARTBEAT' => ClientConstants::GRPC_HEARTBEAT_TIMEOUT,
            'END_TRANSACTION' => ClientConstants::GRPC_END_TRANSACTION_TIMEOUT,
            'CHANGE_INVISIBLE' => ClientConstants::GRPC_CHANGE_INVISIBLE_TIMEOUT,
            default => ClientConstants::GRPC_DEFAULT_TIMEOUT,
        };
    }

    /**
     * Extract lite topic from a message view's system properties.
     *
     * @param MessageViewInterface|object $messageView
     * @return string|null
     */
    protected function extractLiteTopic($messageView): ?string
    {
        if ($messageView instanceof MessageViewInterface) {
            $sysProps = $messageView->getSystemProperties();
            if ($sysProps !== null && $sysProps->hasLiteTopic()) {
                return $sysProps->getLiteTopic();
            }
        }
        return null;
    }
}
