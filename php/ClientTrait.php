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
 * - extractReceiptHandle(), extractMessageId(), extractTopic() (appeared in ConsumeService, SimpleConsumerOptimized)
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
     * @return array
     */
    protected function buildMetadata(): array
    {
        return Signature::sign(
            $this->getCredentials(),
            $this->getClientIdValue(),
            ClientConstants::LANGUAGE,
            ClientConstants::CLIENT_VERSION,
            $this->getNamespaceValue(),
            'v2'
        );
    }

    /**
     * Parse endpoints string into protobuf Endpoints object.
     *
     * @param string $endpoints e.g. "127.0.0.1:8080" or "example.com:8080"
     * @return Endpoints
     */
    protected function parseEndpoints(string $endpoints): Endpoints
    {
        $cleaned = $endpoints;
        if (strpos($cleaned, 'http://') === 0) {
            $cleaned = substr($cleaned, 7);
        } elseif (strpos($cleaned, 'https://') === 0) {
            $cleaned = substr($cleaned, 8);
        }

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
     * @param object $messageView
     * @return string|null
     */
    protected function extractReceiptHandle($messageView): ?string
    {
        if (method_exists($messageView, 'getSystemProperties')) {
            $sysProps = $messageView->getSystemProperties();
            if ($sysProps !== null && method_exists($sysProps, 'getReceiptHandle')) {
                return $sysProps->getReceiptHandle();
            }
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
        if (method_exists($messageView, 'getSystemProperties')) {
            $sysProps = $messageView->getSystemProperties();
            if ($sysProps !== null && method_exists($sysProps, 'getMessageId')) {
                return $sysProps->getMessageId();
            }
        }
        return null;
    }

    /**
     * Extract topic name from a message view.
     *
     * @param object $messageView
     * @return string|null
     */
    protected function extractTopic($messageView): ?string
    {
        if (method_exists($messageView, 'getTopic')) {
            $topic = $messageView->getTopic();
            if ($topic !== null && $topic !== '') {
                if (is_object($topic) && method_exists($topic, 'getName')) {
                    return $topic->getName();
                }
                return (string)$topic;
            }
        }
        return null;
    }

    /**
     * Extract lite topic from a message view's system properties.
     *
     * @param object $messageView
     * @return string|null
     */
    protected function extractLiteTopic($messageView): ?string
    {
        if (method_exists($messageView, 'getSystemProperties')) {
            $sysProps = $messageView->getSystemProperties();
            if (method_exists($sysProps, 'getLiteTopic') && $sysProps->hasLiteTopic()) {
                return $sysProps->getLiteTopic();
            }
        }
        return null;
    }
}
