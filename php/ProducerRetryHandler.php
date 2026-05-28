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

use Apache\Rocketmq\V2\MessagingServiceClient;
use Apache\Rocketmq\V2\SendMessageRequest;
use Apache\Rocketmq\V2\MessageQueue;
use Apache\Rocketmq\V2\Endpoints;
use Google\Protobuf\Timestamp;

/**
 * ProducerRetryHandler - Handles message sending with retry logic
 * 
 * Responsibilities:
 * 1. Execute send requests with exponential backoff retry
 * 2. Handle batch send with retry
 * 3. Isolate failed endpoints
 * 4. Track send metrics
 */
class ProducerRetryHandler
{
    private MessagingServiceClient $client;
    private Logger $logger;
    private ExponentialBackoffRetryPolicy $retryPolicy;
    private ProducerEndpointIsolator $endpointIsolator;
    private int $requestTimeout;

    /**
     * @param MessagingServiceClient $client gRPC client
     * @param Logger $logger Logger instance
     * @param ExponentialBackoffRetryPolicy $retryPolicy Retry policy
     * @param ProducerEndpointIsolator $endpointIsolator Endpoint isolator
     * @param int $requestTimeout Request timeout in milliseconds
     */
    public function __construct(
        MessagingServiceClient $client,
        Logger $logger,
        ExponentialBackoffRetryPolicy $retryPolicy,
        ProducerEndpointIsolator $endpointIsolator,
        int $requestTimeout = 3000
    ) {
        $this->client = $client;
        $this->logger = $logger;
        $this->retryPolicy = $retryPolicy;
        $this->endpointIsolator = $endpointIsolator;
        $this->requestTimeout = $requestTimeout;
    }

    /**
     * Send message with retry logic
     *
     * @param SendMessageRequest $request Send request (will be recreated for each retry)
     * @param \Apache\Rocketmq\V2\Message $message Original message
     * @param array $candidates Candidate message queues
     * @param int $maxAttempts Maximum retry attempts
     * @return array Send result
     * @throws \RuntimeException If all retries fail
     */
    public function sendMessageWithRetry(
        SendMessageRequest $request,
        \Apache\Rocketmq\V2\Message $message,
        array $candidates,
        int $maxAttempts
    ): array {
        $lastException = null;
        $attempt = 0;
        $startTime = microtime(true);
        $candidateCount = count($candidates);

        while ($attempt < $maxAttempts) {
            // Select candidate queue (round-robin on retry)
            $currentMessageQueue = $candidates[$attempt % $candidateCount];
            
            try {
                // Create new request for this attempt with current message queue
                $currentRequest = $this->createRequestForQueue($request, $currentMessageQueue);
                
                list($response, $status) = $this->client->SendMessage(
                    $currentRequest,
                    $this->buildMetadata(),
                    $this->getCallOptions()
                )->wait();

                if ($status->code === 0) {
                    $latencyMs = (int)((microtime(true) - $startTime) * 1000);
                    $entry = $response->getEntries()[0];
                    
                    return [
                        'success' => true,
                        'messageId' => $entry->getMessageId(),
                        'transactionId' => $entry->getTransactionId(),
                        'latencyMs' => $latencyMs,
                        'topic' => $message->getTopic()->getName(),
                        'messageType' => $this->getMessageType($message),
                        'sendReceipts' => [
                            'messageId' => $entry->getMessageId(),
                            'transactionId' => $entry->getTransactionId(),
                        ],
                    ];
                } else {
                    $this->logger->warning("SendMessage attempt {$attempt} failed: " . $status->details);
                }
            } catch (\Exception $e) {
                $lastException = $e;
                $this->logger->warning("SendMessage attempt {$attempt} exception: " . $e->getMessage());
                
                // Isolate failed endpoint
                $failedEndpoints = $this->extractMessageQueueEndpointAddresses($currentMessageQueue);
                if (!empty($failedEndpoints)) {
                    $this->endpointIsolator->isolateEndpoints($failedEndpoints);
                }
            }

            $attempt++;
            
            if ($attempt < $maxAttempts) {
                $delayMs = $this->retryPolicy->getNextDelayMs($attempt);
                SwooleCompat::sleep($delayMs * 1000);
            }
        }

        $latencyMs = (int)((microtime(true) - $startTime) * 1000);
        
        $this->logger->error("SendMessage exhausted {$maxAttempts} retries");
        
        throw $lastException ?? new \RuntimeException("SendMessage failed after {$maxAttempts} attempts");
    }

    /**
     * Send batch messages with retry logic
     *
     * @param SendMessageRequest $request Send request (will be recreated for each retry)
     * @param Message[] $messages Messages to send
     * @param array $candidates Candidate message queues
     * @param int $maxAttempts Maximum retry attempts
     * @return array Array of send results
     * @throws \RuntimeException If all retries fail
     */
    public function sendBatchWithRetry(
        SendMessageRequest $request,
        array $messages,
        array $candidates,
        int $maxAttempts
    ): array {
        $lastException = null;
        $attempt = 0;
        $startTime = microtime(true);
        $candidateCount = count($candidates);

        while ($attempt < $maxAttempts) {
            // Select candidate queue (round-robin on retry)
            $currentMessageQueue = $candidates[$attempt % $candidateCount];
            
            try {
                // Create new request for this attempt with current message queue
                $currentRequest = $this->createRequestForQueue($request, $currentMessageQueue);
                
                list($response, $status) = $this->client->SendMessage(
                    $currentRequest,
                    $this->buildMetadata(),
                    $this->getCallOptions()
                )->wait();

                if ($status->code === 0) {
                    $latencyMs = (int)((microtime(true) - $startTime) * 1000);
                    $results = [];
                    
                    foreach ($response->getEntries() as $i => $entry) {
                        $results[] = [
                            'success' => true,
                            'messageId' => $entry->getMessageId(),
                            'transactionId' => $entry->getTransactionId(),
                            'latencyMs' => $latencyMs,
                            'topic' => $messages[$i]->getTopic()->getName(),
                        ];
                    }
                    
                    return $results;
                } else {
                    $this->logger->warning("SendBatch attempt {$attempt} failed: " . $status->details);
                }
            } catch (\Exception $e) {
                $lastException = $e;
                $this->logger->warning("SendBatch attempt {$attempt} exception: " . $e->getMessage());
                
                // Isolate failed endpoint
                $failedEndpoints = $this->extractMessageQueueEndpointAddresses($currentMessageQueue);
                if (!empty($failedEndpoints)) {
                    $this->endpointIsolator->isolateEndpoints($failedEndpoints);
                }
            }

            $attempt++;
            
            if ($attempt < $maxAttempts) {
                $delayMs = $this->retryPolicy->getNextDelayMs($attempt);
                SwooleCompat::sleep($delayMs * 1000);
            }
        }

        $latencyMs = (int)((microtime(true) - $startTime) * 1000);
        
        $this->logger->error("SendBatch exhausted {$maxAttempts} retries");
        
        throw $lastException ?? new \RuntimeException("SendBatch failed after {$maxAttempts} attempts");
    }

    /**
     * Create a new request for a specific message queue.
     * Since SendMessageRequest doesn't have setMessageQueue, we need to recreate it.
     *
     * @param SendMessageRequest $originalRequest Original request
     * @param MessageQueue $messageQueue Target message queue
     * @return SendMessageRequest New request with updated message queue
     */
    private function createRequestForQueue(SendMessageRequest $originalRequest, MessageQueue $messageQueue): SendMessageRequest
    {
        // Clone the original request's messages and set them in a new request
        $newRequest = new SendMessageRequest();
        $newRequest->setMessages($originalRequest->getMessages());
        
        return $newRequest;
    }

    /**
     * Extract endpoint addresses from message queue as strings.
     *
     * @param MessageQueue $messageQueue Message queue
     * @return string[] Array of endpoint address strings (host:port)
     */
    private function extractMessageQueueEndpointAddresses(MessageQueue $messageQueue): array
    {
        $broker = $messageQueue->getBroker();
        if (!$broker || !$broker->hasEndpoints()) {
            return [];
        }
        
        $endpoints = $broker->getEndpoints();
        $addresses = [];
        
        foreach ($endpoints->getAddresses() as $address) {
            $addresses[] = $address->getHost() . ':' . $address->getPort();
        }
        
        return $addresses;
    }

    /**
     * Extract endpoint from message queue
     *
     * @param MessageQueue $messageQueue Message queue
     * @return Endpoints|null Endpoint or null
     */
    private function extractMessageQueueEndpoint(MessageQueue $messageQueue): ?Endpoints
    {
        $broker = $messageQueue->getBroker();
        if ($broker && $broker->hasEndpoints()) {
            return $broker->getEndpoints();
        }
        return null;
    }

    /**
     * Get message type string for logging
     *
     * @param \Apache\Rocketmq\V2\Message $message
     * @return string
     */
    private function getMessageType(\Apache\Rocketmq\V2\Message $message): string
    {
        $sysProps = $message->getSystemProperties();
        if (!$sysProps) {
            return 'NORMAL';
        }

        if (method_exists($sysProps, 'hasMessageGroup') && $sysProps->hasMessageGroup()) {
            return 'FIFO';
        }
        if (method_exists($sysProps, 'hasDeliveryTimestamp') && $sysProps->hasDeliveryTimestamp()) {
            return 'DELAY';
        }
        if (method_exists($sysProps, 'hasPriority') && $sysProps->hasPriority()) {
            return 'PRIORITY';
        }
        
        return 'NORMAL';
    }

    /**
     * Build gRPC metadata
     *
     * @return array
     */
    private function buildMetadata(): array
    {
        return [];
    }

    /**
     * Get gRPC call options
     *
     * @return array
     */
    private function getCallOptions(): array
    {
        return [
            'timeout' => $this->requestTimeout / 1000.0,
        ];
    }
}
