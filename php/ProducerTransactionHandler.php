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
use Apache\Rocketmq\V2\EndTransactionRequest;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\TransactionResolution;
use Apache\Rocketmq\V2\TransactionSource;
use Apache\Rocketmq\V2\Endpoints;

/**
 * ProducerTransactionHandler - Manages transaction message lifecycle
 * 
 * Responsibilities:
 * 1. Begin/commit/rollback transactions
 * 2. End transaction (send commit/rollback to broker)
 * 3. Handle orphaned transaction recovery via TransactionChecker
 * 4. Execute local transaction callbacks
 */
class ProducerTransactionHandler
{
    private MessagingServiceClient $client;
    private string $clientId;
    private ?SessionCredentials $credentials;
    private string $namespace;
    private Logger $logger;
    
    private ?TransactionChecker $transactionChecker = null;
    private ?LocalTransactionExecuter $localTransactionExecuter = null;

    /**
     * @param MessagingServiceClient $client gRPC client
     * @param string|null $clientId Client identifier (nullable for testing)
     * @param SessionCredentials|null $credentials AK/SK credentials
     * @param string $namespace Namespace
     * @param Logger|null $logger Logger instance (optional)
     */
    public function __construct(
        MessagingServiceClient $client,
        ?string $clientId,
        ?SessionCredentials $credentials,
        string $namespace = '',
        ?Logger $logger = null
    ) {
        $this->client = $client;
        $this->clientId = $clientId ?? ('php-producer-' . getmypid() . '-' . time());
        $this->credentials = $credentials;
        $this->namespace = $namespace;
        $this->logger = $logger ?? Logger::getInstance('Producer');
    }

    /**
     * Set transaction checker for orphaned transaction recovery
     *
     * @param TransactionChecker $checker
     */
    public function setTransactionChecker(TransactionChecker $checker): void
    {
        $this->transactionChecker = $checker;
    }

    /**
     * Set local transaction executer for auto commit/rollback
     *
     * @param LocalTransactionExecuter $executer
     */
    public function setLocalTransactionExecuter(LocalTransactionExecuter $executer): void
    {
        $this->localTransactionExecuter = $executer;
    }

    /**
     * Begin a new transaction
     *
     * @return Transaction
     */
    public function beginTransaction(): Transaction
    {
        return new Transaction($this->clientId);
    }

    /**
     * Commit a transaction
     *
     * @param string $messageId Message ID
     * @param string $transactionId Transaction ID
     * @param string $topic Topic name
     * @param Endpoints|null $endpoints Broker endpoints
     */
    public function commitTransaction(
        string $messageId,
        string $transactionId,
        string $topic,
        ?Endpoints $endpoints = null
    ): void {
        $this->endTransaction(
            $messageId,
            $transactionId,
            $topic,
            TransactionResolution::COMMIT,
            $endpoints
        );
    }

    /**
     * Rollback a transaction
     *
     * @param string $messageId Message ID
     * @param string $transactionId Transaction ID
     * @param string $topic Topic name
     * @param Endpoints|null $endpoints Broker endpoints
     */
    public function rollbackTransaction(
        string $messageId,
        string $transactionId,
        string $topic,
        ?Endpoints $endpoints = null
    ): void {
        $this->endTransaction(
            $messageId,
            $transactionId,
            $topic,
            TransactionResolution::ROLLBACK,
            $endpoints
        );
    }

    /**
     * End transaction (send commit/rollback to broker)
     *
     * @param string $messageId Message ID
     * @param string $transactionId Transaction ID
     * @param string $topic Topic name
     * @param TransactionResolution $resolution Commit or rollback
     * @param Endpoints|null $endpoints Broker endpoints
     * @param TransactionSource $source Transaction source
     */
    public function endTransaction(
        string $messageId,
        string $transactionId,
        string $topic,
        TransactionResolution $resolution,
        ?Endpoints $endpoints = null,
        TransactionSource $source = TransactionSource::SOURCE_CLIENT
    ): void {
        $request = new EndTransactionRequest();
        
        $topicResource = new Resource();
        $topicResource->setName($topic);
        $request->setTopic($topicResource);
        
        $request->setMessageId($messageId);
        $request->setTransactionId($transactionId);
        $request->setResolution($resolution);
        $request->setSource($source);

        try {
            list($response, $status) = $this->client->EndTransaction(
                $request,
                $this->buildMetadata(),
                $this->getCallOptions()
            )->wait();
            
            if ($status->code !== 0) {
                $this->logger->error("EndTransaction failed: " . $status->details);
                throw new \RuntimeException("EndTransaction failed: " . $status->details);
            }
            
            $this->logger->debug("EndTransaction completed: messageId={$messageId}, resolution=" . $resolution->name());
        } catch (\Exception $e) {
            $this->logger->error("EndTransaction exception: " . $e->getMessage());
            throw $e;
        }
    }

    /**
     * Handle orphaned transaction from server callback
     *
     * @param TelemetryCommand $command Telemetry command containing transaction info
     * @return TransactionResolution Resolution from checker
     */
    public function handleOrphanedTransaction($command): TransactionResolution
    {
        if ($this->transactionChecker === null) {
            $this->logger->warning("TransactionChecker not set, cannot handle orphaned transaction");
            return TransactionResolution::UNSPECIFIED;
        }

        try {
            $recoveredMessage = $command->getRecoverOrphanedTransactionCommand()->getMessage();
            $transactionId = $command->getRecoverOrphanedTransactionCommand()->getTransactionId();
            
            // Convert protobuf message to application message
            $message = $this->convertToApplicationMessage($recoveredMessage);
            $messageView = new MessageView($message, null, null, 1);
            
            // Query local transaction status
            $resolution = $this->transactionChecker->check($messageView, $transactionId);
            
            $this->logger->info("Orphaned transaction checked: transactionId={$transactionId}, resolution=" . $resolution->name());
            
            return $resolution;
        } catch (\Exception $e) {
            $this->logger->error("Failed to handle orphaned transaction: " . $e->getMessage());
            return TransactionResolution::UNSPECIFIED;
        }
    }

    /**
     * Execute local transaction and auto-commit/rollback
     *
     * @param Message $message Application message
     * @param array $sendResult Send result from half-message
     * @param LocalTransactionExecuter|null $executor Local transaction callback
     * @return TransactionResolution Resolution based on executor result
     */
    public function executeLocalTransaction(
        Message $message,
        array $sendResult,
        ?LocalTransactionExecuter $executor = null
    ): TransactionResolution {
        if ($executor === null && $this->localTransactionExecuter === null) {
            $this->logger->warning("No LocalTransactionExecuter available");
            return TransactionResolution::UNSPECIFIED;
        }

        $executer = $executor ?? $this->localTransactionExecuter;
        $messageView = new MessageView($message, $sendResult['recallHandle'] ?? null, null, 1);
        
        try {
            $resolution = $executer->execute($messageView);
            $this->logger->debug("Local transaction executed: resolution=" . $resolution->name());
            return $resolution;
        } catch (\Exception $e) {
            $this->logger->error("Local transaction execution failed: " . $e->getMessage());
            return TransactionResolution::ROLLBACK;
        }
    }

    /**
     * Build gRPC metadata
     *
     * @return array
     */
    private function buildMetadata(): array
    {
        $metadata = [];
        
        if ($this->credentials !== null) {
            $signature = Signature::sign($this->credentials, $this->clientId);
            $metadata['authorization'] = [$signature];
        }
        
        return $metadata;
    }

    /**
     * Get gRPC call options
     *
     * @return array
     */
    private function getCallOptions(): array
    {
        return [
            'timeout' => 3.0, // 3 seconds timeout
        ];
    }

    /**
     * Convert protobuf message to application message
     *
     * @param \Apache\Rocketmq\V2\Message $protoMsg Protobuf message
     * @return Message Application message
     */
    private function convertToApplicationMessage(\Apache\Rocketmq\V2\Message $protoMsg): Message
    {
        $topic = $protoMsg->getTopic()->getName();
        $body = $protoMsg->getBody();
        
        $message = MessageBuilder::newMessage($topic, $body);
        
        // Copy system properties
        $sysProps = $protoMsg->getSystemProperties();
        if ($sysProps) {
            if (method_exists($sysProps, 'getTag') && $sysProps->hasTag()) {
                $message->setTag($sysProps->getTag());
            }
            if (method_exists($sysProps, 'getKeys')) {
                $keys = $sysProps->getKeys();
                if (!ProtobufUtil::isRepeatedFieldEmpty($keys)) {
                    $message->setKeys(iterator_to_array($keys));
                }
            }
        }
        
        // Copy user properties
        $userProps = $protoMsg->getUserProperties();
        if (!ProtobufUtil::isMapFieldEmpty($userProps)) {
            foreach ($userProps as $key => $value) {
                $message->putUserProperty($key, $value);
            }
        }
        
        return $message;
    }
}
