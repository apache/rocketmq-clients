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

use Apache\Rocketmq\V2\EndTransactionRequest;
use Apache\Rocketmq\V2\TransactionResolution;
use Apache\Rocketmq\V2\TransactionSource;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\Message;

/**
 * TransactionTrait — Transaction (half-message) send, commit, rollback, and orphaned recovery.
 *
 * Extracted from Producer. The using class must provide:
 * - ClientTrait methods (buildMetadata, getOperationTimeout, etc.)
 * - sendMessageWithRetry()
 * - Core send infrastructure (validateMessage, detectMessageType, getPublishingLoadBalancer, etc.)
 */
trait TransactionTrait
{
    private ?TransactionChecker $transactionChecker = null;
    private ?LocalTransactionExecuter $localTransactionExecuter = null;

    /**
     * Set transaction checker for orphaned transaction recovery.
     */
    public function setTransactionChecker(TransactionChecker $checker): self
    {
        $this->transactionChecker = $checker;
        return $this;
    }

    /**
     * Set local transaction executer for auto commit/rollback of half-messages.
     */
    public function setLocalTransactionExecuter(LocalTransactionExecuter $executer): self
    {
        $this->localTransactionExecuter = $executer;
        return $this;
    }

    /**
     * Send a transaction message (half-message + local transaction + commit/rollback).
     */
    public function sendWithTransaction(Message $message, $transaction, ?LocalTransactionExecuter $executor = null)
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("Producer is not running now");
        }

        $this->validateMessage($message);

        $sysProps = $message->getSystemProperties();
        $hasMessageGroup = $sysProps && method_exists($sysProps, 'hasMessageGroup') && $sysProps->hasMessageGroup();
        $hasLiteTopic = $sysProps && method_exists($sysProps, 'hasLiteTopic') && $sysProps->hasLiteTopic();
        $hasDeliveryTimestamp = $sysProps && method_exists($sysProps, 'hasDeliveryTimestamp') && $sysProps->hasDeliveryTimestamp();
        $hasPriority = $sysProps && method_exists($sysProps, 'hasPriority') && $sysProps->hasPriority();

        if ($hasMessageGroup || $hasLiteTopic || $hasDeliveryTimestamp || $hasPriority) {
            throw new \InvalidArgumentException(
                "Transactional message should not set messageGroup, deliveryTimestamp, liteTopic, or priority"
            );
        }

        $topic = $message->getTopic()->getName();
        $loadBalancer = $this->getPublishingLoadBalancer($topic);
        $messageQueue = $loadBalancer->takeMessageQueue($this->getIsolatedBrokerNames(), $this->maxAttempts);

        if (empty($messageQueue)) {
            throw new \RuntimeException("No available message queue for topic: {$topic}");
        }

        if ($this->validateMessageType) {
            $msgType = $this->detectMessageType($message, true);
            $loadBalancer->validateMessageTypeAgainstQueue($messageQueue[0], $msgType, $topic);
        }

        $request = $this->wrapTransactionMessageRequest([$message], $messageQueue[0]);
        $result = $this->sendMessageWithRetry($request, $message, $messageQueue, $this->maxAttempts);

        if (isset($result['transactionId'])) {
            $transaction->tryAddMessage($message);
            $transaction->tryAddReceipt($message, $result, $this->extractMessageQueueEndpoint($messageQueue[0]));
        }

        if ($executor !== null) {
            $messageView = new MessageView($message, $result['recallHandle'] ?? null, null, 1);
            $resolution = $executor->execute($messageView);

            if ($resolution === TransactionResolution::COMMIT) {
                $transaction->commit();
            } elseif ($resolution === TransactionResolution::ROLLBACK) {
                $transaction->rollback();
            }
        }

        return $result;
    }

    /**
     * Begin a new transaction.
     */
    public function beginTransaction()
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("Producer is not running now");
        }

        if ($this->transactionChecker === null) {
            throw new \RuntimeException("Transaction checker should not be null. Please set TransactionChecker the Producer.");
        }
        return new Transaction($this);
    }

    /**
     * Commit a transaction by messageId and transactionId.
     */
    public function commitTransaction(string $messageId, string $transactionId, string $topic, ?\Apache\Rocketmq\V2\Endpoints $endpoints = null)
    {
        $this->endTransaction($messageId, $transactionId, $topic, TransactionResolution::COMMIT, $endpoints);
    }

    /**
     * Rollback a transaction by messageId and transactionId.
     */
    public function rollbackTransaction(string $messageId, string $transactionId, string $topic, ?\Apache\Rocketmq\V2\Endpoints $endpoints = null)
    {
        $this->endTransaction($messageId, $transactionId, $topic, TransactionResolution::ROLLBACK, $endpoints);
    }

    /**
     * End (commit or rollback) a transaction via gRPC.
     */
    private function endTransaction($messageId, $transactionId, $topic, $resolution, ?\Apache\Rocketmq\V2\Endpoints $endpoints = null, $source = TransactionSource::SOURCE_CLIENT)
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("Producer is not running now");
        }

        $hookPoint = match ($resolution) {
            TransactionResolution::COMMIT => MessageHookPoints::COMMIT_TRANSACTION,
            TransactionResolution::ROLLBACK => MessageHookPoints::ROLLBACK_TRANSACTION,
            default => MessageHookPoints::COMMIT_TRANSACTION,
        };

        $this->executeInterceptors($hookPoint, [
            'messageId' => $messageId,
            'transactionId' => $transactionId,
            'topic' => $topic,
        ]);

        $topicResource = new Resource();
        $topicResource->setName($topic);

        $request = new EndTransactionRequest();
        $request->setMessageId($messageId);
        $request->setTransactionId($transactionId);
        $request->setTopic($topicResource);
        $request->setResolution($resolution);
        $request->setSource($source);

        $timeoutMs = (int)($this->getOperationTimeout('END_TRANSACTION') / 1000);
        $metadata = $this->buildMetadata($timeoutMs);
        $callOptions = ['timeout' => $this->getOperationTimeout('END_TRANSACTION')];

        if ($endpoints !== null) {
            $address = $endpoints->getAddresses();
            if (!empty($address) && $address[0] !== null) {
                $brokerKey = $address[0]->getHost() . ':' . $address[0]->getPort();
                $brokerClient = RpcClientManager::getInstance()->getClient($brokerKey, [
                    'tlsCredentials' => $this->tlsCredentials,
                ]);
                list($response, $status) = $brokerClient->EndTransaction($request, $metadata, $callOptions)->wait();
            } else {
                list($response, $status) = $this->client->EndTransaction($request, $metadata, $callOptions)->wait();
            }
        } else {
            list($response, $status) = $this->client->EndTransaction($request, $metadata, $callOptions)->wait();
        }

        if ($status->code !== 0) {
            throw new \RuntimeException("End transaction failed: " . $status->details);
        }

        if ($response->hasStatus()) {
            $statusCode = $response->getStatus()->getCode();
            if ($statusCode !== 20000) {
                throw new \RuntimeException("End transaction failed with code: " . $statusCode);
            }
        }
    }

    /**
     * Register TransactionChecker callback on TelemetrySession.
     */
    private function registerTransactionCheckerCallback()
    {
        if ($this->transactionChecker === null) {
            return;
        }

        $self = $this;
        $this->telemetrySession->setOnRecoverOrphanedTransaction(function ($command) use ($self) {
            $self->handleOrphanedTransaction($command);
        });
    }

    /**
     * Handle an orphaned transaction command from the server.
     */
    private function handleOrphanedTransaction($command)
    {
        if ($this->transactionChecker === null) {
            $this->logger->warning("Received orphaned transaction command but no TransactionChecker registered");
            return;
        }

        try {
            $message = null;
            if (method_exists($command, 'getMessage')) {
                $message = $command->getMessage();
            }

            if ($message === null) {
                $this->logger->warning("Orphaned transaction command has no message");
                return;
            }

            $messageView = new MessageView($message, null, null, 1);
            $resolution = $this->transactionChecker->check($messageView);

            if ($resolution === null || $resolution === TransactionResolution::TRANSACTION_RESOLUTION_UNSPECIFIED) {
                $this->logger->debug("Transaction checker returned TRANSACTION_RESOLUTION_UNSPECIFIED, leaving transaction unresolved.");
                return;
            }

            $transactionId = '';
            if (method_exists($command, 'getTransactionId')) {
                $transactionId = $command->getTransactionId();
            }

            $messageId = '';
            $topicName = '';
            $sysProps = $message->getSystemProperties();
            if ($sysProps && method_exists($sysProps, 'getMessageId')) {
                $messageId = $sysProps->getMessageId();
            }
            if (method_exists($message, 'getTopic') && method_exists($message->getTopic(), 'getName')) {
                $topicName = $message->getTopic()->getName();
            }

            $endpoints = null;
            if (method_exists($message, 'getEndpoints') && $message->hasEndpoints()) {
                $endpoints = $message->getEndpoints();
            }
            if (!empty($messageId) && !empty($topicName)) {
                $this->endTransaction($messageId, $transactionId, $topicName, $resolution, $endpoints, TransactionSource::SOURCE_SERVER_CHECK);
            }
        } catch (\Exception $e) {
            $this->logger->error("TransactionChecker threw exception: " . $e->getMessage());
        }
    }
}
