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

require_once __DIR__ . '/ConsumeService.php';
require_once __DIR__ . '/ProcessQueue.php';

/**
 * LiteFifoConsumeService - FIFO consume service for lite consumers.
 *
 * Extends FifoConsumeService to group messages by liteTopic instead of messageGroup.
 * For lite consumers, FIFO ordering is enforced per liteTopic, and the accelerator
 * parallelizes across different liteTopics.
 */
class LiteFifoConsumeService extends FifoConsumeService
{
    /**
     * Get the group key for a lite message (uses liteTopic).
     *
     * @param object $messageView
     * @return string
     */
    protected function getMessageGroupKey($messageView)
    {
        $sysProps = $messageView->getSystemProperties();
        if ($sysProps && method_exists($sysProps, 'hasLiteTopic') && $sysProps->hasLiteTopic()) {
            return $sysProps->getLiteTopic();
        }
        return 'default';
    }

    protected function handleSuspend(ProcessQueue $pq, $messageView, ConsumeResultSuspend $suspendResult)
    {
        if ($pq->isDropped()) {
            return;
        }
        $targetLiteTopic = null;
        if (method_exists($messageView, 'getSystemProperties')) {
            $sysProps = $messageView->getSystemProperties();
            if (method_exists($sysProps, 'getLiteTopic') && $sysProps->hasLiteTopic()) {
                $targetLiteTopic = $sysProps->getLiteTopic();
            }
        }
        $suspendSec = (int)ceil($suspendResult->getSuspendTimeMs() / 1000);
        $cachedMessages = $pq->getCachedMessages();
        $suspendedCount = 0;
        foreach ($cachedMessages as $msg) {
            if ($targetLiteTopic !== null) {
                $msgLiteTopic = null;
                if (method_exists($msg, 'getSystemProperties')) {
                    $msgSysProps = $msg->getSystemProperties();
                    if (method_exists($msgSysProps, "getLiteTopic") && $msgSysProps->hasLiteTopic()) {
                        $msgLiteTopic = $msgSysProps->getLiteTopic();
                    }
                }
                if ($msgLiteTopic === $targetLiteTopic) {
                    $this->nackMessage($msg, 1, $suspendSec);
                    $pq->evictMessage($msg);
                    $suspendedCount++;
                }
            }
        }
        $this->logger->debug("LiteFifoConsumeService batch-suspended {$suspendedCount} messages with same liteTopic, suspendSec={$suspendSec}");
        usleep($suspendResult->getSuspendTimeMs() * 1000);
    }
}
