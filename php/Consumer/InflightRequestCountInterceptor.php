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

namespace Apache\Rocketmq\Consumer;

/**
 * InflightRequestCountInterceptor - Tracks inflight receive requests
 * 
 * Reference Java InflightRequestCountInterceptor:
 * - Tracks the number of inflight receive requests
 * - Used for graceful shutdown (wait for inflight requests to complete)
 * - Helps prevent message loss during shutdown
 */
class InflightRequestCountInterceptor implements MessageInterceptor
{
    /**
     * @var int Inflight receive request count
     */
    private $inflightReceiveRequestCount = 0;
    
    /**
     * @var int Inflight ack request count
     */
    private $inflightAckRequestCount = 0;
    
    /**
     * {@inheritdoc}
     */
    public function before(MessageInterceptorContext $context, array $messages): void
    {
        $phase = $context->getPhase();
        
        if ($phase === 'RECEIVE') {
            $this->inflightReceiveRequestCount++;
        } elseif ($phase === 'ACK') {
            $this->inflightAckRequestCount++;
        }
    }
    
    /**
     * {@inheritdoc}
     */
    public function after(MessageInterceptorContext $context, array $messages): void
    {
        $phase = $context->getPhase();
        
        if ($phase === 'RECEIVE') {
            $this->inflightReceiveRequestCount = max(0, $this->inflightReceiveRequestCount - 1);
        } elseif ($phase === 'ACK') {
            $this->inflightAckRequestCount = max(0, $this->inflightAckRequestCount - 1);
        }
    }
    
    /**
     * Get inflight receive request count
     * 
     * @return int
     */
    public function getInflightReceiveRequestCount(): int
    {
        return $this->inflightReceiveRequestCount;
    }
    
    /**
     * Get inflight ack request count
     * 
     * @return int
     */
    public function getInflightAckRequestCount(): int
    {
        return $this->inflightAckRequestCount;
    }
    
    /**
     * Get total inflight request count
     * 
     * @return int
     */
    public function getTotalInflightRequestCount(): int
    {
        return $this->inflightReceiveRequestCount + $this->inflightAckRequestCount;
    }
}
