<?php

namespace Apache\Rocketmq\Consumer;

use Apache\Rocketmq\Route\MessageQueue;

/**
 * Assignment represents a message queue assignment for PushConsumer.
 * 
 * Aligned with Java: org.apache.rocketmq.client.java.impl.consumer.Assignment
 */
class Assignment
{
    /**
     * @var MessageQueue The assigned message queue
     */
    private $messageQueue;
    
    /**
     * Constructor
     * 
     * @param MessageQueue $messageQueue The message queue to assign
     */
    public function __construct(MessageQueue $messageQueue)
    {
        $this->messageQueue = $messageQueue;
    }
    
    /**
     * Get the message queue
     * 
     * @return MessageQueue The assigned message queue
     */
    public function getMessageQueue(): MessageQueue
    {
        return $this->messageQueue;
    }
    
    /**
     * Check equality with another Assignment
     * 
     * @param Assignment $other The other assignment to compare
     * @return bool True if equal
     */
    public function equals(Assignment $other): bool
    {
        // Compare message queues
        return $this->messageQueue === $other->messageQueue || 
               $this->messageQueue == $other->messageQueue;
    }
    
    /**
     * Get string representation
     * 
     * @return string String representation
     */
    public function toString(): string
    {
        return sprintf(
            'Assignment{messageQueue=%s}',
            $this->messageQueue ? $this->messageQueue->toString() : 'null'
        );
    }
}
