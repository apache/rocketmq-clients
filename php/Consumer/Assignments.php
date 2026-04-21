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
 * Assignments represents a collection of message queue assignments.
 * 
 * Aligned with Java: org.apache.rocketmq.client.java.impl.consumer.Assignments
 */
class Assignments
{
    /**
     * @var Assignment[] Array of assignments
     */
    private $assignmentList;
    
    /**
     * Constructor
     * 
     * @param array $assignmentList Array of Assignment objects
     */
    public function __construct(array $assignmentList)
    {
        $this->assignmentList = $assignmentList;
    }
    
    /**
     * Get the assignment list
     * 
     * @return Assignment[] Array of Assignment objects
     */
    public function getAssignmentList(): array
    {
        return $this->assignmentList;
    }
    
    /**
     * Check if assignments are empty
     * 
     * @return bool True if no assignments
     */
    public function isEmpty(): bool
    {
        return empty($this->assignmentList);
    }
    
    /**
     * Get assignment count
     * 
     * @return int Number of assignments
     */
    public function count(): int
    {
        return count($this->assignmentList);
    }
    
    /**
     * Check equality with another Assignments
     * 
     * @param Assignments $other The other assignments to compare
     * @return bool True if equal
     */
    public function equals(Assignments $other): bool
    {
        if (count($this->assignmentList) !== count($other->assignmentList)) {
            return false;
        }
        
        // Compare each assignment
        foreach ($this->assignmentList as $index => $assignment) {
            if (!$assignment->equals($other->assignmentList[$index])) {
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * Get string representation
     * 
     * @return string String representation
     */
    public function toString(): string
    {
        $assignmentsStr = implode(', ', array_map(function($a) {
            return $a->toString();
        }, $this->assignmentList));
        
        return sprintf('Assignments{assignmentList=[%s]}', $assignmentsStr);
    }
}
