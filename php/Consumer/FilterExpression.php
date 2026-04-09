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
 * Filter expression class
 * 
 * Used to filter messages based on tags or SQL92 expressions
 */
class FilterExpression {
    /**
     * @var string Filter expression
     */
    private $expression;
    
    /**
     * @var FilterExpressionType Filter expression type
     */
    private $type;
    
    /**
     * Constructor
     * 
     * @param string $expression Filter expression
     * @param FilterExpressionType $type Filter expression type
     */
    public function __construct(string $expression, FilterExpressionType $type) {
        $this->expression = $expression;
        $this->type = $type;
    }
    
    /**
     * Get filter expression
     * 
     * @return string Filter expression
     */
    public function getExpression(): string {
        return $this->expression;
    }
    
    /**
     * Get filter expression type
     * 
     * @return FilterExpressionType Filter expression type
     */
    public function getType(): FilterExpressionType {
        return $this->type;
    }
    
    /**
     * Get string representation
     * 
     * @return string
     */
    public function __toString(): string {
        return sprintf("FilterExpression[expression=%s, type=%s]", $this->expression, $this->type);
    }
}
