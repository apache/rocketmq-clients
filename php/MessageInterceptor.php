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

interface MessageInterceptor
{
    /**
     * Intercept at a given hook point.
     *
     * @param string $hookPoint One of MessageHookPoints constants
     * @param array $context Context data specific to the hook point
     * @return void
     */
    public function intercept($hookPoint, array $context = []);
}

class CompositedMessageInterceptor implements MessageInterceptor
{
    private $interceptors = [];

    /**
     * Register an interceptor to be called at each hook point.
     *
     * @param MessageInterceptor $interceptor The interceptor instance to add
     * @return void
     */
    public function addInterceptor(MessageInterceptor $interceptor)
    {
        $this->interceptors[] = $interceptor;
    }

    /**
     * Call all registered interceptors for the given hook point.
     *
     * @param string $hookPoint One of MessageHookPoints constants
     * @param array $context Context data specific to the hook point
     * @return void
     */
    public function intercept($hookPoint, array $context = [])
    {
        foreach ($this->interceptors as $interceptor) {
            $interceptor->intercept($hookPoint, $context);
        }
    }
}
