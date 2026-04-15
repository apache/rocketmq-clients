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

namespace Apache\Rocketmq\Exception;

/**
 * Base exception class for RocketMQ client
 */
class ClientException extends \Exception {
    /**
     * ClientException constructor
     *
     * @param string $message
     * @param int $code
     * @param \Exception|null $previous
     */
    public function __construct($message, $code = 0, \Exception $previous = null) {
        parent::__construct($message, $code, $previous);
    }
}

/**
 * Exception thrown when client configuration is invalid
 */
class ClientConfigurationException extends ClientException {
    /**
     * ClientConfigurationException constructor
     *
     * @param string $message
     * @param int $code
     * @param \Exception|null $previous
     */
    public function __construct($message, $code = 400, \Exception $previous = null) {
        parent::__construct($message, $code, $previous);
    }
}

/**
 * Exception thrown when client state is invalid
 */
class ClientStateException extends ClientException {
    /**
     * ClientStateException constructor
     *
     * @param string $message
     * @param int $code
     * @param \Exception|null $previous
     */
    public function __construct($message, $code = 409, \Exception $previous = null) {
        parent::__construct($message, $code, $previous);
    }
}

/**
 * Exception thrown when network error occurs
 */
class NetworkException extends ClientException {
    /**
     * NetworkException constructor
     *
     * @param string $message
     * @param int $code
     * @param \Exception|null $previous
     */
    public function __construct($message, $code = 503, \Exception $previous = null) {
        parent::__construct($message, $code, $previous);
    }
}

/**
 * Exception thrown when server returns error
 */
class ServerException extends ClientException {
    /**
     * ServerException constructor
     *
     * @param string $message
     * @param int $code
     * @param \Exception|null $previous
     */
    public function __construct($message, $code = 500, \Exception $previous = null) {
        parent::__construct($message, $code, $previous);
    }
}

/**
 * Exception thrown when transaction-related error occurs
 */
class TransactionException extends ClientException {
    /**
     * TransactionException constructor
     *
     * @param string $message
     * @param int $code
     * @param \Exception|null $previous
     */
    public function __construct($message, $code = 409, \Exception $previous = null) {
        parent::__construct($message, $code, $previous);
    }
}
