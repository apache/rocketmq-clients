<?php
// GENERATED CODE -- DO NOT EDIT!

// Original file comments:
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
namespace Apache\Rocketmq\V2;

/**
 */
class AdminClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * @param \Apache\Rocketmq\V2\ChangeLogLevelRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function ChangeLogLevel(\Apache\Rocketmq\V2\ChangeLogLevelRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/apache.rocketmq.v2.Admin/ChangeLogLevel',
        $argument,
        ['\Apache\Rocketmq\V2\ChangeLogLevelResponse', 'decode'],
        $metadata, $options);
    }

}
