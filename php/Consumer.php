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

require 'vendor/autoload.php';

use Apache\Rocketmq\V2\MessageQueue;
use Apache\Rocketmq\V2\MessageType;
use Apache\Rocketmq\V2\MessagingServiceClient;
use Apache\Rocketmq\V2\ReceiveMessageRequest;
use Apache\Rocketmq\V2\Resource;
use Grpc\ChannelCredentials;

class Consumer
{
    public function init()
    {
        $client = new MessagingServiceClient('rmq-cn-cs02xhf2k01.cn-hangzhou.rmq.aliyuncs.com:8080', ['credentials' => ChannelCredentials::createInsecure()]);
        $request = new ReceiveMessageRequest();
        $mq = new MessageQueue();
        $resource = new Resource();
        $resource->setName('normal_topic');
        $mq->setAcceptMessageTypes([MessageType::NORMAL]);
        $mq->setTopic($resource);
        $request->setMessageQueue($mq);
        $msg = $client->ReceiveMessage($request);
        var_dump($msg);
    }
}

$x = new Consumer();
$x->init();