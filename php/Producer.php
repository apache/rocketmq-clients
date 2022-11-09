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
use Apache\Rocketmq\V2\MessagingServiceClient;
use Apache\Rocketmq\V2\QueryRouteRequest;
use Apache\Rocketmq\V2\ReceiveMessageRequest;
use Apache\Rocketmq\V2\Resource;
use Grpc\ChannelCredentials;
use const Grpc\STATUS_OK;

class Producer
{

    public function init()
    {
        /**
         *  Client ID Because my host name is currently in Chinese,
         * I am afraid that there will be problems with parsing,
         * so for the time being, the host name will be written dead
         */
        $clientId = 'missyourlove' . '@' . posix_getpid() . '@' . rand(0, 10) . '@' . $this->getRandStr(10);
        $client = new MessagingServiceClient('rmq-cn-cs02xhf2k01.cn-hangzhou.rmq.aliyuncs.com:8080', [
            'credentials' => ChannelCredentials::createInsecure(),
            'update_metadata' => function ($metaData) use ($clientId) {
                $metaData['headers'] = ['clientID' => $clientId]; // Pass the ClientID to the server through the header
                return $metaData;
            }
        ]);

        $qr = new QueryRouteRequest();
        $rs = new Resource();
        $rs->setResourceNamespace('');
        $rs->setName('normal_topic');
        $qr->setTopic($rs);
       $status = $client->QueryRoute($qr)->wait();
       var_dump($status); // This prints out the response data returned by the server
    }

    public function getRandStr($length){
        //Character combinations
        $str = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
        $len = strlen($str)-1;
        $randstr = '';
        for ($i=0;$i<$length;$i++) {
            $num=mt_rand(0,$len);
            $randstr .= $str[$num];
        }
        return $randstr;
    }
}

$xx = new Producer();
$xx->init();
