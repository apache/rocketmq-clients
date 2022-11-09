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
        // 客户端ID 因为目前我的主机名称是中文的，害怕解析会有问题，所以暂时先将主机名称写死
        $clientId = 'missyourlove' . '@' . posix_getpid() . '@' . rand(0, 10) . '@' . $this->getRandStr(10);
        $client = new MessagingServiceClient('rmq-cn-cs02xhf2k01.cn-hangzhou.rmq.aliyuncs.com:8080', [
            'credentials' => ChannelCredentials::createInsecure(),
            'update_metadata' => function ($metaData) use ($clientId) {
                $metaData['headers'] = ['clientID' => $clientId]; // 通过header将ClientID传递到服务端
                return $metaData;
            }
        ]);

        $qr = new QueryRouteRequest();
        $rs = new Resource();
        $rs->setResourceNamespace('');
        $rs->setName('normal_topic');
        $qr->setTopic($rs);
       $status = $client->QueryRoute($qr)->wait();
       var_dump($status); // 此处打印出服务端返回的响应数据
    }

    public function getRandStr($length){
        //字符组合
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
