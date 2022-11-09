<?php
/**
 * Created by PhpStorm.
 * User: 消逝文字
 * Date: 2022/10/26
 * Time: 15:35
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