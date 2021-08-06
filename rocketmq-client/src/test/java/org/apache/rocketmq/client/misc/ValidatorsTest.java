/*
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

package org.apache.rocketmq.client.misc;

public class ValidatorsTest {

//    private void checkIllegalTopic(String topic) {
//        try {
//            Validators.topicCheck(topic);
//            Assert.fail();
//        } catch (MQClientException ignore) {
//        }
//    }
//
//    @Test
//    public void testTopicCheck() throws MQClientException {
//        Validators.topicCheck("abc");
//        // Blank case 1.
//        checkIllegalTopic(" ");
//        checkIllegalTopic(" abc ");
//        checkIllegalTopic("abc ");
//        // Blank case 2
//        checkIllegalTopic("abc\t");
//        checkIllegalTopic("abc\t");
//        checkIllegalTopic("abc\n");
//        checkIllegalTopic("abc\f");
//        checkIllegalTopic("abc\r");
//        // Illegal case.
//        checkIllegalTopic("[abc]");
//        // Too long case.
//        final String tooLongTopic = StringUtils.repeat("a", Validators.TOPIC_MAX_LENGTH + 1);
//        checkIllegalTopic(tooLongTopic);
//        // Equals to default topic.
//        checkIllegalTopic(SystemTopic.DEFAULT_TOPIC);
//    }
//
//    private void checkIllegalConsumerGroup(String consumerGroup) {
//        try {
//            Validators.consumerGroupCheck(consumerGroup);
//            Assert.fail();
//        } catch (MQClientException ignore) {
//        }
//    }
//
//    @Test
//    public void testConsumerGroupCheck() throws MQClientException {
//        Validators.consumerGroupCheck("abc");
//        // Blank case 1.
//        checkIllegalConsumerGroup(" ");
//        checkIllegalConsumerGroup(" abc ");
//        checkIllegalConsumerGroup("abc ");
//        // Blank case 2
//        checkIllegalConsumerGroup("abc\t");
//        checkIllegalConsumerGroup("abc\t");
//        checkIllegalConsumerGroup("abc\n");
//        checkIllegalConsumerGroup("abc\f");
//        checkIllegalConsumerGroup("abc\r");
//        // Illegal case.
//        checkIllegalConsumerGroup("[abc]");
//        // Too long case.
//        final String tooLongConsumerGroup =
//                StringUtils.repeat("a", Validators.CONSUMER_GROUP_MAX_LENGTH + 1);
//        checkIllegalConsumerGroup(tooLongConsumerGroup);
//    }
//
//    private void checkIllegalMessage(final Message message, final int bodyMaxSize) {
//        try {
//            Validators.messageCheck(message, bodyMaxSize);
//            Assert.fail();
//        } catch (MQClientException ignore) {
//        }
//    }
//
//    @Test
//    public void testMessageCheck() throws MQClientException {
//        int bodyMaxSize = 3;
//
//        {
//            final Message message = new Message();
//            message.setTopic("abc");
//            message.setBody(new byte[bodyMaxSize]);
//            Validators.messageCheck(message, bodyMaxSize);
//        }
//        // Null case.
//        checkIllegalMessage(null, bodyMaxSize);
//        // Topic is blank.
//        {
//            final Message message = new Message();
//            message.setTopic("");
//            checkIllegalMessage(message, bodyMaxSize);
//        }
//        // Body is null.
//        {
//            final Message message = new Message();
//            message.setTopic("abc");
//            checkIllegalMessage(message, bodyMaxSize);
//        }
//        // Body length is zero.
//        {
//            final Message message = new Message();
//            message.setTopic("abc");
//            message.setBody(new byte[0]);
//            checkIllegalMessage(message, bodyMaxSize);
//        }
//        // Body length exceeds.
//        {
//            final Message message = new Message();
//            message.setTopic("abc");
//            message.setBody(new byte[bodyMaxSize + 1]);
//            checkIllegalMessage(message, bodyMaxSize);
//        }
//    }
}
