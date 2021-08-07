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

import static org.testng.Assert.fail;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.ClientException;
import org.apache.rocketmq.client.message.Message;
import org.apache.rocketmq.client.tools.TestBase;
import org.testng.annotations.Test;

public class ValidatorsTest extends TestBase {

    private void checkIllegalTopic(String topic) {
        try {
            Validators.checkTopic(topic);
            fail();
        } catch (ClientException ignore) {
            // ignore on purpose.
        }
    }

    @Test
    public void testTopicCheck() throws ClientException {
        Validators.checkTopic("abc");
        // Blank case 1.
        checkIllegalTopic(" ");
        checkIllegalTopic(" abc ");
        checkIllegalTopic("abc ");
        // Blank case 2
        checkIllegalTopic("abc\t");
        checkIllegalTopic("abc\t");
        checkIllegalTopic("abc\n");
        checkIllegalTopic("abc\f");
        checkIllegalTopic("abc\r");
        // Illegal case.
        checkIllegalTopic("[abc]");
        // Too long case.
        final String tooLongTopic = StringUtils.repeat("a", Validators.TOPIC_MAX_LENGTH + 1);
        checkIllegalTopic(tooLongTopic);
    }

    private void checkGroup(String group) {
        try {
            Validators.checkGroup(group);
            fail();
        } catch (ClientException ignore) {
            // ignore on purpose.
        }
    }

    @Test
    public void testConsumerGroupCheck() throws ClientException {
        Validators.checkGroup("abc");
        // Blank case 1.
        checkGroup(" ");
        checkGroup(" abc ");
        checkGroup("abc ");
        // Blank case 2
        checkGroup("abc\t");
        checkGroup("abc\t");
        checkGroup("abc\n");
        checkGroup("abc\f");
        checkGroup("abc\r");
        // Illegal case.
        checkGroup("[abc]");
        // Too long case.
        final String tooLongConsumerGroup =
                StringUtils.repeat("a", Validators.CONSUMER_GROUP_MAX_LENGTH + 1);
        checkGroup(tooLongConsumerGroup);
    }

    private void checkMessage(Message message) {
        try {
            Validators.checkMessage(message);
            fail();
        } catch (ClientException ignore) {
            // ignore on purpose.
        }
    }

    @Test
    public void testMessageCheck() throws ClientException {
        Validators.checkMessage(dummyMessage());
        // Null case.
        checkMessage(null);
        // Topic is blank.
        {
            final Message message = new Message("", "tag", new byte[1]);
            checkMessage(message);
        }
        // Body is null.
        {
            final Message message = new Message(dummyTopic0, dummyTag0, null);
            checkMessage(message);
        }
        // Body length is zero.
        {
            final Message message = new Message(dummyTopic0, dummyTag0, new byte[0]);
            checkMessage(message);
        }
        // Body length exceeds.
        {
            final Message message = dummyMessage(1 + Validators.MESSAGE_BODY_MAX_SIZE);
            checkMessage(message);
        }
    }
}
