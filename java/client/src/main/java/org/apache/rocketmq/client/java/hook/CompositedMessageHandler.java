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

package org.apache.rocketmq.client.java.hook;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.client.java.message.GeneralMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("rawtypes")
public class CompositedMessageHandler implements MessageHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageHandler.class);
    private static final AttributeKey<Map<Integer, Map<AttributeKey, Attribute>>> HANDLER_ATTRIBUTES_KEY =
        AttributeKey.create("composited_handler_attributes");
    private final List<MessageHandler> handlers;

    public CompositedMessageHandler(List<MessageHandler> handlers) {
        this.handlers = handlers;
    }

    @Override
    public void doBefore(MessageHandlerContext context0, List<GeneralMessage> messages) {
        final HashMap<Integer, Map<AttributeKey, Attribute>> attributeMap = new HashMap<>();
        for (int index = 0; index < handlers.size(); index++) {
            MessageHandler handler = handlers.get(index);
            final MessageHookPoints messageHookPoints = context0.getMessageHookPoints();
            final MessageHookPointsStatus status = context0.getStatus();
            final MessageHandlerContextImpl context = new MessageHandlerContextImpl(messageHookPoints, status);
            try {
                handler.doBefore(context, messages);
            } catch (Throwable t) {
                LOGGER.error("Exception raised while handing messages", t);
            }
            final Map<AttributeKey, Attribute> attributes = context.getAttributes();
            attributeMap.put(index, attributes);
        }
        context0.putAttribute(HANDLER_ATTRIBUTES_KEY, Attribute.create(attributeMap));
    }

    @Override
    public void doAfter(MessageHandlerContext context0, List<GeneralMessage> messages) {
        for (int index = handlers.size() - 1; index >= 0; index--) {
            final Map<Integer, Map<AttributeKey, Attribute>> attributeMap =
                context0.getAttribute(HANDLER_ATTRIBUTES_KEY).get();
            final Map<AttributeKey, Attribute> attributes = attributeMap.get(index);
            final MessageHookPoints messageHookPoints = context0.getMessageHookPoints();
            final MessageHookPointsStatus status = context0.getStatus();
            final MessageHandlerContextImpl context = new MessageHandlerContextImpl(messageHookPoints, status,
                attributes);
            MessageHandler handler = handlers.get(index);
            try {
                handler.doAfter(context, messages);
            } catch (Throwable t) {
                LOGGER.error("Exception raised while handing messages", t);
            }
        }
    }
}
