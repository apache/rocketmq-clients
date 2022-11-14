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
public class CompositedMessageInterceptor implements MessageInterceptor {
    private static final Logger log = LoggerFactory.getLogger(MessageInterceptor.class);
    private static final AttributeKey<Map<Integer, Map<AttributeKey, Attribute>>> INTERCEPTOR_ATTRIBUTES_KEY =
        AttributeKey.create("composited_interceptor_attributes");
    private final List<MessageInterceptor> interceptors;

    public CompositedMessageInterceptor(List<MessageInterceptor> interceptors) {
        this.interceptors = interceptors;
    }

    @Override
    public void doBefore(MessageInterceptorContext context0, List<GeneralMessage> messages) {
        final HashMap<Integer, Map<AttributeKey, Attribute>> attributeMap = new HashMap<>();
        for (int index = 0; index < interceptors.size(); index++) {
            MessageInterceptor interceptor = interceptors.get(index);
            final MessageHookPoints messageHookPoints = context0.getMessageHookPoints();
            final MessageHookPointsStatus status = context0.getStatus();
            final MessageInterceptorContextImpl context = new MessageInterceptorContextImpl(messageHookPoints, status);
            try {
                interceptor.doBefore(context, messages);
            } catch (Throwable t) {
                log.error("Exception raised while handing messages", t);
            }
            final Map<AttributeKey, Attribute> attributes = context.getAttributes();
            attributeMap.put(index, attributes);
        }
        context0.putAttribute(INTERCEPTOR_ATTRIBUTES_KEY, Attribute.create(attributeMap));
    }

    @Override
    public void doAfter(MessageInterceptorContext context0, List<GeneralMessage> messages) {
        for (int index = interceptors.size() - 1; index >= 0; index--) {
            final Map<Integer, Map<AttributeKey, Attribute>> attributeMap =
                context0.getAttribute(INTERCEPTOR_ATTRIBUTES_KEY).get();
            final Map<AttributeKey, Attribute> attributes = attributeMap.get(index);
            final MessageHookPoints messageHookPoints = context0.getMessageHookPoints();
            final MessageHookPointsStatus status = context0.getStatus();
            final MessageInterceptorContextImpl context = new MessageInterceptorContextImpl(messageHookPoints, status,
                attributes);
            MessageInterceptor interceptor = interceptors.get(index);
            try {
                interceptor.doAfter(context, messages);
            } catch (Throwable t) {
                log.error("Exception raised while handing messages", t);
            }
        }
    }
}
