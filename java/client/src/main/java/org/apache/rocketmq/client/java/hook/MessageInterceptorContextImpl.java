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
import java.util.Map;

@SuppressWarnings("rawtypes")
public class MessageInterceptorContextImpl implements MessageInterceptorContext {
    private final MessageHookPoints messageHookPoints;
    private MessageHookPointsStatus status;
    private final Map<AttributeKey, Attribute> attributes;

    public MessageInterceptorContextImpl(MessageHookPoints messageHookPoints) {
        this.messageHookPoints = messageHookPoints;
        this.status = MessageHookPointsStatus.UNSET;
        this.attributes = new HashMap<>();
    }

    public MessageInterceptorContextImpl(MessageHookPoints messageHookPoints, MessageHookPointsStatus status) {
        this.messageHookPoints = messageHookPoints;
        this.status = status;
        this.attributes = new HashMap<>();
    }

    public MessageInterceptorContextImpl(MessageHookPoints messageHookPoints, MessageHookPointsStatus status,
        Map<AttributeKey, Attribute> attributes) {
        this.messageHookPoints = messageHookPoints;
        this.status = status;
        this.attributes = attributes;
    }

    public MessageInterceptorContextImpl(MessageInterceptorContextImpl context, MessageHookPointsStatus status) {
        this.messageHookPoints = context.messageHookPoints;
        this.status = status;
        this.attributes = context.attributes;
    }

    @Override
    public MessageHookPoints getMessageHookPoints() {
        return messageHookPoints;
    }

    @Override
    public MessageHookPointsStatus getStatus() {
        return status;
    }

    public void setStatus(MessageHookPointsStatus status) {
        this.status = status;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Attribute<T> getAttribute(AttributeKey<T> key) {
        return attributes.get(key);
    }

    @Override
    public <T> void putAttribute(AttributeKey<T> key, Attribute<T> attribute) {
        attributes.put(key, attribute);
    }

    public Map<AttributeKey, Attribute> getAttributes() {
        return attributes;
    }
}
