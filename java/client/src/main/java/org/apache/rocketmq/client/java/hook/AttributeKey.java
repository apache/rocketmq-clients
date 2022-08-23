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

import com.google.common.base.Objects;

/**
 * Key which can be used to access {@link Attribute}. Be aware that it is not possible to have multiple keys with the
 * same name.
 *
 * @param <T> the type of the {@link Attribute} which can be accessed via this {@link AttributeKey}.
 */
@SuppressWarnings("UnusedDeclaration") // 'T' is used only at compile time
public class AttributeKey<T> {
    private final String name;

    private AttributeKey(String name) {
        this.name = name;
    }

    public static <T> AttributeKey<T> create(String name) {
        return new AttributeKey<>(name);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AttributeKey<?> that = (AttributeKey<?>) o;
        return Objects.equal(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name);
    }
}
