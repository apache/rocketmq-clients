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

package org.apache.rocketmq.client.java.misc;

import java.util.Iterator;

/**
 * Linked element is a simple class that allows you to link elements together.
 *
 * @param <T> the type of the elements to be linked.
 */
public interface LinkedElement<T> {
    /**
     * @return the next element in the linked list.
     */
    T getNext();

    /**
     * @return the iterator over the linked list.
     */
    Iterator<T> iterator();
}