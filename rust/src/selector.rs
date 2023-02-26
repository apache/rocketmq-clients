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
use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use crate::pb;

pub trait QueueSelect {
    fn select(&self, msg: &pb::Message, mqs: Vec<pb::MessageQueue>) -> Option<pb::MessageQueue>;
}

#[derive(Debug, Clone)]
pub enum QueueSelector {
    RoundRobin(RoundRobinQueueSelector),
}

impl QueueSelect for QueueSelector {
    fn select(&self, msg: &pb::Message, mqs: Vec<pb::MessageQueue>) -> Option<pb::MessageQueue> {
        match self {
            QueueSelector::RoundRobin(inner) => inner.select(msg, mqs),
        }
    }
}

impl Default for QueueSelector {
    fn default() -> Self {
        Self::RoundRobin(RoundRobinQueueSelector::new())
    }
}

#[derive(Debug, Clone)]
pub struct RoundRobinQueueSelector {
    // topic -> current index
    indexer: Arc<Mutex<HashMap<String, usize>>>,
}

impl RoundRobinQueueSelector {
    pub fn new() -> Self {
        Self {
            indexer: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl QueueSelect for RoundRobinQueueSelector {
    fn select(&self, msg: &pb::Message, mqs: Vec<pb::MessageQueue>) -> Option<pb::MessageQueue> {
        let topic = msg.topic.clone().unwrap();
        let mut indexer = self.indexer.lock();
        let i = indexer
            .entry(topic.name)
            .and_modify(|e| *e = e.wrapping_add(1))
            .or_insert(1);
        let index = *i % mqs.len();
        mqs.get(index).cloned()
    }
}