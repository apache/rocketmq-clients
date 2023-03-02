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

use crate::pb;
use parking_lot::Mutex;

pub trait ConsumerQueueSelect {
    fn select(&self, mqs: Vec<pb::MessageQueue>) -> Option<pb::MessageQueue>;
}

#[derive(Debug, Clone)]
pub enum ConsumeQueueSelector {
    RoundRobin(RoundRobinQueueSelector),
}

impl ConsumerQueueSelect for ConsumeQueueSelector {
    fn select(&self, mqs: Vec<pb::MessageQueue>) -> Option<pb::MessageQueue> {
        match self {
            ConsumeQueueSelector::RoundRobin(inner) => inner.select(mqs),
        }
    }
}

impl Default for ConsumeQueueSelector {
    fn default() -> Self {
        Self::RoundRobin(RoundRobinQueueSelector::new())
    }
}

#[derive(Debug, Clone)]
pub struct RoundRobinQueueSelector {
    // topic -> current index
    indexer: Arc<Mutex<usize>>,
}

impl RoundRobinQueueSelector {
    pub fn new() -> Self {
        Self {
            indexer: Arc::new(Mutex::new(0)),
        }
    }
}

impl ConsumerQueueSelect for RoundRobinQueueSelector {
    fn select(&self, mqs: Vec<pb::MessageQueue>) -> Option<pb::MessageQueue> {
        let mut indexer = self.indexer.lock();
        let i = indexer
            .wrapping_add(1);
        let index = i % mqs.len();
        mqs.get(index).cloned()
    }
}
