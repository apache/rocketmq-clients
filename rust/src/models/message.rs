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

use byteorder::WriteBytesExt;
use mac_address::get_mac_address;
use std::{
    collections::HashMap,
    io::Write,
    mem, process,
    sync::Arc,
    sync::{atomic::AtomicUsize, Weak},
};
use time::{Date, OffsetDateTime, PrimitiveDateTime, Time};

pub(crate) struct MessageImpl {
    pub(crate) keys: Vec<String>,
    pub(crate) body: Vec<u8>,
    pub(crate) topic: String,
    pub(crate) tags: String,
    pub(crate) messageGroup: String,
    pub(crate) deliveryTimestamp: i64,
    pub(crate) properties: HashMap<String, String>,
}

impl MessageImpl {
    pub fn new(topic: &str, tags: &str, keys: Vec<String>, body: &str) -> Self {
        MessageImpl {
            keys: keys,
            body: body.as_bytes().to_vec(),
            topic: topic.to_string(),
            tags: tags.to_string(),
            messageGroup: "".to_string(),
            deliveryTimestamp: 0,
            properties: HashMap::new(),
        }
    }
}