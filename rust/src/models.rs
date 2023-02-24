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

/**
 * The codec for the message-id.
 *
 * <p>Codec here provides the following two functions:
 * 1. Provide decoding function of message-id of all versions above v0.
 * 2. Provide a generator of message-id of v1 version.
 *
 * <p>The message-id of versions above V1 consists of 17 bytes in total. The first two bytes represent the version
 * number. For V1, these two bytes are 0x0001.
 *
 * <h3>V1 message id example</h3>
 *
 * <pre>
 * ┌──┬────────────┬────┬────────┬────────┐
 * │01│56F7E71C361B│21BC│024CCDBE│00000000│
 * └──┴────────────┴────┴────────┴────────┘
 * </pre>
 *
 * <h3>V1 version message id generation rules</h3>
 *
 * <pre>
 *                     process id(lower 2bytes)
 *                             ▲
 * mac address(lower 6bytes)   │   sequence number(big endian)
 *                    ▲        │          ▲ (4bytes)
 *                    │        │          │
 *              ┌─────┴─────┐ ┌┴┐ ┌───┐ ┌─┴─┐
 *       0x01+  │     6     │ │2│ │ 4 │ │ 4 │
 *              └───────────┘ └─┘ └─┬─┘ └───┘
 *                                  │
 *                                  ▼
 *           seconds since 2021-01-01 00:00:00(UTC+0)
 *                         (lower 4bytes)
 * </pre>
 */
pub fn generateMessageId() -> String {
    //(<MyStruct as Trait1>::write_u8)(&mut my_struct, byte);
    let mut wtr = Vec::new();
    wtr.write_u8(1).unwrap();
    //mac
    let x = mac_address::get_mac_address().unwrap();
    let ma = match x {
        Some(ma) => ma,
        None => {
            panic!("mac address is none")
        }
    };
    wtr.write_all(&ma.bytes()).unwrap();
    //processid
    wtr.write_u16::<byteorder::BigEndian>(process::id() as u16)
        .unwrap();

    let now = OffsetDateTime::now_utc();
    let year = now.year();
    let month = now.month();
    let start_timestamp = PrimitiveDateTime::new(
        Date::from_calendar_date(year, month, 1).unwrap(),
        Time::from_hms(0, 0, 0).unwrap(),
    )
    .assume_offset(now.offset())
    .unix_timestamp();

    wtr.write_u32::<byteorder::BigEndian>(
        ((OffsetDateTime::now_utc().unix_timestamp() - start_timestamp) * 1000) as u32,
    )
    .unwrap();

    wtr.write_u32::<byteorder::BigEndian>(45).unwrap();
    hex::encode_upper(wtr)
}

//test generateMessageId
#[test]
fn test_generateMessageId() {
    let id = generateMessageId();
    println!("id:{}", id);
}
