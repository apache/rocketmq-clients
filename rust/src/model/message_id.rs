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
use std::io::Write;
use std::process;
use std::time::SystemTime;

use byteorder::{BigEndian, WriteBytesExt};
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use time::{Date, OffsetDateTime, PrimitiveDateTime, Time};

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

// inspired by https://github.com/messense/rocketmq-rs
pub(crate) static UNIQ_ID_GENERATOR: Lazy<Mutex<UniqueIdGenerator>> = Lazy::new(|| {
    let mut wtr = Vec::new();
    wtr.write_u8(1).unwrap();
    // mac
    let x = mac_address::get_mac_address().unwrap();
    let ma = match x {
        Some(ma) => ma,
        None => {
            panic!("mac address is none")
        }
    };
    wtr.write_all(&ma.bytes()).unwrap();
    // process id
    wtr.write_u16::<BigEndian>(process::id() as u16).unwrap();
    let generator = UniqueIdGenerator {
        counter: 0,
        start_timestamp: 0,
        next_timestamp: 0,
        prefix: hex::encode_upper(wtr),
    };
    Mutex::new(generator)
});

pub(crate) struct UniqueIdGenerator {
    counter: i32,
    prefix: String,
    start_timestamp: i64,
    next_timestamp: i64,
}

impl UniqueIdGenerator {
    pub fn next_id(&mut self) -> String {
        if SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
            > self.next_timestamp
        {
            // update timestamp
            let now = OffsetDateTime::now_utc();
            let year = now.year();
            let month = now.month();
            self.start_timestamp = PrimitiveDateTime::new(
                Date::from_calendar_date(year, month, 1).unwrap(),
                Time::from_hms(0, 0, 0).unwrap(),
            )
            .assume_offset(now.offset())
            .unix_timestamp();
            self.next_timestamp = (PrimitiveDateTime::new(
                Date::from_calendar_date(year, month, 1).unwrap(),
                Time::from_hms(0, 0, 0).unwrap(),
            )
            .assume_offset(now.offset())
                + time::Duration::days(30))
            .unix_timestamp();
        }
        self.counter = self.counter.wrapping_add(1);
        let mut buf = Vec::new();
        buf.write_i32::<BigEndian>(
            ((OffsetDateTime::now_utc().unix_timestamp() - self.start_timestamp) * 1000) as i32,
        )
        .unwrap();
        buf.write_i32::<BigEndian>(self.counter).unwrap();
        self.prefix.clone() + &hex::encode(buf)
    }
}

#[cfg(test)]
mod test {
    #[ignore]
    #[test]
    fn generate_uniq_id() {
        use super::UNIQ_ID_GENERATOR;
        for i in 1..17 {
            let uid = UNIQ_ID_GENERATOR.lock().next_id();
            assert_eq!(uid.len(), 34);
            assert_eq!(uid.get(26..).unwrap(), hex::encode(vec![0, 0, 0, i as u8]));
        }
    }
}
