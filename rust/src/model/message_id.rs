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
use std::time::Instant;

use byteorder::{BigEndian, WriteBytesExt};
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use time::OffsetDateTime;

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
// Reference Java: org.apache.rocketmq.client.java.message.MessageIdCodec
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
    Mutex::new(UniqueIdGenerator::new(hex::encode_upper(wtr)))
});

/// 2021-01-01 00:00:00 UTC, the custom epoch of the V1 message id.
const CUSTOM_EPOCH_UNIX_SECONDS: i64 = 1_609_459_200;

pub(crate) struct UniqueIdGenerator {
    prefix: String,
    seconds_since_custom_epoch: i64,
    start_instant: Instant,
    sequence: i32,
}

impl UniqueIdGenerator {
    fn new(prefix: String) -> Self {
        Self::start_at(prefix, OffsetDateTime::now_utc())
    }

    /// Read the wall clock once, then advance with the monotonic clock, same as Java.
    fn start_at(prefix: String, now: OffsetDateTime) -> Self {
        UniqueIdGenerator {
            prefix,
            seconds_since_custom_epoch: now.unix_timestamp() - CUSTOM_EPOCH_UNIX_SECONDS,
            start_instant: Instant::now(),
            sequence: 0,
        }
    }

    fn delta_seconds(&self) -> i64 {
        self.seconds_since_custom_epoch + self.start_instant.elapsed().as_secs() as i64
    }

    pub fn next_id(&mut self) -> String {
        let id = encode_message_id(&self.prefix, self.delta_seconds(), self.sequence);
        self.sequence = self.sequence.wrapping_add(1);
        id
    }
}

fn encode_message_id(prefix: &str, delta_seconds: i64, sequence: i32) -> String {
    let mut buf = Vec::with_capacity(8);
    // lower 4 bytes of the seconds since the custom epoch
    buf.write_u32::<BigEndian>(delta_seconds as u32).unwrap();
    buf.write_i32::<BigEndian>(sequence).unwrap();
    format!("{}{}", prefix, hex::encode_upper(buf))
}

#[cfg(test)]
mod test {
    use std::time::{Duration, Instant};

    use time::{Date, Month, OffsetDateTime};

    use super::{encode_message_id, UniqueIdGenerator, CUSTOM_EPOCH_UNIX_SECONDS};

    fn utc(year: i32, month: Month, day: u8, hour: u8, minute: u8, second: u8) -> OffsetDateTime {
        Date::from_calendar_date(year, month, day)
            .unwrap()
            .with_hms(hour, minute, second)
            .unwrap()
            .assume_utc()
    }

    /// Decode the time field the same way as the broker does for a V1 message id.
    fn decode_time(message_id: &str) -> OffsetDateTime {
        assert_eq!(message_id.len(), 34);
        assert_eq!(&message_id[..2], "01");
        let seconds = u32::from_str_radix(&message_id[18..26], 16).unwrap();
        OffsetDateTime::from_unix_timestamp(CUSTOM_EPOCH_UNIX_SECONDS + seconds as i64).unwrap()
    }

    /// A message id generated by the previous implementation on 2026-11-05 09:03:40 UTC: the time
    /// field `168b2de0` is milliseconds since the start of the month, which the broker decodes as
    /// 2032-12-26.
    #[test]
    fn regression_legacy_message_id_decoded_as_2032() {
        const LEGACY_ID: &str = "0156F7E71C361B21BC168b2de000000132";
        assert_eq!(
            decode_time(LEGACY_ID),
            utc(2032, Month::December, 26, 13, 6, 40)
        );

        let sent_at = utc(2026, Month::November, 5, 9, 3, 40);
        let mut generator = UniqueIdGenerator::start_at(LEGACY_ID[..18].to_string(), sent_at);
        generator.sequence = 0x132;
        let id = generator.next_id();
        assert_eq!(id, "0156F7E71C361B21BC0AFDE06C00000132");
        assert_eq!(decode_time(&id), sent_at);
    }

    #[test]
    fn time_field_decodes_to_send_time_on_any_day_of_month() {
        let cases = [
            utc(2021, Month::January, 1, 0, 0, 0),
            // The previous implementation looked right only before ~03:13 on the 3rd.
            utc(2026, Month::November, 1, 0, 0, 0),
            utc(2026, Month::November, 3, 3, 13, 0),
            utc(2026, Month::November, 3, 3, 14, 0),
            // Milliseconds since the start of the month overflowed i32 from 24 days 20:31:23.
            utc(2026, Month::November, 25, 20, 31, 24),
            utc(2026, Month::November, 30, 23, 59, 59),
            // The previous implementation kept February until 30 days had passed.
            utc(2026, Month::March, 1, 0, 0, 1),
            utc(2026, Month::March, 2, 12, 0, 0),
            utc(2028, Month::February, 29, 12, 0, 0),
            utc(2050, Month::December, 31, 23, 59, 59),
        ];
        for sent_at in cases {
            let id =
                UniqueIdGenerator::start_at("0156F7E71C361B21BC".to_string(), sent_at).next_id();
            assert_eq!(decode_time(&id), sent_at, "id={id}");
        }
    }

    #[test]
    fn time_field_advances_with_monotonic_clock() {
        let started_at = utc(2026, Month::November, 5, 9, 3, 40);
        let mut generator =
            UniqueIdGenerator::start_at("0156F7E71C361B21BC".to_string(), started_at);
        generator.start_instant = Instant::now().checked_sub(Duration::from_secs(90)).unwrap();
        assert_eq!(
            decode_time(&generator.next_id()),
            started_at + time::Duration::seconds(90)
        );
    }

    #[test]
    fn global_generator_uses_current_time_and_upper_case() {
        use super::UNIQ_ID_GENERATOR;
        let id = UNIQ_ID_GENERATOR.lock().next_id();
        assert_eq!(id, id.to_uppercase());
        let drift = decode_time(&id) - OffsetDateTime::now_utc();
        assert!(
            drift.abs() <= time::Duration::seconds(2),
            "id={id}, drift={drift}"
        );
    }

    #[test]
    fn encode_matches_java_v1_layout() {
        // The example in the Java MessageIdCodec doc.
        assert_eq!(
            encode_message_id("0156F7E71C361B21BC", 0x024CCDBE, 0),
            "0156F7E71C361B21BC024CCDBE00000000"
        );
    }

    #[test]
    fn encode_keeps_lower_4_bytes_of_seconds() {
        assert_eq!(encode_message_id("", 0x1_0000_0001, 0), "0000000100000000");
        assert_eq!(encode_message_id("", -1, 0), "FFFFFFFF00000000");
    }

    #[test]
    fn sequence_starts_from_zero_and_wraps() {
        let mut generator = UniqueIdGenerator::new("0156F7E71C361B21BC".to_string());
        for i in 0..16u8 {
            let id = generator.next_id();
            assert_eq!(id.len(), 34);
            assert_eq!(&id[26..], hex::encode_upper([0, 0, 0, i]));
        }

        generator.sequence = i32::MAX;
        assert!(generator.next_id().ends_with("7FFFFFFF"));
        assert!(generator.next_id().ends_with("80000000"));
    }
}
