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

//! OffsetOption for specifying consume from offset in Lite subscription.

use crate::pb;

/// Policy for determining where to start consuming messages
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OffsetPolicy {
    /// Start from the last consumed offset (default behavior)
    Last = 0,
    /// Start from the minimum available offset (earliest message)
    Min = 1,
    /// Start from the maximum available offset (latest message)
    Max = 2,
}

impl From<OffsetPolicy> for i32 {
    fn from(policy: OffsetPolicy) -> Self {
        policy as i32
    }
}

impl TryFrom<i32> for OffsetPolicy {
    type Error = String;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(OffsetPolicy::Last),
            1 => Ok(OffsetPolicy::Min),
            2 => Ok(OffsetPolicy::Max),
            _ => Err(format!("Invalid offset policy value: {}", value)),
        }
    }
}

/// Offset option for specifying where to start consuming messages
#[derive(Debug, Clone)]
pub struct OffsetOption {
    offset_type: OffsetType,
}

#[derive(Debug, Clone)]
enum OffsetType {
    Policy(OffsetPolicy),
    Offset(i64),
    TailN(i64),
    Timestamp(i64),
}

impl OffsetOption {
    /// Create an OffsetOption with a policy
    pub fn from_policy(policy: OffsetPolicy) -> Self {
        Self {
            offset_type: OffsetType::Policy(policy),
        }
    }

    /// Create an OffsetOption from a specific offset
    pub fn from_offset(offset: i64) -> Self {
        Self {
            offset_type: OffsetType::Offset(offset),
        }
    }

    /// Create an OffsetOption from tail N messages
    pub fn from_tail_n(n: i64) -> Self {
        Self {
            offset_type: OffsetType::TailN(n),
        }
    }

    /// Create an OffsetOption from a timestamp (milliseconds since epoch)
    pub fn from_timestamp(timestamp: i64) -> Self {
        Self {
            offset_type: OffsetType::Timestamp(timestamp),
        }
    }

    /// Convert to protobuf OffsetOption
    pub(crate) fn to_protobuf(&self) -> pb::OffsetOption {
        let offset_type = match &self.offset_type {
            OffsetType::Policy(policy) => pb::offset_option::OffsetType::Policy(*policy as i32),
            OffsetType::Offset(offset) => pb::offset_option::OffsetType::Offset(*offset),
            OffsetType::TailN(n) => pb::offset_option::OffsetType::TailN(*n),
            OffsetType::Timestamp(timestamp) => {
                pb::offset_option::OffsetType::Timestamp(*timestamp)
            }
        };

        pb::OffsetOption {
            offset_type: Some(offset_type),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_offset_option_from_policy() {
        let option = OffsetOption::from_policy(OffsetPolicy::Last);
        let pb = option.to_protobuf();
        assert!(pb.offset_type.is_some());

        if let Some(pb::offset_option::OffsetType::Policy(policy)) = pb.offset_type {
            assert_eq!(policy, 0);
        } else {
            panic!("Expected Policy variant");
        }
    }

    #[test]
    fn test_offset_option_from_policy_min() {
        let option = OffsetOption::from_policy(OffsetPolicy::Min);
        let pb = option.to_protobuf();

        if let Some(pb::offset_option::OffsetType::Policy(policy)) = pb.offset_type {
            assert_eq!(policy, 1);
        } else {
            panic!("Expected Policy variant");
        }
    }

    #[test]
    fn test_offset_option_from_policy_max() {
        let option = OffsetOption::from_policy(OffsetPolicy::Max);
        let pb = option.to_protobuf();

        if let Some(pb::offset_option::OffsetType::Policy(policy)) = pb.offset_type {
            assert_eq!(policy, 2);
        } else {
            panic!("Expected Policy variant");
        }
    }

    #[test]
    fn test_offset_option_from_offset() {
        let option = OffsetOption::from_offset(12345);
        let pb = option.to_protobuf();

        if let Some(pb::offset_option::OffsetType::Offset(offset)) = pb.offset_type {
            assert_eq!(offset, 12345);
        } else {
            panic!("Expected Offset variant");
        }
    }

    #[test]
    fn test_offset_option_from_offset_negative() {
        let option = OffsetOption::from_offset(-100);
        let pb = option.to_protobuf();

        if let Some(pb::offset_option::OffsetType::Offset(offset)) = pb.offset_type {
            assert_eq!(offset, -100);
        } else {
            panic!("Expected Offset variant");
        }
    }

    #[test]
    fn test_offset_option_from_tail_n() {
        let option = OffsetOption::from_tail_n(100);
        let pb = option.to_protobuf();

        if let Some(pb::offset_option::OffsetType::TailN(n)) = pb.offset_type {
            assert_eq!(n, 100);
        } else {
            panic!("Expected TailN variant");
        }
    }

    #[test]
    fn test_offset_option_from_timestamp() {
        let timestamp = 1234567890000;
        let option = OffsetOption::from_timestamp(timestamp);
        let pb = option.to_protobuf();

        if let Some(pb::offset_option::OffsetType::Timestamp(ts)) = pb.offset_type {
            assert_eq!(ts, timestamp);
        } else {
            panic!("Expected Timestamp variant");
        }
    }

    #[test]
    fn test_offset_policy_conversion() {
        assert_eq!(i32::from(OffsetPolicy::Last), 0);
        assert_eq!(i32::from(OffsetPolicy::Min), 1);
        assert_eq!(i32::from(OffsetPolicy::Max), 2);

        assert_eq!(OffsetPolicy::try_from(0).unwrap(), OffsetPolicy::Last);
        assert_eq!(OffsetPolicy::try_from(1).unwrap(), OffsetPolicy::Min);
        assert_eq!(OffsetPolicy::try_from(2).unwrap(), OffsetPolicy::Max);
        assert!(OffsetPolicy::try_from(3).is_err());
        assert!(OffsetPolicy::try_from(-1).is_err());
    }

    #[test]
    fn test_offset_policy_clone_and_copy() {
        let policy1 = OffsetPolicy::Last;
        let policy2 = policy1; // Copy
        let policy3 = policy1.clone(); // Clone

        assert_eq!(policy1, policy2);
        assert_eq!(policy1, policy3);
    }

    #[test]
    fn test_offset_policy_debug() {
        let policy = OffsetPolicy::Min;
        let debug_str = format!("{:?}", policy);
        assert!(debug_str.contains("Min"));
    }
}
