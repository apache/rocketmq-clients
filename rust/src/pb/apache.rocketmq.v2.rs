#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FilterExpression {
    #[prost(enumeration = "FilterType", tag = "1")]
    pub r#type: i32,
    #[prost(string, tag = "2")]
    pub expression: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RetryPolicy {
    #[prost(int32, tag = "1")]
    pub max_attempts: i32,
    #[prost(oneof = "retry_policy::Strategy", tags = "2, 3")]
    pub strategy: ::core::option::Option<retry_policy::Strategy>,
}
/// Nested message and enum types in `RetryPolicy`.
pub mod retry_policy {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Strategy {
        #[prost(message, tag = "2")]
        ExponentialBackoff(super::ExponentialBackoff),
        #[prost(message, tag = "3")]
        CustomizedBackoff(super::CustomizedBackoff),
    }
}
/// <https://en.wikipedia.org/wiki/Exponential_backoff>
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExponentialBackoff {
    #[prost(message, optional, tag = "1")]
    pub initial: ::core::option::Option<::prost_types::Duration>,
    #[prost(message, optional, tag = "2")]
    pub max: ::core::option::Option<::prost_types::Duration>,
    #[prost(float, tag = "3")]
    pub multiplier: f32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CustomizedBackoff {
    /// To support classic backoff strategy which is arbitrary defined by end users.
    /// Typical values are: `1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h`
    #[prost(message, repeated, tag = "1")]
    pub next: ::prost::alloc::vec::Vec<::prost_types::Duration>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Resource {
    #[prost(string, tag = "1")]
    pub resource_namespace: ::prost::alloc::string::String,
    /// Resource name identifier, which remains unique within the abstract resource
    /// namespace.
    #[prost(string, tag = "2")]
    pub name: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SubscriptionEntry {
    #[prost(message, optional, tag = "1")]
    pub topic: ::core::option::Option<Resource>,
    #[prost(message, optional, tag = "2")]
    pub expression: ::core::option::Option<FilterExpression>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Address {
    #[prost(string, tag = "1")]
    pub host: ::prost::alloc::string::String,
    #[prost(int32, tag = "2")]
    pub port: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Endpoints {
    #[prost(enumeration = "AddressScheme", tag = "1")]
    pub scheme: i32,
    #[prost(message, repeated, tag = "2")]
    pub addresses: ::prost::alloc::vec::Vec<Address>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Broker {
    /// Name of the broker
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    /// Broker index. Canonically, index = 0 implies that the broker is playing
    /// leader role while brokers with index > 0 play follower role.
    #[prost(int32, tag = "2")]
    pub id: i32,
    /// Address of the broker, complying with the following scheme
    /// 1. dns:\[//authority/]host[:port\]
    /// 2. ipv4:address\[:port][,address[:port],...\] – IPv4 addresses
    /// 3. ipv6:address\[:port][,address[:port],...\] – IPv6 addresses
    #[prost(message, optional, tag = "3")]
    pub endpoints: ::core::option::Option<Endpoints>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MessageQueue {
    #[prost(message, optional, tag = "1")]
    pub topic: ::core::option::Option<Resource>,
    #[prost(int32, tag = "2")]
    pub id: i32,
    #[prost(enumeration = "Permission", tag = "3")]
    pub permission: i32,
    #[prost(message, optional, tag = "4")]
    pub broker: ::core::option::Option<Broker>,
    #[prost(enumeration = "MessageType", repeated, tag = "5")]
    pub accept_message_types: ::prost::alloc::vec::Vec<i32>,
}
/// When publishing messages to or subscribing messages from brokers, clients
/// shall include or validate digests of message body to ensure data integrity.
///
/// For message publishing, when an invalid digest were detected, brokers need
/// respond client with BAD_REQUEST.
///
/// For messages subscription, when an invalid digest were detected, consumers
/// need to handle this case according to message type:
/// 1) Standard messages should be negatively acknowledged instantly, causing
/// immediate re-delivery; 2) FIFO messages require special RPC, to re-fetch
/// previously acquired messages batch;
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Digest {
    #[prost(enumeration = "DigestType", tag = "1")]
    pub r#type: i32,
    #[prost(string, tag = "2")]
    pub checksum: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SystemProperties {
    /// Tag, which is optional.
    #[prost(string, optional, tag = "1")]
    pub tag: ::core::option::Option<::prost::alloc::string::String>,
    /// Message keys
    #[prost(string, repeated, tag = "2")]
    pub keys: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// Message identifier, client-side generated, remains unique.
    /// if message_id is empty, the send message request will be aborted with
    /// status `INVALID_ARGUMENT`
    #[prost(string, tag = "3")]
    pub message_id: ::prost::alloc::string::String,
    /// Message body digest
    #[prost(message, optional, tag = "4")]
    pub body_digest: ::core::option::Option<Digest>,
    /// Message body encoding. Candidate options are identity, gzip, snappy etc.
    #[prost(enumeration = "Encoding", tag = "5")]
    pub body_encoding: i32,
    /// Message type, normal, FIFO or transactional.
    #[prost(enumeration = "MessageType", tag = "6")]
    pub message_type: i32,
    /// Message born time-point.
    #[prost(message, optional, tag = "7")]
    pub born_timestamp: ::core::option::Option<::prost_types::Timestamp>,
    /// Message born host. Valid options are IPv4, IPv6 or client host domain name.
    #[prost(string, tag = "8")]
    pub born_host: ::prost::alloc::string::String,
    /// Time-point at which the message is stored in the broker, which is absent
    /// for message publishing.
    #[prost(message, optional, tag = "9")]
    pub store_timestamp: ::core::option::Option<::prost_types::Timestamp>,
    /// The broker that stores this message. It may be broker name, IP or arbitrary
    /// identifier that uniquely identify the server.
    #[prost(string, tag = "10")]
    pub store_host: ::prost::alloc::string::String,
    /// Time-point at which broker delivers to clients, which is optional.
    #[prost(message, optional, tag = "11")]
    pub delivery_timestamp: ::core::option::Option<::prost_types::Timestamp>,
    /// If a message is acquired by way of POP, this field holds the receipt,
    /// which is absent for message publishing.
    /// Clients use the receipt to acknowledge or negatively acknowledge the
    /// message.
    #[prost(string, optional, tag = "12")]
    pub receipt_handle: ::core::option::Option<::prost::alloc::string::String>,
    /// Message queue identifier in which a message is physically stored.
    #[prost(int32, tag = "13")]
    pub queue_id: i32,
    /// Message-queue offset at which a message is stored, which is absent for
    /// message publishing.
    #[prost(int64, optional, tag = "14")]
    pub queue_offset: ::core::option::Option<i64>,
    /// Period of time servers would remain invisible once a message is acquired.
    #[prost(message, optional, tag = "15")]
    pub invisible_duration: ::core::option::Option<::prost_types::Duration>,
    /// Business code may failed to process messages for the moment. Hence, clients
    /// may request servers to deliver them again using certain back-off strategy,
    /// the attempt is 1 not 0 if message is delivered first time, and it is absent
    /// for message publishing.
    #[prost(int32, optional, tag = "16")]
    pub delivery_attempt: ::core::option::Option<i32>,
    /// Define the group name of message in the same topic, which is optional.
    #[prost(string, optional, tag = "17")]
    pub message_group: ::core::option::Option<::prost::alloc::string::String>,
    /// Trace context for each message, which is optional.
    #[prost(string, optional, tag = "18")]
    pub trace_context: ::core::option::Option<::prost::alloc::string::String>,
    /// If a transactional message stay unresolved for more than
    /// `transaction_orphan_threshold`, it would be regarded as an
    /// orphan. Servers that manages orphan messages would pick up
    /// a capable publisher to resolve
    #[prost(message, optional, tag = "19")]
    pub orphaned_transaction_recovery_duration: ::core::option::Option<
        ::prost_types::Duration,
    >,
    /// Information to identify whether this message is from dead letter queue.
    #[prost(message, optional, tag = "20")]
    pub dead_letter_queue: ::core::option::Option<DeadLetterQueue>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeadLetterQueue {
    /// Original topic for this DLQ message.
    #[prost(string, tag = "1")]
    pub topic: ::prost::alloc::string::String,
    /// Original message id for this DLQ message.
    #[prost(string, tag = "2")]
    pub message_id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Message {
    #[prost(message, optional, tag = "1")]
    pub topic: ::core::option::Option<Resource>,
    /// User defined key-value pairs.
    /// If user_properties contain the reserved keys by RocketMQ,
    /// the send message request will be aborted with status `INVALID_ARGUMENT`.
    /// See below links for the reserved keys
    /// <https://github.com/apache/rocketmq/blob/master/common/src/main/java/org/apache/rocketmq/common/message/MessageConst.java#L58>
    #[prost(map = "string, string", tag = "2")]
    pub user_properties: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        ::prost::alloc::string::String,
    >,
    #[prost(message, optional, tag = "3")]
    pub system_properties: ::core::option::Option<SystemProperties>,
    #[prost(bytes = "vec", tag = "4")]
    pub body: ::prost::alloc::vec::Vec<u8>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Assignment {
    #[prost(message, optional, tag = "1")]
    pub message_queue: ::core::option::Option<MessageQueue>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Status {
    #[prost(enumeration = "Code", tag = "1")]
    pub code: i32,
    #[prost(string, tag = "2")]
    pub message: ::prost::alloc::string::String,
}
/// User Agent
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Ua {
    /// SDK language
    #[prost(enumeration = "Language", tag = "1")]
    pub language: i32,
    /// SDK version
    #[prost(string, tag = "2")]
    pub version: ::prost::alloc::string::String,
    /// Platform details, including OS name, version, arch etc.
    #[prost(string, tag = "3")]
    pub platform: ::prost::alloc::string::String,
    /// Hostname of the node
    #[prost(string, tag = "4")]
    pub hostname: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Settings {
    /// Configurations for all clients.
    #[prost(enumeration = "ClientType", optional, tag = "1")]
    pub client_type: ::core::option::Option<i32>,
    #[prost(message, optional, tag = "2")]
    pub access_point: ::core::option::Option<Endpoints>,
    /// If publishing of messages encounters throttling or server internal errors,
    /// publishers should implement automatic retries after progressive longer
    /// back-offs for consecutive errors.
    ///
    /// When processing message fails, `backoff_policy` describes an interval
    /// after which the message should be available to consume again.
    ///
    /// For FIFO messages, the interval should be relatively small because
    /// messages of the same message group would not be readily available until
    /// the prior one depletes its lifecycle.
    #[prost(message, optional, tag = "3")]
    pub backoff_policy: ::core::option::Option<RetryPolicy>,
    /// Request timeout for RPCs excluding long-polling.
    #[prost(message, optional, tag = "4")]
    pub request_timeout: ::core::option::Option<::prost_types::Duration>,
    /// User agent details
    #[prost(message, optional, tag = "7")]
    pub user_agent: ::core::option::Option<Ua>,
    #[prost(message, optional, tag = "8")]
    pub metric: ::core::option::Option<Metric>,
    #[prost(oneof = "settings::PubSub", tags = "5, 6")]
    pub pub_sub: ::core::option::Option<settings::PubSub>,
}
/// Nested message and enum types in `Settings`.
pub mod settings {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum PubSub {
        #[prost(message, tag = "5")]
        Publishing(super::Publishing),
        #[prost(message, tag = "6")]
        Subscription(super::Subscription),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Publishing {
    /// Publishing settings below here is appointed by client, thus it is
    /// unnecessary for server to push at present.
    ///
    /// List of topics to which messages will publish to.
    #[prost(message, repeated, tag = "1")]
    pub topics: ::prost::alloc::vec::Vec<Resource>,
    /// If the message body size exceeds `max_body_size`, broker servers would
    /// reject the request. As a result, it is advisable that Producer performs
    /// client-side check validation.
    #[prost(int32, tag = "2")]
    pub max_body_size: i32,
    /// When `validate_message_type` flag set `false`, no need to validate message's type
    /// with messageQueue's `accept_message_types` before publishing.
    #[prost(bool, tag = "3")]
    pub validate_message_type: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Subscription {
    /// Subscription settings below here is appointed by client, thus it is
    /// unnecessary for server to push at present.
    ///
    /// Consumer group.
    #[prost(message, optional, tag = "1")]
    pub group: ::core::option::Option<Resource>,
    /// Subscription for consumer.
    #[prost(message, repeated, tag = "2")]
    pub subscriptions: ::prost::alloc::vec::Vec<SubscriptionEntry>,
    /// Subscription settings below here are from server, it is essential for
    /// server to push.
    ///
    /// When FIFO flag is `true`, messages of the same message group are processed
    /// in first-in-first-out manner.
    ///
    /// Brokers will not deliver further messages of the same group until prior
    /// ones are completely acknowledged.
    #[prost(bool, optional, tag = "3")]
    pub fifo: ::core::option::Option<bool>,
    /// Message receive batch size here is essential for push consumer.
    #[prost(int32, optional, tag = "4")]
    pub receive_batch_size: ::core::option::Option<i32>,
    /// Long-polling timeout for `ReceiveMessageRequest`, which is essential for
    /// push consumer.
    #[prost(message, optional, tag = "5")]
    pub long_polling_timeout: ::core::option::Option<::prost_types::Duration>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Metric {
    /// Indicates that if client should export local metrics to server.
    #[prost(bool, tag = "1")]
    pub on: bool,
    /// The endpoint that client metrics should be exported to, which is required if the switch is on.
    #[prost(message, optional, tag = "2")]
    pub endpoints: ::core::option::Option<Endpoints>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum TransactionResolution {
    Unspecified = 0,
    Commit = 1,
    Rollback = 2,
}
impl TransactionResolution {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            TransactionResolution::Unspecified => "TRANSACTION_RESOLUTION_UNSPECIFIED",
            TransactionResolution::Commit => "COMMIT",
            TransactionResolution::Rollback => "ROLLBACK",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "TRANSACTION_RESOLUTION_UNSPECIFIED" => Some(Self::Unspecified),
            "COMMIT" => Some(Self::Commit),
            "ROLLBACK" => Some(Self::Rollback),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum TransactionSource {
    SourceUnspecified = 0,
    SourceClient = 1,
    SourceServerCheck = 2,
}
impl TransactionSource {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            TransactionSource::SourceUnspecified => "SOURCE_UNSPECIFIED",
            TransactionSource::SourceClient => "SOURCE_CLIENT",
            TransactionSource::SourceServerCheck => "SOURCE_SERVER_CHECK",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "SOURCE_UNSPECIFIED" => Some(Self::SourceUnspecified),
            "SOURCE_CLIENT" => Some(Self::SourceClient),
            "SOURCE_SERVER_CHECK" => Some(Self::SourceServerCheck),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum Permission {
    Unspecified = 0,
    None = 1,
    Read = 2,
    Write = 3,
    ReadWrite = 4,
}
impl Permission {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Permission::Unspecified => "PERMISSION_UNSPECIFIED",
            Permission::None => "NONE",
            Permission::Read => "READ",
            Permission::Write => "WRITE",
            Permission::ReadWrite => "READ_WRITE",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "PERMISSION_UNSPECIFIED" => Some(Self::Unspecified),
            "NONE" => Some(Self::None),
            "READ" => Some(Self::Read),
            "WRITE" => Some(Self::Write),
            "READ_WRITE" => Some(Self::ReadWrite),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum FilterType {
    Unspecified = 0,
    Tag = 1,
    Sql = 2,
}
impl FilterType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            FilterType::Unspecified => "FILTER_TYPE_UNSPECIFIED",
            FilterType::Tag => "TAG",
            FilterType::Sql => "SQL",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "FILTER_TYPE_UNSPECIFIED" => Some(Self::Unspecified),
            "TAG" => Some(Self::Tag),
            "SQL" => Some(Self::Sql),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum AddressScheme {
    Unspecified = 0,
    IPv4 = 1,
    IPv6 = 2,
    DomainName = 3,
}
impl AddressScheme {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            AddressScheme::Unspecified => "ADDRESS_SCHEME_UNSPECIFIED",
            AddressScheme::IPv4 => "IPv4",
            AddressScheme::IPv6 => "IPv6",
            AddressScheme::DomainName => "DOMAIN_NAME",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "ADDRESS_SCHEME_UNSPECIFIED" => Some(Self::Unspecified),
            "IPv4" => Some(Self::IPv4),
            "IPv6" => Some(Self::IPv6),
            "DOMAIN_NAME" => Some(Self::DomainName),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum MessageType {
    Unspecified = 0,
    Normal = 1,
    /// Sequenced message
    Fifo = 2,
    /// Messages that are delivered after the specified duration.
    Delay = 3,
    /// Messages that are transactional. Only committed messages are delivered to
    /// subscribers.
    Transaction = 4,
}
impl MessageType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            MessageType::Unspecified => "MESSAGE_TYPE_UNSPECIFIED",
            MessageType::Normal => "NORMAL",
            MessageType::Fifo => "FIFO",
            MessageType::Delay => "DELAY",
            MessageType::Transaction => "TRANSACTION",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "MESSAGE_TYPE_UNSPECIFIED" => Some(Self::Unspecified),
            "NORMAL" => Some(Self::Normal),
            "FIFO" => Some(Self::Fifo),
            "DELAY" => Some(Self::Delay),
            "TRANSACTION" => Some(Self::Transaction),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum DigestType {
    Unspecified = 0,
    /// CRC algorithm achieves goal of detecting random data error with lowest
    /// computation overhead.
    Crc32 = 1,
    /// MD5 algorithm achieves good balance between collision rate and computation
    /// overhead.
    Md5 = 2,
    /// SHA-family has substantially fewer collision with fair amount of
    /// computation.
    Sha1 = 3,
}
impl DigestType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            DigestType::Unspecified => "DIGEST_TYPE_UNSPECIFIED",
            DigestType::Crc32 => "CRC32",
            DigestType::Md5 => "MD5",
            DigestType::Sha1 => "SHA1",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "DIGEST_TYPE_UNSPECIFIED" => Some(Self::Unspecified),
            "CRC32" => Some(Self::Crc32),
            "MD5" => Some(Self::Md5),
            "SHA1" => Some(Self::Sha1),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ClientType {
    Unspecified = 0,
    Producer = 1,
    PushConsumer = 2,
    SimpleConsumer = 3,
    PullConsumer = 4,
}
impl ClientType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            ClientType::Unspecified => "CLIENT_TYPE_UNSPECIFIED",
            ClientType::Producer => "PRODUCER",
            ClientType::PushConsumer => "PUSH_CONSUMER",
            ClientType::SimpleConsumer => "SIMPLE_CONSUMER",
            ClientType::PullConsumer => "PULL_CONSUMER",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "CLIENT_TYPE_UNSPECIFIED" => Some(Self::Unspecified),
            "PRODUCER" => Some(Self::Producer),
            "PUSH_CONSUMER" => Some(Self::PushConsumer),
            "SIMPLE_CONSUMER" => Some(Self::SimpleConsumer),
            "PULL_CONSUMER" => Some(Self::PullConsumer),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum Encoding {
    Unspecified = 0,
    Identity = 1,
    Gzip = 2,
}
impl Encoding {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Encoding::Unspecified => "ENCODING_UNSPECIFIED",
            Encoding::Identity => "IDENTITY",
            Encoding::Gzip => "GZIP",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "ENCODING_UNSPECIFIED" => Some(Self::Unspecified),
            "IDENTITY" => Some(Self::Identity),
            "GZIP" => Some(Self::Gzip),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum Code {
    Unspecified = 0,
    /// Generic code for success.
    Ok = 20000,
    /// Generic code for multiple return results.
    MultipleResults = 30000,
    /// Generic code for bad request, indicating that required fields or headers are missing.
    BadRequest = 40000,
    /// Format of access point is illegal.
    IllegalAccessPoint = 40001,
    /// Format of topic is illegal.
    IllegalTopic = 40002,
    /// Format of consumer group is illegal.
    IllegalConsumerGroup = 40003,
    /// Format of message tag is illegal.
    IllegalMessageTag = 40004,
    /// Format of message key is illegal.
    IllegalMessageKey = 40005,
    /// Format of message group is illegal.
    IllegalMessageGroup = 40006,
    /// Format of message property key is illegal.
    IllegalMessagePropertyKey = 40007,
    /// Transaction id is invalid.
    InvalidTransactionId = 40008,
    /// Format of message id is illegal.
    IllegalMessageId = 40009,
    /// Format of filter expression is illegal.
    IllegalFilterExpression = 40010,
    /// The invisible time of request is invalid.
    IllegalInvisibleTime = 40011,
    /// The delivery timestamp of message is invalid.
    IllegalDeliveryTime = 40012,
    /// Receipt handle of message is invalid.
    InvalidReceiptHandle = 40013,
    /// Message property conflicts with its type.
    MessagePropertyConflictWithType = 40014,
    /// Client type could not be recognized.
    UnrecognizedClientType = 40015,
    /// Message is corrupted.
    MessageCorrupted = 40016,
    /// Request is rejected due to missing of x-mq-client-id header.
    ClientIdRequired = 40017,
    /// Polling time is illegal.
    IllegalPollingTime = 40018,
    /// Generic code indicates that the client request lacks valid authentication
    /// credentials for the requested resource.
    Unauthorized = 40100,
    /// Generic code indicates that the account is suspended due to overdue of payment.
    PaymentRequired = 40200,
    /// Generic code for the case that user does not have the permission to operate.
    Forbidden = 40300,
    /// Generic code for resource not found.
    NotFound = 40400,
    /// Message not found from server.
    MessageNotFound = 40401,
    /// Topic resource does not exist.
    TopicNotFound = 40402,
    /// Consumer group resource does not exist.
    ConsumerGroupNotFound = 40403,
    /// Generic code representing client side timeout when connecting to, reading data from, or write data to server.
    RequestTimeout = 40800,
    /// Generic code represents that the request entity is larger than limits defined by server.
    PayloadTooLarge = 41300,
    /// Message body size exceeds the threshold.
    MessageBodyTooLarge = 41301,
    /// Generic code for use cases where pre-conditions are not met.
    /// For example, if a producer instance is used to publish messages without prior start() invocation,
    /// this error code will be raised.
    PreconditionFailed = 42800,
    /// Generic code indicates that too many requests are made in short period of duration.
    /// Requests are throttled.
    TooManyRequests = 42900,
    /// Generic code for the case that the server is unwilling to process the request because its header fields are too large.
    /// The request may be resubmitted after reducing the size of the request header fields.
    RequestHeaderFieldsTooLarge = 43100,
    /// Message properties total size exceeds the threshold.
    MessagePropertiesTooLarge = 43101,
    /// Generic code indicates that server/client encountered an unexpected
    /// condition that prevented it from fulfilling the request.
    InternalError = 50000,
    /// Code indicates that the server encountered an unexpected condition
    /// that prevented it from fulfilling the request.
    /// This error response is a generic "catch-all" response.
    /// Usually, this indicates the server cannot find a better alternative
    /// error code to response. Sometimes, server administrators log error
    /// responses like the 500 status code with more details about the request
    /// to prevent the error from happening again in the future.
    ///
    /// See <https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/500>
    InternalServerError = 50001,
    /// The HA-mechanism is not working now.
    HaNotAvailable = 50002,
    /// Generic code means that the server or client does not support the
    /// functionality required to fulfill the request.
    NotImplemented = 50100,
    /// Generic code represents that the server, which acts as a gateway or proxy,
    /// does not get an satisfied response in time from its upstream servers.
    /// See <https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/504>
    ProxyTimeout = 50400,
    /// Message persistence timeout.
    MasterPersistenceTimeout = 50401,
    /// Slave persistence timeout.
    SlavePersistenceTimeout = 50402,
    /// Generic code for unsupported operation.
    Unsupported = 50500,
    /// Operation is not allowed in current version.
    VersionUnsupported = 50501,
    /// Not allowed to verify message. Chances are that you are verifying
    /// a FIFO message, as is violating FIFO semantics.
    VerifyFifoMessageUnsupported = 50502,
    /// Generic code for failed message consumption.
    FailedToConsumeMessage = 60000,
}
impl Code {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Code::Unspecified => "CODE_UNSPECIFIED",
            Code::Ok => "OK",
            Code::MultipleResults => "MULTIPLE_RESULTS",
            Code::BadRequest => "BAD_REQUEST",
            Code::IllegalAccessPoint => "ILLEGAL_ACCESS_POINT",
            Code::IllegalTopic => "ILLEGAL_TOPIC",
            Code::IllegalConsumerGroup => "ILLEGAL_CONSUMER_GROUP",
            Code::IllegalMessageTag => "ILLEGAL_MESSAGE_TAG",
            Code::IllegalMessageKey => "ILLEGAL_MESSAGE_KEY",
            Code::IllegalMessageGroup => "ILLEGAL_MESSAGE_GROUP",
            Code::IllegalMessagePropertyKey => "ILLEGAL_MESSAGE_PROPERTY_KEY",
            Code::InvalidTransactionId => "INVALID_TRANSACTION_ID",
            Code::IllegalMessageId => "ILLEGAL_MESSAGE_ID",
            Code::IllegalFilterExpression => "ILLEGAL_FILTER_EXPRESSION",
            Code::IllegalInvisibleTime => "ILLEGAL_INVISIBLE_TIME",
            Code::IllegalDeliveryTime => "ILLEGAL_DELIVERY_TIME",
            Code::InvalidReceiptHandle => "INVALID_RECEIPT_HANDLE",
            Code::MessagePropertyConflictWithType => {
                "MESSAGE_PROPERTY_CONFLICT_WITH_TYPE"
            }
            Code::UnrecognizedClientType => "UNRECOGNIZED_CLIENT_TYPE",
            Code::MessageCorrupted => "MESSAGE_CORRUPTED",
            Code::ClientIdRequired => "CLIENT_ID_REQUIRED",
            Code::IllegalPollingTime => "ILLEGAL_POLLING_TIME",
            Code::Unauthorized => "UNAUTHORIZED",
            Code::PaymentRequired => "PAYMENT_REQUIRED",
            Code::Forbidden => "FORBIDDEN",
            Code::NotFound => "NOT_FOUND",
            Code::MessageNotFound => "MESSAGE_NOT_FOUND",
            Code::TopicNotFound => "TOPIC_NOT_FOUND",
            Code::ConsumerGroupNotFound => "CONSUMER_GROUP_NOT_FOUND",
            Code::RequestTimeout => "REQUEST_TIMEOUT",
            Code::PayloadTooLarge => "PAYLOAD_TOO_LARGE",
            Code::MessageBodyTooLarge => "MESSAGE_BODY_TOO_LARGE",
            Code::PreconditionFailed => "PRECONDITION_FAILED",
            Code::TooManyRequests => "TOO_MANY_REQUESTS",
            Code::RequestHeaderFieldsTooLarge => "REQUEST_HEADER_FIELDS_TOO_LARGE",
            Code::MessagePropertiesTooLarge => "MESSAGE_PROPERTIES_TOO_LARGE",
            Code::InternalError => "INTERNAL_ERROR",
            Code::InternalServerError => "INTERNAL_SERVER_ERROR",
            Code::HaNotAvailable => "HA_NOT_AVAILABLE",
            Code::NotImplemented => "NOT_IMPLEMENTED",
            Code::ProxyTimeout => "PROXY_TIMEOUT",
            Code::MasterPersistenceTimeout => "MASTER_PERSISTENCE_TIMEOUT",
            Code::SlavePersistenceTimeout => "SLAVE_PERSISTENCE_TIMEOUT",
            Code::Unsupported => "UNSUPPORTED",
            Code::VersionUnsupported => "VERSION_UNSUPPORTED",
            Code::VerifyFifoMessageUnsupported => "VERIFY_FIFO_MESSAGE_UNSUPPORTED",
            Code::FailedToConsumeMessage => "FAILED_TO_CONSUME_MESSAGE",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "CODE_UNSPECIFIED" => Some(Self::Unspecified),
            "OK" => Some(Self::Ok),
            "MULTIPLE_RESULTS" => Some(Self::MultipleResults),
            "BAD_REQUEST" => Some(Self::BadRequest),
            "ILLEGAL_ACCESS_POINT" => Some(Self::IllegalAccessPoint),
            "ILLEGAL_TOPIC" => Some(Self::IllegalTopic),
            "ILLEGAL_CONSUMER_GROUP" => Some(Self::IllegalConsumerGroup),
            "ILLEGAL_MESSAGE_TAG" => Some(Self::IllegalMessageTag),
            "ILLEGAL_MESSAGE_KEY" => Some(Self::IllegalMessageKey),
            "ILLEGAL_MESSAGE_GROUP" => Some(Self::IllegalMessageGroup),
            "ILLEGAL_MESSAGE_PROPERTY_KEY" => Some(Self::IllegalMessagePropertyKey),
            "INVALID_TRANSACTION_ID" => Some(Self::InvalidTransactionId),
            "ILLEGAL_MESSAGE_ID" => Some(Self::IllegalMessageId),
            "ILLEGAL_FILTER_EXPRESSION" => Some(Self::IllegalFilterExpression),
            "ILLEGAL_INVISIBLE_TIME" => Some(Self::IllegalInvisibleTime),
            "ILLEGAL_DELIVERY_TIME" => Some(Self::IllegalDeliveryTime),
            "INVALID_RECEIPT_HANDLE" => Some(Self::InvalidReceiptHandle),
            "MESSAGE_PROPERTY_CONFLICT_WITH_TYPE" => {
                Some(Self::MessagePropertyConflictWithType)
            }
            "UNRECOGNIZED_CLIENT_TYPE" => Some(Self::UnrecognizedClientType),
            "MESSAGE_CORRUPTED" => Some(Self::MessageCorrupted),
            "CLIENT_ID_REQUIRED" => Some(Self::ClientIdRequired),
            "ILLEGAL_POLLING_TIME" => Some(Self::IllegalPollingTime),
            "UNAUTHORIZED" => Some(Self::Unauthorized),
            "PAYMENT_REQUIRED" => Some(Self::PaymentRequired),
            "FORBIDDEN" => Some(Self::Forbidden),
            "NOT_FOUND" => Some(Self::NotFound),
            "MESSAGE_NOT_FOUND" => Some(Self::MessageNotFound),
            "TOPIC_NOT_FOUND" => Some(Self::TopicNotFound),
            "CONSUMER_GROUP_NOT_FOUND" => Some(Self::ConsumerGroupNotFound),
            "REQUEST_TIMEOUT" => Some(Self::RequestTimeout),
            "PAYLOAD_TOO_LARGE" => Some(Self::PayloadTooLarge),
            "MESSAGE_BODY_TOO_LARGE" => Some(Self::MessageBodyTooLarge),
            "PRECONDITION_FAILED" => Some(Self::PreconditionFailed),
            "TOO_MANY_REQUESTS" => Some(Self::TooManyRequests),
            "REQUEST_HEADER_FIELDS_TOO_LARGE" => Some(Self::RequestHeaderFieldsTooLarge),
            "MESSAGE_PROPERTIES_TOO_LARGE" => Some(Self::MessagePropertiesTooLarge),
            "INTERNAL_ERROR" => Some(Self::InternalError),
            "INTERNAL_SERVER_ERROR" => Some(Self::InternalServerError),
            "HA_NOT_AVAILABLE" => Some(Self::HaNotAvailable),
            "NOT_IMPLEMENTED" => Some(Self::NotImplemented),
            "PROXY_TIMEOUT" => Some(Self::ProxyTimeout),
            "MASTER_PERSISTENCE_TIMEOUT" => Some(Self::MasterPersistenceTimeout),
            "SLAVE_PERSISTENCE_TIMEOUT" => Some(Self::SlavePersistenceTimeout),
            "UNSUPPORTED" => Some(Self::Unsupported),
            "VERSION_UNSUPPORTED" => Some(Self::VersionUnsupported),
            "VERIFY_FIFO_MESSAGE_UNSUPPORTED" => Some(Self::VerifyFifoMessageUnsupported),
            "FAILED_TO_CONSUME_MESSAGE" => Some(Self::FailedToConsumeMessage),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum Language {
    Unspecified = 0,
    Java = 1,
    Cpp = 2,
    DotNet = 3,
    Golang = 4,
    Rust = 5,
    Python = 6,
    Php = 7,
    NodeJs = 8,
    Ruby = 9,
    ObjectiveC = 10,
    Dart = 11,
    Kotlin = 12,
}
impl Language {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Language::Unspecified => "LANGUAGE_UNSPECIFIED",
            Language::Java => "JAVA",
            Language::Cpp => "CPP",
            Language::DotNet => "DOT_NET",
            Language::Golang => "GOLANG",
            Language::Rust => "RUST",
            Language::Python => "PYTHON",
            Language::Php => "PHP",
            Language::NodeJs => "NODE_JS",
            Language::Ruby => "RUBY",
            Language::ObjectiveC => "OBJECTIVE_C",
            Language::Dart => "DART",
            Language::Kotlin => "KOTLIN",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "LANGUAGE_UNSPECIFIED" => Some(Self::Unspecified),
            "JAVA" => Some(Self::Java),
            "CPP" => Some(Self::Cpp),
            "DOT_NET" => Some(Self::DotNet),
            "GOLANG" => Some(Self::Golang),
            "RUST" => Some(Self::Rust),
            "PYTHON" => Some(Self::Python),
            "PHP" => Some(Self::Php),
            "NODE_JS" => Some(Self::NodeJs),
            "RUBY" => Some(Self::Ruby),
            "OBJECTIVE_C" => Some(Self::ObjectiveC),
            "DART" => Some(Self::Dart),
            "KOTLIN" => Some(Self::Kotlin),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum QueryOffsetPolicy {
    /// Use this option if client wishes to playback all existing messages.
    Beginning = 0,
    /// Use this option if client wishes to skip all existing messages.
    End = 1,
    /// Use this option if time-based seek is targeted.
    Timestamp = 2,
}
impl QueryOffsetPolicy {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            QueryOffsetPolicy::Beginning => "BEGINNING",
            QueryOffsetPolicy::End => "END",
            QueryOffsetPolicy::Timestamp => "TIMESTAMP",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "BEGINNING" => Some(Self::Beginning),
            "END" => Some(Self::End),
            "TIMESTAMP" => Some(Self::Timestamp),
            _ => None,
        }
    }
}
/// Topics are destination of messages to publish to or subscribe from. Similar
/// to domain names, they will be addressable after resolution through the
/// provided access point.
///
/// Access points are usually the addresses of name servers, which fulfill
/// service discovery, load-balancing and other auxiliary services. Name servers
/// receive periodic heartbeats from affiliate brokers and erase those which
/// failed to maintain alive status.
///
/// Name servers answer queries of QueryRouteRequest, responding clients with
/// addressable message-queues, which they may directly publish messages to or
/// subscribe messages from.
///
/// QueryRouteRequest shall include source endpoints, aka, configured
/// access-point, which annotates tenant-id, instance-id or other
/// vendor-specific settings. Purpose-built name servers may respond customized
/// results based on these particular requirements.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueryRouteRequest {
    #[prost(message, optional, tag = "1")]
    pub topic: ::core::option::Option<Resource>,
    #[prost(message, optional, tag = "2")]
    pub endpoints: ::core::option::Option<Endpoints>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueryRouteResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<Status>,
    #[prost(message, repeated, tag = "2")]
    pub message_queues: ::prost::alloc::vec::Vec<MessageQueue>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SendMessageRequest {
    #[prost(message, repeated, tag = "1")]
    pub messages: ::prost::alloc::vec::Vec<Message>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SendResultEntry {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<Status>,
    #[prost(string, tag = "2")]
    pub message_id: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub transaction_id: ::prost::alloc::string::String,
    #[prost(int64, tag = "4")]
    pub offset: i64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SendMessageResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<Status>,
    /// Some implementation may have partial failure issues. Client SDK developers are expected to inspect
    /// each entry for best certainty.
    #[prost(message, repeated, tag = "2")]
    pub entries: ::prost::alloc::vec::Vec<SendResultEntry>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueryAssignmentRequest {
    #[prost(message, optional, tag = "1")]
    pub topic: ::core::option::Option<Resource>,
    #[prost(message, optional, tag = "2")]
    pub group: ::core::option::Option<Resource>,
    #[prost(message, optional, tag = "3")]
    pub endpoints: ::core::option::Option<Endpoints>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueryAssignmentResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<Status>,
    #[prost(message, repeated, tag = "2")]
    pub assignments: ::prost::alloc::vec::Vec<Assignment>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReceiveMessageRequest {
    #[prost(message, optional, tag = "1")]
    pub group: ::core::option::Option<Resource>,
    #[prost(message, optional, tag = "2")]
    pub message_queue: ::core::option::Option<MessageQueue>,
    #[prost(message, optional, tag = "3")]
    pub filter_expression: ::core::option::Option<FilterExpression>,
    #[prost(int32, tag = "4")]
    pub batch_size: i32,
    /// Required if client type is simple consumer.
    #[prost(message, optional, tag = "5")]
    pub invisible_duration: ::core::option::Option<::prost_types::Duration>,
    /// For message auto renew and clean
    #[prost(bool, tag = "6")]
    pub auto_renew: bool,
    #[prost(message, optional, tag = "7")]
    pub long_polling_timeout: ::core::option::Option<::prost_types::Duration>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReceiveMessageResponse {
    #[prost(oneof = "receive_message_response::Content", tags = "1, 2, 3")]
    pub content: ::core::option::Option<receive_message_response::Content>,
}
/// Nested message and enum types in `ReceiveMessageResponse`.
pub mod receive_message_response {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Content {
        #[prost(message, tag = "1")]
        Status(super::Status),
        #[prost(message, tag = "2")]
        Message(super::Message),
        /// The timestamp that brokers start to deliver status line or message.
        #[prost(message, tag = "3")]
        DeliveryTimestamp(::prost_types::Timestamp),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AckMessageEntry {
    #[prost(string, tag = "1")]
    pub message_id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub receipt_handle: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AckMessageRequest {
    #[prost(message, optional, tag = "1")]
    pub group: ::core::option::Option<Resource>,
    #[prost(message, optional, tag = "2")]
    pub topic: ::core::option::Option<Resource>,
    #[prost(message, repeated, tag = "3")]
    pub entries: ::prost::alloc::vec::Vec<AckMessageEntry>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AckMessageResultEntry {
    #[prost(string, tag = "1")]
    pub message_id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub receipt_handle: ::prost::alloc::string::String,
    /// Acknowledge result may be acquired through inspecting
    /// `status.code`; In case acknowledgement failed, `status.message`
    /// is the explanation of the failure.
    #[prost(message, optional, tag = "3")]
    pub status: ::core::option::Option<Status>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AckMessageResponse {
    /// RPC tier status, which is used to represent RPC-level errors including
    /// authentication, authorization, throttling and other general failures.
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<Status>,
    #[prost(message, repeated, tag = "2")]
    pub entries: ::prost::alloc::vec::Vec<AckMessageResultEntry>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ForwardMessageToDeadLetterQueueRequest {
    #[prost(message, optional, tag = "1")]
    pub group: ::core::option::Option<Resource>,
    #[prost(message, optional, tag = "2")]
    pub topic: ::core::option::Option<Resource>,
    #[prost(string, tag = "3")]
    pub receipt_handle: ::prost::alloc::string::String,
    #[prost(string, tag = "4")]
    pub message_id: ::prost::alloc::string::String,
    #[prost(int32, tag = "5")]
    pub delivery_attempt: i32,
    #[prost(int32, tag = "6")]
    pub max_delivery_attempts: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ForwardMessageToDeadLetterQueueResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<Status>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HeartbeatRequest {
    #[prost(message, optional, tag = "1")]
    pub group: ::core::option::Option<Resource>,
    #[prost(enumeration = "ClientType", tag = "2")]
    pub client_type: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HeartbeatResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<Status>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EndTransactionRequest {
    #[prost(message, optional, tag = "1")]
    pub topic: ::core::option::Option<Resource>,
    #[prost(string, tag = "2")]
    pub message_id: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub transaction_id: ::prost::alloc::string::String,
    #[prost(enumeration = "TransactionResolution", tag = "4")]
    pub resolution: i32,
    #[prost(enumeration = "TransactionSource", tag = "5")]
    pub source: i32,
    #[prost(string, tag = "6")]
    pub trace_context: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EndTransactionResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<Status>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PrintThreadStackTraceCommand {
    #[prost(string, tag = "1")]
    pub nonce: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ThreadStackTrace {
    #[prost(string, tag = "1")]
    pub nonce: ::prost::alloc::string::String,
    #[prost(string, optional, tag = "2")]
    pub thread_stack_trace: ::core::option::Option<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct VerifyMessageCommand {
    #[prost(string, tag = "1")]
    pub nonce: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub message: ::core::option::Option<Message>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct VerifyMessageResult {
    #[prost(string, tag = "1")]
    pub nonce: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RecoverOrphanedTransactionCommand {
    #[prost(message, optional, tag = "1")]
    pub message: ::core::option::Option<Message>,
    #[prost(string, tag = "2")]
    pub transaction_id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TelemetryCommand {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<Status>,
    #[prost(oneof = "telemetry_command::Command", tags = "2, 3, 4, 5, 6, 7")]
    pub command: ::core::option::Option<telemetry_command::Command>,
}
/// Nested message and enum types in `TelemetryCommand`.
pub mod telemetry_command {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Command {
        /// Client settings
        #[prost(message, tag = "2")]
        Settings(super::Settings),
        /// These messages are from client.
        ///
        /// Report thread stack trace to server.
        #[prost(message, tag = "3")]
        ThreadStackTrace(super::ThreadStackTrace),
        /// Report message verify result to server.
        #[prost(message, tag = "4")]
        VerifyMessageResult(super::VerifyMessageResult),
        /// There messages are from server.
        ///
        /// Request client to recover the orphaned transaction message.
        #[prost(message, tag = "5")]
        RecoverOrphanedTransactionCommand(super::RecoverOrphanedTransactionCommand),
        /// Request client to print thread stack trace.
        #[prost(message, tag = "6")]
        PrintThreadStackTraceCommand(super::PrintThreadStackTraceCommand),
        /// Request client to verify the consumption of the appointed message.
        #[prost(message, tag = "7")]
        VerifyMessageCommand(super::VerifyMessageCommand),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NotifyClientTerminationRequest {
    /// Consumer group, which is absent for producer.
    #[prost(message, optional, tag = "1")]
    pub group: ::core::option::Option<Resource>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NotifyClientTerminationResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<Status>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ChangeInvisibleDurationRequest {
    #[prost(message, optional, tag = "1")]
    pub group: ::core::option::Option<Resource>,
    #[prost(message, optional, tag = "2")]
    pub topic: ::core::option::Option<Resource>,
    /// Unique receipt handle to identify message to change
    #[prost(string, tag = "3")]
    pub receipt_handle: ::prost::alloc::string::String,
    /// New invisible duration
    #[prost(message, optional, tag = "4")]
    pub invisible_duration: ::core::option::Option<::prost_types::Duration>,
    /// For message tracing
    #[prost(string, tag = "5")]
    pub message_id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ChangeInvisibleDurationResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<Status>,
    /// Server may generate a new receipt handle for the message.
    #[prost(string, tag = "2")]
    pub receipt_handle: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PullMessageRequest {
    #[prost(message, optional, tag = "1")]
    pub group: ::core::option::Option<Resource>,
    #[prost(message, optional, tag = "2")]
    pub message_queue: ::core::option::Option<MessageQueue>,
    #[prost(int64, tag = "3")]
    pub offset: i64,
    #[prost(int32, tag = "4")]
    pub batch_size: i32,
    #[prost(message, optional, tag = "5")]
    pub filter_expression: ::core::option::Option<FilterExpression>,
    #[prost(message, optional, tag = "6")]
    pub long_polling_timeout: ::core::option::Option<::prost_types::Duration>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PullMessageResponse {
    #[prost(oneof = "pull_message_response::Content", tags = "1, 2, 3")]
    pub content: ::core::option::Option<pull_message_response::Content>,
}
/// Nested message and enum types in `PullMessageResponse`.
pub mod pull_message_response {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Content {
        #[prost(message, tag = "1")]
        Status(super::Status),
        #[prost(message, tag = "2")]
        Message(super::Message),
        #[prost(int64, tag = "3")]
        NextOffset(i64),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateOffsetRequest {
    #[prost(message, optional, tag = "1")]
    pub group: ::core::option::Option<Resource>,
    #[prost(message, optional, tag = "2")]
    pub message_queue: ::core::option::Option<MessageQueue>,
    #[prost(int64, tag = "3")]
    pub offset: i64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateOffsetResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<Status>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetOffsetRequest {
    #[prost(message, optional, tag = "1")]
    pub group: ::core::option::Option<Resource>,
    #[prost(message, optional, tag = "2")]
    pub message_queue: ::core::option::Option<MessageQueue>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetOffsetResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<Status>,
    #[prost(int64, tag = "2")]
    pub offset: i64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueryOffsetRequest {
    #[prost(message, optional, tag = "1")]
    pub message_queue: ::core::option::Option<MessageQueue>,
    #[prost(enumeration = "QueryOffsetPolicy", tag = "2")]
    pub query_offset_policy: i32,
    #[prost(message, optional, tag = "3")]
    pub timestamp: ::core::option::Option<::prost_types::Timestamp>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueryOffsetResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<Status>,
    #[prost(int64, tag = "2")]
    pub offset: i64,
}
/// Generated client implementations.
pub mod messaging_service_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    /// For all the RPCs in MessagingService, the following error handling policies
    /// apply:
    ///
    /// If the request doesn't bear a valid authentication credential, return a
    /// response with common.status.code == `UNAUTHENTICATED`. If the authenticated
    /// user is not granted with sufficient permission to execute the requested
    /// operation, return a response with common.status.code == `PERMISSION_DENIED`.
    /// If the per-user-resource-based quota is exhausted, return a response with
    /// common.status.code == `RESOURCE_EXHAUSTED`. If any unexpected server-side
    /// errors raise, return a response with common.status.code == `INTERNAL`.
    #[derive(Debug, Clone)]
    pub struct MessagingServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl MessagingServiceClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> MessagingServiceClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> MessagingServiceClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            MessagingServiceClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }
        /// Queries the route entries of the requested topic in the perspective of the
        /// given endpoints. On success, servers should return a collection of
        /// addressable message-queues. Note servers may return customized route
        /// entries based on endpoints provided.
        ///
        /// If the requested topic doesn't exist, returns `NOT_FOUND`.
        /// If the specific endpoints is empty, returns `INVALID_ARGUMENT`.
        pub async fn query_route(
            &mut self,
            request: impl tonic::IntoRequest<super::QueryRouteRequest>,
        ) -> std::result::Result<
            tonic::Response<super::QueryRouteResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/apache.rocketmq.v2.MessagingService/QueryRoute",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("apache.rocketmq.v2.MessagingService", "QueryRoute"),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Producer or consumer sends HeartbeatRequest to servers periodically to
        /// keep-alive. Additionally, it also reports client-side configuration,
        /// including topic subscription, load-balancing group name, etc.
        ///
        /// Returns `OK` if success.
        ///
        /// If a client specifies a language that is not yet supported by servers,
        /// returns `INVALID_ARGUMENT`
        pub async fn heartbeat(
            &mut self,
            request: impl tonic::IntoRequest<super::HeartbeatRequest>,
        ) -> std::result::Result<
            tonic::Response<super::HeartbeatResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/apache.rocketmq.v2.MessagingService/Heartbeat",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("apache.rocketmq.v2.MessagingService", "Heartbeat"),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Delivers messages to brokers.
        /// Clients may further:
        /// 1. Refine a message destination to message-queues which fulfills parts of
        /// FIFO semantic;
        /// 2. Flag a message as transactional, which keeps it invisible to consumers
        /// until it commits;
        /// 3. Time a message, making it invisible to consumers till specified
        /// time-point;
        /// 4. And more...
        ///
        /// Returns message-id or transaction-id with status `OK` on success.
        ///
        /// If the destination topic doesn't exist, returns `NOT_FOUND`.
        pub async fn send_message(
            &mut self,
            request: impl tonic::IntoRequest<super::SendMessageRequest>,
        ) -> std::result::Result<
            tonic::Response<super::SendMessageResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/apache.rocketmq.v2.MessagingService/SendMessage",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("apache.rocketmq.v2.MessagingService", "SendMessage"),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Queries the assigned route info of a topic for current consumer,
        /// the returned assignment result is decided by server-side load balancer.
        ///
        /// If the corresponding topic doesn't exist, returns `NOT_FOUND`.
        /// If the specific endpoints is empty, returns `INVALID_ARGUMENT`.
        pub async fn query_assignment(
            &mut self,
            request: impl tonic::IntoRequest<super::QueryAssignmentRequest>,
        ) -> std::result::Result<
            tonic::Response<super::QueryAssignmentResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/apache.rocketmq.v2.MessagingService/QueryAssignment",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "apache.rocketmq.v2.MessagingService",
                        "QueryAssignment",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Receives messages from the server in batch manner, returns a set of
        /// messages if success. The received messages should be acked or redelivered
        /// after processed.
        ///
        /// If the pending concurrent receive requests exceed the quota of the given
        /// consumer group, returns `UNAVAILABLE`. If the upstream store server hangs,
        /// return `DEADLINE_EXCEEDED` in a timely manner. If the corresponding topic
        /// or consumer group doesn't exist, returns `NOT_FOUND`. If there is no new
        /// message in the specific topic, returns `OK` with an empty message set.
        /// Please note that client may suffer from false empty responses.
        ///
        /// If failed to receive message from remote, server must return only one
        /// `ReceiveMessageResponse` as the reply to the request, whose `Status` indicates
        /// the specific reason of failure, otherwise, the reply is considered successful.
        pub async fn receive_message(
            &mut self,
            request: impl tonic::IntoRequest<super::ReceiveMessageRequest>,
        ) -> std::result::Result<
            tonic::Response<tonic::codec::Streaming<super::ReceiveMessageResponse>>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/apache.rocketmq.v2.MessagingService/ReceiveMessage",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "apache.rocketmq.v2.MessagingService",
                        "ReceiveMessage",
                    ),
                );
            self.inner.server_streaming(req, path, codec).await
        }
        /// Acknowledges the message associated with the `receipt_handle` or `offset`
        /// in the `AckMessageRequest`, it means the message has been successfully
        /// processed. Returns `OK` if the message server remove the relevant message
        /// successfully.
        ///
        /// If the given receipt_handle is illegal or out of date, returns
        /// `INVALID_ARGUMENT`.
        pub async fn ack_message(
            &mut self,
            request: impl tonic::IntoRequest<super::AckMessageRequest>,
        ) -> std::result::Result<
            tonic::Response<super::AckMessageResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/apache.rocketmq.v2.MessagingService/AckMessage",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("apache.rocketmq.v2.MessagingService", "AckMessage"),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Forwards one message to dead letter queue if the max delivery attempts is
        /// exceeded by this message at client-side, return `OK` if success.
        pub async fn forward_message_to_dead_letter_queue(
            &mut self,
            request: impl tonic::IntoRequest<
                super::ForwardMessageToDeadLetterQueueRequest,
            >,
        ) -> std::result::Result<
            tonic::Response<super::ForwardMessageToDeadLetterQueueResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/apache.rocketmq.v2.MessagingService/ForwardMessageToDeadLetterQueue",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "apache.rocketmq.v2.MessagingService",
                        "ForwardMessageToDeadLetterQueue",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn pull_message(
            &mut self,
            request: impl tonic::IntoRequest<super::PullMessageRequest>,
        ) -> std::result::Result<
            tonic::Response<tonic::codec::Streaming<super::PullMessageResponse>>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/apache.rocketmq.v2.MessagingService/PullMessage",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("apache.rocketmq.v2.MessagingService", "PullMessage"),
                );
            self.inner.server_streaming(req, path, codec).await
        }
        pub async fn update_offset(
            &mut self,
            request: impl tonic::IntoRequest<super::UpdateOffsetRequest>,
        ) -> std::result::Result<
            tonic::Response<super::UpdateOffsetResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/apache.rocketmq.v2.MessagingService/UpdateOffset",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "apache.rocketmq.v2.MessagingService",
                        "UpdateOffset",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn get_offset(
            &mut self,
            request: impl tonic::IntoRequest<super::GetOffsetRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetOffsetResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/apache.rocketmq.v2.MessagingService/GetOffset",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("apache.rocketmq.v2.MessagingService", "GetOffset"),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn query_offset(
            &mut self,
            request: impl tonic::IntoRequest<super::QueryOffsetRequest>,
        ) -> std::result::Result<
            tonic::Response<super::QueryOffsetResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/apache.rocketmq.v2.MessagingService/QueryOffset",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("apache.rocketmq.v2.MessagingService", "QueryOffset"),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Commits or rollback one transactional message.
        pub async fn end_transaction(
            &mut self,
            request: impl tonic::IntoRequest<super::EndTransactionRequest>,
        ) -> std::result::Result<
            tonic::Response<super::EndTransactionResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/apache.rocketmq.v2.MessagingService/EndTransaction",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "apache.rocketmq.v2.MessagingService",
                        "EndTransaction",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Once a client starts, it would immediately establishes bi-lateral stream
        /// RPCs with brokers, reporting its settings as the initiative command.
        ///
        /// When servers have need of inspecting client status, they would issue
        /// telemetry commands to clients. After executing received instructions,
        /// clients shall report command execution results through client-side streams.
        pub async fn telemetry(
            &mut self,
            request: impl tonic::IntoStreamingRequest<Message = super::TelemetryCommand>,
        ) -> std::result::Result<
            tonic::Response<tonic::codec::Streaming<super::TelemetryCommand>>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/apache.rocketmq.v2.MessagingService/Telemetry",
            );
            let mut req = request.into_streaming_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("apache.rocketmq.v2.MessagingService", "Telemetry"),
                );
            self.inner.streaming(req, path, codec).await
        }
        /// Notify the server that the client is terminated.
        pub async fn notify_client_termination(
            &mut self,
            request: impl tonic::IntoRequest<super::NotifyClientTerminationRequest>,
        ) -> std::result::Result<
            tonic::Response<super::NotifyClientTerminationResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/apache.rocketmq.v2.MessagingService/NotifyClientTermination",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "apache.rocketmq.v2.MessagingService",
                        "NotifyClientTermination",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Once a message is retrieved from consume queue on behalf of the group, it
        /// will be kept invisible to other clients of the same group for a period of
        /// time. The message is supposed to be processed within the invisible
        /// duration. If the client, which is in charge of the invisible message, is
        /// not capable of processing the message timely, it may use
        /// ChangeInvisibleDuration to lengthen invisible duration.
        pub async fn change_invisible_duration(
            &mut self,
            request: impl tonic::IntoRequest<super::ChangeInvisibleDurationRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ChangeInvisibleDurationResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/apache.rocketmq.v2.MessagingService/ChangeInvisibleDuration",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "apache.rocketmq.v2.MessagingService",
                        "ChangeInvisibleDuration",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ChangeLogLevelRequest {
    #[prost(enumeration = "change_log_level_request::Level", tag = "1")]
    pub level: i32,
}
/// Nested message and enum types in `ChangeLogLevelRequest`.
pub mod change_log_level_request {
    #[derive(
        Clone,
        Copy,
        Debug,
        PartialEq,
        Eq,
        Hash,
        PartialOrd,
        Ord,
        ::prost::Enumeration
    )]
    #[repr(i32)]
    pub enum Level {
        Trace = 0,
        Debug = 1,
        Info = 2,
        Warn = 3,
        Error = 4,
    }
    impl Level {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Level::Trace => "TRACE",
                Level::Debug => "DEBUG",
                Level::Info => "INFO",
                Level::Warn => "WARN",
                Level::Error => "ERROR",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "TRACE" => Some(Self::Trace),
                "DEBUG" => Some(Self::Debug),
                "INFO" => Some(Self::Info),
                "WARN" => Some(Self::Warn),
                "ERROR" => Some(Self::Error),
                _ => None,
            }
        }
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ChangeLogLevelResponse {
    #[prost(string, tag = "1")]
    pub remark: ::prost::alloc::string::String,
}
/// Generated client implementations.
pub mod admin_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct AdminClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl AdminClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> AdminClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> AdminClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            AdminClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }
        pub async fn change_log_level(
            &mut self,
            request: impl tonic::IntoRequest<super::ChangeLogLevelRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ChangeLogLevelResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/apache.rocketmq.v2.Admin/ChangeLogLevel",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("apache.rocketmq.v2.Admin", "ChangeLogLevel"));
            self.inner.unary(req, path, codec).await
        }
    }
}
