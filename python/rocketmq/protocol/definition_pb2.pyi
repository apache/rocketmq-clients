# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf import duration_pb2 as _duration_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

ADDRESS_SCHEME_UNSPECIFIED: AddressScheme
BAD_REQUEST: Code
CLIENT_ID_REQUIRED: Code
CLIENT_TYPE_UNSPECIFIED: ClientType
CODE_UNSPECIFIED: Code
COMMIT: TransactionResolution
CONSUMER_GROUP_NOT_FOUND: Code
CPP: Language
CRC32: DigestType
DART: Language
DELAY: MessageType
DESCRIPTOR: _descriptor.FileDescriptor
DIGEST_TYPE_UNSPECIFIED: DigestType
DOMAIN_NAME: AddressScheme
DOT_NET: Language
ENCODING_UNSPECIFIED: Encoding
FAILED_TO_CONSUME_MESSAGE: Code
FIFO: MessageType
FILTER_TYPE_UNSPECIFIED: FilterType
FORBIDDEN: Code
GOLANG: Language
GZIP: Encoding
HA_NOT_AVAILABLE: Code
IDENTITY: Encoding
ILLEGAL_ACCESS_POINT: Code
ILLEGAL_CONSUMER_GROUP: Code
ILLEGAL_DELIVERY_TIME: Code
ILLEGAL_FILTER_EXPRESSION: Code
ILLEGAL_INVISIBLE_TIME: Code
ILLEGAL_MESSAGE_GROUP: Code
ILLEGAL_MESSAGE_ID: Code
ILLEGAL_MESSAGE_KEY: Code
ILLEGAL_MESSAGE_PROPERTY_KEY: Code
ILLEGAL_MESSAGE_TAG: Code
ILLEGAL_POLLING_TIME: Code
ILLEGAL_TOPIC: Code
INTERNAL_ERROR: Code
INTERNAL_SERVER_ERROR: Code
INVALID_RECEIPT_HANDLE: Code
INVALID_TRANSACTION_ID: Code
IPv4: AddressScheme
IPv6: AddressScheme
JAVA: Language
KOTLIN: Language
LANGUAGE_UNSPECIFIED: Language
MASTER_PERSISTENCE_TIMEOUT: Code
MD5: DigestType
MESSAGE_BODY_TOO_LARGE: Code
MESSAGE_CORRUPTED: Code
MESSAGE_NOT_FOUND: Code
MESSAGE_PROPERTIES_TOO_LARGE: Code
MESSAGE_PROPERTY_CONFLICT_WITH_TYPE: Code
MESSAGE_TYPE_UNSPECIFIED: MessageType
MULTIPLE_RESULTS: Code
NODE_JS: Language
NONE: Permission
NORMAL: MessageType
NOT_FOUND: Code
NOT_IMPLEMENTED: Code
OBJECTIVE_C: Language
OK: Code
PAYLOAD_TOO_LARGE: Code
PAYMENT_REQUIRED: Code
PERMISSION_UNSPECIFIED: Permission
PHP: Language
PRECONDITION_FAILED: Code
PRODUCER: ClientType
PROXY_TIMEOUT: Code
PUSH_CONSUMER: ClientType
PYTHON: Language
READ: Permission
READ_WRITE: Permission
REQUEST_HEADER_FIELDS_TOO_LARGE: Code
REQUEST_TIMEOUT: Code
ROLLBACK: TransactionResolution
RUBY: Language
RUST: Language
SHA1: DigestType
SIMPLE_CONSUMER: ClientType
SLAVE_PERSISTENCE_TIMEOUT: Code
SOURCE_CLIENT: TransactionSource
SOURCE_SERVER_CHECK: TransactionSource
SOURCE_UNSPECIFIED: TransactionSource
SQL: FilterType
TAG: FilterType
TOO_MANY_REQUESTS: Code
TOPIC_NOT_FOUND: Code
TRANSACTION: MessageType
TRANSACTION_RESOLUTION_UNSPECIFIED: TransactionResolution
UNAUTHORIZED: Code
UNRECOGNIZED_CLIENT_TYPE: Code
UNSUPPORTED: Code
VERIFY_FIFO_MESSAGE_UNSUPPORTED: Code
VERSION_UNSUPPORTED: Code
WRITE: Permission

class Address(_message.Message):
    __slots__ = ["host", "port"]
    HOST_FIELD_NUMBER: _ClassVar[int]
    PORT_FIELD_NUMBER: _ClassVar[int]
    host: str
    port: int
    def __init__(self, host: _Optional[str] = ..., port: _Optional[int] = ...) -> None: ...

class Assignment(_message.Message):
    __slots__ = ["message_queue"]
    MESSAGE_QUEUE_FIELD_NUMBER: _ClassVar[int]
    message_queue: MessageQueue
    def __init__(self, message_queue: _Optional[_Union[MessageQueue, _Mapping]] = ...) -> None: ...

class Broker(_message.Message):
    __slots__ = ["endpoints", "id", "name"]
    ENDPOINTS_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    endpoints: Endpoints
    id: int
    name: str
    def __init__(self, name: _Optional[str] = ..., id: _Optional[int] = ..., endpoints: _Optional[_Union[Endpoints, _Mapping]] = ...) -> None: ...

class CustomizedBackoff(_message.Message):
    __slots__ = ["next"]
    NEXT_FIELD_NUMBER: _ClassVar[int]
    next: _containers.RepeatedCompositeFieldContainer[_duration_pb2.Duration]
    def __init__(self, next: _Optional[_Iterable[_Union[_duration_pb2.Duration, _Mapping]]] = ...) -> None: ...

class DeadLetterQueue(_message.Message):
    __slots__ = ["message_id", "topic"]
    MESSAGE_ID_FIELD_NUMBER: _ClassVar[int]
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    message_id: str
    topic: str
    def __init__(self, topic: _Optional[str] = ..., message_id: _Optional[str] = ...) -> None: ...

class Digest(_message.Message):
    __slots__ = ["checksum", "type"]
    CHECKSUM_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    checksum: str
    type: DigestType
    def __init__(self, type: _Optional[_Union[DigestType, str]] = ..., checksum: _Optional[str] = ...) -> None: ...

class Endpoints(_message.Message):
    __slots__ = ["addresses", "scheme"]
    ADDRESSES_FIELD_NUMBER: _ClassVar[int]
    SCHEME_FIELD_NUMBER: _ClassVar[int]
    addresses: _containers.RepeatedCompositeFieldContainer[Address]
    scheme: AddressScheme
    def __init__(self, scheme: _Optional[_Union[AddressScheme, str]] = ..., addresses: _Optional[_Iterable[_Union[Address, _Mapping]]] = ...) -> None: ...

class ExponentialBackoff(_message.Message):
    __slots__ = ["initial", "max", "multiplier"]
    INITIAL_FIELD_NUMBER: _ClassVar[int]
    MAX_FIELD_NUMBER: _ClassVar[int]
    MULTIPLIER_FIELD_NUMBER: _ClassVar[int]
    initial: _duration_pb2.Duration
    max: _duration_pb2.Duration
    multiplier: float
    def __init__(self, initial: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., max: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., multiplier: _Optional[float] = ...) -> None: ...

class FilterExpression(_message.Message):
    __slots__ = ["expression", "type"]
    EXPRESSION_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    expression: str
    type: FilterType
    def __init__(self, type: _Optional[_Union[FilterType, str]] = ..., expression: _Optional[str] = ...) -> None: ...

class Message(_message.Message):
    __slots__ = ["body", "system_properties", "topic", "user_properties"]
    class UserPropertiesEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    BODY_FIELD_NUMBER: _ClassVar[int]
    SYSTEM_PROPERTIES_FIELD_NUMBER: _ClassVar[int]
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    USER_PROPERTIES_FIELD_NUMBER: _ClassVar[int]
    body: bytes
    system_properties: SystemProperties
    topic: Resource
    user_properties: _containers.ScalarMap[str, str]
    def __init__(self, topic: _Optional[_Union[Resource, _Mapping]] = ..., user_properties: _Optional[_Mapping[str, str]] = ..., system_properties: _Optional[_Union[SystemProperties, _Mapping]] = ..., body: _Optional[bytes] = ...) -> None: ...

class MessageQueue(_message.Message):
    __slots__ = ["accept_message_types", "broker", "id", "permission", "topic"]
    ACCEPT_MESSAGE_TYPES_FIELD_NUMBER: _ClassVar[int]
    BROKER_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    PERMISSION_FIELD_NUMBER: _ClassVar[int]
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    accept_message_types: _containers.RepeatedScalarFieldContainer[MessageType]
    broker: Broker
    id: int
    permission: Permission
    topic: Resource
    def __init__(self, topic: _Optional[_Union[Resource, _Mapping]] = ..., id: _Optional[int] = ..., permission: _Optional[_Union[Permission, str]] = ..., broker: _Optional[_Union[Broker, _Mapping]] = ..., accept_message_types: _Optional[_Iterable[_Union[MessageType, str]]] = ...) -> None: ...

class Metric(_message.Message):
    __slots__ = ["endpoints", "on"]
    ENDPOINTS_FIELD_NUMBER: _ClassVar[int]
    ON_FIELD_NUMBER: _ClassVar[int]
    endpoints: Endpoints
    on: bool
    def __init__(self, on: bool = ..., endpoints: _Optional[_Union[Endpoints, _Mapping]] = ...) -> None: ...

class Publishing(_message.Message):
    __slots__ = ["max_body_size", "topics", "validate_message_type"]
    MAX_BODY_SIZE_FIELD_NUMBER: _ClassVar[int]
    TOPICS_FIELD_NUMBER: _ClassVar[int]
    VALIDATE_MESSAGE_TYPE_FIELD_NUMBER: _ClassVar[int]
    max_body_size: int
    topics: _containers.RepeatedCompositeFieldContainer[Resource]
    validate_message_type: bool
    def __init__(self, topics: _Optional[_Iterable[_Union[Resource, _Mapping]]] = ..., max_body_size: _Optional[int] = ..., validate_message_type: bool = ...) -> None: ...

class Resource(_message.Message):
    __slots__ = ["name", "resource_namespace"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    name: str
    resource_namespace: str
    def __init__(self, resource_namespace: _Optional[str] = ..., name: _Optional[str] = ...) -> None: ...

class RetryPolicy(_message.Message):
    __slots__ = ["customized_backoff", "exponential_backoff", "max_attempts"]
    CUSTOMIZED_BACKOFF_FIELD_NUMBER: _ClassVar[int]
    EXPONENTIAL_BACKOFF_FIELD_NUMBER: _ClassVar[int]
    MAX_ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    customized_backoff: CustomizedBackoff
    exponential_backoff: ExponentialBackoff
    max_attempts: int
    def __init__(self, max_attempts: _Optional[int] = ..., exponential_backoff: _Optional[_Union[ExponentialBackoff, _Mapping]] = ..., customized_backoff: _Optional[_Union[CustomizedBackoff, _Mapping]] = ...) -> None: ...

class Settings(_message.Message):
    __slots__ = ["access_point", "backoff_policy", "client_type", "metric", "publishing", "request_timeout", "subscription", "user_agent"]
    ACCESS_POINT_FIELD_NUMBER: _ClassVar[int]
    BACKOFF_POLICY_FIELD_NUMBER: _ClassVar[int]
    CLIENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    METRIC_FIELD_NUMBER: _ClassVar[int]
    PUBLISHING_FIELD_NUMBER: _ClassVar[int]
    REQUEST_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    SUBSCRIPTION_FIELD_NUMBER: _ClassVar[int]
    USER_AGENT_FIELD_NUMBER: _ClassVar[int]
    access_point: Endpoints
    backoff_policy: RetryPolicy
    client_type: ClientType
    metric: Metric
    publishing: Publishing
    request_timeout: _duration_pb2.Duration
    subscription: Subscription
    user_agent: UA
    def __init__(self, client_type: _Optional[_Union[ClientType, str]] = ..., access_point: _Optional[_Union[Endpoints, _Mapping]] = ..., backoff_policy: _Optional[_Union[RetryPolicy, _Mapping]] = ..., request_timeout: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., publishing: _Optional[_Union[Publishing, _Mapping]] = ..., subscription: _Optional[_Union[Subscription, _Mapping]] = ..., user_agent: _Optional[_Union[UA, _Mapping]] = ..., metric: _Optional[_Union[Metric, _Mapping]] = ...) -> None: ...

class Status(_message.Message):
    __slots__ = ["code", "message"]
    CODE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    code: Code
    message: str
    def __init__(self, code: _Optional[_Union[Code, str]] = ..., message: _Optional[str] = ...) -> None: ...

class Subscription(_message.Message):
    __slots__ = ["fifo", "group", "long_polling_timeout", "receive_batch_size", "subscriptions"]
    FIFO_FIELD_NUMBER: _ClassVar[int]
    GROUP_FIELD_NUMBER: _ClassVar[int]
    LONG_POLLING_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    RECEIVE_BATCH_SIZE_FIELD_NUMBER: _ClassVar[int]
    SUBSCRIPTIONS_FIELD_NUMBER: _ClassVar[int]
    fifo: bool
    group: Resource
    long_polling_timeout: _duration_pb2.Duration
    receive_batch_size: int
    subscriptions: _containers.RepeatedCompositeFieldContainer[SubscriptionEntry]
    def __init__(self, group: _Optional[_Union[Resource, _Mapping]] = ..., subscriptions: _Optional[_Iterable[_Union[SubscriptionEntry, _Mapping]]] = ..., fifo: bool = ..., receive_batch_size: _Optional[int] = ..., long_polling_timeout: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ...) -> None: ...

class SubscriptionEntry(_message.Message):
    __slots__ = ["expression", "topic"]
    EXPRESSION_FIELD_NUMBER: _ClassVar[int]
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    expression: FilterExpression
    topic: Resource
    def __init__(self, topic: _Optional[_Union[Resource, _Mapping]] = ..., expression: _Optional[_Union[FilterExpression, _Mapping]] = ...) -> None: ...

class SystemProperties(_message.Message):
    __slots__ = ["body_digest", "body_encoding", "born_host", "born_timestamp", "dead_letter_queue", "delivery_attempt", "delivery_timestamp", "invisible_duration", "keys", "message_group", "message_id", "message_type", "orphaned_transaction_recovery_duration", "queue_id", "queue_offset", "receipt_handle", "store_host", "store_timestamp", "tag", "trace_context"]
    BODY_DIGEST_FIELD_NUMBER: _ClassVar[int]
    BODY_ENCODING_FIELD_NUMBER: _ClassVar[int]
    BORN_HOST_FIELD_NUMBER: _ClassVar[int]
    BORN_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    DEAD_LETTER_QUEUE_FIELD_NUMBER: _ClassVar[int]
    DELIVERY_ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    DELIVERY_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    INVISIBLE_DURATION_FIELD_NUMBER: _ClassVar[int]
    KEYS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_GROUP_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_ID_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_TYPE_FIELD_NUMBER: _ClassVar[int]
    ORPHANED_TRANSACTION_RECOVERY_DURATION_FIELD_NUMBER: _ClassVar[int]
    QUEUE_ID_FIELD_NUMBER: _ClassVar[int]
    QUEUE_OFFSET_FIELD_NUMBER: _ClassVar[int]
    RECEIPT_HANDLE_FIELD_NUMBER: _ClassVar[int]
    STORE_HOST_FIELD_NUMBER: _ClassVar[int]
    STORE_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    TAG_FIELD_NUMBER: _ClassVar[int]
    TRACE_CONTEXT_FIELD_NUMBER: _ClassVar[int]
    body_digest: Digest
    body_encoding: Encoding
    born_host: str
    born_timestamp: _timestamp_pb2.Timestamp
    dead_letter_queue: DeadLetterQueue
    delivery_attempt: int
    delivery_timestamp: _timestamp_pb2.Timestamp
    invisible_duration: _duration_pb2.Duration
    keys: _containers.RepeatedScalarFieldContainer[str]
    message_group: str
    message_id: str
    message_type: MessageType
    orphaned_transaction_recovery_duration: _duration_pb2.Duration
    queue_id: int
    queue_offset: int
    receipt_handle: str
    store_host: str
    store_timestamp: _timestamp_pb2.Timestamp
    tag: str
    trace_context: str
    def __init__(self, tag: _Optional[str] = ..., keys: _Optional[_Iterable[str]] = ..., message_id: _Optional[str] = ..., body_digest: _Optional[_Union[Digest, _Mapping]] = ..., body_encoding: _Optional[_Union[Encoding, str]] = ..., message_type: _Optional[_Union[MessageType, str]] = ..., born_timestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., born_host: _Optional[str] = ..., store_timestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., store_host: _Optional[str] = ..., delivery_timestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., receipt_handle: _Optional[str] = ..., queue_id: _Optional[int] = ..., queue_offset: _Optional[int] = ..., invisible_duration: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., delivery_attempt: _Optional[int] = ..., message_group: _Optional[str] = ..., trace_context: _Optional[str] = ..., orphaned_transaction_recovery_duration: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., dead_letter_queue: _Optional[_Union[DeadLetterQueue, _Mapping]] = ...) -> None: ...

class UA(_message.Message):
    __slots__ = ["hostname", "language", "platform", "version"]
    HOSTNAME_FIELD_NUMBER: _ClassVar[int]
    LANGUAGE_FIELD_NUMBER: _ClassVar[int]
    PLATFORM_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    hostname: str
    language: Language
    platform: str
    version: str
    def __init__(self, language: _Optional[_Union[Language, str]] = ..., version: _Optional[str] = ..., platform: _Optional[str] = ..., hostname: _Optional[str] = ...) -> None: ...

class TransactionResolution(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []

class TransactionSource(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []

class Permission(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []

class FilterType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []

class AddressScheme(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []

class MessageType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []

class DigestType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []

class ClientType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []

class Encoding(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []

class Code(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []

class Language(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
