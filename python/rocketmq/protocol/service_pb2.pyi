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

from google.protobuf import duration_pb2 as _duration_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from protocol import definition_pb2 as _definition_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class AckMessageEntry(_message.Message):
    __slots__ = ["message_id", "receipt_handle"]
    MESSAGE_ID_FIELD_NUMBER: _ClassVar[int]
    RECEIPT_HANDLE_FIELD_NUMBER: _ClassVar[int]
    message_id: str
    receipt_handle: str
    def __init__(self, message_id: _Optional[str] = ..., receipt_handle: _Optional[str] = ...) -> None: ...

class AckMessageRequest(_message.Message):
    __slots__ = ["entries", "group", "topic"]
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    GROUP_FIELD_NUMBER: _ClassVar[int]
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    entries: _containers.RepeatedCompositeFieldContainer[AckMessageEntry]
    group: _definition_pb2.Resource
    topic: _definition_pb2.Resource
    def __init__(self, group: _Optional[_Union[_definition_pb2.Resource, _Mapping]] = ..., topic: _Optional[_Union[_definition_pb2.Resource, _Mapping]] = ..., entries: _Optional[_Iterable[_Union[AckMessageEntry, _Mapping]]] = ...) -> None: ...

class AckMessageResponse(_message.Message):
    __slots__ = ["entries", "status"]
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    entries: _containers.RepeatedCompositeFieldContainer[AckMessageResultEntry]
    status: _definition_pb2.Status
    def __init__(self, status: _Optional[_Union[_definition_pb2.Status, _Mapping]] = ..., entries: _Optional[_Iterable[_Union[AckMessageResultEntry, _Mapping]]] = ...) -> None: ...

class AckMessageResultEntry(_message.Message):
    __slots__ = ["message_id", "receipt_handle", "status"]
    MESSAGE_ID_FIELD_NUMBER: _ClassVar[int]
    RECEIPT_HANDLE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    message_id: str
    receipt_handle: str
    status: _definition_pb2.Status
    def __init__(self, message_id: _Optional[str] = ..., receipt_handle: _Optional[str] = ..., status: _Optional[_Union[_definition_pb2.Status, _Mapping]] = ...) -> None: ...

class ChangeInvisibleDurationRequest(_message.Message):
    __slots__ = ["group", "invisible_duration", "message_id", "receipt_handle", "topic"]
    GROUP_FIELD_NUMBER: _ClassVar[int]
    INVISIBLE_DURATION_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_ID_FIELD_NUMBER: _ClassVar[int]
    RECEIPT_HANDLE_FIELD_NUMBER: _ClassVar[int]
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    group: _definition_pb2.Resource
    invisible_duration: _duration_pb2.Duration
    message_id: str
    receipt_handle: str
    topic: _definition_pb2.Resource
    def __init__(self, group: _Optional[_Union[_definition_pb2.Resource, _Mapping]] = ..., topic: _Optional[_Union[_definition_pb2.Resource, _Mapping]] = ..., receipt_handle: _Optional[str] = ..., invisible_duration: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., message_id: _Optional[str] = ...) -> None: ...

class ChangeInvisibleDurationResponse(_message.Message):
    __slots__ = ["receipt_handle", "status"]
    RECEIPT_HANDLE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    receipt_handle: str
    status: _definition_pb2.Status
    def __init__(self, status: _Optional[_Union[_definition_pb2.Status, _Mapping]] = ..., receipt_handle: _Optional[str] = ...) -> None: ...

class EndTransactionRequest(_message.Message):
    __slots__ = ["message_id", "resolution", "source", "topic", "trace_context", "transaction_id"]
    MESSAGE_ID_FIELD_NUMBER: _ClassVar[int]
    RESOLUTION_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    TRACE_CONTEXT_FIELD_NUMBER: _ClassVar[int]
    TRANSACTION_ID_FIELD_NUMBER: _ClassVar[int]
    message_id: str
    resolution: _definition_pb2.TransactionResolution
    source: _definition_pb2.TransactionSource
    topic: _definition_pb2.Resource
    trace_context: str
    transaction_id: str
    def __init__(self, topic: _Optional[_Union[_definition_pb2.Resource, _Mapping]] = ..., message_id: _Optional[str] = ..., transaction_id: _Optional[str] = ..., resolution: _Optional[_Union[_definition_pb2.TransactionResolution, str]] = ..., source: _Optional[_Union[_definition_pb2.TransactionSource, str]] = ..., trace_context: _Optional[str] = ...) -> None: ...

class EndTransactionResponse(_message.Message):
    __slots__ = ["status"]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: _definition_pb2.Status
    def __init__(self, status: _Optional[_Union[_definition_pb2.Status, _Mapping]] = ...) -> None: ...

class ForwardMessageToDeadLetterQueueRequest(_message.Message):
    __slots__ = ["delivery_attempt", "group", "max_delivery_attempts", "message_id", "receipt_handle", "topic"]
    DELIVERY_ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    GROUP_FIELD_NUMBER: _ClassVar[int]
    MAX_DELIVERY_ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_ID_FIELD_NUMBER: _ClassVar[int]
    RECEIPT_HANDLE_FIELD_NUMBER: _ClassVar[int]
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    delivery_attempt: int
    group: _definition_pb2.Resource
    max_delivery_attempts: int
    message_id: str
    receipt_handle: str
    topic: _definition_pb2.Resource
    def __init__(self, group: _Optional[_Union[_definition_pb2.Resource, _Mapping]] = ..., topic: _Optional[_Union[_definition_pb2.Resource, _Mapping]] = ..., receipt_handle: _Optional[str] = ..., message_id: _Optional[str] = ..., delivery_attempt: _Optional[int] = ..., max_delivery_attempts: _Optional[int] = ...) -> None: ...

class ForwardMessageToDeadLetterQueueResponse(_message.Message):
    __slots__ = ["status"]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: _definition_pb2.Status
    def __init__(self, status: _Optional[_Union[_definition_pb2.Status, _Mapping]] = ...) -> None: ...

class HeartbeatRequest(_message.Message):
    __slots__ = ["client_type", "group"]
    CLIENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    GROUP_FIELD_NUMBER: _ClassVar[int]
    client_type: _definition_pb2.ClientType
    group: _definition_pb2.Resource
    def __init__(self, group: _Optional[_Union[_definition_pb2.Resource, _Mapping]] = ..., client_type: _Optional[_Union[_definition_pb2.ClientType, str]] = ...) -> None: ...

class HeartbeatResponse(_message.Message):
    __slots__ = ["status"]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: _definition_pb2.Status
    def __init__(self, status: _Optional[_Union[_definition_pb2.Status, _Mapping]] = ...) -> None: ...

class NotifyClientTerminationRequest(_message.Message):
    __slots__ = ["group"]
    GROUP_FIELD_NUMBER: _ClassVar[int]
    group: _definition_pb2.Resource
    def __init__(self, group: _Optional[_Union[_definition_pb2.Resource, _Mapping]] = ...) -> None: ...

class NotifyClientTerminationResponse(_message.Message):
    __slots__ = ["status"]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: _definition_pb2.Status
    def __init__(self, status: _Optional[_Union[_definition_pb2.Status, _Mapping]] = ...) -> None: ...

class PrintThreadStackTraceCommand(_message.Message):
    __slots__ = ["nonce"]
    NONCE_FIELD_NUMBER: _ClassVar[int]
    nonce: str
    def __init__(self, nonce: _Optional[str] = ...) -> None: ...

class QueryAssignmentRequest(_message.Message):
    __slots__ = ["endpoints", "group", "topic"]
    ENDPOINTS_FIELD_NUMBER: _ClassVar[int]
    GROUP_FIELD_NUMBER: _ClassVar[int]
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    endpoints: _definition_pb2.Endpoints
    group: _definition_pb2.Resource
    topic: _definition_pb2.Resource
    def __init__(self, topic: _Optional[_Union[_definition_pb2.Resource, _Mapping]] = ..., group: _Optional[_Union[_definition_pb2.Resource, _Mapping]] = ..., endpoints: _Optional[_Union[_definition_pb2.Endpoints, _Mapping]] = ...) -> None: ...

class QueryAssignmentResponse(_message.Message):
    __slots__ = ["assignments", "status"]
    ASSIGNMENTS_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    assignments: _containers.RepeatedCompositeFieldContainer[_definition_pb2.Assignment]
    status: _definition_pb2.Status
    def __init__(self, status: _Optional[_Union[_definition_pb2.Status, _Mapping]] = ..., assignments: _Optional[_Iterable[_Union[_definition_pb2.Assignment, _Mapping]]] = ...) -> None: ...

class QueryRouteRequest(_message.Message):
    __slots__ = ["endpoints", "topic"]
    ENDPOINTS_FIELD_NUMBER: _ClassVar[int]
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    endpoints: _definition_pb2.Endpoints
    topic: _definition_pb2.Resource
    def __init__(self, topic: _Optional[_Union[_definition_pb2.Resource, _Mapping]] = ..., endpoints: _Optional[_Union[_definition_pb2.Endpoints, _Mapping]] = ...) -> None: ...

class QueryRouteResponse(_message.Message):
    __slots__ = ["message_queues", "status"]
    MESSAGE_QUEUES_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    message_queues: _containers.RepeatedCompositeFieldContainer[_definition_pb2.MessageQueue]
    status: _definition_pb2.Status
    def __init__(self, status: _Optional[_Union[_definition_pb2.Status, _Mapping]] = ..., message_queues: _Optional[_Iterable[_Union[_definition_pb2.MessageQueue, _Mapping]]] = ...) -> None: ...

class ReceiveMessageRequest(_message.Message):
    __slots__ = ["auto_renew", "batch_size", "filter_expression", "group", "invisible_duration", "long_polling_timeout", "message_queue"]
    AUTO_RENEW_FIELD_NUMBER: _ClassVar[int]
    BATCH_SIZE_FIELD_NUMBER: _ClassVar[int]
    FILTER_EXPRESSION_FIELD_NUMBER: _ClassVar[int]
    GROUP_FIELD_NUMBER: _ClassVar[int]
    INVISIBLE_DURATION_FIELD_NUMBER: _ClassVar[int]
    LONG_POLLING_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_QUEUE_FIELD_NUMBER: _ClassVar[int]
    auto_renew: bool
    batch_size: int
    filter_expression: _definition_pb2.FilterExpression
    group: _definition_pb2.Resource
    invisible_duration: _duration_pb2.Duration
    long_polling_timeout: _duration_pb2.Duration
    message_queue: _definition_pb2.MessageQueue
    def __init__(self, group: _Optional[_Union[_definition_pb2.Resource, _Mapping]] = ..., message_queue: _Optional[_Union[_definition_pb2.MessageQueue, _Mapping]] = ..., filter_expression: _Optional[_Union[_definition_pb2.FilterExpression, _Mapping]] = ..., batch_size: _Optional[int] = ..., invisible_duration: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., auto_renew: bool = ..., long_polling_timeout: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ...) -> None: ...

class ReceiveMessageResponse(_message.Message):
    __slots__ = ["delivery_timestamp", "message", "status"]
    DELIVERY_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    delivery_timestamp: _timestamp_pb2.Timestamp
    message: _definition_pb2.Message
    status: _definition_pb2.Status
    def __init__(self, status: _Optional[_Union[_definition_pb2.Status, _Mapping]] = ..., message: _Optional[_Union[_definition_pb2.Message, _Mapping]] = ..., delivery_timestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class RecoverOrphanedTransactionCommand(_message.Message):
    __slots__ = ["message", "transaction_id"]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    TRANSACTION_ID_FIELD_NUMBER: _ClassVar[int]
    message: _definition_pb2.Message
    transaction_id: str
    def __init__(self, message: _Optional[_Union[_definition_pb2.Message, _Mapping]] = ..., transaction_id: _Optional[str] = ...) -> None: ...

class SendMessageRequest(_message.Message):
    __slots__ = ["messages"]
    MESSAGES_FIELD_NUMBER: _ClassVar[int]
    messages: _containers.RepeatedCompositeFieldContainer[_definition_pb2.Message]
    def __init__(self, messages: _Optional[_Iterable[_Union[_definition_pb2.Message, _Mapping]]] = ...) -> None: ...

class SendMessageResponse(_message.Message):
    __slots__ = ["entries", "status"]
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    entries: _containers.RepeatedCompositeFieldContainer[SendResultEntry]
    status: _definition_pb2.Status
    def __init__(self, status: _Optional[_Union[_definition_pb2.Status, _Mapping]] = ..., entries: _Optional[_Iterable[_Union[SendResultEntry, _Mapping]]] = ...) -> None: ...

class SendResultEntry(_message.Message):
    __slots__ = ["message_id", "offset", "status", "transaction_id"]
    MESSAGE_ID_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    TRANSACTION_ID_FIELD_NUMBER: _ClassVar[int]
    message_id: str
    offset: int
    status: _definition_pb2.Status
    transaction_id: str
    def __init__(self, status: _Optional[_Union[_definition_pb2.Status, _Mapping]] = ..., message_id: _Optional[str] = ..., transaction_id: _Optional[str] = ..., offset: _Optional[int] = ...) -> None: ...

class TelemetryCommand(_message.Message):
    __slots__ = ["print_thread_stack_trace_command", "recover_orphaned_transaction_command", "settings", "status", "thread_stack_trace", "verify_message_command", "verify_message_result"]
    PRINT_THREAD_STACK_TRACE_COMMAND_FIELD_NUMBER: _ClassVar[int]
    RECOVER_ORPHANED_TRANSACTION_COMMAND_FIELD_NUMBER: _ClassVar[int]
    SETTINGS_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    THREAD_STACK_TRACE_FIELD_NUMBER: _ClassVar[int]
    VERIFY_MESSAGE_COMMAND_FIELD_NUMBER: _ClassVar[int]
    VERIFY_MESSAGE_RESULT_FIELD_NUMBER: _ClassVar[int]
    print_thread_stack_trace_command: PrintThreadStackTraceCommand
    recover_orphaned_transaction_command: RecoverOrphanedTransactionCommand
    settings: _definition_pb2.Settings
    status: _definition_pb2.Status
    thread_stack_trace: ThreadStackTrace
    verify_message_command: VerifyMessageCommand
    verify_message_result: VerifyMessageResult
    def __init__(self, status: _Optional[_Union[_definition_pb2.Status, _Mapping]] = ..., settings: _Optional[_Union[_definition_pb2.Settings, _Mapping]] = ..., thread_stack_trace: _Optional[_Union[ThreadStackTrace, _Mapping]] = ..., verify_message_result: _Optional[_Union[VerifyMessageResult, _Mapping]] = ..., recover_orphaned_transaction_command: _Optional[_Union[RecoverOrphanedTransactionCommand, _Mapping]] = ..., print_thread_stack_trace_command: _Optional[_Union[PrintThreadStackTraceCommand, _Mapping]] = ..., verify_message_command: _Optional[_Union[VerifyMessageCommand, _Mapping]] = ...) -> None: ...

class ThreadStackTrace(_message.Message):
    __slots__ = ["nonce", "thread_stack_trace"]
    NONCE_FIELD_NUMBER: _ClassVar[int]
    THREAD_STACK_TRACE_FIELD_NUMBER: _ClassVar[int]
    nonce: str
    thread_stack_trace: str
    def __init__(self, nonce: _Optional[str] = ..., thread_stack_trace: _Optional[str] = ...) -> None: ...

class VerifyMessageCommand(_message.Message):
    __slots__ = ["message", "nonce"]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    NONCE_FIELD_NUMBER: _ClassVar[int]
    message: _definition_pb2.Message
    nonce: str
    def __init__(self, nonce: _Optional[str] = ..., message: _Optional[_Union[_definition_pb2.Message, _Mapping]] = ...) -> None: ...

class VerifyMessageResult(_message.Message):
    __slots__ = ["nonce"]
    NONCE_FIELD_NUMBER: _ClassVar[int]
    nonce: str
    def __init__(self, nonce: _Optional[str] = ...) -> None: ...
