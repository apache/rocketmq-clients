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

from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import duration_pb2 as google_dot_protobuf_dot_duration__pb2
from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2
from rocketmq.grpc_protocol import definition_pb2 as definition__pb2

DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\rservice.proto\x12\x12\x61pache.rocketmq.v2\x1a\x1egoogle/protobuf/duration.proto\x1a\x1fgoogle/protobuf/timestamp.proto\x1a\x10\x64\x65\x66inition.proto\"r\n\x11QueryRouteRequest\x12+\n\x05topic\x18\x01 \x01(\x0b\x32\x1c.apache.rocketmq.v2.Resource\x12\x30\n\tendpoints\x18\x02 \x01(\x0b\x32\x1d.apache.rocketmq.v2.Endpoints\"z\n\x12QueryRouteResponse\x12*\n\x06status\x18\x01 \x01(\x0b\x32\x1a.apache.rocketmq.v2.Status\x12\x38\n\x0emessage_queues\x18\x02 \x03(\x0b\x32 .apache.rocketmq.v2.MessageQueue\"C\n\x12SendMessageRequest\x12-\n\x08messages\x18\x01 \x03(\x0b\x32\x1b.apache.rocketmq.v2.Message\"\x90\x01\n\x0fSendResultEntry\x12*\n\x06status\x18\x01 \x01(\x0b\x32\x1a.apache.rocketmq.v2.Status\x12\x12\n\nmessage_id\x18\x02 \x01(\t\x12\x16\n\x0etransaction_id\x18\x03 \x01(\t\x12\x0e\n\x06offset\x18\x04 \x01(\x03\x12\x15\n\rrecall_handle\x18\x05 \x01(\t\"w\n\x13SendMessageResponse\x12*\n\x06status\x18\x01 \x01(\x0b\x32\x1a.apache.rocketmq.v2.Status\x12\x34\n\x07\x65ntries\x18\x02 \x03(\x0b\x32#.apache.rocketmq.v2.SendResultEntry\"\xa4\x01\n\x16QueryAssignmentRequest\x12+\n\x05topic\x18\x01 \x01(\x0b\x32\x1c.apache.rocketmq.v2.Resource\x12+\n\x05group\x18\x02 \x01(\x0b\x32\x1c.apache.rocketmq.v2.Resource\x12\x30\n\tendpoints\x18\x03 \x01(\x0b\x32\x1d.apache.rocketmq.v2.Endpoints\"z\n\x17QueryAssignmentResponse\x12*\n\x06status\x18\x01 \x01(\x0b\x32\x1a.apache.rocketmq.v2.Status\x12\x33\n\x0b\x61ssignments\x18\x02 \x03(\x0b\x32\x1e.apache.rocketmq.v2.Assignment\"\xb8\x03\n\x15ReceiveMessageRequest\x12+\n\x05group\x18\x01 \x01(\x0b\x32\x1c.apache.rocketmq.v2.Resource\x12\x37\n\rmessage_queue\x18\x02 \x01(\x0b\x32 .apache.rocketmq.v2.MessageQueue\x12?\n\x11\x66ilter_expression\x18\x03 \x01(\x0b\x32$.apache.rocketmq.v2.FilterExpression\x12\x12\n\nbatch_size\x18\x04 \x01(\x05\x12:\n\x12invisible_duration\x18\x05 \x01(\x0b\x32\x19.google.protobuf.DurationH\x00\x88\x01\x01\x12\x12\n\nauto_renew\x18\x06 \x01(\x08\x12<\n\x14long_polling_timeout\x18\x07 \x01(\x0b\x32\x19.google.protobuf.DurationH\x01\x88\x01\x01\x12\x17\n\nattempt_id\x18\x08 \x01(\tH\x02\x88\x01\x01\x42\x15\n\x13_invisible_durationB\x17\n\x15_long_polling_timeoutB\r\n\x0b_attempt_id\"\xbb\x01\n\x16ReceiveMessageResponse\x12,\n\x06status\x18\x01 \x01(\x0b\x32\x1a.apache.rocketmq.v2.StatusH\x00\x12.\n\x07message\x18\x02 \x01(\x0b\x32\x1b.apache.rocketmq.v2.MessageH\x00\x12\x38\n\x12\x64\x65livery_timestamp\x18\x03 \x01(\x0b\x32\x1a.google.protobuf.TimestampH\x00\x42\t\n\x07\x63ontent\"e\n\x0f\x41\x63kMessageEntry\x12\x12\n\nmessage_id\x18\x01 \x01(\t\x12\x16\n\x0ereceipt_handle\x18\x02 \x01(\t\x12\x17\n\nlite_topic\x18\x03 \x01(\tH\x00\x88\x01\x01\x42\r\n\x0b_lite_topic\"\xa3\x01\n\x11\x41\x63kMessageRequest\x12+\n\x05group\x18\x01 \x01(\x0b\x32\x1c.apache.rocketmq.v2.Resource\x12+\n\x05topic\x18\x02 \x01(\x0b\x32\x1c.apache.rocketmq.v2.Resource\x12\x34\n\x07\x65ntries\x18\x03 \x03(\x0b\x32#.apache.rocketmq.v2.AckMessageEntry\"o\n\x15\x41\x63kMessageResultEntry\x12\x12\n\nmessage_id\x18\x01 \x01(\t\x12\x16\n\x0ereceipt_handle\x18\x02 \x01(\t\x12*\n\x06status\x18\x03 \x01(\x0b\x32\x1a.apache.rocketmq.v2.Status\"|\n\x12\x41\x63kMessageResponse\x12*\n\x06status\x18\x01 \x01(\x0b\x32\x1a.apache.rocketmq.v2.Status\x12:\n\x07\x65ntries\x18\x02 \x03(\x0b\x32).apache.rocketmq.v2.AckMessageResultEntry\"\x8f\x02\n&ForwardMessageToDeadLetterQueueRequest\x12+\n\x05group\x18\x01 \x01(\x0b\x32\x1c.apache.rocketmq.v2.Resource\x12+\n\x05topic\x18\x02 \x01(\x0b\x32\x1c.apache.rocketmq.v2.Resource\x12\x16\n\x0ereceipt_handle\x18\x03 \x01(\t\x12\x12\n\nmessage_id\x18\x04 \x01(\t\x12\x18\n\x10\x64\x65livery_attempt\x18\x05 \x01(\x05\x12\x1d\n\x15max_delivery_attempts\x18\x06 \x01(\x05\x12\x17\n\nlite_topic\x18\x07 \x01(\tH\x00\x88\x01\x01\x42\r\n\x0b_lite_topic\"U\n\'ForwardMessageToDeadLetterQueueResponse\x12*\n\x06status\x18\x01 \x01(\x0b\x32\x1a.apache.rocketmq.v2.Status\"\x83\x01\n\x10HeartbeatRequest\x12\x30\n\x05group\x18\x01 \x01(\x0b\x32\x1c.apache.rocketmq.v2.ResourceH\x00\x88\x01\x01\x12\x33\n\x0b\x63lient_type\x18\x02 \x01(\x0e\x32\x1e.apache.rocketmq.v2.ClientTypeB\x08\n\x06_group\"?\n\x11HeartbeatResponse\x12*\n\x06status\x18\x01 \x01(\x0b\x32\x1a.apache.rocketmq.v2.Status\"\xfd\x01\n\x15\x45ndTransactionRequest\x12+\n\x05topic\x18\x01 \x01(\x0b\x32\x1c.apache.rocketmq.v2.Resource\x12\x12\n\nmessage_id\x18\x02 \x01(\t\x12\x16\n\x0etransaction_id\x18\x03 \x01(\t\x12=\n\nresolution\x18\x04 \x01(\x0e\x32).apache.rocketmq.v2.TransactionResolution\x12\x35\n\x06source\x18\x05 \x01(\x0e\x32%.apache.rocketmq.v2.TransactionSource\x12\x15\n\rtrace_context\x18\x06 \x01(\t\"D\n\x16\x45ndTransactionResponse\x12*\n\x06status\x18\x01 \x01(\x0b\x32\x1a.apache.rocketmq.v2.Status\"-\n\x1cPrintThreadStackTraceCommand\x12\r\n\x05nonce\x18\x01 \x01(\t\"*\n\x19ReconnectEndpointsCommand\x12\r\n\x05nonce\x18\x01 \x01(\t\"Y\n\x10ThreadStackTrace\x12\r\n\x05nonce\x18\x01 \x01(\t\x12\x1f\n\x12thread_stack_trace\x18\x02 \x01(\tH\x00\x88\x01\x01\x42\x15\n\x13_thread_stack_trace\"S\n\x14VerifyMessageCommand\x12\r\n\x05nonce\x18\x01 \x01(\t\x12,\n\x07message\x18\x02 \x01(\x0b\x32\x1b.apache.rocketmq.v2.Message\"$\n\x13VerifyMessageResult\x12\r\n\x05nonce\x18\x01 \x01(\t\"i\n!RecoverOrphanedTransactionCommand\x12,\n\x07message\x18\x01 \x01(\x0b\x32\x1b.apache.rocketmq.v2.Message\x12\x16\n\x0etransaction_id\x18\x02 \x01(\t\"2\n\x1cNotifyUnsubscribeLiteCommand\x12\x12\n\nlite_topic\x18\x01 \x01(\t\"\xdd\x05\n\x10TelemetryCommand\x12/\n\x06status\x18\x01 \x01(\x0b\x32\x1a.apache.rocketmq.v2.StatusH\x01\x88\x01\x01\x12\x30\n\x08settings\x18\x02 \x01(\x0b\x32\x1c.apache.rocketmq.v2.SettingsH\x00\x12\x42\n\x12thread_stack_trace\x18\x03 \x01(\x0b\x32$.apache.rocketmq.v2.ThreadStackTraceH\x00\x12H\n\x15verify_message_result\x18\x04 \x01(\x0b\x32\'.apache.rocketmq.v2.VerifyMessageResultH\x00\x12\x65\n$recover_orphaned_transaction_command\x18\x05 \x01(\x0b\x32\x35.apache.rocketmq.v2.RecoverOrphanedTransactionCommandH\x00\x12\\\n print_thread_stack_trace_command\x18\x06 \x01(\x0b\x32\x30.apache.rocketmq.v2.PrintThreadStackTraceCommandH\x00\x12J\n\x16verify_message_command\x18\x07 \x01(\x0b\x32(.apache.rocketmq.v2.VerifyMessageCommandH\x00\x12T\n\x1breconnect_endpoints_command\x18\x08 \x01(\x0b\x32-.apache.rocketmq.v2.ReconnectEndpointsCommandH\x00\x12[\n\x1fnotify_unsubscribe_lite_command\x18\t \x01(\x0b\x32\x30.apache.rocketmq.v2.NotifyUnsubscribeLiteCommandH\x00\x42\t\n\x07\x63ommandB\t\n\x07_status\"\\\n\x1eNotifyClientTerminationRequest\x12\x30\n\x05group\x18\x01 \x01(\x0b\x32\x1c.apache.rocketmq.v2.ResourceH\x00\x88\x01\x01\x42\x08\n\x06_group\"M\n\x1fNotifyClientTerminationResponse\x12*\n\x06status\x18\x01 \x01(\x0b\x32\x1a.apache.rocketmq.v2.Status\"\xdd\x01\n\x1e\x43hangeInvisibleDurationRequest\x12+\n\x05group\x18\x01 \x01(\x0b\x32\x1c.apache.rocketmq.v2.Resource\x12+\n\x05topic\x18\x02 \x01(\x0b\x32\x1c.apache.rocketmq.v2.Resource\x12\x16\n\x0ereceipt_handle\x18\x03 \x01(\t\x12\x35\n\x12invisible_duration\x18\x04 \x01(\x0b\x32\x19.google.protobuf.Duration\x12\x12\n\nmessage_id\x18\x05 \x01(\t\"e\n\x1f\x43hangeInvisibleDurationResponse\x12*\n\x06status\x18\x01 \x01(\x0b\x32\x1a.apache.rocketmq.v2.Status\x12\x16\n\x0ereceipt_handle\x18\x02 \x01(\t\"\x98\x02\n\x12PullMessageRequest\x12+\n\x05group\x18\x01 \x01(\x0b\x32\x1c.apache.rocketmq.v2.Resource\x12\x37\n\rmessage_queue\x18\x02 \x01(\x0b\x32 .apache.rocketmq.v2.MessageQueue\x12\x0e\n\x06offset\x18\x03 \x01(\x03\x12\x12\n\nbatch_size\x18\x04 \x01(\x05\x12?\n\x11\x66ilter_expression\x18\x05 \x01(\x0b\x32$.apache.rocketmq.v2.FilterExpression\x12\x37\n\x14long_polling_timeout\x18\x06 \x01(\x0b\x32\x19.google.protobuf.Duration\"\x95\x01\n\x13PullMessageResponse\x12,\n\x06status\x18\x01 \x01(\x0b\x32\x1a.apache.rocketmq.v2.StatusH\x00\x12.\n\x07message\x18\x02 \x01(\x0b\x32\x1b.apache.rocketmq.v2.MessageH\x00\x12\x15\n\x0bnext_offset\x18\x03 \x01(\x03H\x00\x42\t\n\x07\x63ontent\"\x8b\x01\n\x13UpdateOffsetRequest\x12+\n\x05group\x18\x01 \x01(\x0b\x32\x1c.apache.rocketmq.v2.Resource\x12\x37\n\rmessage_queue\x18\x02 \x01(\x0b\x32 .apache.rocketmq.v2.MessageQueue\x12\x0e\n\x06offset\x18\x03 \x01(\x03\"B\n\x14UpdateOffsetResponse\x12*\n\x06status\x18\x01 \x01(\x0b\x32\x1a.apache.rocketmq.v2.Status\"x\n\x10GetOffsetRequest\x12+\n\x05group\x18\x01 \x01(\x0b\x32\x1c.apache.rocketmq.v2.Resource\x12\x37\n\rmessage_queue\x18\x02 \x01(\x0b\x32 .apache.rocketmq.v2.MessageQueue\"O\n\x11GetOffsetResponse\x12*\n\x06status\x18\x01 \x01(\x0b\x32\x1a.apache.rocketmq.v2.Status\x12\x0e\n\x06offset\x18\x02 \x01(\x03\"\xd3\x01\n\x12QueryOffsetRequest\x12\x37\n\rmessage_queue\x18\x01 \x01(\x0b\x32 .apache.rocketmq.v2.MessageQueue\x12\x42\n\x13query_offset_policy\x18\x02 \x01(\x0e\x32%.apache.rocketmq.v2.QueryOffsetPolicy\x12\x32\n\ttimestamp\x18\x03 \x01(\x0b\x32\x1a.google.protobuf.TimestampH\x00\x88\x01\x01\x42\x0c\n\n_timestamp\"Q\n\x13QueryOffsetResponse\x12*\n\x06status\x18\x01 \x01(\x0b\x32\x1a.apache.rocketmq.v2.Status\x12\x0e\n\x06offset\x18\x02 \x01(\x03\"Z\n\x14RecallMessageRequest\x12+\n\x05topic\x18\x01 \x01(\x0b\x32\x1c.apache.rocketmq.v2.Resource\x12\x15\n\rrecall_handle\x18\x02 \x01(\t\"W\n\x15RecallMessageResponse\x12*\n\x06status\x18\x01 \x01(\x0b\x32\x1a.apache.rocketmq.v2.Status\x12\x12\n\nmessage_id\x18\x02 \x01(\t\"\xed\x01\n\x1bSyncLiteSubscriptionRequest\x12:\n\x06\x61\x63tion\x18\x01 \x01(\x0e\x32*.apache.rocketmq.v2.LiteSubscriptionAction\x12+\n\x05topic\x18\x02 \x01(\x0b\x32\x1c.apache.rocketmq.v2.Resource\x12+\n\x05group\x18\x03 \x01(\x0b\x32\x1c.apache.rocketmq.v2.Resource\x12\x16\n\x0elite_topic_set\x18\x04 \x03(\t\x12\x14\n\x07version\x18\x05 \x01(\x03H\x00\x88\x01\x01\x42\n\n\x08_version\"J\n\x1cSyncLiteSubscriptionResponse\x12*\n\x06status\x18\x01 \x01(\x0b\x32\x1a.apache.rocketmq.v2.Status2\xcc\x0e\n\x10MessagingService\x12]\n\nQueryRoute\x12%.apache.rocketmq.v2.QueryRouteRequest\x1a&.apache.rocketmq.v2.QueryRouteResponse\"\x00\x12Z\n\tHeartbeat\x12$.apache.rocketmq.v2.HeartbeatRequest\x1a%.apache.rocketmq.v2.HeartbeatResponse\"\x00\x12`\n\x0bSendMessage\x12&.apache.rocketmq.v2.SendMessageRequest\x1a\'.apache.rocketmq.v2.SendMessageResponse\"\x00\x12l\n\x0fQueryAssignment\x12*.apache.rocketmq.v2.QueryAssignmentRequest\x1a+.apache.rocketmq.v2.QueryAssignmentResponse\"\x00\x12k\n\x0eReceiveMessage\x12).apache.rocketmq.v2.ReceiveMessageRequest\x1a*.apache.rocketmq.v2.ReceiveMessageResponse\"\x00\x30\x01\x12]\n\nAckMessage\x12%.apache.rocketmq.v2.AckMessageRequest\x1a&.apache.rocketmq.v2.AckMessageResponse\"\x00\x12\x9c\x01\n\x1f\x46orwardMessageToDeadLetterQueue\x12:.apache.rocketmq.v2.ForwardMessageToDeadLetterQueueRequest\x1a;.apache.rocketmq.v2.ForwardMessageToDeadLetterQueueResponse\"\x00\x12\x62\n\x0bPullMessage\x12&.apache.rocketmq.v2.PullMessageRequest\x1a\'.apache.rocketmq.v2.PullMessageResponse\"\x00\x30\x01\x12\x63\n\x0cUpdateOffset\x12\'.apache.rocketmq.v2.UpdateOffsetRequest\x1a(.apache.rocketmq.v2.UpdateOffsetResponse\"\x00\x12Z\n\tGetOffset\x12$.apache.rocketmq.v2.GetOffsetRequest\x1a%.apache.rocketmq.v2.GetOffsetResponse\"\x00\x12`\n\x0bQueryOffset\x12&.apache.rocketmq.v2.QueryOffsetRequest\x1a\'.apache.rocketmq.v2.QueryOffsetResponse\"\x00\x12i\n\x0e\x45ndTransaction\x12).apache.rocketmq.v2.EndTransactionRequest\x1a*.apache.rocketmq.v2.EndTransactionResponse\"\x00\x12]\n\tTelemetry\x12$.apache.rocketmq.v2.TelemetryCommand\x1a$.apache.rocketmq.v2.TelemetryCommand\"\x00(\x01\x30\x01\x12\x84\x01\n\x17NotifyClientTermination\x12\x32.apache.rocketmq.v2.NotifyClientTerminationRequest\x1a\x33.apache.rocketmq.v2.NotifyClientTerminationResponse\"\x00\x12\x84\x01\n\x17\x43hangeInvisibleDuration\x12\x32.apache.rocketmq.v2.ChangeInvisibleDurationRequest\x1a\x33.apache.rocketmq.v2.ChangeInvisibleDurationResponse\"\x00\x12\x66\n\rRecallMessage\x12(.apache.rocketmq.v2.RecallMessageRequest\x1a).apache.rocketmq.v2.RecallMessageResponse\"\x00\x12{\n\x14SyncLiteSubscription\x12/.apache.rocketmq.v2.SyncLiteSubscriptionRequest\x1a\x30.apache.rocketmq.v2.SyncLiteSubscriptionResponse\"\x00\x42<\n\x12\x61pache.rocketmq.v2B\tMQServiceP\x01\xa0\x01\x01\xd8\x01\x01\xaa\x02\x12\x41pache.Rocketmq.V2b\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'service_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
  _globals['DESCRIPTOR']._loaded_options = None
  _globals['DESCRIPTOR']._serialized_options = b'\n\022apache.rocketmq.v2B\tMQServiceP\001\240\001\001\330\001\001\252\002\022Apache.Rocketmq.V2'
  _globals['_QUERYROUTEREQUEST']._serialized_start=120
  _globals['_QUERYROUTEREQUEST']._serialized_end=234
  _globals['_QUERYROUTERESPONSE']._serialized_start=236
  _globals['_QUERYROUTERESPONSE']._serialized_end=358
  _globals['_SENDMESSAGEREQUEST']._serialized_start=360
  _globals['_SENDMESSAGEREQUEST']._serialized_end=427
  _globals['_SENDRESULTENTRY']._serialized_start=430
  _globals['_SENDRESULTENTRY']._serialized_end=574
  _globals['_SENDMESSAGERESPONSE']._serialized_start=576
  _globals['_SENDMESSAGERESPONSE']._serialized_end=695
  _globals['_QUERYASSIGNMENTREQUEST']._serialized_start=698
  _globals['_QUERYASSIGNMENTREQUEST']._serialized_end=862
  _globals['_QUERYASSIGNMENTRESPONSE']._serialized_start=864
  _globals['_QUERYASSIGNMENTRESPONSE']._serialized_end=986
  _globals['_RECEIVEMESSAGEREQUEST']._serialized_start=989
  _globals['_RECEIVEMESSAGEREQUEST']._serialized_end=1429
  _globals['_RECEIVEMESSAGERESPONSE']._serialized_start=1432
  _globals['_RECEIVEMESSAGERESPONSE']._serialized_end=1619
  _globals['_ACKMESSAGEENTRY']._serialized_start=1621
  _globals['_ACKMESSAGEENTRY']._serialized_end=1722
  _globals['_ACKMESSAGEREQUEST']._serialized_start=1725
  _globals['_ACKMESSAGEREQUEST']._serialized_end=1888
  _globals['_ACKMESSAGERESULTENTRY']._serialized_start=1890
  _globals['_ACKMESSAGERESULTENTRY']._serialized_end=2001
  _globals['_ACKMESSAGERESPONSE']._serialized_start=2003
  _globals['_ACKMESSAGERESPONSE']._serialized_end=2127
  _globals['_FORWARDMESSAGETODEADLETTERQUEUEREQUEST']._serialized_start=2130
  _globals['_FORWARDMESSAGETODEADLETTERQUEUEREQUEST']._serialized_end=2401
  _globals['_FORWARDMESSAGETODEADLETTERQUEUERESPONSE']._serialized_start=2403
  _globals['_FORWARDMESSAGETODEADLETTERQUEUERESPONSE']._serialized_end=2488
  _globals['_HEARTBEATREQUEST']._serialized_start=2491
  _globals['_HEARTBEATREQUEST']._serialized_end=2622
  _globals['_HEARTBEATRESPONSE']._serialized_start=2624
  _globals['_HEARTBEATRESPONSE']._serialized_end=2687
  _globals['_ENDTRANSACTIONREQUEST']._serialized_start=2690
  _globals['_ENDTRANSACTIONREQUEST']._serialized_end=2943
  _globals['_ENDTRANSACTIONRESPONSE']._serialized_start=2945
  _globals['_ENDTRANSACTIONRESPONSE']._serialized_end=3013
  _globals['_PRINTTHREADSTACKTRACECOMMAND']._serialized_start=3015
  _globals['_PRINTTHREADSTACKTRACECOMMAND']._serialized_end=3060
  _globals['_RECONNECTENDPOINTSCOMMAND']._serialized_start=3062
  _globals['_RECONNECTENDPOINTSCOMMAND']._serialized_end=3104
  _globals['_THREADSTACKTRACE']._serialized_start=3106
  _globals['_THREADSTACKTRACE']._serialized_end=3195
  _globals['_VERIFYMESSAGECOMMAND']._serialized_start=3197
  _globals['_VERIFYMESSAGECOMMAND']._serialized_end=3280
  _globals['_VERIFYMESSAGERESULT']._serialized_start=3282
  _globals['_VERIFYMESSAGERESULT']._serialized_end=3318
  _globals['_RECOVERORPHANEDTRANSACTIONCOMMAND']._serialized_start=3320
  _globals['_RECOVERORPHANEDTRANSACTIONCOMMAND']._serialized_end=3425
  _globals['_NOTIFYUNSUBSCRIBELITECOMMAND']._serialized_start=3427
  _globals['_NOTIFYUNSUBSCRIBELITECOMMAND']._serialized_end=3477
  _globals['_TELEMETRYCOMMAND']._serialized_start=3480
  _globals['_TELEMETRYCOMMAND']._serialized_end=4213
  _globals['_NOTIFYCLIENTTERMINATIONREQUEST']._serialized_start=4215
  _globals['_NOTIFYCLIENTTERMINATIONREQUEST']._serialized_end=4307
  _globals['_NOTIFYCLIENTTERMINATIONRESPONSE']._serialized_start=4309
  _globals['_NOTIFYCLIENTTERMINATIONRESPONSE']._serialized_end=4386
  _globals['_CHANGEINVISIBLEDURATIONREQUEST']._serialized_start=4389
  _globals['_CHANGEINVISIBLEDURATIONREQUEST']._serialized_end=4610
  _globals['_CHANGEINVISIBLEDURATIONRESPONSE']._serialized_start=4612
  _globals['_CHANGEINVISIBLEDURATIONRESPONSE']._serialized_end=4713
  _globals['_PULLMESSAGEREQUEST']._serialized_start=4716
  _globals['_PULLMESSAGEREQUEST']._serialized_end=4996
  _globals['_PULLMESSAGERESPONSE']._serialized_start=4999
  _globals['_PULLMESSAGERESPONSE']._serialized_end=5148
  _globals['_UPDATEOFFSETREQUEST']._serialized_start=5151
  _globals['_UPDATEOFFSETREQUEST']._serialized_end=5290
  _globals['_UPDATEOFFSETRESPONSE']._serialized_start=5292
  _globals['_UPDATEOFFSETRESPONSE']._serialized_end=5358
  _globals['_GETOFFSETREQUEST']._serialized_start=5360
  _globals['_GETOFFSETREQUEST']._serialized_end=5480
  _globals['_GETOFFSETRESPONSE']._serialized_start=5482
  _globals['_GETOFFSETRESPONSE']._serialized_end=5561
  _globals['_QUERYOFFSETREQUEST']._serialized_start=5564
  _globals['_QUERYOFFSETREQUEST']._serialized_end=5775
  _globals['_QUERYOFFSETRESPONSE']._serialized_start=5777
  _globals['_QUERYOFFSETRESPONSE']._serialized_end=5858
  _globals['_RECALLMESSAGEREQUEST']._serialized_start=5860
  _globals['_RECALLMESSAGEREQUEST']._serialized_end=5950
  _globals['_RECALLMESSAGERESPONSE']._serialized_start=5952
  _globals['_RECALLMESSAGERESPONSE']._serialized_end=6039
  _globals['_SYNCLITESUBSCRIPTIONREQUEST']._serialized_start=6042
  _globals['_SYNCLITESUBSCRIPTIONREQUEST']._serialized_end=6279
  _globals['_SYNCLITESUBSCRIPTIONRESPONSE']._serialized_start=6281
  _globals['_SYNCLITESUBSCRIPTIONRESPONSE']._serialized_end=6355
  _globals['_MESSAGINGSERVICE']._serialized_start=6358
  _globals['_MESSAGINGSERVICE']._serialized_end=8226
# @@protoc_insertion_point(module_scope)
