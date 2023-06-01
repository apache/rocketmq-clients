# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: apache/rocketmq/v2/service.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import duration_pb2 as google_dot_protobuf_dot_duration__pb2
from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2
from protocol import definition_pb2 as apache_dot_rocketmq_dot_v2_dot_definition__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n apache/rocketmq/v2/service.proto\x12\x12\x61pache.rocketmq.v2\x1a\x1egoogle/protobuf/duration.proto\x1a\x1fgoogle/protobuf/timestamp.proto\x1a#apache/rocketmq/v2/definition.proto\"r\n\x11QueryRouteRequest\x12+\n\x05topic\x18\x01 \x01(\x0b\x32\x1c.apache.rocketmq.v2.Resource\x12\x30\n\tendpoints\x18\x02 \x01(\x0b\x32\x1d.apache.rocketmq.v2.Endpoints\"z\n\x12QueryRouteResponse\x12*\n\x06status\x18\x01 \x01(\x0b\x32\x1a.apache.rocketmq.v2.Status\x12\x38\n\x0emessage_queues\x18\x02 \x03(\x0b\x32 .apache.rocketmq.v2.MessageQueue\"C\n\x12SendMessageRequest\x12-\n\x08messages\x18\x01 \x03(\x0b\x32\x1b.apache.rocketmq.v2.Message\"y\n\x0fSendResultEntry\x12*\n\x06status\x18\x01 \x01(\x0b\x32\x1a.apache.rocketmq.v2.Status\x12\x12\n\nmessage_id\x18\x02 \x01(\t\x12\x16\n\x0etransaction_id\x18\x03 \x01(\t\x12\x0e\n\x06offset\x18\x04 \x01(\x03\"w\n\x13SendMessageResponse\x12*\n\x06status\x18\x01 \x01(\x0b\x32\x1a.apache.rocketmq.v2.Status\x12\x34\n\x07\x65ntries\x18\x02 \x03(\x0b\x32#.apache.rocketmq.v2.SendResultEntry\"\xa4\x01\n\x16QueryAssignmentRequest\x12+\n\x05topic\x18\x01 \x01(\x0b\x32\x1c.apache.rocketmq.v2.Resource\x12+\n\x05group\x18\x02 \x01(\x0b\x32\x1c.apache.rocketmq.v2.Resource\x12\x30\n\tendpoints\x18\x03 \x01(\x0b\x32\x1d.apache.rocketmq.v2.Endpoints\"z\n\x17QueryAssignmentResponse\x12*\n\x06status\x18\x01 \x01(\x0b\x32\x1a.apache.rocketmq.v2.Status\x12\x33\n\x0b\x61ssignments\x18\x02 \x03(\x0b\x32\x1e.apache.rocketmq.v2.Assignment\"\x90\x03\n\x15ReceiveMessageRequest\x12+\n\x05group\x18\x01 \x01(\x0b\x32\x1c.apache.rocketmq.v2.Resource\x12\x37\n\rmessage_queue\x18\x02 \x01(\x0b\x32 .apache.rocketmq.v2.MessageQueue\x12?\n\x11\x66ilter_expression\x18\x03 \x01(\x0b\x32$.apache.rocketmq.v2.FilterExpression\x12\x12\n\nbatch_size\x18\x04 \x01(\x05\x12:\n\x12invisible_duration\x18\x05 \x01(\x0b\x32\x19.google.protobuf.DurationH\x00\x88\x01\x01\x12\x12\n\nauto_renew\x18\x06 \x01(\x08\x12<\n\x14long_polling_timeout\x18\x07 \x01(\x0b\x32\x19.google.protobuf.DurationH\x01\x88\x01\x01\x42\x15\n\x13_invisible_durationB\x17\n\x15_long_polling_timeout\"\xbb\x01\n\x16ReceiveMessageResponse\x12,\n\x06status\x18\x01 \x01(\x0b\x32\x1a.apache.rocketmq.v2.StatusH\x00\x12.\n\x07message\x18\x02 \x01(\x0b\x32\x1b.apache.rocketmq.v2.MessageH\x00\x12\x38\n\x12\x64\x65livery_timestamp\x18\x03 \x01(\x0b\x32\x1a.google.protobuf.TimestampH\x00\x42\t\n\x07\x63ontent\"=\n\x0f\x41\x63kMessageEntry\x12\x12\n\nmessage_id\x18\x01 \x01(\t\x12\x16\n\x0ereceipt_handle\x18\x02 \x01(\t\"\xa3\x01\n\x11\x41\x63kMessageRequest\x12+\n\x05group\x18\x01 \x01(\x0b\x32\x1c.apache.rocketmq.v2.Resource\x12+\n\x05topic\x18\x02 \x01(\x0b\x32\x1c.apache.rocketmq.v2.Resource\x12\x34\n\x07\x65ntries\x18\x03 \x03(\x0b\x32#.apache.rocketmq.v2.AckMessageEntry\"o\n\x15\x41\x63kMessageResultEntry\x12\x12\n\nmessage_id\x18\x01 \x01(\t\x12\x16\n\x0ereceipt_handle\x18\x02 \x01(\t\x12*\n\x06status\x18\x03 \x01(\x0b\x32\x1a.apache.rocketmq.v2.Status\"|\n\x12\x41\x63kMessageResponse\x12*\n\x06status\x18\x01 \x01(\x0b\x32\x1a.apache.rocketmq.v2.Status\x12:\n\x07\x65ntries\x18\x02 \x03(\x0b\x32).apache.rocketmq.v2.AckMessageResultEntry\"\xe7\x01\n&ForwardMessageToDeadLetterQueueRequest\x12+\n\x05group\x18\x01 \x01(\x0b\x32\x1c.apache.rocketmq.v2.Resource\x12+\n\x05topic\x18\x02 \x01(\x0b\x32\x1c.apache.rocketmq.v2.Resource\x12\x16\n\x0ereceipt_handle\x18\x03 \x01(\t\x12\x12\n\nmessage_id\x18\x04 \x01(\t\x12\x18\n\x10\x64\x65livery_attempt\x18\x05 \x01(\x05\x12\x1d\n\x15max_delivery_attempts\x18\x06 \x01(\x05\"U\n\'ForwardMessageToDeadLetterQueueResponse\x12*\n\x06status\x18\x01 \x01(\x0b\x32\x1a.apache.rocketmq.v2.Status\"\x83\x01\n\x10HeartbeatRequest\x12\x30\n\x05group\x18\x01 \x01(\x0b\x32\x1c.apache.rocketmq.v2.ResourceH\x00\x88\x01\x01\x12\x33\n\x0b\x63lient_type\x18\x02 \x01(\x0e\x32\x1e.apache.rocketmq.v2.ClientTypeB\x08\n\x06_group\"?\n\x11HeartbeatResponse\x12*\n\x06status\x18\x01 \x01(\x0b\x32\x1a.apache.rocketmq.v2.Status\"\xfd\x01\n\x15\x45ndTransactionRequest\x12+\n\x05topic\x18\x01 \x01(\x0b\x32\x1c.apache.rocketmq.v2.Resource\x12\x12\n\nmessage_id\x18\x02 \x01(\t\x12\x16\n\x0etransaction_id\x18\x03 \x01(\t\x12=\n\nresolution\x18\x04 \x01(\x0e\x32).apache.rocketmq.v2.TransactionResolution\x12\x35\n\x06source\x18\x05 \x01(\x0e\x32%.apache.rocketmq.v2.TransactionSource\x12\x15\n\rtrace_context\x18\x06 \x01(\t\"D\n\x16\x45ndTransactionResponse\x12*\n\x06status\x18\x01 \x01(\x0b\x32\x1a.apache.rocketmq.v2.Status\"-\n\x1cPrintThreadStackTraceCommand\x12\r\n\x05nonce\x18\x01 \x01(\t\"Y\n\x10ThreadStackTrace\x12\r\n\x05nonce\x18\x01 \x01(\t\x12\x1f\n\x12thread_stack_trace\x18\x02 \x01(\tH\x00\x88\x01\x01\x42\x15\n\x13_thread_stack_trace\"S\n\x14VerifyMessageCommand\x12\r\n\x05nonce\x18\x01 \x01(\t\x12,\n\x07message\x18\x02 \x01(\x0b\x32\x1b.apache.rocketmq.v2.Message\"$\n\x13VerifyMessageResult\x12\r\n\x05nonce\x18\x01 \x01(\t\"i\n!RecoverOrphanedTransactionCommand\x12,\n\x07message\x18\x01 \x01(\x0b\x32\x1b.apache.rocketmq.v2.Message\x12\x16\n\x0etransaction_id\x18\x02 \x01(\t\"\xaa\x04\n\x10TelemetryCommand\x12/\n\x06status\x18\x01 \x01(\x0b\x32\x1a.apache.rocketmq.v2.StatusH\x01\x88\x01\x01\x12\x30\n\x08settings\x18\x02 \x01(\x0b\x32\x1c.apache.rocketmq.v2.SettingsH\x00\x12\x42\n\x12thread_stack_trace\x18\x03 \x01(\x0b\x32$.apache.rocketmq.v2.ThreadStackTraceH\x00\x12H\n\x15verify_message_result\x18\x04 \x01(\x0b\x32\'.apache.rocketmq.v2.VerifyMessageResultH\x00\x12\x65\n$recover_orphaned_transaction_command\x18\x05 \x01(\x0b\x32\x35.apache.rocketmq.v2.RecoverOrphanedTransactionCommandH\x00\x12\\\n print_thread_stack_trace_command\x18\x06 \x01(\x0b\x32\x30.apache.rocketmq.v2.PrintThreadStackTraceCommandH\x00\x12J\n\x16verify_message_command\x18\x07 \x01(\x0b\x32(.apache.rocketmq.v2.VerifyMessageCommandH\x00\x42\t\n\x07\x63ommandB\t\n\x07_status\"\\\n\x1eNotifyClientTerminationRequest\x12\x30\n\x05group\x18\x01 \x01(\x0b\x32\x1c.apache.rocketmq.v2.ResourceH\x00\x88\x01\x01\x42\x08\n\x06_group\"M\n\x1fNotifyClientTerminationResponse\x12*\n\x06status\x18\x01 \x01(\x0b\x32\x1a.apache.rocketmq.v2.Status\"\xdd\x01\n\x1e\x43hangeInvisibleDurationRequest\x12+\n\x05group\x18\x01 \x01(\x0b\x32\x1c.apache.rocketmq.v2.Resource\x12+\n\x05topic\x18\x02 \x01(\x0b\x32\x1c.apache.rocketmq.v2.Resource\x12\x16\n\x0ereceipt_handle\x18\x03 \x01(\t\x12\x35\n\x12invisible_duration\x18\x04 \x01(\x0b\x32\x19.google.protobuf.Duration\x12\x12\n\nmessage_id\x18\x05 \x01(\t\"e\n\x1f\x43hangeInvisibleDurationResponse\x12*\n\x06status\x18\x01 \x01(\x0b\x32\x1a.apache.rocketmq.v2.Status\x12\x16\n\x0ereceipt_handle\x18\x02 \x01(\t2\xe0\t\n\x10MessagingService\x12]\n\nQueryRoute\x12%.apache.rocketmq.v2.QueryRouteRequest\x1a&.apache.rocketmq.v2.QueryRouteResponse\"\x00\x12Z\n\tHeartbeat\x12$.apache.rocketmq.v2.HeartbeatRequest\x1a%.apache.rocketmq.v2.HeartbeatResponse\"\x00\x12`\n\x0bSendMessage\x12&.apache.rocketmq.v2.SendMessageRequest\x1a\'.apache.rocketmq.v2.SendMessageResponse\"\x00\x12l\n\x0fQueryAssignment\x12*.apache.rocketmq.v2.QueryAssignmentRequest\x1a+.apache.rocketmq.v2.QueryAssignmentResponse\"\x00\x12k\n\x0eReceiveMessage\x12).apache.rocketmq.v2.ReceiveMessageRequest\x1a*.apache.rocketmq.v2.ReceiveMessageResponse\"\x00\x30\x01\x12]\n\nAckMessage\x12%.apache.rocketmq.v2.AckMessageRequest\x1a&.apache.rocketmq.v2.AckMessageResponse\"\x00\x12\x9c\x01\n\x1f\x46orwardMessageToDeadLetterQueue\x12:.apache.rocketmq.v2.ForwardMessageToDeadLetterQueueRequest\x1a;.apache.rocketmq.v2.ForwardMessageToDeadLetterQueueResponse\"\x00\x12i\n\x0e\x45ndTransaction\x12).apache.rocketmq.v2.EndTransactionRequest\x1a*.apache.rocketmq.v2.EndTransactionResponse\"\x00\x12]\n\tTelemetry\x12$.apache.rocketmq.v2.TelemetryCommand\x1a$.apache.rocketmq.v2.TelemetryCommand\"\x00(\x01\x30\x01\x12\x84\x01\n\x17NotifyClientTermination\x12\x32.apache.rocketmq.v2.NotifyClientTerminationRequest\x1a\x33.apache.rocketmq.v2.NotifyClientTerminationResponse\"\x00\x12\x84\x01\n\x17\x43hangeInvisibleDuration\x12\x32.apache.rocketmq.v2.ChangeInvisibleDurationRequest\x1a\x33.apache.rocketmq.v2.ChangeInvisibleDurationResponse\"\x00\x42<\n\x12\x61pache.rocketmq.v2B\tMQServiceP\x01\xa0\x01\x01\xd8\x01\x01\xaa\x02\x12\x41pache.Rocketmq.V2b\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'apache.rocketmq.v2.service_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  DESCRIPTOR._serialized_options = b'\n\022apache.rocketmq.v2B\tMQServiceP\001\240\001\001\330\001\001\252\002\022Apache.Rocketmq.V2'
  _QUERYROUTEREQUEST._serialized_start=158
  _QUERYROUTEREQUEST._serialized_end=272
  _QUERYROUTERESPONSE._serialized_start=274
  _QUERYROUTERESPONSE._serialized_end=396
  _SENDMESSAGEREQUEST._serialized_start=398
  _SENDMESSAGEREQUEST._serialized_end=465
  _SENDRESULTENTRY._serialized_start=467
  _SENDRESULTENTRY._serialized_end=588
  _SENDMESSAGERESPONSE._serialized_start=590
  _SENDMESSAGERESPONSE._serialized_end=709
  _QUERYASSIGNMENTREQUEST._serialized_start=712
  _QUERYASSIGNMENTREQUEST._serialized_end=876
  _QUERYASSIGNMENTRESPONSE._serialized_start=878
  _QUERYASSIGNMENTRESPONSE._serialized_end=1000
  _RECEIVEMESSAGEREQUEST._serialized_start=1003
  _RECEIVEMESSAGEREQUEST._serialized_end=1403
  _RECEIVEMESSAGERESPONSE._serialized_start=1406
  _RECEIVEMESSAGERESPONSE._serialized_end=1593
  _ACKMESSAGEENTRY._serialized_start=1595
  _ACKMESSAGEENTRY._serialized_end=1656
  _ACKMESSAGEREQUEST._serialized_start=1659
  _ACKMESSAGEREQUEST._serialized_end=1822
  _ACKMESSAGERESULTENTRY._serialized_start=1824
  _ACKMESSAGERESULTENTRY._serialized_end=1935
  _ACKMESSAGERESPONSE._serialized_start=1937
  _ACKMESSAGERESPONSE._serialized_end=2061
  _FORWARDMESSAGETODEADLETTERQUEUEREQUEST._serialized_start=2064
  _FORWARDMESSAGETODEADLETTERQUEUEREQUEST._serialized_end=2295
  _FORWARDMESSAGETODEADLETTERQUEUERESPONSE._serialized_start=2297
  _FORWARDMESSAGETODEADLETTERQUEUERESPONSE._serialized_end=2382
  _HEARTBEATREQUEST._serialized_start=2385
  _HEARTBEATREQUEST._serialized_end=2516
  _HEARTBEATRESPONSE._serialized_start=2518
  _HEARTBEATRESPONSE._serialized_end=2581
  _ENDTRANSACTIONREQUEST._serialized_start=2584
  _ENDTRANSACTIONREQUEST._serialized_end=2837
  _ENDTRANSACTIONRESPONSE._serialized_start=2839
  _ENDTRANSACTIONRESPONSE._serialized_end=2907
  _PRINTTHREADSTACKTRACECOMMAND._serialized_start=2909
  _PRINTTHREADSTACKTRACECOMMAND._serialized_end=2954
  _THREADSTACKTRACE._serialized_start=2956
  _THREADSTACKTRACE._serialized_end=3045
  _VERIFYMESSAGECOMMAND._serialized_start=3047
  _VERIFYMESSAGECOMMAND._serialized_end=3130
  _VERIFYMESSAGERESULT._serialized_start=3132
  _VERIFYMESSAGERESULT._serialized_end=3168
  _RECOVERORPHANEDTRANSACTIONCOMMAND._serialized_start=3170
  _RECOVERORPHANEDTRANSACTIONCOMMAND._serialized_end=3275
  _TELEMETRYCOMMAND._serialized_start=3278
  _TELEMETRYCOMMAND._serialized_end=3832
  _NOTIFYCLIENTTERMINATIONREQUEST._serialized_start=3834
  _NOTIFYCLIENTTERMINATIONREQUEST._serialized_end=3926
  _NOTIFYCLIENTTERMINATIONRESPONSE._serialized_start=3928
  _NOTIFYCLIENTTERMINATIONRESPONSE._serialized_end=4005
  _CHANGEINVISIBLEDURATIONREQUEST._serialized_start=4008
  _CHANGEINVISIBLEDURATIONREQUEST._serialized_end=4229
  _CHANGEINVISIBLEDURATIONRESPONSE._serialized_start=4231
  _CHANGEINVISIBLEDURATIONRESPONSE._serialized_end=4332
  _MESSAGINGSERVICE._serialized_start=4335
  _MESSAGINGSERVICE._serialized_end=5583
# @@protoc_insertion_point(module_scope)
