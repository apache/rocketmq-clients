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

# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: apache/rocketmq/v2/definition.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2
from google.protobuf import duration_pb2 as google_dot_protobuf_dot_duration__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n#apache/rocketmq/v2/definition.proto\x12\x12\x61pache.rocketmq.v2\x1a\x1fgoogle/protobuf/timestamp.proto\x1a\x1egoogle/protobuf/duration.proto\"T\n\x10\x46ilterExpression\x12,\n\x04type\x18\x01 \x01(\x0e\x32\x1e.apache.rocketmq.v2.FilterType\x12\x12\n\nexpression\x18\x02 \x01(\t\"\xbb\x01\n\x0bRetryPolicy\x12\x14\n\x0cmax_attempts\x18\x01 \x01(\x05\x12\x45\n\x13\x65xponential_backoff\x18\x02 \x01(\x0b\x32&.apache.rocketmq.v2.ExponentialBackoffH\x00\x12\x43\n\x12\x63ustomized_backoff\x18\x03 \x01(\x0b\x32%.apache.rocketmq.v2.CustomizedBackoffH\x00\x42\n\n\x08strategy\"|\n\x12\x45xponentialBackoff\x12*\n\x07initial\x18\x01 \x01(\x0b\x32\x19.google.protobuf.Duration\x12&\n\x03max\x18\x02 \x01(\x0b\x32\x19.google.protobuf.Duration\x12\x12\n\nmultiplier\x18\x03 \x01(\x02\"<\n\x11\x43ustomizedBackoff\x12\'\n\x04next\x18\x01 \x03(\x0b\x32\x19.google.protobuf.Duration\"4\n\x08Resource\x12\x1a\n\x12resource_namespace\x18\x01 \x01(\t\x12\x0c\n\x04name\x18\x02 \x01(\t\"z\n\x11SubscriptionEntry\x12+\n\x05topic\x18\x01 \x01(\x0b\x32\x1c.apache.rocketmq.v2.Resource\x12\x38\n\nexpression\x18\x02 \x01(\x0b\x32$.apache.rocketmq.v2.FilterExpression\"%\n\x07\x41\x64\x64ress\x12\x0c\n\x04host\x18\x01 \x01(\t\x12\x0c\n\x04port\x18\x02 \x01(\x05\"n\n\tEndpoints\x12\x31\n\x06scheme\x18\x01 \x01(\x0e\x32!.apache.rocketmq.v2.AddressScheme\x12.\n\taddresses\x18\x02 \x03(\x0b\x32\x1b.apache.rocketmq.v2.Address\"T\n\x06\x42roker\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\n\n\x02id\x18\x02 \x01(\x05\x12\x30\n\tendpoints\x18\x03 \x01(\x0b\x32\x1d.apache.rocketmq.v2.Endpoints\"\xe6\x01\n\x0cMessageQueue\x12+\n\x05topic\x18\x01 \x01(\x0b\x32\x1c.apache.rocketmq.v2.Resource\x12\n\n\x02id\x18\x02 \x01(\x05\x12\x32\n\npermission\x18\x03 \x01(\x0e\x32\x1e.apache.rocketmq.v2.Permission\x12*\n\x06\x62roker\x18\x04 \x01(\x0b\x32\x1a.apache.rocketmq.v2.Broker\x12=\n\x14\x61\x63\x63\x65pt_message_types\x18\x05 \x03(\x0e\x32\x1f.apache.rocketmq.v2.MessageType\"H\n\x06\x44igest\x12,\n\x04type\x18\x01 \x01(\x0e\x32\x1e.apache.rocketmq.v2.DigestType\x12\x10\n\x08\x63hecksum\x18\x02 \x01(\t\"\x8f\x08\n\x10SystemProperties\x12\x10\n\x03tag\x18\x01 \x01(\tH\x00\x88\x01\x01\x12\x0c\n\x04keys\x18\x02 \x03(\t\x12\x12\n\nmessage_id\x18\x03 \x01(\t\x12/\n\x0b\x62ody_digest\x18\x04 \x01(\x0b\x32\x1a.apache.rocketmq.v2.Digest\x12\x33\n\rbody_encoding\x18\x05 \x01(\x0e\x32\x1c.apache.rocketmq.v2.Encoding\x12\x35\n\x0cmessage_type\x18\x06 \x01(\x0e\x32\x1f.apache.rocketmq.v2.MessageType\x12\x32\n\x0e\x62orn_timestamp\x18\x07 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x11\n\tborn_host\x18\x08 \x01(\t\x12\x38\n\x0fstore_timestamp\x18\t \x01(\x0b\x32\x1a.google.protobuf.TimestampH\x01\x88\x01\x01\x12\x12\n\nstore_host\x18\n \x01(\t\x12;\n\x12\x64\x65livery_timestamp\x18\x0b \x01(\x0b\x32\x1a.google.protobuf.TimestampH\x02\x88\x01\x01\x12\x1b\n\x0ereceipt_handle\x18\x0c \x01(\tH\x03\x88\x01\x01\x12\x10\n\x08queue_id\x18\r \x01(\x05\x12\x19\n\x0cqueue_offset\x18\x0e \x01(\x03H\x04\x88\x01\x01\x12:\n\x12invisible_duration\x18\x0f \x01(\x0b\x32\x19.google.protobuf.DurationH\x05\x88\x01\x01\x12\x1d\n\x10\x64\x65livery_attempt\x18\x10 \x01(\x05H\x06\x88\x01\x01\x12\x1a\n\rmessage_group\x18\x11 \x01(\tH\x07\x88\x01\x01\x12\x1a\n\rtrace_context\x18\x12 \x01(\tH\x08\x88\x01\x01\x12N\n&orphaned_transaction_recovery_duration\x18\x13 \x01(\x0b\x32\x19.google.protobuf.DurationH\t\x88\x01\x01\x12\x43\n\x11\x64\x65\x61\x64_letter_queue\x18\x14 \x01(\x0b\x32#.apache.rocketmq.v2.DeadLetterQueueH\n\x88\x01\x01\x42\x06\n\x04_tagB\x12\n\x10_store_timestampB\x15\n\x13_delivery_timestampB\x11\n\x0f_receipt_handleB\x0f\n\r_queue_offsetB\x15\n\x13_invisible_durationB\x13\n\x11_delivery_attemptB\x10\n\x0e_message_groupB\x10\n\x0e_trace_contextB)\n\'_orphaned_transaction_recovery_durationB\x14\n\x12_dead_letter_queue\"4\n\x0f\x44\x65\x61\x64LetterQueue\x12\r\n\x05topic\x18\x01 \x01(\t\x12\x12\n\nmessage_id\x18\x02 \x01(\t\"\x86\x02\n\x07Message\x12+\n\x05topic\x18\x01 \x01(\x0b\x32\x1c.apache.rocketmq.v2.Resource\x12H\n\x0fuser_properties\x18\x02 \x03(\x0b\x32/.apache.rocketmq.v2.Message.UserPropertiesEntry\x12?\n\x11system_properties\x18\x03 \x01(\x0b\x32$.apache.rocketmq.v2.SystemProperties\x12\x0c\n\x04\x62ody\x18\x04 \x01(\x0c\x1a\x35\n\x13UserPropertiesEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"E\n\nAssignment\x12\x37\n\rmessage_queue\x18\x01 \x01(\x0b\x32 .apache.rocketmq.v2.MessageQueue\"A\n\x06Status\x12&\n\x04\x63ode\x18\x01 \x01(\x0e\x32\x18.apache.rocketmq.v2.Code\x12\x0f\n\x07message\x18\x02 \x01(\t\"i\n\x02UA\x12.\n\x08language\x18\x01 \x01(\x0e\x32\x1c.apache.rocketmq.v2.Language\x12\x0f\n\x07version\x18\x02 \x01(\t\x12\x10\n\x08platform\x18\x03 \x01(\t\x12\x10\n\x08hostname\x18\x04 \x01(\t\"\x90\x04\n\x08Settings\x12\x38\n\x0b\x63lient_type\x18\x01 \x01(\x0e\x32\x1e.apache.rocketmq.v2.ClientTypeH\x01\x88\x01\x01\x12\x38\n\x0c\x61\x63\x63\x65ss_point\x18\x02 \x01(\x0b\x32\x1d.apache.rocketmq.v2.EndpointsH\x02\x88\x01\x01\x12<\n\x0e\x62\x61\x63koff_policy\x18\x03 \x01(\x0b\x32\x1f.apache.rocketmq.v2.RetryPolicyH\x03\x88\x01\x01\x12\x37\n\x0frequest_timeout\x18\x04 \x01(\x0b\x32\x19.google.protobuf.DurationH\x04\x88\x01\x01\x12\x34\n\npublishing\x18\x05 \x01(\x0b\x32\x1e.apache.rocketmq.v2.PublishingH\x00\x12\x38\n\x0csubscription\x18\x06 \x01(\x0b\x32 .apache.rocketmq.v2.SubscriptionH\x00\x12*\n\nuser_agent\x18\x07 \x01(\x0b\x32\x16.apache.rocketmq.v2.UA\x12*\n\x06metric\x18\x08 \x01(\x0b\x32\x1a.apache.rocketmq.v2.MetricB\t\n\x07pub_subB\x0e\n\x0c_client_typeB\x0f\n\r_access_pointB\x11\n\x0f_backoff_policyB\x12\n\x10_request_timeout\"p\n\nPublishing\x12,\n\x06topics\x18\x01 \x03(\x0b\x32\x1c.apache.rocketmq.v2.Resource\x12\x15\n\rmax_body_size\x18\x02 \x01(\x05\x12\x1d\n\x15validate_message_type\x18\x03 \x01(\x08\"\xb3\x02\n\x0cSubscription\x12\x30\n\x05group\x18\x01 \x01(\x0b\x32\x1c.apache.rocketmq.v2.ResourceH\x00\x88\x01\x01\x12<\n\rsubscriptions\x18\x02 \x03(\x0b\x32%.apache.rocketmq.v2.SubscriptionEntry\x12\x11\n\x04\x66ifo\x18\x03 \x01(\x08H\x01\x88\x01\x01\x12\x1f\n\x12receive_batch_size\x18\x04 \x01(\x05H\x02\x88\x01\x01\x12<\n\x14long_polling_timeout\x18\x05 \x01(\x0b\x32\x19.google.protobuf.DurationH\x03\x88\x01\x01\x42\x08\n\x06_groupB\x07\n\x05_fifoB\x15\n\x13_receive_batch_sizeB\x17\n\x15_long_polling_timeout\"Y\n\x06Metric\x12\n\n\x02on\x18\x01 \x01(\x08\x12\x35\n\tendpoints\x18\x02 \x01(\x0b\x32\x1d.apache.rocketmq.v2.EndpointsH\x00\x88\x01\x01\x42\x0c\n\n_endpoints*Y\n\x15TransactionResolution\x12&\n\"TRANSACTION_RESOLUTION_UNSPECIFIED\x10\x00\x12\n\n\x06\x43OMMIT\x10\x01\x12\x0c\n\x08ROLLBACK\x10\x02*W\n\x11TransactionSource\x12\x16\n\x12SOURCE_UNSPECIFIED\x10\x00\x12\x11\n\rSOURCE_CLIENT\x10\x01\x12\x17\n\x13SOURCE_SERVER_CHECK\x10\x02*W\n\nPermission\x12\x1a\n\x16PERMISSION_UNSPECIFIED\x10\x00\x12\x08\n\x04NONE\x10\x01\x12\x08\n\x04READ\x10\x02\x12\t\n\x05WRITE\x10\x03\x12\x0e\n\nREAD_WRITE\x10\x04*;\n\nFilterType\x12\x1b\n\x17\x46ILTER_TYPE_UNSPECIFIED\x10\x00\x12\x07\n\x03TAG\x10\x01\x12\x07\n\x03SQL\x10\x02*T\n\rAddressScheme\x12\x1e\n\x1a\x41\x44\x44RESS_SCHEME_UNSPECIFIED\x10\x00\x12\x08\n\x04IPv4\x10\x01\x12\x08\n\x04IPv6\x10\x02\x12\x0f\n\x0b\x44OMAIN_NAME\x10\x03*]\n\x0bMessageType\x12\x1c\n\x18MESSAGE_TYPE_UNSPECIFIED\x10\x00\x12\n\n\x06NORMAL\x10\x01\x12\x08\n\x04\x46IFO\x10\x02\x12\t\n\x05\x44\x45LAY\x10\x03\x12\x0f\n\x0bTRANSACTION\x10\x04*G\n\nDigestType\x12\x1b\n\x17\x44IGEST_TYPE_UNSPECIFIED\x10\x00\x12\t\n\x05\x43RC32\x10\x01\x12\x07\n\x03MD5\x10\x02\x12\x08\n\x04SHA1\x10\x03*_\n\nClientType\x12\x1b\n\x17\x43LIENT_TYPE_UNSPECIFIED\x10\x00\x12\x0c\n\x08PRODUCER\x10\x01\x12\x11\n\rPUSH_CONSUMER\x10\x02\x12\x13\n\x0fSIMPLE_CONSUMER\x10\x03*<\n\x08\x45ncoding\x12\x18\n\x14\x45NCODING_UNSPECIFIED\x10\x00\x12\x0c\n\x08IDENTITY\x10\x01\x12\x08\n\x04GZIP\x10\x02*\xfe\t\n\x04\x43ode\x12\x14\n\x10\x43ODE_UNSPECIFIED\x10\x00\x12\x08\n\x02OK\x10\xa0\x9c\x01\x12\x16\n\x10MULTIPLE_RESULTS\x10\xb0\xea\x01\x12\x11\n\x0b\x42\x41\x44_REQUEST\x10\xc0\xb8\x02\x12\x1a\n\x14ILLEGAL_ACCESS_POINT\x10\xc1\xb8\x02\x12\x13\n\rILLEGAL_TOPIC\x10\xc2\xb8\x02\x12\x1c\n\x16ILLEGAL_CONSUMER_GROUP\x10\xc3\xb8\x02\x12\x19\n\x13ILLEGAL_MESSAGE_TAG\x10\xc4\xb8\x02\x12\x19\n\x13ILLEGAL_MESSAGE_KEY\x10\xc5\xb8\x02\x12\x1b\n\x15ILLEGAL_MESSAGE_GROUP\x10\xc6\xb8\x02\x12\"\n\x1cILLEGAL_MESSAGE_PROPERTY_KEY\x10\xc7\xb8\x02\x12\x1c\n\x16INVALID_TRANSACTION_ID\x10\xc8\xb8\x02\x12\x18\n\x12ILLEGAL_MESSAGE_ID\x10\xc9\xb8\x02\x12\x1f\n\x19ILLEGAL_FILTER_EXPRESSION\x10\xca\xb8\x02\x12\x1c\n\x16ILLEGAL_INVISIBLE_TIME\x10\xcb\xb8\x02\x12\x1b\n\x15ILLEGAL_DELIVERY_TIME\x10\xcc\xb8\x02\x12\x1c\n\x16INVALID_RECEIPT_HANDLE\x10\xcd\xb8\x02\x12)\n#MESSAGE_PROPERTY_CONFLICT_WITH_TYPE\x10\xce\xb8\x02\x12\x1e\n\x18UNRECOGNIZED_CLIENT_TYPE\x10\xcf\xb8\x02\x12\x17\n\x11MESSAGE_CORRUPTED\x10\xd0\xb8\x02\x12\x18\n\x12\x43LIENT_ID_REQUIRED\x10\xd1\xb8\x02\x12\x1a\n\x14ILLEGAL_POLLING_TIME\x10\xd2\xb8\x02\x12\x12\n\x0cUNAUTHORIZED\x10\xa4\xb9\x02\x12\x16\n\x10PAYMENT_REQUIRED\x10\x88\xba\x02\x12\x0f\n\tFORBIDDEN\x10\xec\xba\x02\x12\x0f\n\tNOT_FOUND\x10\xd0\xbb\x02\x12\x17\n\x11MESSAGE_NOT_FOUND\x10\xd1\xbb\x02\x12\x15\n\x0fTOPIC_NOT_FOUND\x10\xd2\xbb\x02\x12\x1e\n\x18\x43ONSUMER_GROUP_NOT_FOUND\x10\xd3\xbb\x02\x12\x15\n\x0fREQUEST_TIMEOUT\x10\xe0\xbe\x02\x12\x17\n\x11PAYLOAD_TOO_LARGE\x10\xd4\xc2\x02\x12\x1c\n\x16MESSAGE_BODY_TOO_LARGE\x10\xd5\xc2\x02\x12\x19\n\x13PRECONDITION_FAILED\x10\xb0\xce\x02\x12\x17\n\x11TOO_MANY_REQUESTS\x10\x94\xcf\x02\x12%\n\x1fREQUEST_HEADER_FIELDS_TOO_LARGE\x10\xdc\xd0\x02\x12\"\n\x1cMESSAGE_PROPERTIES_TOO_LARGE\x10\xdd\xd0\x02\x12\x14\n\x0eINTERNAL_ERROR\x10\xd0\x86\x03\x12\x1b\n\x15INTERNAL_SERVER_ERROR\x10\xd1\x86\x03\x12\x16\n\x10HA_NOT_AVAILABLE\x10\xd2\x86\x03\x12\x15\n\x0fNOT_IMPLEMENTED\x10\xb4\x87\x03\x12\x13\n\rPROXY_TIMEOUT\x10\xe0\x89\x03\x12 \n\x1aMASTER_PERSISTENCE_TIMEOUT\x10\xe1\x89\x03\x12\x1f\n\x19SLAVE_PERSISTENCE_TIMEOUT\x10\xe2\x89\x03\x12\x11\n\x0bUNSUPPORTED\x10\xc4\x8a\x03\x12\x19\n\x13VERSION_UNSUPPORTED\x10\xc5\x8a\x03\x12%\n\x1fVERIFY_FIFO_MESSAGE_UNSUPPORTED\x10\xc6\x8a\x03\x12\x1f\n\x19\x46\x41ILED_TO_CONSUME_MESSAGE\x10\xe0\xd4\x03*\xad\x01\n\x08Language\x12\x18\n\x14LANGUAGE_UNSPECIFIED\x10\x00\x12\x08\n\x04JAVA\x10\x01\x12\x07\n\x03\x43PP\x10\x02\x12\x0b\n\x07\x44OT_NET\x10\x03\x12\n\n\x06GOLANG\x10\x04\x12\x08\n\x04RUST\x10\x05\x12\n\n\x06PYTHON\x10\x06\x12\x07\n\x03PHP\x10\x07\x12\x0b\n\x07NODE_JS\x10\x08\x12\x08\n\x04RUBY\x10\t\x12\x0f\n\x0bOBJECTIVE_C\x10\n\x12\x08\n\x04\x44\x41RT\x10\x0b\x12\n\n\x06KOTLIN\x10\x0c\x42;\n\x12\x61pache.rocketmq.v2B\x08MQDomainP\x01\xa0\x01\x01\xd8\x01\x01\xaa\x02\x12\x41pache.Rocketmq.V2b\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'apache.rocketmq.v2.definition_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  DESCRIPTOR._serialized_options = b'\n\022apache.rocketmq.v2B\010MQDomainP\001\240\001\001\330\001\001\252\002\022Apache.Rocketmq.V2'
  _MESSAGE_USERPROPERTIESENTRY._options = None
  _MESSAGE_USERPROPERTIESENTRY._serialized_options = b'8\001'
  _TRANSACTIONRESOLUTION._serialized_start=3962
  _TRANSACTIONRESOLUTION._serialized_end=4051
  _TRANSACTIONSOURCE._serialized_start=4053
  _TRANSACTIONSOURCE._serialized_end=4140
  _PERMISSION._serialized_start=4142
  _PERMISSION._serialized_end=4229
  _FILTERTYPE._serialized_start=4231
  _FILTERTYPE._serialized_end=4290
  _ADDRESSSCHEME._serialized_start=4292
  _ADDRESSSCHEME._serialized_end=4376
  _MESSAGETYPE._serialized_start=4378
  _MESSAGETYPE._serialized_end=4471
  _DIGESTTYPE._serialized_start=4473
  _DIGESTTYPE._serialized_end=4544
  _CLIENTTYPE._serialized_start=4546
  _CLIENTTYPE._serialized_end=4641
  _ENCODING._serialized_start=4643
  _ENCODING._serialized_end=4703
  _CODE._serialized_start=4706
  _CODE._serialized_end=5984
  _LANGUAGE._serialized_start=5987
  _LANGUAGE._serialized_end=6160
  _FILTEREXPRESSION._serialized_start=124
  _FILTEREXPRESSION._serialized_end=208
  _RETRYPOLICY._serialized_start=211
  _RETRYPOLICY._serialized_end=398
  _EXPONENTIALBACKOFF._serialized_start=400
  _EXPONENTIALBACKOFF._serialized_end=524
  _CUSTOMIZEDBACKOFF._serialized_start=526
  _CUSTOMIZEDBACKOFF._serialized_end=586
  _RESOURCE._serialized_start=588
  _RESOURCE._serialized_end=640
  _SUBSCRIPTIONENTRY._serialized_start=642
  _SUBSCRIPTIONENTRY._serialized_end=764
  _ADDRESS._serialized_start=766
  _ADDRESS._serialized_end=803
  _ENDPOINTS._serialized_start=805
  _ENDPOINTS._serialized_end=915
  _BROKER._serialized_start=917
  _BROKER._serialized_end=1001
  _MESSAGEQUEUE._serialized_start=1004
  _MESSAGEQUEUE._serialized_end=1234
  _DIGEST._serialized_start=1236
  _DIGEST._serialized_end=1308
  _SYSTEMPROPERTIES._serialized_start=1311
  _SYSTEMPROPERTIES._serialized_end=2350
  _DEADLETTERQUEUE._serialized_start=2352
  _DEADLETTERQUEUE._serialized_end=2404
  _MESSAGE._serialized_start=2407
  _MESSAGE._serialized_end=2669
  _MESSAGE_USERPROPERTIESENTRY._serialized_start=2616
  _MESSAGE_USERPROPERTIESENTRY._serialized_end=2669
  _ASSIGNMENT._serialized_start=2671
  _ASSIGNMENT._serialized_end=2740
  _STATUS._serialized_start=2742
  _STATUS._serialized_end=2807
  _UA._serialized_start=2809
  _UA._serialized_end=2914
  _SETTINGS._serialized_start=2917
  _SETTINGS._serialized_end=3445
  _PUBLISHING._serialized_start=3447
  _PUBLISHING._serialized_end=3559
  _SUBSCRIPTION._serialized_start=3562
  _SUBSCRIPTION._serialized_end=3869
  _METRIC._serialized_start=3871
  _METRIC._serialized_end=3960
# @@protoc_insertion_point(module_scope)
