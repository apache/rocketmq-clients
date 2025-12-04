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


from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2
from google.protobuf import duration_pb2 as google_dot_protobuf_dot_duration__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x10\x64\x65\x66inition.proto\x12\x12\x61pache.rocketmq.v2\x1a\x1fgoogle/protobuf/timestamp.proto\x1a\x1egoogle/protobuf/duration.proto\"T\n\x10\x46ilterExpression\x12,\n\x04type\x18\x01 \x01(\x0e\x32\x1e.apache.rocketmq.v2.FilterType\x12\x12\n\nexpression\x18\x02 \x01(\t\"\xbb\x01\n\x0bRetryPolicy\x12\x14\n\x0cmax_attempts\x18\x01 \x01(\x05\x12\x45\n\x13\x65xponential_backoff\x18\x02 \x01(\x0b\x32&.apache.rocketmq.v2.ExponentialBackoffH\x00\x12\x43\n\x12\x63ustomized_backoff\x18\x03 \x01(\x0b\x32%.apache.rocketmq.v2.CustomizedBackoffH\x00\x42\n\n\x08strategy\"|\n\x12\x45xponentialBackoff\x12*\n\x07initial\x18\x01 \x01(\x0b\x32\x19.google.protobuf.Duration\x12&\n\x03max\x18\x02 \x01(\x0b\x32\x19.google.protobuf.Duration\x12\x12\n\nmultiplier\x18\x03 \x01(\x02\"<\n\x11\x43ustomizedBackoff\x12\'\n\x04next\x18\x01 \x03(\x0b\x32\x19.google.protobuf.Duration\"4\n\x08Resource\x12\x1a\n\x12resource_namespace\x18\x01 \x01(\t\x12\x0c\n\x04name\x18\x02 \x01(\t\"z\n\x11SubscriptionEntry\x12+\n\x05topic\x18\x01 \x01(\x0b\x32\x1c.apache.rocketmq.v2.Resource\x12\x38\n\nexpression\x18\x02 \x01(\x0b\x32$.apache.rocketmq.v2.FilterExpression\"%\n\x07\x41\x64\x64ress\x12\x0c\n\x04host\x18\x01 \x01(\t\x12\x0c\n\x04port\x18\x02 \x01(\x05\"n\n\tEndpoints\x12\x31\n\x06scheme\x18\x01 \x01(\x0e\x32!.apache.rocketmq.v2.AddressScheme\x12.\n\taddresses\x18\x02 \x03(\x0b\x32\x1b.apache.rocketmq.v2.Address\"T\n\x06\x42roker\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\n\n\x02id\x18\x02 \x01(\x05\x12\x30\n\tendpoints\x18\x03 \x01(\x0b\x32\x1d.apache.rocketmq.v2.Endpoints\"\xe6\x01\n\x0cMessageQueue\x12+\n\x05topic\x18\x01 \x01(\x0b\x32\x1c.apache.rocketmq.v2.Resource\x12\n\n\x02id\x18\x02 \x01(\x05\x12\x32\n\npermission\x18\x03 \x01(\x0e\x32\x1e.apache.rocketmq.v2.Permission\x12*\n\x06\x62roker\x18\x04 \x01(\x0b\x32\x1a.apache.rocketmq.v2.Broker\x12=\n\x14\x61\x63\x63\x65pt_message_types\x18\x05 \x03(\x0e\x32\x1f.apache.rocketmq.v2.MessageType\"H\n\x06\x44igest\x12,\n\x04type\x18\x01 \x01(\x0e\x32\x1e.apache.rocketmq.v2.DigestType\x12\x10\n\x08\x63hecksum\x18\x02 \x01(\t\"\x8f\x08\n\x10SystemProperties\x12\x10\n\x03tag\x18\x01 \x01(\tH\x00\x88\x01\x01\x12\x0c\n\x04keys\x18\x02 \x03(\t\x12\x12\n\nmessage_id\x18\x03 \x01(\t\x12/\n\x0b\x62ody_digest\x18\x04 \x01(\x0b\x32\x1a.apache.rocketmq.v2.Digest\x12\x33\n\rbody_encoding\x18\x05 \x01(\x0e\x32\x1c.apache.rocketmq.v2.Encoding\x12\x35\n\x0cmessage_type\x18\x06 \x01(\x0e\x32\x1f.apache.rocketmq.v2.MessageType\x12\x32\n\x0e\x62orn_timestamp\x18\x07 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x11\n\tborn_host\x18\x08 \x01(\t\x12\x38\n\x0fstore_timestamp\x18\t \x01(\x0b\x32\x1a.google.protobuf.TimestampH\x01\x88\x01\x01\x12\x12\n\nstore_host\x18\n \x01(\t\x12;\n\x12\x64\x65livery_timestamp\x18\x0b \x01(\x0b\x32\x1a.google.protobuf.TimestampH\x02\x88\x01\x01\x12\x1b\n\x0ereceipt_handle\x18\x0c \x01(\tH\x03\x88\x01\x01\x12\x10\n\x08queue_id\x18\r \x01(\x05\x12\x19\n\x0cqueue_offset\x18\x0e \x01(\x03H\x04\x88\x01\x01\x12:\n\x12invisible_duration\x18\x0f \x01(\x0b\x32\x19.google.protobuf.DurationH\x05\x88\x01\x01\x12\x1d\n\x10\x64\x65livery_attempt\x18\x10 \x01(\x05H\x06\x88\x01\x01\x12\x1a\n\rmessage_group\x18\x11 \x01(\tH\x07\x88\x01\x01\x12\x1a\n\rtrace_context\x18\x12 \x01(\tH\x08\x88\x01\x01\x12N\n&orphaned_transaction_recovery_duration\x18\x13 \x01(\x0b\x32\x19.google.protobuf.DurationH\t\x88\x01\x01\x12\x43\n\x11\x64\x65\x61\x64_letter_queue\x18\x14 \x01(\x0b\x32#.apache.rocketmq.v2.DeadLetterQueueH\n\x88\x01\x01\x42\x06\n\x04_tagB\x12\n\x10_store_timestampB\x15\n\x13_delivery_timestampB\x11\n\x0f_receipt_handleB\x0f\n\r_queue_offsetB\x15\n\x13_invisible_durationB\x13\n\x11_delivery_attemptB\x10\n\x0e_message_groupB\x10\n\x0e_trace_contextB)\n\'_orphaned_transaction_recovery_durationB\x14\n\x12_dead_letter_queue\"4\n\x0f\x44\x65\x61\x64LetterQueue\x12\r\n\x05topic\x18\x01 \x01(\t\x12\x12\n\nmessage_id\x18\x02 \x01(\t\"\x86\x02\n\x07Message\x12+\n\x05topic\x18\x01 \x01(\x0b\x32\x1c.apache.rocketmq.v2.Resource\x12H\n\x0fuser_properties\x18\x02 \x03(\x0b\x32/.apache.rocketmq.v2.Message.UserPropertiesEntry\x12?\n\x11system_properties\x18\x03 \x01(\x0b\x32$.apache.rocketmq.v2.SystemProperties\x12\x0c\n\x04\x62ody\x18\x04 \x01(\x0c\x1a\x35\n\x13UserPropertiesEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"E\n\nAssignment\x12\x37\n\rmessage_queue\x18\x01 \x01(\x0b\x32 .apache.rocketmq.v2.MessageQueue\"A\n\x06Status\x12&\n\x04\x63ode\x18\x01 \x01(\x0e\x32\x18.apache.rocketmq.v2.Code\x12\x0f\n\x07message\x18\x02 \x01(\t\"i\n\x02UA\x12.\n\x08language\x18\x01 \x01(\x0e\x32\x1c.apache.rocketmq.v2.Language\x12\x0f\n\x07version\x18\x02 \x01(\t\x12\x10\n\x08platform\x18\x03 \x01(\t\x12\x10\n\x08hostname\x18\x04 \x01(\t\"\x90\x04\n\x08Settings\x12\x38\n\x0b\x63lient_type\x18\x01 \x01(\x0e\x32\x1e.apache.rocketmq.v2.ClientTypeH\x01\x88\x01\x01\x12\x38\n\x0c\x61\x63\x63\x65ss_point\x18\x02 \x01(\x0b\x32\x1d.apache.rocketmq.v2.EndpointsH\x02\x88\x01\x01\x12<\n\x0e\x62\x61\x63koff_policy\x18\x03 \x01(\x0b\x32\x1f.apache.rocketmq.v2.RetryPolicyH\x03\x88\x01\x01\x12\x37\n\x0frequest_timeout\x18\x04 \x01(\x0b\x32\x19.google.protobuf.DurationH\x04\x88\x01\x01\x12\x34\n\npublishing\x18\x05 \x01(\x0b\x32\x1e.apache.rocketmq.v2.PublishingH\x00\x12\x38\n\x0csubscription\x18\x06 \x01(\x0b\x32 .apache.rocketmq.v2.SubscriptionH\x00\x12*\n\nuser_agent\x18\x07 \x01(\x0b\x32\x16.apache.rocketmq.v2.UA\x12*\n\x06metric\x18\x08 \x01(\x0b\x32\x1a.apache.rocketmq.v2.MetricB\t\n\x07pub_subB\x0e\n\x0c_client_typeB\x0f\n\r_access_pointB\x11\n\x0f_backoff_policyB\x12\n\x10_request_timeout\"p\n\nPublishing\x12,\n\x06topics\x18\x01 \x03(\x0b\x32\x1c.apache.rocketmq.v2.Resource\x12\x15\n\rmax_body_size\x18\x02 \x01(\x05\x12\x1d\n\x15validate_message_type\x18\x03 \x01(\x08\"\xb3\x02\n\x0cSubscription\x12\x30\n\x05group\x18\x01 \x01(\x0b\x32\x1c.apache.rocketmq.v2.ResourceH\x00\x88\x01\x01\x12<\n\rsubscriptions\x18\x02 \x03(\x0b\x32%.apache.rocketmq.v2.SubscriptionEntry\x12\x11\n\x04\x66ifo\x18\x03 \x01(\x08H\x01\x88\x01\x01\x12\x1f\n\x12receive_batch_size\x18\x04 \x01(\x05H\x02\x88\x01\x01\x12<\n\x14long_polling_timeout\x18\x05 \x01(\x0b\x32\x19.google.protobuf.DurationH\x03\x88\x01\x01\x42\x08\n\x06_groupB\x07\n\x05_fifoB\x15\n\x13_receive_batch_sizeB\x17\n\x15_long_polling_timeout\"Y\n\x06Metric\x12\n\n\x02on\x18\x01 \x01(\x08\x12\x35\n\tendpoints\x18\x02 \x01(\x0b\x32\x1d.apache.rocketmq.v2.EndpointsH\x00\x88\x01\x01\x42\x0c\n\n_endpoints*Y\n\x15TransactionResolution\x12&\n\"TRANSACTION_RESOLUTION_UNSPECIFIED\x10\x00\x12\n\n\x06\x43OMMIT\x10\x01\x12\x0c\n\x08ROLLBACK\x10\x02*W\n\x11TransactionSource\x12\x16\n\x12SOURCE_UNSPECIFIED\x10\x00\x12\x11\n\rSOURCE_CLIENT\x10\x01\x12\x17\n\x13SOURCE_SERVER_CHECK\x10\x02*W\n\nPermission\x12\x1a\n\x16PERMISSION_UNSPECIFIED\x10\x00\x12\x08\n\x04NONE\x10\x01\x12\x08\n\x04READ\x10\x02\x12\t\n\x05WRITE\x10\x03\x12\x0e\n\nREAD_WRITE\x10\x04*;\n\nFilterType\x12\x1b\n\x17\x46ILTER_TYPE_UNSPECIFIED\x10\x00\x12\x07\n\x03TAG\x10\x01\x12\x07\n\x03SQL\x10\x02*T\n\rAddressScheme\x12\x1e\n\x1a\x41\x44\x44RESS_SCHEME_UNSPECIFIED\x10\x00\x12\x08\n\x04IPv4\x10\x01\x12\x08\n\x04IPv6\x10\x02\x12\x0f\n\x0b\x44OMAIN_NAME\x10\x03*]\n\x0bMessageType\x12\x1c\n\x18MESSAGE_TYPE_UNSPECIFIED\x10\x00\x12\n\n\x06NORMAL\x10\x01\x12\x08\n\x04\x46IFO\x10\x02\x12\t\n\x05\x44\x45LAY\x10\x03\x12\x0f\n\x0bTRANSACTION\x10\x04*G\n\nDigestType\x12\x1b\n\x17\x44IGEST_TYPE_UNSPECIFIED\x10\x00\x12\t\n\x05\x43RC32\x10\x01\x12\x07\n\x03MD5\x10\x02\x12\x08\n\x04SHA1\x10\x03*r\n\nClientType\x12\x1b\n\x17\x43LIENT_TYPE_UNSPECIFIED\x10\x00\x12\x0c\n\x08PRODUCER\x10\x01\x12\x11\n\rPUSH_CONSUMER\x10\x02\x12\x13\n\x0fSIMPLE_CONSUMER\x10\x03\x12\x11\n\rPULL_CONSUMER\x10\x04*<\n\x08\x45ncoding\x12\x18\n\x14\x45NCODING_UNSPECIFIED\x10\x00\x12\x0c\n\x08IDENTITY\x10\x01\x12\x08\n\x04GZIP\x10\x02*\xc6\n\n\x04\x43ode\x12\x14\n\x10\x43ODE_UNSPECIFIED\x10\x00\x12\x08\n\x02OK\x10\xa0\x9c\x01\x12\x16\n\x10MULTIPLE_RESULTS\x10\xb0\xea\x01\x12\x11\n\x0b\x42\x41\x44_REQUEST\x10\xc0\xb8\x02\x12\x1a\n\x14ILLEGAL_ACCESS_POINT\x10\xc1\xb8\x02\x12\x13\n\rILLEGAL_TOPIC\x10\xc2\xb8\x02\x12\x1c\n\x16ILLEGAL_CONSUMER_GROUP\x10\xc3\xb8\x02\x12\x19\n\x13ILLEGAL_MESSAGE_TAG\x10\xc4\xb8\x02\x12\x19\n\x13ILLEGAL_MESSAGE_KEY\x10\xc5\xb8\x02\x12\x1b\n\x15ILLEGAL_MESSAGE_GROUP\x10\xc6\xb8\x02\x12\"\n\x1cILLEGAL_MESSAGE_PROPERTY_KEY\x10\xc7\xb8\x02\x12\x1c\n\x16INVALID_TRANSACTION_ID\x10\xc8\xb8\x02\x12\x18\n\x12ILLEGAL_MESSAGE_ID\x10\xc9\xb8\x02\x12\x1f\n\x19ILLEGAL_FILTER_EXPRESSION\x10\xca\xb8\x02\x12\x1c\n\x16ILLEGAL_INVISIBLE_TIME\x10\xcb\xb8\x02\x12\x1b\n\x15ILLEGAL_DELIVERY_TIME\x10\xcc\xb8\x02\x12\x1c\n\x16INVALID_RECEIPT_HANDLE\x10\xcd\xb8\x02\x12)\n#MESSAGE_PROPERTY_CONFLICT_WITH_TYPE\x10\xce\xb8\x02\x12\x1e\n\x18UNRECOGNIZED_CLIENT_TYPE\x10\xcf\xb8\x02\x12\x17\n\x11MESSAGE_CORRUPTED\x10\xd0\xb8\x02\x12\x18\n\x12\x43LIENT_ID_REQUIRED\x10\xd1\xb8\x02\x12\x1a\n\x14ILLEGAL_POLLING_TIME\x10\xd2\xb8\x02\x12\x14\n\x0eILLEGAL_OFFSET\x10\xd3\xb8\x02\x12\x12\n\x0cUNAUTHORIZED\x10\xa4\xb9\x02\x12\x16\n\x10PAYMENT_REQUIRED\x10\x88\xba\x02\x12\x0f\n\tFORBIDDEN\x10\xec\xba\x02\x12\x0f\n\tNOT_FOUND\x10\xd0\xbb\x02\x12\x17\n\x11MESSAGE_NOT_FOUND\x10\xd1\xbb\x02\x12\x15\n\x0fTOPIC_NOT_FOUND\x10\xd2\xbb\x02\x12\x1e\n\x18\x43ONSUMER_GROUP_NOT_FOUND\x10\xd3\xbb\x02\x12\x16\n\x10OFFSET_NOT_FOUND\x10\xd4\xbb\x02\x12\x15\n\x0fREQUEST_TIMEOUT\x10\xe0\xbe\x02\x12\x17\n\x11PAYLOAD_TOO_LARGE\x10\xd4\xc2\x02\x12\x1c\n\x16MESSAGE_BODY_TOO_LARGE\x10\xd5\xc2\x02\x12\x18\n\x12MESSAGE_BODY_EMPTY\x10\xd6\xc2\x02\x12\x19\n\x13PRECONDITION_FAILED\x10\xb0\xce\x02\x12\x17\n\x11TOO_MANY_REQUESTS\x10\x94\xcf\x02\x12%\n\x1fREQUEST_HEADER_FIELDS_TOO_LARGE\x10\xdc\xd0\x02\x12\"\n\x1cMESSAGE_PROPERTIES_TOO_LARGE\x10\xdd\xd0\x02\x12\x14\n\x0eINTERNAL_ERROR\x10\xd0\x86\x03\x12\x1b\n\x15INTERNAL_SERVER_ERROR\x10\xd1\x86\x03\x12\x16\n\x10HA_NOT_AVAILABLE\x10\xd2\x86\x03\x12\x15\n\x0fNOT_IMPLEMENTED\x10\xb4\x87\x03\x12\x13\n\rPROXY_TIMEOUT\x10\xe0\x89\x03\x12 \n\x1aMASTER_PERSISTENCE_TIMEOUT\x10\xe1\x89\x03\x12\x1f\n\x19SLAVE_PERSISTENCE_TIMEOUT\x10\xe2\x89\x03\x12\x11\n\x0bUNSUPPORTED\x10\xc4\x8a\x03\x12\x19\n\x13VERSION_UNSUPPORTED\x10\xc5\x8a\x03\x12%\n\x1fVERIFY_FIFO_MESSAGE_UNSUPPORTED\x10\xc6\x8a\x03\x12\x1f\n\x19\x46\x41ILED_TO_CONSUME_MESSAGE\x10\xe0\xd4\x03*\xad\x01\n\x08Language\x12\x18\n\x14LANGUAGE_UNSPECIFIED\x10\x00\x12\x08\n\x04JAVA\x10\x01\x12\x07\n\x03\x43PP\x10\x02\x12\x0b\n\x07\x44OT_NET\x10\x03\x12\n\n\x06GOLANG\x10\x04\x12\x08\n\x04RUST\x10\x05\x12\n\n\x06PYTHON\x10\x06\x12\x07\n\x03PHP\x10\x07\x12\x0b\n\x07NODE_JS\x10\x08\x12\x08\n\x04RUBY\x10\t\x12\x0f\n\x0bOBJECTIVE_C\x10\n\x12\x08\n\x04\x44\x41RT\x10\x0b\x12\n\n\x06KOTLIN\x10\x0c*:\n\x11QueryOffsetPolicy\x12\r\n\tBEGINNING\x10\x00\x12\x07\n\x03\x45ND\x10\x01\x12\r\n\tTIMESTAMP\x10\x02\x42;\n\x12\x61pache.rocketmq.v2B\x08MQDomainP\x01\xa0\x01\x01\xd8\x01\x01\xaa\x02\x12\x41pache.Rocketmq.V2b\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'definition_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
  _globals['DESCRIPTOR']._loaded_options = None
  _globals['DESCRIPTOR']._serialized_options = b'\n\022apache.rocketmq.v2B\010MQDomainP\001\240\001\001\330\001\001\252\002\022Apache.Rocketmq.V2'
  _globals['_MESSAGE_USERPROPERTIESENTRY']._loaded_options = None
  _globals['_MESSAGE_USERPROPERTIESENTRY']._serialized_options = b'8\001'
  _globals['_TRANSACTIONRESOLUTION']._serialized_start=3943
  _globals['_TRANSACTIONRESOLUTION']._serialized_end=4032
  _globals['_TRANSACTIONSOURCE']._serialized_start=4034
  _globals['_TRANSACTIONSOURCE']._serialized_end=4121
  _globals['_PERMISSION']._serialized_start=4123
  _globals['_PERMISSION']._serialized_end=4210
  _globals['_FILTERTYPE']._serialized_start=4212
  _globals['_FILTERTYPE']._serialized_end=4271
  _globals['_ADDRESSSCHEME']._serialized_start=4273
  _globals['_ADDRESSSCHEME']._serialized_end=4357
  _globals['_MESSAGETYPE']._serialized_start=4359
  _globals['_MESSAGETYPE']._serialized_end=4452
  _globals['_DIGESTTYPE']._serialized_start=4454
  _globals['_DIGESTTYPE']._serialized_end=4525
  _globals['_CLIENTTYPE']._serialized_start=4527
  _globals['_CLIENTTYPE']._serialized_end=4641
  _globals['_ENCODING']._serialized_start=4643
  _globals['_ENCODING']._serialized_end=4703
  _globals['_CODE']._serialized_start=4706
  _globals['_CODE']._serialized_end=6056
  _globals['_LANGUAGE']._serialized_start=6059
  _globals['_LANGUAGE']._serialized_end=6232
  _globals['_QUERYOFFSETPOLICY']._serialized_start=6234
  _globals['_QUERYOFFSETPOLICY']._serialized_end=6292
  _globals['_FILTEREXPRESSION']._serialized_start=105
  _globals['_FILTEREXPRESSION']._serialized_end=189
  _globals['_RETRYPOLICY']._serialized_start=192
  _globals['_RETRYPOLICY']._serialized_end=379
  _globals['_EXPONENTIALBACKOFF']._serialized_start=381
  _globals['_EXPONENTIALBACKOFF']._serialized_end=505
  _globals['_CUSTOMIZEDBACKOFF']._serialized_start=507
  _globals['_CUSTOMIZEDBACKOFF']._serialized_end=567
  _globals['_RESOURCE']._serialized_start=569
  _globals['_RESOURCE']._serialized_end=621
  _globals['_SUBSCRIPTIONENTRY']._serialized_start=623
  _globals['_SUBSCRIPTIONENTRY']._serialized_end=745
  _globals['_ADDRESS']._serialized_start=747
  _globals['_ADDRESS']._serialized_end=784
  _globals['_ENDPOINTS']._serialized_start=786
  _globals['_ENDPOINTS']._serialized_end=896
  _globals['_BROKER']._serialized_start=898
  _globals['_BROKER']._serialized_end=982
  _globals['_MESSAGEQUEUE']._serialized_start=985
  _globals['_MESSAGEQUEUE']._serialized_end=1215
  _globals['_DIGEST']._serialized_start=1217
  _globals['_DIGEST']._serialized_end=1289
  _globals['_SYSTEMPROPERTIES']._serialized_start=1292
  _globals['_SYSTEMPROPERTIES']._serialized_end=2331
  _globals['_DEADLETTERQUEUE']._serialized_start=2333
  _globals['_DEADLETTERQUEUE']._serialized_end=2385
  _globals['_MESSAGE']._serialized_start=2388
  _globals['_MESSAGE']._serialized_end=2650
  _globals['_MESSAGE_USERPROPERTIESENTRY']._serialized_start=2597
  _globals['_MESSAGE_USERPROPERTIESENTRY']._serialized_end=2650
  _globals['_ASSIGNMENT']._serialized_start=2652
  _globals['_ASSIGNMENT']._serialized_end=2721
  _globals['_STATUS']._serialized_start=2723
  _globals['_STATUS']._serialized_end=2788
  _globals['_UA']._serialized_start=2790
  _globals['_UA']._serialized_end=2895
  _globals['_SETTINGS']._serialized_start=2898
  _globals['_SETTINGS']._serialized_end=3426
  _globals['_PUBLISHING']._serialized_start=3428
  _globals['_PUBLISHING']._serialized_end=3540
  _globals['_SUBSCRIPTION']._serialized_start=3543
  _globals['_SUBSCRIPTION']._serialized_end=3850
  _globals['_METRIC']._serialized_start=3852
  _globals['_METRIC']._serialized_end=3941
# @@protoc_insertion_point(module_scope)
