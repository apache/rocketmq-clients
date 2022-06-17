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
#include "MetadataConstants.h"

ROCKETMQ_NAMESPACE_BEGIN

#ifndef CLIENT_VERSION_MAJOR
#define CLIENT_VERSION_MAJOR "5"
#endif

#ifndef CLIENT_VERSION_MINOR
#define CLIENT_VERSION_MINOR "0"
#endif

#ifndef CLIENT_VERSION_PATCH
#define CLIENT_VERSION_PATCH "0"
#endif

const char* MetadataConstants::CLIENT_VERSION = CLIENT_VERSION_MAJOR "." CLIENT_VERSION_MINOR "." CLIENT_VERSION_PATCH;

const char* MetadataConstants::TENANT_ID_KEY        = "x-mq-tenant-id";
const char* MetadataConstants::CLIENT_ID_KEY        = "x-mq-client-id";
const char* MetadataConstants::NAMESPACE_KEY        = "x-mq-namespace";
const char* MetadataConstants::AUTHORIZATION        = "authorization";
const char* MetadataConstants::STS_SESSION_TOKEN    = "x-mq-session-token";
const char* MetadataConstants::DATE_TIME_KEY        = "x-mq-date-time";
const char* MetadataConstants::ALGORITHM_KEY        = "MQv2-HMAC-SHA1";
const char* MetadataConstants::CREDENTIAL_KEY       = "Credential";
const char* MetadataConstants::SIGNED_HEADERS_KEY   = "SignedHeaders";
const char* MetadataConstants::SIGNATURE_KEY        = "Signature";
const char* MetadataConstants::DATE_TIME_FORMAT     = "%Y%m%dT%H%M%SZ";
const char* MetadataConstants::LANGUAGE_KEY         = "x-mq-language";
const char* MetadataConstants::CLIENT_VERSION_KEY   = "x-mq-client-version";
const char* MetadataConstants::PROTOCOL_VERSION_KEY = "x-mq-protocol-version";
const char* MetadataConstants::REQUEST_ID_KEY       = "x-mq-request-id";
const char* MetadataConstants::SERVICE_NAME         = "RocketMQ";

ROCKETMQ_NAMESPACE_END