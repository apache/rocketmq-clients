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
#include "TlsHelper.h"

#include <memory>

#include "MixAll.h"
#include "OpenSSLCompatible.h"
#include "openssl/hmac.h"

ROCKETMQ_NAMESPACE_BEGIN

std::string TlsHelper::sign(const std::string& access_secret, const std::string& content) {
  HMAC_CTX* ctx = HMAC_CTX_new();
  HMAC_Init_ex(ctx, access_secret.c_str(), access_secret.length(), EVP_sha1(), nullptr);
  HMAC_Update(ctx, reinterpret_cast<const unsigned char*>(content.c_str()), content.length());
  auto result = new unsigned char[EVP_MD_size(EVP_sha1())];
  unsigned int len;
  HMAC_Final(ctx, result, &len);
  HMAC_CTX_free(ctx);

  std::string hex_str = MixAll::hex(result, len);
  delete[] result;
  return hex_str;
}

ROCKETMQ_NAMESPACE_END
