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
#include "UtilAll.h"

#include "rocketmq/Logger.h"
#include "spdlog/spdlog.h"
#include "zlib.h"
#include <cstring>

#if defined(__APPLE__)
#include <net/if_dl.h>
#define AF_FAMILY AF_LINK
#elif defined(__linux__)
#include <arpa/inet.h>
#include <net/ethernet.h>
#include <netpacket/packet.h>
#define AF_FAMILY AF_PACKET
#elif defined(_WIN32)
#include <Windows.h>
#include <iphlpapi.h>
#pragma comment(lib, "iphlpapi.lib")
#endif

#ifndef _WIN32
#include <ifaddrs.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#endif

#include "asio.hpp"

ROCKETMQ_NAMESPACE_BEGIN

std::string UtilAll::hostname() {
  return asio::ip::host_name();
}

bool UtilAll::macAddress(std::vector<unsigned char>& mac) {
  static std::vector<unsigned char> cache;
  static bool mac_cached = false;

  if (mac_cached) {
    mac = cache;
    return true;
  }

#ifndef _WIN32
  struct ifaddrs *head = nullptr, *node;
  if (getifaddrs(&head)) {
    return false;
  }

  for (node = head; node; node = node->ifa_next) {
    if (node->ifa_addr->sa_family == AF_FAMILY) {
#if defined(__APPLE__)
      auto* ptr = reinterpret_cast<unsigned char*>(LLADDR(reinterpret_cast<struct sockaddr_dl*>(node->ifa_addr)));
#elif defined(__linux__)
      auto* ptr = reinterpret_cast<unsigned char*>(reinterpret_cast<struct sockaddr_ll*>(node->ifa_addr)->sll_addr);
#endif
      bool all_zero = true;
      for (int i = 0; i < 6; i++) {
        if (*(ptr + i) != 0) {
          all_zero = false;
          break;
        }
      }
      if (all_zero) {
        SPDLOG_TRACE("Skip MAC address of network interface {}", node->ifa_name);
        continue;
      }
      SPDLOG_DEBUG("Use MAC address of network interface {}", node->ifa_name);
      // MAC address has 48 bits
      cache.resize(6);
      memcpy(cache.data(), ptr, 6);
      mac_cached = true;
      mac = cache;
      break;
    }
  }
  freeifaddrs(head);
  return true;
#else
  PIP_ADAPTER_INFO adaptor_info;
  DWORD buf_len = sizeof(IP_ADAPTER_INFO);
  char mac_address[18];
  adaptor_info = (IP_ADAPTER_INFO*)malloc(buf_len);
  if (!adaptor_info) {
    // TODO: running out of memory
  }

  if (GetAdaptersInfo(adaptor_info, &buf_len) == NO_ERROR) {
    PIP_ADAPTER_INFO item = adaptor_info;
    do {
      bool all_zero = true;
      for (auto& b : item->Address) {
        if (b != 0) {
          all_zero = false;
          break;
        }
      }
      if (!all_zero) {
        cache.resize(6);
        memcpy(cache.data(), item->Address, 6);
        mac_cached = true;
        mac = cache;
        break;
      }
      item = item->Next;
    } while (item);
  } else {
    free(adaptor_info);
  }
#endif
}

bool UtilAll::compress(const std::string& src, std::string& dst) {
  z_stream stream;
  stream.zalloc = Z_NULL;
  stream.zfree = Z_NULL;
  stream.opaque = Z_NULL;
  stream.next_in = reinterpret_cast<unsigned char*>(const_cast<char*>(src.c_str()));
  stream.avail_in = src.length();

  deflateInit(&stream, Z_DEFAULT_COMPRESSION);
  uint32_t bound = deflateBound(&stream, src.length());
  std::vector<unsigned char> buffer(bound);
  stream.next_out = buffer.data();
  stream.avail_out = bound;

  int status = deflate(&stream, Z_FINISH);

  // zlib is unexpected wrong.
  if (Z_STREAM_END != status) {
    deflateEnd(&stream);
    return false;
  }

  dst.reserve(stream.total_out);
  assert(stream.total_out == bound - stream.avail_out);
  std::copy(buffer.data(), buffer.data() + stream.total_out, std::back_inserter(dst));
  deflateEnd(&stream);
  return true;
}

bool UtilAll::uncompress(const std::string& src, std::string& dst) {
  int status;
  uint32_t buffer_length = 4096;
  std::vector<unsigned char> buffer(buffer_length);
  z_stream stream;

  // Use default malloc / free allocator.
  stream.zalloc = Z_NULL;
  stream.zfree = Z_NULL;
  stream.opaque = Z_NULL;
  stream.next_in = reinterpret_cast<unsigned char*>(const_cast<char*>(src.c_str()));
  stream.avail_in = src.length();

  inflateInit(&stream);
  stream.next_out = buffer.data();
  stream.avail_out = buffer_length;

  while (true) {
    status = inflate(&stream, Z_SYNC_FLUSH);

    if (Z_STREAM_ERROR == status) {
      return false;
    }
    std::copy(buffer.data(), buffer.data() + buffer_length - stream.avail_out, std::back_inserter(dst));
    stream.avail_out = buffer_length;
    stream.next_out = buffer.data();

    // inflation completed OK
    if (Z_STREAM_END == status) {
      inflateEnd(&stream);
      return true;
    }

    // inflation made some progress
    if (Z_OK == status) {
      continue;
    }

    // Something is wrong
    inflateEnd(&stream);
    return false;
  }
}

ROCKETMQ_NAMESPACE_END