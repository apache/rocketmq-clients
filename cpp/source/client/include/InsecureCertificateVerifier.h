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
#include "grpcpp/security/tls_certificate_verifier.h"
#include "rocketmq/RocketMQ.h"

ROCKETMQ_NAMESPACE_BEGIN

class InsecureCertificateVerifier : public grpc::experimental::ExternalCertificateVerifier {
public:
  // The verification logic that will be performed after the TLS handshake
  // completes. Implementers can choose to do their checks synchronously or
  // asynchronously.
  //
  // request: the verification information associated with this request
  // callback: This should only be used if your check is done asynchronously.
  //           When the asynchronous work is done, invoke this callback function
  //           with the proper status, indicating the success or the failure of
  //           the check. The implementer MUST NOT invoke this |callback| in the
  //           same thread before Verify() returns, otherwise it can lead to
  //           deadlocks.
  // sync_status: This should only be used if your check is done synchronously.
  //              Modifies this value to indicate the success or the failure of
  //              the check.
  // return: return true if your check is done synchronously, otherwise return
  //         false
  bool Verify(grpc::experimental::TlsCustomVerificationCheckRequest* request,
              std::function<void(grpc::Status)>                      callback,
              grpc::Status*                                          sync_status) override {
    *sync_status = grpc::Status(grpc::StatusCode::OK, "");
    return true;
  }

  // Cancels a verification request previously started via Verify().
  // Used when the connection attempt times out or is cancelled while an async
  // verification request is pending. The implementation should abort whatever
  // async operation it is waiting for and quickly invoke the callback that was
  // passed to Verify() with a status indicating the cancellation.
  //
  // request: the verification information associated with this request
  void Cancel(grpc::experimental::TlsCustomVerificationCheckRequest* request) override {
  }
};

ROCKETMQ_NAMESPACE_END