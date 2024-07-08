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

using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.Rocketmq;
using Org.Apache.Rocketmq.Error;
using Proto = Apache.Rocketmq.V2;

namespace tests
{
    [TestClass]
    public class StatusCheckerTests
    {
        [TestMethod]
        public void TestCheckStatusOk()
        {
            var status = new Proto.Status { Code = Proto.Code.Ok, Message = "OK" };
            var request = new Proto.ReceiveMessageRequest();
            var requestId = "requestId";

            Exception exception = null;
            try
            {
                StatusChecker.Check(status, request, requestId);
            }
            catch (Exception ex)
            {
                exception = ex;
            }

            Assert.IsNull(exception, "Expected no exception to be thrown, but got: " + exception);
        }

        [TestMethod]
        public void TestCheckStatusMultipleResults()
        {
            var status = new Proto.Status { Code = Proto.Code.MultipleResults, Message = "Multiple Results" };
            var request = new Proto.ReceiveMessageRequest();
            var requestId = "requestId";

            Exception exception = null;
            try
            {
                StatusChecker.Check(status, request, requestId);
            }
            catch (Exception ex)
            {
                exception = ex;
            }

            Assert.IsNull(exception, "Expected no exception to be thrown, but got: " + exception);
        }

        [TestMethod]
        public void TestCheckStatusBadRequest()
        {
            var status = new Proto.Status { Code = Proto.Code.BadRequest, Message = "Bad Request" };
            var request = new Proto.ReceiveMessageRequest();
            var requestId = "requestId";

            Assert.ThrowsException<BadRequestException>(() => StatusChecker.Check(status, request, requestId));
        }

        [TestMethod]
        public void TestCheckStatusUnauthorized()
        {
            var status = new Proto.Status { Code = Proto.Code.Unauthorized, Message = "Unauthorized" };
            var request = new Proto.ReceiveMessageRequest();
            var requestId = "requestId";

            Assert.ThrowsException<UnauthorizedException>(() => StatusChecker.Check(status, request, requestId));
        }

        [TestMethod]
        public void TestCheckStatusPaymentRequired()
        {
            var status = new Proto.Status { Code = Proto.Code.PaymentRequired, Message = "Payment Required" };
            var request = new Proto.ReceiveMessageRequest();
            var requestId = "requestId";

            Assert.ThrowsException<PaymentRequiredException>(() => StatusChecker.Check(status, request, requestId));
        }

        [TestMethod]
        public void TestCheckStatusForbidden()
        {
            var status = new Proto.Status { Code = Proto.Code.Forbidden, Message = "Forbidden" };
            var request = new Proto.ReceiveMessageRequest();
            var requestId = "requestId";

            Assert.ThrowsException<ForbiddenException>(() => StatusChecker.Check(status, request, requestId));
        }

        [TestMethod]
        public void TestCheckStatusMessageNotFoundForNonReceiveRequest()
        {
            var status = new Proto.Status { Code = Proto.Code.MessageNotFound, Message = "Message Not Found" };
            var request = new Proto.SendMessageRequest();
            var requestId = "requestId";

            Assert.ThrowsException<NotFoundException>(() => StatusChecker.Check(status, request, requestId));
        }

        [TestMethod]
        public void TestCheckStatusNotFound()
        {
            var status = new Proto.Status { Code = Proto.Code.NotFound, Message = "Not Found" };
            var request = new Proto.ReceiveMessageRequest();
            var requestId = "requestId";

            Assert.ThrowsException<NotFoundException>(() => StatusChecker.Check(status, request, requestId));
        }

        [TestMethod]
        public void TestCheckStatusPayloadTooLarge()
        {
            var status = new Proto.Status { Code = Proto.Code.PayloadTooLarge, Message = "Payload Too Large" };
            var request = new Proto.ReceiveMessageRequest();
            var requestId = "requestId";

            Assert.ThrowsException<PayloadTooLargeException>(() => StatusChecker.Check(status, request, requestId));
        }

        [TestMethod]
        public void TestCheckStatusTooManyRequests()
        {
            var status = new Proto.Status { Code = Proto.Code.TooManyRequests, Message = "Too Many Requests" };
            var request = new Proto.ReceiveMessageRequest();
            var requestId = "requestId";

            Assert.ThrowsException<TooManyRequestsException>(() => StatusChecker.Check(status, request, requestId));
        }

        [TestMethod]
        public void TestCheckStatusRequestHeaderFieldsTooLarge()
        {
            var status = new Proto.Status { Code = Proto.Code.RequestHeaderFieldsTooLarge, Message = "Request Header Fields Too Large" };
            var request = new Proto.ReceiveMessageRequest();
            var requestId = "requestId";

            Assert.ThrowsException<RequestHeaderFieldsTooLargeException>(() => StatusChecker.Check(status, request, requestId));
        }

        [TestMethod]
        public void TestCheckStatusInternalError()
        {
            var status = new Proto.Status { Code = Proto.Code.InternalError, Message = "Internal Error" };
            var request = new Proto.ReceiveMessageRequest();
            var requestId = "requestId";

            Assert.ThrowsException<InternalErrorException>(() => StatusChecker.Check(status, request, requestId));
        }

        [TestMethod]
        public void TestCheckStatusProxyTimeout()
        {
            var status = new Proto.Status { Code = Proto.Code.ProxyTimeout, Message = "Proxy Timeout" };
            var request = new Proto.ReceiveMessageRequest();
            var requestId = "requestId";

            Assert.ThrowsException<ProxyTimeoutException>(() => StatusChecker.Check(status, request, requestId));
        }

        [TestMethod]
        public void TestCheckStatusUnsupported()
        {
            var status = new Proto.Status { Code = Proto.Code.Unsupported, Message = "Unsupported" };
            var request = new Proto.ReceiveMessageRequest();
            var requestId = "requestId";

            Assert.ThrowsException<UnsupportedException>(() => StatusChecker.Check(status, request, requestId));
        }

        [TestMethod]
        public void TestCheckStatusUnrecognized()
        {
            var status = new Proto.Status { Code = (Proto.Code)999, Message = "Unrecognized" };
            var request = new Proto.ReceiveMessageRequest();
            var requestId = "requestId";

            Assert.ThrowsException<UnsupportedException>(() => StatusChecker.Check(status, request, requestId));
        }
    }
}