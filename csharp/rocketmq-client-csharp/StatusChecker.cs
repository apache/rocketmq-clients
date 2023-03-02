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

using Google.Protobuf;
using NLog;
using Org.Apache.Rocketmq.Error;
using Proto = Apache.Rocketmq.V2;

namespace Org.Apache.Rocketmq
{
    public static class StatusChecker
    {
        private static readonly Logger Logger = MqLogManager.Instance.GetCurrentClassLogger();

        public static void Check(Proto.Status status, IMessage request, string requestId)
        {
            var statusCode = status.Code;

            var statusMessage = status.Message;
            switch (statusCode)
            {
                case Proto.Code.Ok:
                case Proto.Code.MultipleResults:
                    return;
                case Proto.Code.BadRequest:
                case Proto.Code.IllegalAccessPoint:
                case Proto.Code.IllegalTopic:
                case Proto.Code.IllegalConsumerGroup:
                case Proto.Code.IllegalMessageTag:
                case Proto.Code.IllegalMessageKey:
                case Proto.Code.IllegalMessageGroup:
                case Proto.Code.IllegalMessagePropertyKey:
                case Proto.Code.InvalidTransactionId:
                case Proto.Code.IllegalMessageId:
                case Proto.Code.IllegalFilterExpression:
                case Proto.Code.IllegalInvisibleTime:
                case Proto.Code.IllegalDeliveryTime:
                case Proto.Code.InvalidReceiptHandle:
                case Proto.Code.MessagePropertyConflictWithType:
                case Proto.Code.UnrecognizedClientType:
                case Proto.Code.MessageCorrupted:
                case Proto.Code.ClientIdRequired:
                case Proto.Code.IllegalPollingTime:
                    throw new BadRequestException((int)statusCode, requestId, statusMessage);
                case Proto.Code.Unauthorized:
                    throw new UnauthorizedException((int)statusCode, requestId, statusMessage);
                case Proto.Code.PaymentRequired:
                    throw new PaymentRequiredException((int)statusCode, requestId, statusMessage);
                case Proto.Code.Forbidden:
                    throw new ForbiddenException((int)statusCode, requestId, statusMessage);
                case Proto.Code.MessageNotFound:
                    if (request is Proto.ReceiveMessageRequest)
                    {
                        return;
                    }

                    // Fall through on purpose.
                    goto case Proto.Code.NotFound;
                case Proto.Code.NotFound:
                case Proto.Code.TopicNotFound:
                case Proto.Code.ConsumerGroupNotFound:
                    throw new NotFoundException((int)statusCode, requestId, statusMessage);
                case Proto.Code.PayloadTooLarge:
                case Proto.Code.MessageBodyTooLarge:
                    throw new PayloadTooLargeException((int)statusCode, requestId, statusMessage);
                case Proto.Code.TooManyRequests:
                    throw new TooManyRequestsException((int)statusCode, requestId, statusMessage);
                case Proto.Code.RequestHeaderFieldsTooLarge:
                case Proto.Code.MessagePropertiesTooLarge:
                    throw new RequestHeaderFieldsTooLargeException((int)statusCode, requestId, statusMessage);
                case Proto.Code.InternalError:
                case Proto.Code.InternalServerError:
                case Proto.Code.HaNotAvailable:
                    throw new InternalErrorException((int)statusCode, requestId, statusMessage);
                case Proto.Code.ProxyTimeout:
                case Proto.Code.MasterPersistenceTimeout:
                case Proto.Code.SlavePersistenceTimeout:
                    throw new ProxyTimeoutException((int)statusCode, requestId, statusMessage);
                case Proto.Code.Unsupported:
                case Proto.Code.VersionUnsupported:
                case Proto.Code.VerifyFifoMessageUnsupported:
                    throw new UnsupportedException((int)statusCode, requestId, statusMessage);
                // Not used code.
                case Proto.Code.RequestTimeout:
                case Proto.Code.PreconditionFailed:
                case Proto.Code.NotImplemented:
                case Proto.Code.FailedToConsumeMessage:
                case Proto.Code.Unspecified:
                default:
                    Logger.Warn($"Unrecognized status code={statusCode}, requestId={requestId}, statusMessage={statusMessage}");
                    throw new UnsupportedException((int)statusCode, requestId, statusMessage);
            }
        }
    }
}