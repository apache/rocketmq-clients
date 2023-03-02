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
use std::vec::IntoIter;
use tokio::sync::oneshot;
use tokio_stream::Iter;
use tonic::{Request, Response};

use crate::error::ClientError;
use crate::pb::{AckMessageRequest, AckMessageResponse, Message, QueryRouteRequest, QueryRouteResponse, ReceiveMessageRequest, ReceiveMessageResponse, SendMessageRequest, SendMessageResponse, Settings, TelemetryCommand};

pub(crate) enum Command {
    QueryRoute {
        peer: String,
        request: Request<QueryRouteRequest>,
        tx: oneshot::Sender<Result<Response<QueryRouteResponse>, ClientError>>,
    },
    Send {
        peer: String,
        request: Request<SendMessageRequest>,
        tx: oneshot::Sender<Result<Response<SendMessageResponse>, ClientError>>,
    },
    Receive {
        peer: String,
        request: Request<ReceiveMessageRequest>,
        tx: oneshot::Sender<Result<Response<Vec<Message>>, ClientError>>,
    },
    Ack {
        peer: String,
        request: Request<AckMessageRequest>,
        tx: oneshot::Sender<Result<Response<AckMessageResponse>, ClientError>>,
    },
    SendClientTelemetry {
        peer: String,
        request: Request<Iter<IntoIter<TelemetryCommand>>>,
        tx: oneshot::Sender<Result<Response<TelemetryCommand>, ClientError>>,
    },
}
