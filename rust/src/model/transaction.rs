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

//! Transaction data model of RocketMQ rust client.

use std::fmt::{Debug, Formatter};

use async_trait::async_trait;

use crate::error::ClientError;
use crate::model::common::SendReceipt;
use crate::model::message::MessageView;
use crate::pb::{EndTransactionRequest, Resource, TransactionSource};
use crate::session::RPCClient;
use crate::util::handle_response_status;

/// An entity to describe an independent transaction.
///
/// Once the request of commit of roll-back reached server, subsequently arrived commit or roll-back request in
/// [`Transaction`] would be ignored by the server.
///
/// If transaction is not commit/roll-back in time, it is suspended until it is solved by [`TransactionChecker`]
/// or reach the end of life.
#[async_trait]
pub trait Transaction {
    /// Try to commit the transaction, which would expose the message before the transaction is closed if no exception thrown.
    /// What you should pay more attention to is that the commitment may be successful even exception is thrown.
    async fn commit(self) -> Result<(), ClientError>;

    /// Try to roll back the transaction, which would expose the message before the transaction is closed if no exception thrown.
    /// What you should pay more attention to is that the roll-back may be successful even exception is thrown.
    async fn rollback(self) -> Result<(), ClientError>;

    /// Get message id
    fn message_id(&self) -> &str;

    /// Get transaction id
    fn transaction_id(&self) -> &str;
}

pub(crate) struct TransactionImpl {
    rpc_client: Box<dyn RPCClient + Send + Sync>,
    topic: Resource,
    send_receipt: SendReceipt,
}

impl Debug for TransactionImpl {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransactionImpl")
            .field("transaction_id", &self.send_receipt.transaction_id())
            .field("message_id", &self.send_receipt.message_id())
            .finish()
    }
}

impl TransactionImpl {
    pub(crate) fn new(
        rpc_client: Box<dyn RPCClient + Send + Sync>,
        topic: Resource,
        send_receipt: SendReceipt,
    ) -> TransactionImpl {
        TransactionImpl {
            rpc_client,
            topic,
            send_receipt,
        }
    }

    async fn end_transaction(
        mut self,
        resolution: TransactionResolution,
    ) -> Result<(), ClientError> {
        let response = self
            .rpc_client
            .end_transaction(EndTransactionRequest {
                topic: Some(self.topic),
                message_id: self.send_receipt.message_id().to_string(),
                transaction_id: self.send_receipt.transaction_id().to_string(),
                resolution: resolution as i32,
                source: TransactionSource::SourceClient as i32,
                trace_context: "".to_string(),
            })
            .await?;
        handle_response_status(response.status, "end transaction")
    }
}

#[async_trait]
impl Transaction for TransactionImpl {
    async fn commit(mut self) -> Result<(), ClientError> {
        self.end_transaction(TransactionResolution::COMMIT).await
    }

    async fn rollback(mut self) -> Result<(), ClientError> {
        self.end_transaction(TransactionResolution::ROLLBACK).await
    }

    fn message_id(&self) -> &str {
        self.send_receipt.message_id()
    }

    fn transaction_id(&self) -> &str {
        self.send_receipt.transaction_id()
    }
}

/// Resolution of Transaction.
#[repr(i32)]
pub enum TransactionResolution {
    /// Notify server that current transaction should be committed.
    COMMIT = 1,
    /// Notify server that current transaction should be roll-backed.
    ROLLBACK = 2,
    /// Notify server that the state of this transaction is not sure. You should be cautious before return unknown
    /// because the examination from the server will be performed periodically.
    UNKNOWN = 0,
}

/// A closure to check the state of transaction.
/// RocketMQ Server will call producer periodically to check the state of uncommitted transaction.
///
/// # Arguments
///
/// * transaction id
/// * message
pub type TransactionChecker = dyn Fn(String, MessageView) -> TransactionResolution + Send + Sync;

#[cfg(test)]
mod tests {
    use crate::error::ClientError;
    use crate::model::common::SendReceipt;
    use crate::model::transaction::{Transaction, TransactionImpl};
    use crate::pb::{Code, EndTransactionResponse, Resource, SendResultEntry, Status};
    use crate::session;

    #[tokio::test]
    async fn transaction_commit() -> Result<(), ClientError> {
        let mut mock = session::MockRPCClient::new();
        mock.expect_end_transaction().return_once(|_| {
            Box::pin(futures::future::ready(Ok(EndTransactionResponse {
                status: Some(Status {
                    code: Code::Ok as i32,
                    message: "".to_string(),
                }),
            })))
        });
        let transaction = TransactionImpl::new(
            Box::new(mock),
            Resource {
                resource_namespace: "".to_string(),
                name: "".to_string(),
            },
            SendReceipt::from_pb_send_result(&SendResultEntry {
                status: None,
                message_id: "".to_string(),
                transaction_id: "".to_string(),
                offset: 0,
            }),
        );
        transaction.commit().await
    }

    #[tokio::test]
    async fn transaction_rollback() -> Result<(), ClientError> {
        let mut mock = session::MockRPCClient::new();
        mock.expect_end_transaction().return_once(|_| {
            Box::pin(futures::future::ready(Ok(EndTransactionResponse {
                status: Some(Status {
                    code: Code::Ok as i32,
                    message: "".to_string(),
                }),
            })))
        });
        let transaction = TransactionImpl::new(
            Box::new(mock),
            Resource {
                resource_namespace: "".to_string(),
                name: "".to_string(),
            },
            SendReceipt::from_pb_send_result(&SendResultEntry {
                status: None,
                message_id: "".to_string(),
                transaction_id: "".to_string(),
                offset: 0,
            }),
        );
        transaction.rollback().await
    }
}
