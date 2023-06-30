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

use crate::error::ClientError;
use crate::model::message::MessageView;

/// An entity to describe an independent transaction.
///
/// Once the request of commit of roll-back reached server, subsequently arrived commit or roll-back request in
/// [`Transaction`] would be ignored by the server.
///
/// If transaction is not commit/roll-back in time, it is suspended until it is solved by [`TransactionChecker`]
/// or reach the end of life.
pub trait Transaction {
    /// Try to commit the transaction, which would expose the message before the transaction is closed if no exception thrown.
    /// What you should pay more attention to is that the commitment may be successful even exception is thrown.
    fn commit() -> Result<(), ClientError>;

    /// Try to roll back the transaction, which would expose the message before the transaction is closed if no exception thrown.
    /// What you should pay more attention to is that the roll-back may be successful even exception is thrown.
    fn rollback() -> Result<(), ClientError>;
}

/// Resolution of Transaction.
pub enum TransactionResolution {
    /// Notify server that current transaction should be committed.
    COMMIT,
    /// Notify server that current transaction should be roll-backed.
    ROLLBACK,
    /// Notify the server that the state of this transaction is not sure. You should be cautious before return unknown
    /// because the examination from the server will be performed periodically.
    UNKNOWN,
}

/// A closure to check the state of transaction.
pub type TransactionChecker = dyn Fn(String, MessageView) -> TransactionResolution + Send + Sync;
