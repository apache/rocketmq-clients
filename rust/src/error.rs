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

//! Error data model of RocketMQ rust client.

use std::error::Error;
use std::fmt;
use std::fmt::{Debug, Display, Formatter};

/// Error type using by [`ClientError`].
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    #[error("Failed to parse config")]
    Config,

    #[error("Failed to create session")]
    Connect,

    #[error("Message is invalid")]
    InvalidMessage,

    #[error("Message type not match with topic accept message type")]
    MessageTypeNotMatch,

    #[error("Message queue is invalid")]
    InvalidMessageQueue,

    #[error("Server error")]
    Server,

    #[error("No broker available to send message")]
    NoBrokerAvailable,

    #[error("Client internal error")]
    ClientInternal,

    #[error("Client is not running")]
    ClientIsNotRunning,

    #[error("Failed to send message via channel")]
    ChannelSend,

    #[error("Failed to receive message via channel")]
    ChannelReceive,

    #[error("Unknown error")]
    Unknown,
}

/// Error returned by producer or consumer.
pub struct ClientError {
    pub(crate) kind: ErrorKind,
    pub(crate) message: String,
    pub(crate) operation: &'static str,
    pub(crate) context: Vec<(&'static str, String)>,
    pub(crate) source: Option<anyhow::Error>,
}

impl Error for ClientError {}

impl ClientError {
    pub(crate) fn new(kind: ErrorKind, message: &str, operation: &'static str) -> Self {
        Self {
            kind,
            message: message.to_string(),
            operation,
            context: Vec::new(),
            source: None,
        }
    }

    /// Error type
    pub fn kind(&self) -> &ErrorKind {
        &self.kind
    }

    /// Error message
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Name of operation that produced this error
    pub fn operation(&self) -> &str {
        self.operation
    }

    /// Error context, formatted in key-value pairs
    pub fn context(&self) -> &Vec<(&'static str, String)> {
        &self.context
    }

    /// Source error
    pub fn source(&self) -> Option<&anyhow::Error> {
        self.source.as_ref()
    }

    pub(crate) fn with_operation(mut self, operation: &'static str) -> Self {
        if !self.operation.is_empty() {
            self.context.push(("called", self.operation.to_string()));
        }

        self.operation = operation;
        self
    }

    pub(crate) fn with_context(mut self, key: &'static str, value: impl Into<String>) -> Self {
        self.context.push((key, value.into()));
        self
    }

    pub(crate) fn set_source(mut self, src: impl Into<anyhow::Error>) -> Self {
        debug_assert!(self.source.is_none(), "the source error has been set");

        self.source = Some(src.into());
        self
    }
}

impl Display for ClientError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} at {}", self.kind, self.operation)?;

        if !self.context.is_empty() {
            write!(f, ", context: {{ ")?;
            write!(
                f,
                "{}",
                self.context
                    .iter()
                    .map(|(k, v)| format!("{k}: {v}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )?;
            write!(f, " }}")?;
        }

        if !self.message.is_empty() {
            write!(f, " => {}", self.message)?;
        }

        if let Some(source) = &self.source {
            write!(f, ", source: {source}")?;
        }

        Ok(())
    }
}

impl Debug for ClientError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // If alternate has been specified, we will print like Debug.
        if f.alternate() {
            let mut debug = f.debug_struct("Error");
            debug.field("kind", &self.kind);
            debug.field("message", &self.message);
            debug.field("operation", &self.operation);
            debug.field("context", &self.context);
            debug.field("source", &self.source);
            return debug.finish();
        }

        write!(f, "{} at {}", self.kind, self.operation)?;
        if !self.message.is_empty() {
            write!(f, " => {}", self.message)?;
        }
        writeln!(f)?;

        if !self.context.is_empty() {
            writeln!(f)?;
            writeln!(f, "Context:")?;
            for (k, v) in self.context.iter() {
                writeln!(f, "    {k}: {v}")?;
            }
        }
        if let Some(source) = &self.source {
            writeln!(f)?;
            writeln!(f, "Source: {source:?}")?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_client_error() {
        let err = ClientError::new(ErrorKind::Config, "fake_message", "error_client_error")
            .with_operation("another_operation")
            .with_context("context_key", "context_value")
            .set_source(anyhow::anyhow!("fake_source_error"));
        assert_eq!(
            err.to_string(),
            "Failed to parse config at another_operation, context: { called: error_client_error, context_key: context_value } => fake_message, source: fake_source_error"
        );
        assert_eq!(format!("{:?}", err), "Failed to parse config at another_operation => fake_message\n\nContext:\n    called: error_client_error\n    context_key: context_value\n\nSource: fake_source_error\n");
        assert_eq!(format!("{:#?}", err), "Error {\n    kind: Config,\n    message: \"fake_message\",\n    operation: \"another_operation\",\n    context: [\n        (\n            \"called\",\n            \"error_client_error\",\n        ),\n        (\n            \"context_key\",\n            \"context_value\",\n        ),\n    ],\n    source: Some(\n        \"fake_source_error\",\n    ),\n}");
    }
}
