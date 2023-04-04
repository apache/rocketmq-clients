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
use std::error::Error;
use std::fmt;
use std::fmt::{Debug, Display, Formatter};

#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    #[error("Failed to parse config")]
    Config,

    #[error("Failed to create session")]
    Connect,

    #[error("Server error")]
    Server,

    #[error("Client internal error")]
    ClientInternal,

    #[error("Failed to send message via channel")]
    ChannelSend,

    #[error("Failed to receive message via channel")]
    ChannelReceive,

    #[error("Unknown error")]
    Unknown,
}

pub struct ClientError {
    pub(crate) kind: ErrorKind,
    pub(crate) message: String,
    pub(crate) operation: &'static str,
    pub(crate) context: Vec<(&'static str, String)>,
    pub(crate) source: Option<anyhow::Error>,
}

impl Error for ClientError {}

impl ClientError {
    pub fn new(kind: ErrorKind, message: String, operation: &'static str) -> Self {
        Self {
            kind,
            message,
            operation,
            context: Vec::new(),
            source: None,
        }
    }

    pub fn with_operation(mut self, operation: &'static str) -> Self {
        if !self.operation.is_empty() {
            self.context.push(("called", self.operation.to_string()));
        }

        self.operation = operation;
        self
    }

    pub fn with_context(mut self, key: &'static str, value: impl Into<String>) -> Self {
        self.context.push((key, value.into()));
        self
    }

    pub fn set_source(mut self, src: impl Into<anyhow::Error>) -> Self {
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
