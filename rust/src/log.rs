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
use std::fs::OpenOptions;

use slog::{o, Drain, Logger};
use slog_async::OverflowStrategy;

use crate::conf::LoggingFormat;

pub(crate) fn terminal_logger() -> Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator)
        .use_file_location()
        .build()
        .fuse();
    let drain = slog_async::Async::new(drain)
        .overflow_strategy(OverflowStrategy::Block)
        .chan_size(1)
        .build()
        .fuse();
    Logger::root(drain, o!())
}

fn json_logger(filepath: &str) -> Logger {
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(filepath)
        .unwrap();
    let decorator = slog_json::Json::default(file).fuse();
    let drain = slog_async::Async::new(decorator).build().fuse();
    Logger::root(drain, o!())
}

pub(crate) fn logger(logging_format: &LoggingFormat) -> Logger {
    match logging_format {
        LoggingFormat::Terminal => terminal_logger(),
        LoggingFormat::Json => json_logger("logs/rocketmq_client.log"),
    }
}
