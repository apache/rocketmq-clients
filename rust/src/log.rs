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
use std::fs;
use std::fs::{File, OpenOptions};
use std::path::{PathBuf};

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

fn json_logger(filepath: &Option<String>) -> Logger {
    // 设置默认值和解析传入的参数
    let fp = match filepath {
        None => {
            PathBuf::from("logs/rocketmq.client.log")
        }
        Some(p) => {
            PathBuf::from(p)
        }
    };
    // first check the director or file is existing?
    let file = if !fp.exists() {
        let create_file = |fp: &PathBuf| -> Option<File> {
            match OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(fp) {
                Ok(f) => Some(f),
                Err(_) => None
            }
        };
        // have the parent dir
        match fp.parent() {
            // or no have
            None => create_file(&fp),
            // have the parent dir, create it, if success create the file.
            Some(p) => match p.exists() {
                true => create_file(&fp),
                false => match fs::create_dir(p) {
                    Ok(_) => create_file(&fp),
                    // failed to return None
                    Err(_) => None
                }
            }
        }
    } else {
        // log file is existed created it.
        match OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(fp) {
            Ok(f) => Some(f),
            Err(_) => None
        }
    };

    match file {
        None => terminal_logger(),
        Some(f) => {
            let decorator = slog_json::Json::default(f).fuse();
            let drain = slog_async::Async::new(decorator).build().fuse();
            Logger::root(drain, o!())
        }
    }
}

pub(crate) fn logger(logging_format: &LoggingFormat) -> Logger {
    match logging_format {
        LoggingFormat::Terminal => terminal_logger(),
        LoggingFormat::Json(p) => json_logger(p),
    }
}

#[cfg(test)]
pub mod tests {
    use std::path::PathBuf;
    use crate::conf::LoggingFormat;
    use crate::log::logger;

    #[test]
    pub fn create_default_log_file() {
        let _ = logger(&LoggingFormat::Json(None));
        let p = PathBuf::from("logs/rocketmq.client.log");
        assert_eq!(p.exists(), true);
    }

    #[test]
    pub fn create_custom_log_file() {
        let _ = logger(&LoggingFormat::Json(Some(String::from("log/log.log"))));
        let p = PathBuf::from("log/log.log");
        assert_eq!(p.exists(), true);
    }

    #[test]
    pub fn create_custom_log_no_parent_dir_failed() {
        let _ = logger(&LoggingFormat::Json(Some(String::from("log.log"))));
        let p = PathBuf::from("log.log");
        assert_eq!(p.exists(), false);
    }

    #[test]
    pub fn create_custom_log_no_parent_dir() {
        let _ = logger(&LoggingFormat::Json(Some(String::from("./log1.log"))));
        let p = PathBuf::from("./log1.log");
        assert_eq!(p.exists(), true);
    }
}