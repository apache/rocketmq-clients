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
use std::path::PathBuf;
use std::process::Command;
use std::{env, str};

use regex::Regex;
use version_check::Version;

fn main() {
    let build_proto = env::var("BUILD_PROTO");
    if build_proto.is_err() {
        return;
    }

    check_protoc_version();

    tonic_build::configure()
        .build_client(true)
        .build_server(false)
        .out_dir("src/pb")
        .compile(
            &[
                "../protos/apache/rocketmq/v2/service.proto",
                "../protos/apache/rocketmq/v2/admin.proto",
            ],
            &["../protos"],
        )
        .unwrap_or_else(|e| panic!("Failed to compile protos {:?}", e));
}

fn check_protoc_version() {
    let protoc = env::var_os("PROTOC")
        .map(PathBuf::from)
        .or_else(|| which::which("protoc").ok());

    if protoc.is_none() {
        panic!("protoc not found");
    }

    let mut cmd = Command::new(protoc.unwrap());
    cmd.arg("--version");
    let result = cmd.output();

    if result.is_err() {
        panic!("failed to invoke protoc: {:?}", result)
    }

    let output = result.unwrap();
    if !output.status.success() {
        panic!("protoc failed: {:?}", output)
    }

    let version_regex = Regex::new(r"(?:(\d+)\.)?(?:(\d+)\.)?(\*|\d+)").unwrap();
    let protoc_version = version_regex.find(str::from_utf8(&output.stdout).unwrap());
    if protoc_version.is_none() {
        panic!("failed to parse protoc version");
    }

    let protoc_version = Version::parse(protoc_version.unwrap().as_str()).unwrap();
    let min_version = Version::from_mmp(3, 15, 0);
    if protoc_version.cmp(&min_version).is_le() {
        panic!("protoc version must be >= 3.15.0");
    }
}
