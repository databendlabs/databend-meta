// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// https://github.com/rust-lang/rustfmt/blob/e1ab878ccb24cda1b9e1c48865b375230385fede/build.rs

use std::env;
use std::path::Path;
use std::path::PathBuf;

fn main() {
    build_proto();
}

fn build_proto() {
    let manifest_dir =
        env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR env variable unset");

    let proto_dir = Path::new(&manifest_dir).join("proto");
    let protos = [
        &Path::new(&proto_dir).join(Path::new("raft.proto")),
        &Path::new(&proto_dir).join(Path::new("meta.proto")),
        &Path::new(&proto_dir).join(Path::new("request.proto")),
        &Path::new(&proto_dir).join(Path::new("log_entry.proto")),
    ];

    for proto in protos.iter() {
        println!("cargo:rerun-if-changed={}", proto.to_str().unwrap());
    }

    println!("cargo:rerun-if-changed=build.rs");

    let mut config = prost_build::Config::new();
    config.protoc_arg("--experimental_allow_proto3_optional");

    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    tonic_build::configure()
        .btree_map(["RaftLogStatus.wal_closed_chunk_sizes"])
        .file_descriptor_set_path(out_dir.join("meta_descriptor.bin"))
        .type_attribute(
            "SeqV",
            "#[derive(Eq, serde::Serialize, serde::Deserialize, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "TxnGetRequest",
            "#[derive(Eq, serde::Serialize, serde::Deserialize, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "TxnPutRequest",
            "#[derive(Eq, serde::Serialize, serde::Deserialize, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "TxnDeleteRequest",
            "#[derive(Eq, serde::Serialize, serde::Deserialize, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "TxnDeleteByPrefixRequest",
            "#[derive(Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "TxnCondition.ConditionResult",
            "#[derive(serde::Serialize, serde::Deserialize, num_derive::FromPrimitive, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "TxnCondition.target",
            "#[derive(Eq,serde::Serialize, serde::Deserialize, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "TxnOp.request",
            "#[derive(Eq,serde::Serialize, serde::Deserialize, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "TxnCondition",
            "#[derive(Eq, serde::Serialize, serde::Deserialize, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "ConditionalOperation",
            "#[derive(Eq, serde::Serialize, serde::Deserialize, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "BooleanExpression",
            "#[derive(Eq, serde::Serialize, serde::Deserialize, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "BooleanExpression.CombiningOperator",
            "#[derive(serde::Serialize, serde::Deserialize, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "TxnOp",
            "#[derive(Eq, serde::Serialize, serde::Deserialize, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "TxnRequest",
            "#[derive(Eq, serde::Serialize, serde::Deserialize, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "TxnGetResponse",
            "#[derive(Eq, serde::Serialize, serde::Deserialize, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "TxnPutResponse",
            "#[derive(Eq, serde::Serialize, serde::Deserialize, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "TxnDeleteResponse",
            "#[derive(Eq, serde::Serialize, serde::Deserialize, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "TxnDeleteByPrefixResponse",
            "#[derive(Eq, serde::Serialize, serde::Deserialize, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "TxnOpResponse.response",
            "#[derive(Eq, serde::Serialize, serde::Deserialize, derive_more::TryInto, derive_more::From, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "TxnOpResponse",
            "#[derive(Eq, serde::Serialize, serde::Deserialize, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "TxnReply",
            "#[derive(Eq, serde::Serialize, serde::Deserialize, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "KvTransactionRequest",
            "#[derive(Eq, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "KvTransactionReply",
            "#[derive(Eq, serde::Serialize, serde::Deserialize, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "WatchRequest",
            "#[derive(Eq, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "WatchResponse",
            "#[derive(Eq, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "Event",
            "#[derive(Eq, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "KVMeta",
            "#[derive(Eq, serde::Serialize, serde::Deserialize, deepsize::DeepSizeOf)]",
        )
        .field_attribute(
            "TxnPutRequest.ttl_ms",
            r#"#[serde(skip_serializing_if = "Option::is_none")]"#,
        )
        .field_attribute(
            "TxnRequest.operations",
            r#"#[serde(skip_serializing_if = "Vec::is_empty")] #[serde(default)]"#,
        )
        .type_attribute(
            "FetchIncreaseU64",
            "#[derive(Eq, serde::Serialize, serde::Deserialize, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "FetchIncreaseU64Response",
            "#[derive(Eq, serde::Serialize, serde::Deserialize, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "PutSequential",
            "#[derive(Eq, serde::Serialize, serde::Deserialize, deepsize::DeepSizeOf)]",
        )
        // raft.proto base types (needed for DeepSizeOf on new types)
        .type_attribute("Vote", "#[derive(Eq, deepsize::DeepSizeOf)]")
        .type_attribute("LogId", "#[derive(Eq, deepsize::DeepSizeOf)]")
        .type_attribute("LeaderId", "#[derive(Eq, deepsize::DeepSizeOf)]")
        .type_attribute("VoteRequest", "#[derive(Eq, deepsize::DeepSizeOf)]")
        .type_attribute("VoteResponse", "#[derive(Eq, deepsize::DeepSizeOf)]")
        // log_entry.proto types
        .type_attribute("Node", "#[derive(Eq, deepsize::DeepSizeOf)]")
        .type_attribute("CmdAddNode", "#[derive(Eq, deepsize::DeepSizeOf)]")
        .type_attribute("CmdRemoveNode", "#[derive(Eq, deepsize::DeepSizeOf)]")
        .type_attribute("CmdSetFeature", "#[derive(Eq, deepsize::DeepSizeOf)]")
        .type_attribute("LogEntry", "#[derive(Eq, deepsize::DeepSizeOf)]")
        .type_attribute("LogEntry.cmd", "#[derive(Eq, deepsize::DeepSizeOf)]")
        .type_attribute("Membership", "#[derive(Eq, deepsize::DeepSizeOf)]")
        .type_attribute("VoterGroup", "#[derive(Eq, deepsize::DeepSizeOf)]")
        .type_attribute("AppendRequest", "#[derive(Eq, deepsize::DeepSizeOf)]")
        .type_attribute("AppendResponse", "#[derive(Eq, deepsize::DeepSizeOf)]")
        .compile_protos_with_config(config, &protos, &[&proto_dir])
        .unwrap();
}
