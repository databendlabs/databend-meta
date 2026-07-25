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

use std::fmt::Display;
use std::io;

use databend_meta_types::raft_types::ErrorSubject;
use databend_meta_types::raft_types::StorageError;
use openraft::AnyError;
use openraft::ErrorVerb;

/// Errors that occur when accessing snapshot store
#[derive(Debug, thiserror::Error)]
#[error("SnapshotStoreError({verb:?}: {source}, while: {context}")]
pub struct SnapshotStoreError {
    verb: ErrorVerb,

    #[source]
    source: io::Error,
    context: String,
}

impl SnapshotStoreError {
    pub fn read(error: io::Error) -> Self {
        Self {
            verb: ErrorVerb::Read,
            source: error,
            context: "".to_string(),
        }
    }

    pub fn write(error: io::Error) -> Self {
        Self {
            verb: ErrorVerb::Write,
            source: error,
            context: "".to_string(),
        }
    }

    pub fn add_context(&mut self, context: impl Display) {
        if self.context.is_empty() {
            self.context = context.to_string();
        } else {
            self.context = format!("{}; while {}", self.context, context);
        }
    }

    pub fn with_context(mut self, context: impl Display) -> Self {
        self.add_context(context);
        self
    }

    /// Add meta and context info to the error.
    ///
    /// meta is anything that can be displayed.
    pub fn with_meta(self, context: impl Display, meta: impl Display) -> Self {
        self.with_context(format_args!("{}: {}", context, meta))
    }
}

impl From<SnapshotStoreError> for StorageError {
    fn from(error: SnapshotStoreError) -> Self {
        StorageError::new(
            ErrorSubject::Snapshot(None),
            error.verb,
            AnyError::new(&error),
        )
    }
}

impl From<SnapshotStoreError> for io::Error {
    fn from(e: SnapshotStoreError) -> Self {
        io::Error::other(e)
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use databend_meta_types::raft_types::ErrorSubject;
    use databend_meta_types::raft_types::StorageError;

    use super::*;

    fn not_found() -> io::Error {
        io::Error::new(io::ErrorKind::NotFound, "no such file")
    }

    #[test]
    fn test_context_accumulates_in_call_order() {
        let e = SnapshotStoreError::read(not_found());
        assert_eq!(
            e.to_string(),
            "SnapshotStoreError(Read: no such file, while: "
        );

        let e = e.with_context("opening").with_context("loading last");
        assert_eq!(
            e.to_string(),
            "SnapshotStoreError(Read: no such file, while: opening; while loading last"
        );

        let e = SnapshotStoreError::write(not_found()).with_meta("opening", "/tmp/a.snap");
        assert_eq!(
            e.to_string(),
            "SnapshotStoreError(Write: no such file, while: opening: /tmp/a.snap"
        );
        assert_eq!(e.source().unwrap().to_string(), "no such file");
    }

    #[test]
    fn test_convert_to_storage_error_and_io_error() {
        let storage_error = StorageError::from(SnapshotStoreError::read(not_found()));
        assert_eq!(
            storage_error,
            StorageError::new(
                ErrorSubject::Snapshot(None),
                ErrorVerb::Read,
                AnyError::new(&SnapshotStoreError::read(not_found())),
            )
        );

        let io_error = io::Error::from(SnapshotStoreError::write(not_found()));
        assert_eq!(io_error.kind(), io::ErrorKind::Other);
        assert_eq!(
            io_error.to_string(),
            "SnapshotStoreError(Write: no such file, while: "
        );
    }
}
