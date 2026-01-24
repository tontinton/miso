use bytesize::ByteSize;
use serde::{Deserialize, Serialize};

use miso_common::bytesize_utils::{deserialize_bytesize, serialize_bytesize};

pub const DEFAULT_SORT_MEMORY_LIMIT: u64 = 500 * 1024 * 1024;
pub const DEFAULT_SUMMARIZE_MEMORY_LIMIT: u64 = 500 * 1024 * 1024;
pub const DEFAULT_JOIN_MEMORY_LIMIT: u64 = 500 * 1024 * 1024;

fn default_sort_memory_limit() -> ByteSize {
    ByteSize::b(DEFAULT_SORT_MEMORY_LIMIT)
}

fn default_summarize_memory_limit() -> ByteSize {
    ByteSize::b(DEFAULT_SUMMARIZE_MEMORY_LIMIT)
}

fn default_join_memory_limit() -> ByteSize {
    ByteSize::b(DEFAULT_JOIN_MEMORY_LIMIT)
}

#[derive(Deserialize, Serialize, Clone)]
pub struct WorkflowLimits {
    #[serde(
        default = "default_sort_memory_limit",
        deserialize_with = "deserialize_bytesize",
        serialize_with = "serialize_bytesize"
    )]
    pub sort_memory_limit: ByteSize,
    #[serde(
        default = "default_summarize_memory_limit",
        deserialize_with = "deserialize_bytesize",
        serialize_with = "serialize_bytesize"
    )]
    pub summarize_memory_limit: ByteSize,
    #[serde(
        default = "default_join_memory_limit",
        deserialize_with = "deserialize_bytesize",
        serialize_with = "serialize_bytesize"
    )]
    pub join_memory_limit: ByteSize,
}

impl Default for WorkflowLimits {
    fn default() -> Self {
        Self {
            sort_memory_limit: default_sort_memory_limit(),
            summarize_memory_limit: default_summarize_memory_limit(),
            join_memory_limit: default_join_memory_limit(),
        }
    }
}
