use crate::Log;
use miso_workflow_types::value::Value;

const VALUE_SIZE: usize = std::mem::size_of::<Value>();
const STRING_SIZE: usize = std::mem::size_of::<String>();
const LOG_SIZE: usize = std::mem::size_of::<Log>();
const BTREE_OVERHEAD_PER_ENTRY: usize = 16;

pub trait MemorySize {
    fn estimate_memory_size(&self) -> usize;
}

impl MemorySize for Value {
    fn estimate_memory_size(&self) -> usize {
        match self {
            Value::Null
            | Value::Bool(_)
            | Value::Int(_)
            | Value::UInt(_)
            | Value::Float(_)
            | Value::Timestamp(_)
            | Value::Timespan(_) => VALUE_SIZE,

            Value::String(s) => VALUE_SIZE + s.capacity(),

            Value::Array(arr) => {
                let elements_heap: usize = arr
                    .iter()
                    .map(|v| match v {
                        Value::String(s) => s.capacity(),
                        Value::Array(_) | Value::Object(_) => v.estimate_memory_size() - VALUE_SIZE,
                        _ => 0,
                    })
                    .sum();
                VALUE_SIZE + arr.capacity() * VALUE_SIZE + elements_heap
            }

            Value::Object(obj) => {
                let entries_size: usize = obj
                    .iter()
                    .map(|(k, v)| {
                        BTREE_OVERHEAD_PER_ENTRY
                            + STRING_SIZE
                            + k.capacity()
                            + v.estimate_memory_size()
                    })
                    .sum();
                VALUE_SIZE + entries_size
            }
        }
    }
}

impl MemorySize for Log {
    fn estimate_memory_size(&self) -> usize {
        let entries_size: usize = self
            .iter()
            .map(|(k, v)| {
                BTREE_OVERHEAD_PER_ENTRY + STRING_SIZE + k.capacity() + v.estimate_memory_size()
            })
            .sum();
        LOG_SIZE + entries_size
    }
}
