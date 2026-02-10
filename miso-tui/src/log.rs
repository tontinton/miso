use color_eyre::{Result, eyre::eyre};
use serde_json::{Map, Value};
use simd_json::from_slice;

#[derive(Debug)]
pub struct Log {
    pub parsed: Map<String, Value>,
    pub len: usize,
}

impl Log {
    pub fn raw(slice: &mut [u8]) -> Result<Self> {
        let len = slice.len();
        if let Value::Object(parsed) = from_slice::<Value>(slice)? {
            Ok(Self { parsed, len })
        } else {
            Err(eyre!("not object"))
        }
    }
}
