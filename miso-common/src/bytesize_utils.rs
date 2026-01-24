use bytesize::ByteSize;
use serde::{Deserialize, Deserializer, Serializer};

pub fn serialize_bytesize<S>(size: &ByteSize, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&size.to_string_as(true))
}

pub fn deserialize_bytesize<'de, D>(deserializer: D) -> Result<ByteSize, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    s.parse::<ByteSize>().map_err(serde::de::Error::custom)
}
