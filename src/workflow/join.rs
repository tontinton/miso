#![allow(clippy::mutable_key_type)]

use std::{collections::BTreeMap, fmt};

use async_stream::try_stream;
use futures_util::StreamExt;
use itertools::iproduct;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::log::{Log, LogStream, LogTryStream};

use super::sortable_value::SortableValue;

type JoinMap = BTreeMap<SortableValue, Vec<Log>>;

#[derive(Debug, Serialize, Deserialize, Default, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum JoinType {
    #[default]
    Inner,
    Outer,
    Left,
    Right,
}

impl fmt::Display for JoinType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JoinType::Inner => write!(f, "inner"),
            JoinType::Outer => write!(f, "outer"),
            JoinType::Left => write!(f, "left"),
            JoinType::Right => write!(f, "right"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Join {
    pub on: (String, String),

    #[serde(default, rename = "type")]
    pub type_: JoinType,
}

fn merge_two_logs(join_value: &Value, left: Log, right: Log) -> Log {
    let mut merged = left;

    for (key, value) in right {
        if let Some(existing_value) = merged.remove(&key) {
            if join_value == &existing_value {
                merged.insert(key, existing_value);
                continue;
            }

            merged.insert(key.as_str().to_string() + "_left", existing_value);
            merged.insert(key.as_str().to_string() + "_right", value);
        } else {
            merged.insert(key, value);
        }
    }

    merged
}

fn inner_join(left: JoinMap, right: JoinMap) -> LogTryStream {
    Box::pin(try_stream! {
        let mut left_iter = left.into_iter();
        let mut right_iter = right.into_iter();
        let mut left_tuple = left_iter.next();
        let mut right_tuple = right_iter.next();

        while let (Some((left_key, left_logs)), Some((right_key, right_logs))) =
            (left_tuple, right_tuple)
        {
            match left_key.cmp(&right_key) {
                std::cmp::Ordering::Less => {
                    left_tuple = left_iter.next();
                    right_tuple = Some((right_key, right_logs));
                }
                std::cmp::Ordering::Greater => {
                    left_tuple = Some((left_key, left_logs));
                    right_tuple = right_iter.next();
                }
                std::cmp::Ordering::Equal => {
                    for (left_log, right_log) in iproduct!(left_logs, right_logs) {
                        yield merge_two_logs(&left_key, left_log, right_log);
                    }
                    left_tuple = left_iter.next();
                    right_tuple = right_iter.next();
                }
            };
        }
    })
}

fn outer_join(mut left: JoinMap, mut right: JoinMap) -> LogTryStream {
    let mut keys: Vec<_> = left.keys().chain(right.keys()).cloned().collect();
    keys.sort();
    keys.dedup();
    Box::pin(try_stream! {
        for key in keys {
            match (left.remove(&key), right.remove(&key)) {
                (Some(left_logs), Some(right_logs)) => {
                    for (left_log, right_log) in iproduct!(left_logs, right_logs) {
                        yield merge_two_logs(&key, left_log, right_log);
                    }
                }
                (Some(left_logs), None) => {
                    for log in left_logs {
                        yield log;
                    }
                }
                (None, Some(right_logs)) => {
                    for log in right_logs {
                        yield log;
                    }
                }
                _ => panic!("Key doesn't exist in both left and right after merge?"),
            }
        }
    })
}

fn left_join(left: JoinMap, mut right: JoinMap) -> LogTryStream {
    Box::pin(try_stream! {
        for (left_key, left_logs) in left {
            for mut left_log in left_logs {
                if let Some(right_logs) = right.remove(&left_key) {
                    for right_log in right_logs {
                        for (right_key, right_value) in right_log {
                            left_log.entry(right_key).or_insert(right_value);
                        }
                    }
                }
                yield left_log;
            }
        }
    })
}

pub async fn join_streams(
    config: Join,
    mut left_stream: LogStream,
    mut right_stream: LogStream,
) -> LogTryStream {
    let mut left = JoinMap::new();
    let mut right = JoinMap::new();
    let left_key = &config.on.0;
    let right_key = &config.on.1;

    loop {
        tokio::select! {
            Some(log) = left_stream.next() => {
                if let Some(value) = log.get(left_key) {
                    left.entry(SortableValue(value.clone())).or_default().push(log);
                }
            },
            Some(log) = right_stream.next() => {
                if let Some(value) = log.get(right_key) {
                    right.entry(SortableValue(value.clone())).or_default().push(log);
                }
            },
            else => break,
        }
    }

    match config.type_ {
        JoinType::Inner => inner_join(left, right),
        JoinType::Outer => outer_join(left, right),
        JoinType::Left => left_join(left, right),
        JoinType::Right => left_join(right, left),
    }
}
