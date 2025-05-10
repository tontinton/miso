use std::{collections::BTreeMap, fmt};

use async_stream::try_stream;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::log::{Log, LogStream, LogTryStream};

use super::sortable_value::SortableValue;

const MERGED_LEFT_SUFFIX: &str = "_left";
const MERGED_RIGHT_SUFFIX: &str = "_right";

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

fn merge_two_logs(join_value: &Value, mut left: Log, right: Log) -> Log {
    for (key, value) in right {
        match left.entry(key) {
            serde_json::map::Entry::Occupied(entry) => {
                if join_value == entry.get() {
                    // Keep existing value.
                    continue;
                }

                let key = entry.key();

                let mut left_key = String::with_capacity(key.len() + MERGED_LEFT_SUFFIX.len());
                left_key.push_str(key);
                left_key.push_str(MERGED_LEFT_SUFFIX);

                let mut right_key = String::with_capacity(key.len() + MERGED_RIGHT_SUFFIX.len());
                right_key.push_str(key);
                right_key.push_str(MERGED_RIGHT_SUFFIX);

                let existing_value = entry.remove();
                left.insert(left_key, existing_value);
                left.insert(right_key, value);
            }
            serde_json::map::Entry::Vacant(entry) => {
                entry.insert(value);
            }
        }
    }

    left
}

fn inner_join(left: JoinMap, right: JoinMap) -> LogTryStream {
    Box::pin(try_stream! {
        let mut left_iter = left.into_iter();
        let mut right_iter = right.into_iter();
        let mut left_tuple = left_iter.next();
        let mut right_tuple = right_iter.next();

        while let (Some((left_key, mut left_logs)), Some((right_key, mut right_logs))) =
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
                    if right_logs.len() == 1 {
                        for left_log in left_logs.clone() {
                            yield merge_two_logs(&left_key, left_log, right_logs.pop().unwrap());
                        }
                    } else if left_logs.len() == 1 {
                        for right_log in right_logs.clone() {
                            yield merge_two_logs(&left_key, left_logs.pop().unwrap(), right_log);
                        }
                    } else {
                        for left_log in &left_logs {
                            for right_log in &right_logs {
                                yield merge_two_logs(&left_key, left_log.clone(), right_log.clone());
                            }
                        }
                    }
                    left_tuple = left_iter.next();
                    right_tuple = right_iter.next();
                }
            };
        }
    })
}

fn outer_join(left: JoinMap, right: JoinMap) -> LogTryStream {
    Box::pin(try_stream! {
        let mut left_iter = left.into_iter();
        let mut right_iter = right.into_iter();
        let mut left_next = left_iter.next();
        let mut right_next = right_iter.next();

        loop {
            match (left_next, right_next) {
                (Some((left_key, mut left_logs)), Some((right_key, mut right_logs))) => {
                    match left_key.cmp(&right_key) {
                        std::cmp::Ordering::Less => {
                            for log in left_logs {
                                yield log;
                            }
                            left_next = left_iter.next();
                            right_next = Some((right_key, right_logs));
                        }
                        std::cmp::Ordering::Greater => {
                            for log in right_logs {
                                yield log;
                            }
                            left_next = Some((left_key, left_logs));
                            right_next = right_iter.next();
                        }
                        std::cmp::Ordering::Equal => {
                            let key = left_key;

                            if right_logs.len() == 1 {
                                for left_log in left_logs {
                                    yield merge_two_logs(&key, left_log, right_logs.pop().unwrap());
                                }
                            } else if left_logs.len() == 1 {
                                for right_log in right_logs {
                                    yield merge_two_logs(&key, left_logs.pop().unwrap(), right_log);
                                }
                            } else {
                                for left_log in &left_logs {
                                    for right_log in &right_logs {
                                        yield merge_two_logs(&key, left_log.clone(), right_log.clone());
                                    }
                                }
                            }

                            left_next = left_iter.next();
                            right_next = right_iter.next();
                        }
                    }
                }
                (Some((_, logs)), None) => {
                    for log in logs {
                        yield log;
                    }
                    for (_, logs) in left_iter {
                        for log in logs {
                            yield log;
                        }
                    }
                    break;
                }
                (None, Some((_, logs))) => {
                    for log in logs {
                        yield log;
                    }
                    for (_, logs) in right_iter {
                        for log in logs {
                            yield log;
                        }
                    }
                    break;
                }
                (None, None) => break,
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
