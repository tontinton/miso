use std::fmt;

use async_stream::try_stream;
use futures_util::StreamExt;
use hashbrown::{hash_map::RawEntryMut, HashMap};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::log::{Log, LogStream, LogTryStream};

const MERGED_LEFT_SUFFIX: &str = "_left";
const MERGED_RIGHT_SUFFIX: &str = "_right";

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

fn default_partitions() -> usize {
    1
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
pub struct Join {
    pub on: (String, String),

    #[serde(default, rename = "type")]
    pub type_: JoinType,

    #[serde(default = "default_partitions")]
    pub partitions: usize,
}

impl fmt::Display for Join {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "on=({}, {}), type={}, partitions={}",
            self.on.0, self.on.1, self.type_, self.partitions
        )
    }
}

fn merge_left_with_right(join_value: &Value, mut left: Log, right: Log) -> Log {
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

fn merge_logs(join_value: &Value, left: Log, right: Log, flip: bool) -> Log {
    if flip {
        merge_left_with_right(join_value, left, right)
    } else {
        merge_left_with_right(join_value, right, left)
    }
}

fn hash_inner_join(
    build: HashMap<Value, Vec<Log>>,
    probe: Vec<(Value, Log)>,
    flip: bool,
) -> LogTryStream {
    Box::pin(try_stream! {
        for (probe_key, probe_log) in probe {
            if let Some(build_logs) = build.get(&probe_key) {
                for build_log in build_logs {
                    yield merge_logs(&probe_key, build_log.clone(), probe_log.clone(), flip);
                }
            }
        }
    })
}

fn hash_outer_join(
    mut build: HashMap<Value, (Vec<Log>, bool)>,
    probe: Vec<(Value, Log)>,
    flip: bool,
) -> LogTryStream {
    Box::pin(try_stream! {
        for (probe_key, probe_log) in probe {
            if let Some((build_logs, matched)) = build.get_mut(&probe_key) {
                for build_log in build_logs {
                    yield merge_logs(&probe_key, build_log.clone(), probe_log.clone(), flip);
                }
                *matched = true;
            } else {
                yield probe_log;
            }
        }

        for (_, (build_logs, matched)) in build {
            if !matched {
                for build_log in build_logs {
                    yield build_log;
                }
            }
        }
    })
}

fn hash_left_join(
    left: HashMap<Value, Vec<Log>>,
    mut right: HashMap<Value, Vec<Log>>,
) -> LogTryStream {
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

async fn collect_to_build_and_probe(
    config: Join,
    mut left_stream: LogStream,
    mut right_stream: LogStream,
) -> (Vec<(Value, Log)>, Vec<(Value, Log)>, bool) {
    let mut left = Vec::new();
    let mut right = Vec::new();
    let (left_key, right_key) = &config.on;

    loop {
        tokio::select! {
            Some(log) = left_stream.next() => {
                if let Some(value) = log.get(left_key) {
                    left.push((value.clone(), log));
                }
            },
            Some(log) = right_stream.next() => {
                if let Some(value) = log.get(right_key) {
                    right.push((value.clone(), log));
                }
            },
            else => break,
        }
    }

    let (build, probe, flip) = if left.len() <= right.len() {
        (left, right, true)
    } else {
        (right, left, false)
    };

    (build, probe, flip)
}

async fn collect_to_hash_maps(
    config: Join,
    mut left_stream: LogStream,
    mut right_stream: LogStream,
) -> (HashMap<Value, Vec<Log>>, HashMap<Value, Vec<Log>>) {
    let mut left: HashMap<Value, Vec<Log>> = HashMap::new();
    let mut right: HashMap<Value, Vec<Log>> = HashMap::new();
    let (left_key, right_key) = &config.on;

    loop {
        tokio::select! {
            Some(log) = left_stream.next() => {
                if let Some(value) = log.get(left_key) {
                    match left.raw_entry_mut().from_key(value) {
                        RawEntryMut::Occupied(mut occ) => {
                            occ.get_mut().push(log);
                        }
                        RawEntryMut::Vacant(vac) => {
                            vac.insert(value.clone(), vec![log]);
                        }
                    }
                }
            },
            Some(log) = right_stream.next() => {
                if let Some(value) = log.get(right_key) {
                    match right.raw_entry_mut().from_key(value) {
                        RawEntryMut::Occupied(mut occ) => {
                            occ.get_mut().push(log);
                        }
                        RawEntryMut::Vacant(vac) => {
                            vac.insert(value.clone(), vec![log]);
                        }
                    }
                }
            },
            else => break,
        }
    }

    (left, right)
}

pub async fn join_streams(
    config: Join,
    left_stream: LogStream,
    right_stream: LogStream,
) -> LogTryStream {
    match config.type_ {
        JoinType::Inner => {
            let (build, probe, flip) =
                collect_to_build_and_probe(config, left_stream, right_stream).await;

            let mut build_map: HashMap<Value, Vec<Log>> = HashMap::new();
            for (key, log) in build {
                build_map.entry(key).or_default().push(log);
            }

            hash_inner_join(build_map, probe, flip)
        }
        JoinType::Outer => {
            let (build, probe, flip) =
                collect_to_build_and_probe(config, left_stream, right_stream).await;

            let mut build_map: HashMap<Value, (Vec<Log>, bool)> = HashMap::new();
            for (key, log) in build {
                build_map.entry(key).or_default().0.push(log);
            }

            hash_outer_join(build_map, probe, flip)
        }
        JoinType::Left => {
            let (left, right) = collect_to_hash_maps(config, left_stream, right_stream).await;
            hash_left_join(left, right)
        }
        JoinType::Right => {
            let (left, right) = collect_to_hash_maps(config, left_stream, right_stream).await;
            hash_left_join(right, left)
        }
    }
}
