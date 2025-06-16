use std::fmt;
use std::{num::NonZero, sync::OnceLock, thread::available_parallelism};

use futures_util::StreamExt;
use hashbrown::{hash_map::RawEntryMut, HashMap};
use rayon::{ThreadPool, ThreadPoolBuilder};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::debug;

use crate::log::{Log, LogStream};

const DEFAULT_NUM_CORES: usize = 10;
const PARALLELISM_MUL: usize = 5;
const MERGED_LEFT_SUFFIX: &str = "_left";
const MERGED_RIGHT_SUFFIX: &str = "_right";

macro_rules! send_ret_on_err {
    ($tx:expr, $log:expr) => {
        if let Err(e) = $tx.send($log) {
            debug!("Join tx closed: {e:?}");
            return;
        }
    };
}

#[derive(Debug, Serialize, Deserialize, Default, Clone, Copy, PartialEq)]
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

fn hash_inner_join(
    tx: flume::Sender<Log>,
    build: HashMap<Value, Vec<Log>>,
    probe: Vec<(Value, Log)>,
    flip: bool,
) {
    let merge = if flip {
        |join_value: &Value, left: Log, right: Log| merge_left_with_right(join_value, left, right)
    } else {
        |join_value: &Value, left: Log, right: Log| merge_left_with_right(join_value, right, left)
    };

    for (probe_key, probe_log) in probe {
        if let Some(build_logs) = build.get(&probe_key) {
            for build_log in build_logs {
                let merged = merge(&probe_key, build_log.clone(), probe_log.clone());
                send_ret_on_err!(tx, merged);
            }
        }
    }
}

fn hash_outer_join(
    tx: flume::Sender<Log>,
    mut build: HashMap<Value, (Vec<Log>, bool)>,
    probe: Vec<(Value, Log)>,
    flip: bool,
) {
    let merge = if flip {
        |join_value: &Value, left: Log, right: Log| merge_left_with_right(join_value, left, right)
    } else {
        |join_value: &Value, left: Log, right: Log| merge_left_with_right(join_value, right, left)
    };

    for (probe_key, probe_log) in probe {
        if let Some((build_logs, matched)) = build.get_mut(&probe_key) {
            for build_log in build_logs {
                let merged = merge(&probe_key, build_log.clone(), probe_log.clone());
                send_ret_on_err!(tx, merged);
            }
            *matched = true;
        } else {
            send_ret_on_err!(tx, probe_log);
        }
    }

    for (_, (build_logs, matched)) in build {
        if !matched {
            for build_log in build_logs {
                send_ret_on_err!(tx, build_log);
            }
        }
    }
}

fn hash_left_join(
    tx: flume::Sender<Log>,
    left: HashMap<Value, Vec<Log>>,
    mut right: HashMap<Value, Vec<Log>>,
) {
    for (left_key, left_logs) in left {
        for mut left_log in left_logs {
            if let Some(right_logs) = right.remove(&left_key) {
                for right_log in right_logs {
                    for (right_key, right_value) in right_log {
                        left_log.entry(right_key).or_insert(right_value);
                    }
                }
            }
            send_ret_on_err!(tx, left_log);
        }
    }
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

fn join_thread_pool() -> &'static ThreadPool {
    static THREAD_POOL: OnceLock<ThreadPool> = OnceLock::new();
    THREAD_POOL.get_or_init(|| {
        let num_cores = available_parallelism()
            .map(NonZero::get)
            .unwrap_or(DEFAULT_NUM_CORES);
        let num_threads = (num_cores * PARALLELISM_MUL).max(1);
        ThreadPoolBuilder::new()
            .thread_name(|i| format!("join-{}", i))
            .num_threads(num_threads)
            .build()
            .expect("failed to create join thread pool")
    })
}

pub async fn join_streams(
    config: Join,
    left_stream: LogStream,
    right_stream: LogStream,
) -> LogStream {
    // flume supports sending from sync and receiving into async.
    let (tx, rx) = flume::bounded(1);

    let type_ = config.type_;
    match type_ {
        JoinType::Inner => {
            let (build, probe, flip) =
                collect_to_build_and_probe(config, left_stream, right_stream).await;

            join_thread_pool().spawn(move || {
                let mut build_map: HashMap<Value, Vec<Log>> = HashMap::new();
                for (key, log) in build {
                    build_map.entry(key).or_default().push(log);
                }
                hash_inner_join(tx, build_map, probe, flip);
            });
        }
        JoinType::Outer => {
            let (build, probe, flip) =
                collect_to_build_and_probe(config, left_stream, right_stream).await;

            join_thread_pool().spawn(move || {
                let mut build_map: HashMap<Value, (Vec<Log>, bool)> = HashMap::new();
                for (key, log) in build {
                    build_map.entry(key).or_default().0.push(log);
                }
                hash_outer_join(tx, build_map, probe, flip);
            });
        }
        JoinType::Left | JoinType::Right => {
            let (mut left, mut right) =
                collect_to_hash_maps(config, left_stream, right_stream).await;
            if matches!(type_, JoinType::Right) {
                std::mem::swap(&mut left, &mut right);
            }
            join_thread_pool().spawn(move || hash_left_join(tx, left, right));
        }
    }

    Box::pin(rx.into_stream())
}
