use std::hash::BuildHasher;
use std::{fmt, iter};

use color_eyre::eyre::Context;
use color_eyre::Result;
use flume::{Receiver, RecvError, Selector, Sender, TryRecvError};
use hashbrown::{hash_map::RawEntryMut, HashMap};
use hashbrown::{DefaultHashBuilder, HashSet};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::cancel_iter::CancelIter;
use crate::log::{Log, LogItem, LogIter};
use crate::send_once::SendOnce;
use crate::spawn_thread::{spawn, ThreadRx};
use crate::watch::Watch;

use super::filter::FilterAst;
use super::WorkflowRx;

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

type VecLogMap = Vec<(Value, Log)>;

fn collect_to_build_and_probe(
    config: Join,
    iter: impl Iterator<Item = (bool, Log)>,
) -> (VecLogMap, VecLogMap, bool) {
    let mut left = Vec::new();
    let mut right = Vec::new();
    let (left_key, right_key) = &config.on;

    for (is_left, log) in iter {
        if is_left {
            if let Some(value) = log.get(left_key) {
                left.push((value.clone(), log));
            }
        } else if let Some(value) = log.get(right_key) {
            right.push((value.clone(), log));
        }
    }

    let (build, probe, flip) = if left.len() <= right.len() {
        (left, right, true)
    } else {
        (right, left, false)
    };

    (build, probe, flip)
}

fn collect_to_hash_maps(
    config: Join,
    iter: impl Iterator<Item = (bool, Log)>,
) -> (HashMap<Value, Vec<Log>>, HashMap<Value, Vec<Log>>) {
    let mut left: HashMap<Value, Vec<Log>> = HashMap::new();
    let mut right: HashMap<Value, Vec<Log>> = HashMap::new();
    let (left_key, right_key) = &config.on;

    for (is_left, log) in iter {
        if is_left {
            let Some(value) = log.get(left_key) else {
                continue;
            };
            match left.raw_entry_mut().from_key(value) {
                RawEntryMut::Occupied(mut occ) => {
                    occ.get_mut().push(log);
                }
                RawEntryMut::Vacant(vac) => {
                    vac.insert(value.clone(), vec![log]);
                }
            }
        } else {
            let Some(value) = log.get(right_key) else {
                continue;
            };
            match right.raw_entry_mut().from_key(value) {
                RawEntryMut::Occupied(mut occ) => {
                    occ.get_mut().push(log);
                }
                RawEntryMut::Vacant(vac) => {
                    vac.insert(value.clone(), vec![log]);
                }
            }
        }
    }

    (left, right)
}

fn pipe_logiter_to_tx(iter: LogIter, tx: Sender<Log>) -> Result<()> {
    for item in iter {
        match item {
            LogItem::Log(log) => tx.send(log).context("pipe log to tx")?,
            LogItem::Err(e) => return Err(e),
            LogItem::OneRxDone | LogItem::PartialStreamLog(..) | LogItem::PartialStreamDone(..) => {
            }
        }
    }
    Ok(())
}

pub struct DynamicFilterTx {
    tx: Watch<FilterAst>,
    is_left: bool,
    field: String,
    values: HashSet<Value>,
}

impl DynamicFilterTx {
    pub fn new(tx: Watch<FilterAst>, is_left: bool, field: String) -> Self {
        Self {
            tx,
            is_left,
            field,
            values: HashSet::new(),
        }
    }

    fn send(self) {
        self.tx.set(FilterAst::In(
            Box::new(FilterAst::Id(self.field)),
            self.values.into_iter().map(FilterAst::Lit).collect(),
        ))
    }
}

struct JoinCollectorIter {
    rxs: Vec<Receiver<Log>>,
    is_left: Vec<bool>,
    dynamic_filter_tx: Option<DynamicFilterTx>,
}

impl JoinCollectorIter {
    fn new(
        left: Vec<Receiver<Log>>,
        right: Vec<Receiver<Log>>,
        dynamic_filter_tx: Option<DynamicFilterTx>,
    ) -> Self {
        let mut is_left = Vec::with_capacity(left.len() + right.len());
        is_left.extend(iter::repeat_n(true, left.len()));
        is_left.extend(iter::repeat_n(false, right.len()));

        let mut rxs = Vec::with_capacity(left.len() + right.len());
        rxs.extend(left);
        rxs.extend(right);

        Self {
            rxs,
            is_left,
            dynamic_filter_tx,
        }
    }

    fn remove(&mut self, i: usize) {
        self.rxs.swap_remove(i);
        self.is_left.swap_remove(i);

        if let Some(DynamicFilterTx { is_left, .. }) = self.dynamic_filter_tx.as_ref() {
            let should_send = if *is_left {
                !self.is_left.iter().any(|x| *x)
            } else {
                self.is_left.iter().all(|x| *x)
            };

            if should_send {
                if let Some(dynamic_filter) = self.dynamic_filter_tx.take() {
                    dynamic_filter.send();
                }
            }
        }
    }

    fn handle_log(&mut self, idx: usize, log: Log) -> (bool, Log) {
        let is_left = self.is_left[idx];

        if let Some(DynamicFilterTx {
            is_left: is_left_dynamic_filter,
            field,
            values,
            ..
        }) = &mut self.dynamic_filter_tx
        {
            if is_left == *is_left_dynamic_filter {
                if let Some(value) = log.get(field) {
                    values.insert(value.clone());
                }
            }
        }

        (is_left, log)
    }
}

impl Iterator for JoinCollectorIter {
    type Item = (bool, Log);

    fn next(&mut self) -> Option<Self::Item> {
        if self.rxs.is_empty() {
            return None;
        }

        let mut i = 0;
        while i < self.rxs.len() {
            match self.rxs[i].try_recv() {
                Ok(log) => return Some(self.handle_log(i, log)),
                Err(TryRecvError::Disconnected) => {
                    self.remove(i);
                    if self.rxs.is_empty() {
                        assert!(self.is_left.is_empty());
                        return None;
                    }
                }
                Err(TryRecvError::Empty) => {
                    i += 1;
                }
            }
        }

        let mut alive = self.rxs.len();
        while alive != 0 {
            let mut selector = Selector::new();
            for (i, rx) in self.rxs.iter().enumerate() {
                selector = selector.recv(rx, move |result| (i, result));
            }

            let (i, result) = selector.wait();
            match result {
                Ok(log) => return Some(self.handle_log(i, log)),
                Err(RecvError::Disconnected) => {
                    self.remove(i);
                    alive -= 1;
                }
            }
        }

        None
    }
}

pub fn join_iter(config: Join, iter: impl Iterator<Item = (bool, Log)>, tx: Sender<Log>) {
    let type_ = config.type_;
    match type_ {
        JoinType::Inner => {
            let (build, probe, flip) = collect_to_build_and_probe(config, iter);

            let mut build_map: HashMap<Value, Vec<Log>> = HashMap::new();
            for (key, log) in build {
                build_map.entry(key).or_default().push(log);
            }
            hash_inner_join(tx, build_map, probe, flip);
        }
        JoinType::Outer => {
            let (build, probe, flip) = collect_to_build_and_probe(config, iter);

            let mut build_map: HashMap<Value, (Vec<Log>, bool)> = HashMap::new();
            for (key, log) in build {
                build_map.entry(key).or_default().0.push(log);
            }
            hash_outer_join(tx, build_map, probe, flip);
        }
        JoinType::Left | JoinType::Right => {
            let (mut left, mut right) = collect_to_hash_maps(config, iter);
            if matches!(type_, JoinType::Right) {
                std::mem::swap(&mut left, &mut right);
            }
            hash_left_join(tx, left, right);
        }
    }
}

fn spawn_join_thread(
    config: Join,
    left_rxs: Vec<Receiver<Log>>,
    right_rxs: Vec<Receiver<Log>>,
    tx: Sender<Log>,
    dynamic_filter_tx: Option<DynamicFilterTx>,
    cancel: CancellationToken,
) -> ThreadRx {
    spawn(
        move || {
            let iter = CancelIter::new(
                JoinCollectorIter::new(left_rxs, right_rxs, dynamic_filter_tx),
                cancel,
            );
            join_iter(config, iter, tx);
            Ok(())
        },
        "join",
    )
}

fn workflow_rx_to_rxs(rx: WorkflowRx) -> (Vec<Receiver<Log>>, Option<ThreadRx>) {
    match rx {
        WorkflowRx::None => panic!("join cannot be the first step"),
        WorkflowRx::Pipeline(iter) => {
            let (tx, rx) = flume::bounded(1);
            let iter = SendOnce::new(iter);
            let thread = spawn(move || pipe_logiter_to_tx(iter.take(), tx), "join-pipe");
            (vec![rx], Some(thread))
        }
        WorkflowRx::MuxPipelines(rxs) => (rxs, None),
    }
}

fn join_rx_partitioned(
    config: Join,
    left: WorkflowRx,
    right_rxs: Vec<Receiver<Log>>,
    dynamic_filter_tx: Option<DynamicFilterTx>,
    partitions: usize,
    cancel: CancellationToken,
) -> (Receiver<Log>, Vec<ThreadRx>) {
    let mut threads = Vec::with_capacity(partitions + 2);

    let (left_rxs, pipe_thread) = workflow_rx_to_rxs(left);
    if let Some(thread) = pipe_thread {
        threads.push(thread);
    }

    let mut left_txs = Vec::with_capacity(partitions);
    let mut right_txs = Vec::with_capacity(partitions);

    let (output_tx, output_rx) = flume::bounded(1);

    for _ in 0..partitions {
        let (left_tx, left_rx) = flume::bounded(1);
        let (right_tx, right_rx) = flume::bounded(1);

        let config = config.clone();
        let output_tx = output_tx.clone();
        let thread = spawn_join_thread(
            config,
            vec![left_rx],
            vec![right_rx],
            output_tx,
            None,
            cancel.clone(),
        );

        threads.push(thread);
        left_txs.push(left_tx);
        right_txs.push(right_tx);
    }

    threads.push(spawn(
        move || {
            let iter = CancelIter::new(
                JoinCollectorIter::new(left_rxs, right_rxs, dynamic_filter_tx),
                cancel,
            );

            let (left_key, right_key) = &config.on;
            let build_hasher = DefaultHashBuilder::default();

            for (is_left, log) in iter {
                if is_left {
                    if let Some(value) = log.get(left_key) {
                        let i = (build_hasher.hash_one(value) % partitions as u64) as usize;
                        if let Err(e) = left_txs[i].send(log) {
                            debug!("Closing partition join step ({i}): {e:?}");
                            break;
                        }
                    }
                } else if let Some(value) = log.get(right_key) {
                    let i = (build_hasher.hash_one(value) % partitions as u64) as usize;
                    if let Err(e) = right_txs[i].send(log) {
                        debug!("Closing partition join step ({i}): {e:?}");
                        break;
                    }
                }
            }

            Ok(())
        },
        "join-partition-hasher",
    ));

    (output_rx, threads)
}

fn join_rx_non_partitioned(
    config: Join,
    left: WorkflowRx,
    right_rxs: Vec<Receiver<Log>>,
    dynamic_filter_tx: Option<DynamicFilterTx>,
    cancel: CancellationToken,
) -> (Receiver<Log>, Vec<ThreadRx>) {
    let mut threads = Vec::new();
    let (left_rxs, pipe_thread) = workflow_rx_to_rxs(left);
    if let Some(thread) = pipe_thread {
        threads.push(thread);
    }

    let (tx, rx) = flume::bounded(1);
    threads.push(spawn_join_thread(
        config,
        left_rxs,
        right_rxs,
        tx,
        dynamic_filter_tx,
        cancel,
    ));
    (rx, threads)
}

pub fn join_rx(
    config: Join,
    left: WorkflowRx,
    right_rxs: Vec<Receiver<Log>>,
    dynamic_filter_tx: Option<DynamicFilterTx>,
    cancel: CancellationToken,
) -> (Receiver<Log>, Vec<ThreadRx>) {
    let partitions = config.partitions;
    if partitions > 1 {
        join_rx_partitioned(
            config,
            left,
            right_rxs,
            dynamic_filter_tx,
            partitions,
            cancel,
        )
    } else {
        join_rx_non_partitioned(config, left, right_rxs, dynamic_filter_tx, cancel)
    }
}
