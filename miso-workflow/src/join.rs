//! Hash join supporting inner, outer, left, and right joins. Can partition across threads
//! for parallelism. Supports dynamic filters to push join keys to the other side. Memory limited.

use std::hash::BuildHasher;
use std::iter;

use bytesize::ByteSize;
use color_eyre::{Result, eyre::Context};
use crossbeam_utils::Backoff;
use flume::{Receiver, RecvError, Sender, TryRecvError};
use futures_lite::future::block_on;
use futures_util::future::select_all;
use hashbrown::{DefaultHashBuilder, HashMap, HashSet, hash_map::RawEntryMut};
use miso_common::{
    metrics::{METRICS, STEP_JOIN},
    watch::Watch,
};
use miso_workflow_types::{
    expr::Expr,
    field::Field,
    join::{Join, JoinType},
    log::{Log, LogItem, LogIter},
    value::{Entry, Value},
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::{
    CHANNEL_CAPACITY, WorkflowRx,
    cancel_iter::CancelIter,
    interpreter::get_field_value,
    memory_size::MemorySize,
    spawn_thread::{ThreadRx, spawn},
};

const MERGED_LEFT_SUFFIX: &str = "_left";
const MERGED_RIGHT_SUFFIX: &str = "_right";

#[derive(Debug, Error)]
pub enum JoinError {
    #[error(
        "Join operation exceeded memory limit: estimated {estimated} memory usage but limit is {limit}. Consider adding filters to reduce the dataset size or reducing the number of partitions."
    )]
    MemoryLimitExceeded {
        estimated: ByteSize,
        limit: ByteSize,
    },
}

macro_rules! send_ret_on_err {
    ($tx:expr, $log:expr) => {
        if let Err(e) = $tx.send($log) {
            debug!("Join tx closed: {e:?}");
            return;
        }
    };
}

fn merge_left_with_right(join_value: &Value, mut left: Log, right: Log) -> Log {
    for (key, value) in right {
        match left.entry(key) {
            Entry::Occupied(entry) => {
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
            Entry::Vacant(entry) => {
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
type HashLogMap = HashMap<Value, Vec<Log>>;

fn collect_to_build_and_probe(
    config: Join,
    iter: impl Iterator<Item = (bool, Log)>,
    memory_limit: Option<u64>,
) -> Result<(VecLogMap, VecLogMap, bool)> {
    let mut left: Vec<(Value, Log)> = Vec::new();
    let mut right: Vec<(Value, Log)> = Vec::new();
    let (left_key, right_key) = &config.on;

    let mut current_memory = 0u64;

    for (is_left, log) in iter {
        let (vec, key) = if is_left {
            (&mut left, left_key)
        } else {
            (&mut right, right_key)
        };

        let Some(value) = get_field_value(&log, key) else {
            continue;
        };

        let old_cap = vec.capacity();
        vec.push((value.clone(), log.clone()));

        let mut delta = value.estimate_memory_size()
            + log.estimate_memory_size()
            + std::mem::size_of::<(Value, Log)>();

        let new_cap = vec.capacity();
        if new_cap > old_cap {
            delta += (new_cap - old_cap) * std::mem::size_of::<(Value, Log)>();
        }

        current_memory += delta as u64;
        if let Some(limit) = memory_limit {
            check_memory_limit(current_memory, limit)?;
        }
    }

    let (build, probe, flip) = if left.len() <= right.len() {
        (left, right, true)
    } else {
        (right, left, false)
    };

    Ok((build, probe, flip))
}

fn collect_to_hash_maps(
    config: Join,
    iter: impl Iterator<Item = (bool, Log)>,
    memory_limit: Option<u64>,
) -> Result<(HashLogMap, HashLogMap)> {
    let mut left = HashMap::<Value, Vec<Log>>::new();
    let mut right = HashMap::<Value, Vec<Log>>::new();
    let (left_key, right_key) = &config.on;

    let mut current_memory: u64 = 0;

    for (is_left, log) in iter {
        let (map, key) = if is_left {
            (&mut left, left_key)
        } else {
            (&mut right, right_key)
        };

        let Some(value) = get_field_value(&log, key) else {
            continue;
        };

        let log_mem = log.estimate_memory_size();

        let delta = match map.raw_entry_mut().from_key(value) {
            RawEntryMut::Occupied(mut occ) => {
                let vec = occ.get_mut();
                let old_cap = vec.capacity();
                vec.push(log);

                let mut d = log_mem;
                let new_cap = vec.capacity();
                if new_cap > old_cap {
                    d += (new_cap - old_cap) * std::mem::size_of::<Log>();
                }
                d
            }

            RawEntryMut::Vacant(vac) => {
                vac.insert(value.clone(), vec![log.clone()]);

                value.estimate_memory_size()
                    + log_mem
                    + std::mem::size_of::<Vec<Log>>()
                    + std::mem::size_of::<(Value, Vec<Log>)>()
                    + std::mem::size_of::<Log>() // initial capacity = 1
                    + 24 // hashmap overhead
            }
        };

        current_memory += delta as u64;
        if let Some(limit) = memory_limit {
            check_memory_limit(current_memory, limit)?;
        }
    }

    Ok((left, right))
}

fn pipe_logiter_to_tx(iter: LogIter, tx: Sender<Log>) -> Result<()> {
    for item in iter {
        match item {
            LogItem::Log(log) => tx.send(log).context("pipe log to tx")?,
            LogItem::Err(e) => return Err(e),
            LogItem::SourceDone(..)
            | LogItem::PartialStreamLog(..)
            | LogItem::PartialStreamDone(..) => {}
        }
    }
    Ok(())
}

pub struct DynamicFilterTx {
    tx: Watch<Expr>,
    is_left: bool,
    field: Field,
    add_not_to_dynamic_filter: bool,
    values: HashSet<Value>,
}

impl DynamicFilterTx {
    pub fn new(
        tx: Watch<Expr>,
        is_left: bool,
        field: Field,
        add_not_to_dynamic_filter: bool,
    ) -> Self {
        Self {
            tx,
            is_left,
            field,
            add_not_to_dynamic_filter,
            values: HashSet::new(),
        }
    }

    fn send(self) {
        let mut expr = Expr::In(
            Box::new(Expr::Field(self.field)),
            self.values.into_iter().map(Expr::Literal).collect(),
        );
        if self.add_not_to_dynamic_filter {
            expr = Expr::Not(Box::new(expr));
        }
        self.tx.set(expr);
    }
}

struct JoinCollectorIter {
    rxs: Vec<Receiver<Log>>,
    is_left: Vec<bool>,
    dynamic_filter_tx: Option<DynamicFilterTx>,
}

enum TryRecv {
    Item((bool, Log)),
    Exhausted,
    None,
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

            if should_send && let Some(dynamic_filter) = self.dynamic_filter_tx.take() {
                dynamic_filter.send();
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
            && is_left == *is_left_dynamic_filter
            && let Some(value) = get_field_value(&log, field)
        {
            values.insert(value.clone());
        }

        (is_left, log)
    }

    fn try_recv(&mut self) -> TryRecv {
        let mut i = 0;
        while i < self.rxs.len() {
            match self.rxs[i].try_recv() {
                Ok(log) => return TryRecv::Item(self.handle_log(i, log)),
                Err(TryRecvError::Disconnected) => {
                    self.remove(i);
                    if self.rxs.is_empty() {
                        assert!(self.is_left.is_empty());
                        return TryRecv::Exhausted;
                    }
                }
                Err(TryRecvError::Empty) => {
                    i += 1;
                }
            }
        }
        TryRecv::None
    }
}

impl Iterator for JoinCollectorIter {
    type Item = (bool, Log);

    fn next(&mut self) -> Option<Self::Item> {
        while !self.rxs.is_empty() {
            let backoff = Backoff::new();
            while !backoff.is_completed() {
                match self.try_recv() {
                    TryRecv::Item(item) => return Some(item),
                    TryRecv::Exhausted => return None,
                    TryRecv::None => {}
                }
                backoff.snooze();
            }

            let futs: Vec<_> = self.rxs.iter().map(|rx| rx.recv_async()).collect();
            let (result, i, _) = block_on(select_all(futs));
            match result {
                Ok(log) => return Some(self.handle_log(i, log)),
                Err(RecvError::Disconnected) => {
                    self.remove(i);
                }
            }
        }

        None
    }
}

fn check_memory_limit(current_memory: u64, limit: u64) -> Result<()> {
    if current_memory > limit {
        return Err(JoinError::MemoryLimitExceeded {
            estimated: ByteSize::b(current_memory),
            limit: ByteSize(limit),
        }
        .into());
    }
    Ok(())
}

fn build_hash_map<V, FVec>(
    build: impl IntoIterator<Item = (Value, Log)>,
    mut make_value: impl FnMut(Log) -> V,
    mut get_vec: FVec,
    memory_limit: Option<u64>,
    extra_entry_overhead: usize,
) -> Result<HashMap<Value, V>>
where
    FVec: FnMut(&mut V) -> &mut Vec<Log>,
{
    let mut map = HashMap::new();
    let mut current_memory = 0u64;

    for (key, log) in build {
        let log_mem = log.estimate_memory_size();

        let delta = match map.raw_entry_mut().from_key(&key) {
            RawEntryMut::Occupied(mut occ) => {
                let vec = get_vec(occ.get_mut());
                let old_cap = vec.capacity();
                vec.push(log);

                let mut d = log_mem;
                let new_cap = vec.capacity();
                if new_cap > old_cap {
                    d += (new_cap - old_cap) * std::mem::size_of::<Log>();
                }
                d
            }

            RawEntryMut::Vacant(vac) => {
                let mut value = make_value(log);
                let vec = get_vec(&mut value);

                let d = key.estimate_memory_size()
                    + log_mem
                    + vec.capacity() * std::mem::size_of::<Log>()
                    + std::mem::size_of::<Vec<Log>>()
                    + extra_entry_overhead
                    + 24;

                vac.insert(key, value);
                d
            }
        };

        current_memory += delta as u64;
        if let Some(limit) = memory_limit {
            check_memory_limit(current_memory, limit)?;
        }
    }

    Ok(map)
}

pub fn join_iter(
    config: Join,
    iter: impl Iterator<Item = (bool, Log)>,
    tx: Sender<Log>,
    memory_limit: Option<u64>,
) -> Result<usize> {
    let type_ = config.type_;

    match type_ {
        JoinType::Inner => {
            let (build, probe, flip) = collect_to_build_and_probe(config, iter, memory_limit)
                .context("inner join collect build and probe")?;
            let rows_processed = build.len() + probe.len();

            let build_map = build_hash_map(
                build,
                |log| vec![log],
                |v| v,
                memory_limit,
                std::mem::size_of::<(Value, Vec<Log>)>(),
            )?;

            hash_inner_join(tx, build_map, probe, flip);
            Ok(rows_processed)
        }

        JoinType::Outer => {
            let (build, probe, flip) = collect_to_build_and_probe(config, iter, memory_limit)
                .context("outer join collect build and probe")?;
            let rows_processed = build.len() + probe.len();

            let build_map = build_hash_map(
                build,
                |log| (vec![log], false),
                |v| &mut v.0,
                memory_limit,
                std::mem::size_of::<(Value, (Vec<Log>, bool))>() + std::mem::size_of::<bool>(),
            )?;

            hash_outer_join(tx, build_map, probe, flip);
            Ok(rows_processed)
        }

        JoinType::Left | JoinType::Right => {
            let (mut left, mut right) = collect_to_hash_maps(config, iter, memory_limit)
                .context("left / right join collect hash maps")?;
            let rows_processed = left.len() + right.len();

            if matches!(type_, JoinType::Right) {
                std::mem::swap(&mut left, &mut right);
            }

            hash_left_join(tx, left, right);
            Ok(rows_processed)
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
    memory_limit: Option<u64>,
) -> ThreadRx {
    spawn(
        move || {
            let iter = CancelIter::new(
                JoinCollectorIter::new(left_rxs, right_rxs, dynamic_filter_tx),
                cancel,
            );
            let rows_processed = join_iter(config, iter, tx, memory_limit)?;

            METRICS
                .workflow_step_rows
                .with_label_values(&[STEP_JOIN])
                .inc_by(rows_processed as u64);

            Ok(())
        },
        "join",
    )
}

fn workflow_rx_to_rxs(rx: WorkflowRx) -> (Vec<Receiver<Log>>, Option<ThreadRx>) {
    match rx {
        WorkflowRx::None => panic!("join cannot be the first step"),
        WorkflowRx::Pipeline(creator) => {
            let (tx, rx) = flume::bounded(CHANNEL_CAPACITY);
            let thread = spawn(
                move || pipe_logiter_to_tx(creator.create(), tx),
                "join-pipe",
            );
            (vec![rx], Some(thread))
        }
        WorkflowRx::MuxPipelines(rxs, _source_ids) => (rxs, None),
    }
}

fn join_rx_partitioned(
    config: Join,
    left: WorkflowRx,
    right_rxs: Vec<Receiver<Log>>,
    dynamic_filter_tx: Option<DynamicFilterTx>,
    partitions: usize,
    cancel: CancellationToken,
    memory_limit: Option<u64>,
) -> (Receiver<Log>, Vec<ThreadRx>) {
    let mut threads = Vec::with_capacity(partitions + 2);

    let (left_rxs, pipe_thread) = workflow_rx_to_rxs(left);
    if let Some(thread) = pipe_thread {
        threads.push(thread);
    }

    let mut left_txs = Vec::with_capacity(partitions);
    let mut right_txs = Vec::with_capacity(partitions);

    let (output_tx, output_rx) = flume::bounded(CHANNEL_CAPACITY);

    for _ in 0..partitions {
        let (left_tx, left_rx) = flume::bounded(CHANNEL_CAPACITY);
        let (right_tx, right_rx) = flume::bounded(CHANNEL_CAPACITY);

        let config = config.clone();
        let output_tx = output_tx.clone();
        let thread = spawn_join_thread(
            config,
            vec![left_rx],
            vec![right_rx],
            output_tx,
            None,
            cancel.clone(),
            memory_limit,
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
                    if let Some(value) = get_field_value(&log, left_key) {
                        let i = (build_hasher.hash_one(value) % partitions as u64) as usize;
                        if let Err(e) = left_txs[i].send(log) {
                            debug!("Closing partition join step ({i}): {e:?}");
                            break;
                        }
                    }
                } else if let Some(value) = get_field_value(&log, right_key) {
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
    memory_limit: Option<u64>,
) -> (Receiver<Log>, Vec<ThreadRx>) {
    let mut threads = Vec::new();
    let (left_rxs, pipe_thread) = workflow_rx_to_rxs(left);
    if let Some(thread) = pipe_thread {
        threads.push(thread);
    }

    let (tx, rx) = flume::bounded(CHANNEL_CAPACITY);
    threads.push(spawn_join_thread(
        config,
        left_rxs,
        right_rxs,
        tx,
        dynamic_filter_tx,
        cancel,
        memory_limit,
    ));
    (rx, threads)
}

pub fn join_rx(
    config: Join,
    left: WorkflowRx,
    right_rxs: Vec<Receiver<Log>>,
    dynamic_filter_tx: Option<DynamicFilterTx>,
    cancel: CancellationToken,
    memory_limit: Option<u64>,
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
            memory_limit,
        )
    } else {
        join_rx_non_partitioned(
            config,
            left,
            right_rxs,
            dynamic_filter_tx,
            cancel,
            memory_limit,
        )
    }
}
