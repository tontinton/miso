use std::{
    iter,
    time::{Duration, Instant},
};

use crossbeam_utils::Backoff;
use flume::{Receiver, RecvError, TryRecvError};
use futures_lite::future::block_on;
use futures_util::future::select_all;
use miso_common::humantime_utils::deserialize_duration;
use miso_workflow_types::{
    json,
    log::{Log, LogItem, LogIter},
    value::{Map, Value},
};
use serde::Deserialize;

use super::MISO_METADATA_FIELD_NAME;

const PARTIAL_STREAM_ID_FIELD_NAME: &str = "id";
const PARTIAL_STREAM_DONE_FIELD_NAME: &str = "done";

fn default_debounce() -> Duration {
    Duration::from_secs(1)
}

#[derive(Debug, Clone, Deserialize)]
pub struct PartialStream {
    /// If a split is currently streaming partial results, and another finishes soon after (less
    /// than the debounce), the partial results of the second iteration won't be sent.
    #[serde(
        default = "default_debounce",
        deserialize_with = "deserialize_duration"
    )]
    debounce: Duration,
}

pub trait PartialLogIter: Iterator<Item = LogItem> {
    fn get_partial(&self) -> LogIter;
}

pub fn add_partial_stream_id(mut log: Log, id: usize) -> Log {
    log.entry(MISO_METADATA_FIELD_NAME.to_string())
        .or_insert_with(|| Value::Object(Map::new()))
        .as_object_mut()
        .unwrap()
        .insert(PARTIAL_STREAM_ID_FIELD_NAME.to_string(), Value::from(id));
    log
}

pub fn build_partial_stream_id_done_log(id: usize) -> Log {
    let mut log = Map::new();
    log.insert(
        MISO_METADATA_FIELD_NAME.to_string(),
        json!({
            PARTIAL_STREAM_ID_FIELD_NAME: id,
            PARTIAL_STREAM_DONE_FIELD_NAME: true,
        }),
    );
    log
}

pub struct UnionIter {
    rxs: Vec<Receiver<Log>>,
}

enum TryRecv {
    Item(LogItem),
    Exhausted,
    None,
}

impl UnionIter {
    pub fn new(rxs: Vec<Receiver<Log>>) -> Self {
        Self { rxs }
    }

    fn try_recv(&mut self) -> TryRecv {
        for i in 0..self.rxs.len() {
            match self.rxs[i].try_recv() {
                Ok(log) => return TryRecv::Item(LogItem::Log(log)),
                Err(TryRecvError::Disconnected) => {
                    self.rxs.swap_remove(i);
                    if self.rxs.is_empty() {
                        return TryRecv::Exhausted;
                    }
                    return TryRecv::Item(LogItem::UnionSomePipelineDone);
                }
                Err(TryRecvError::Empty) => {}
            }
        }
        TryRecv::None
    }
}

impl Iterator for UnionIter {
    type Item = LogItem;

    fn next(&mut self) -> Option<Self::Item> {
        if self.rxs.is_empty() {
            return None;
        }

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
            Ok(log) => Some(LogItem::Log(log)),
            Err(RecvError::Disconnected) => {
                self.rxs.swap_remove(i);
                (!self.rxs.is_empty()).then_some(LogItem::UnionSomePipelineDone)
            }
        }
    }
}

pub struct PartialStreamIter {
    input: Box<dyn PartialLogIter>,
    config: PartialStream,
    id: usize,
    partial_iter: LogIter,
    partial_iter_start: Option<Instant>,
    debounced_partial_iter: Option<(Instant, LogIter)>,
}

impl PartialStreamIter {
    pub fn new(input: Box<dyn PartialLogIter>, config: PartialStream) -> Self {
        Self {
            input,
            config,
            id: 0,
            partial_iter: Box::new(iter::empty()),
            partial_iter_start: None,
            debounced_partial_iter: None,
        }
    }

    fn set_partial_iter(&mut self, partial_iter: LogIter, now: Instant) {
        let id = self.id;
        self.id += 1;

        self.partial_iter = Box::new(
            partial_iter
                .map(move |item| item.attach_partial_stream_id(id))
                .chain(iter::once(LogItem::PartialStreamDone(id))),
        );
        self.partial_iter_start = Some(now);
    }

    fn handle_debounced_partial_iter(&mut self) -> bool {
        if let Some((debounced_time, partial_iter)) = self.debounced_partial_iter.take() {
            let now = Instant::now();
            if now >= debounced_time {
                self.set_partial_iter(partial_iter, now);
                return true;
            }
            self.debounced_partial_iter = Some((debounced_time, partial_iter));
        }
        false
    }

    fn update_partial_iter(&mut self) {
        let now = Instant::now();
        let partial_iter = self.input.get_partial();

        let debounced_time = self
            .partial_iter_start
            .filter(|start| now.duration_since(*start) <= self.config.debounce)
            .and_then(|start| start.checked_add(self.config.debounce));

        if let Some(debounced_time) = debounced_time {
            self.debounced_partial_iter = Some((debounced_time, partial_iter));
        } else {
            self.set_partial_iter(partial_iter, now);
        }
    }
}

impl Iterator for PartialStreamIter {
    type Item = LogItem;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(item) = self.partial_iter.next() {
                return Some(item);
            }

            if self.handle_debounced_partial_iter() {
                continue;
            }

            match self.input.next()? {
                LogItem::Log(log) => return Some(LogItem::Log(log)),
                LogItem::Err(e) => return Some(LogItem::Err(e)),
                LogItem::UnionSomePipelineDone => self.update_partial_iter(),
                LogItem::PartialStreamLog(..) | LogItem::PartialStreamDone(..) => {
                    panic!("partial stream items should not reach the partial stream log generator")
                }
            }
        }
    }
}
