use std::{
    iter,
    time::{Duration, Instant},
};

use miso_common::humantime_utils::deserialize_duration;
use miso_workflow_types::{
    json,
    log::{Log, LogItem, LogIter},
    value::{Map, Value},
};
use serde::Deserialize;

use super::MISO_METADATA_FIELD_NAME;

const PARTIAL_STREAM_ID_FIELD_NAME: &str = "id";
pub const PARTIAL_STREAM_DONE_FIELD_NAME: &str = "done";

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
    pub debounce: Duration,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    struct MockPartialIter {
        items: VecDeque<LogItem>,
        partial_count: u64,
    }

    impl MockPartialIter {
        fn new(items: Vec<LogItem>) -> Self {
            Self {
                items: items.into(),
                partial_count: 0,
            }
        }
    }

    impl Iterator for MockPartialIter {
        type Item = LogItem;
        fn next(&mut self) -> Option<Self::Item> {
            let item = self.items.pop_front()?;
            if matches!(item, LogItem::Log(_)) {
                self.partial_count += 1;
            }
            Some(item)
        }
    }

    impl PartialLogIter for MockPartialIter {
        fn get_partial(&self) -> LogIter {
            let mut log = Log::new();
            log.insert("count".into(), Value::from(self.partial_count));
            Box::new(iter::once(LogItem::Log(log)))
        }
    }

    fn no_debounce() -> PartialStream {
        PartialStream {
            debounce: Duration::ZERO,
        }
    }

    fn log(key: &str, val: i64) -> LogItem {
        let mut l = Log::new();
        l.insert(key.into(), Value::from(val));
        LogItem::Log(l)
    }

    #[test]
    fn union_done() {
        let mock = MockPartialIter::new(vec![
            log("x", 1),
            log("x", 2),
            LogItem::UnionSomePipelineDone,
            log("x", 3),
        ]);
        let mut iter = PartialStreamIter::new(Box::new(mock), no_debounce());

        let items: Vec<_> = iter.by_ref().collect();

        assert_eq!(items.len(), 5);
        assert!(matches!(&items[0], LogItem::Log(_)));
        assert!(matches!(&items[1], LogItem::Log(_)));
        assert!(matches!(&items[2], LogItem::PartialStreamLog(_, 0)));
        assert!(matches!(&items[3], LogItem::PartialStreamDone(0)));
        assert!(matches!(&items[4], LogItem::Log(_)));
    }

    #[test]
    fn multiple_unions_increment_ids() {
        let mock = MockPartialIter::new(vec![
            log("x", 1),
            LogItem::UnionSomePipelineDone,
            log("x", 2),
            LogItem::UnionSomePipelineDone,
        ]);
        let mut iter = PartialStreamIter::new(Box::new(mock), no_debounce());

        let items: Vec<_> = iter.by_ref().collect();

        let partial_logs: Vec<_> = items
            .iter()
            .filter_map(|i| match i {
                LogItem::PartialStreamLog(_, id) => Some(*id),
                _ => None,
            })
            .collect();
        let done_markers: Vec<_> = items
            .iter()
            .filter_map(|i| match i {
                LogItem::PartialStreamDone(id) => Some(*id),
                _ => None,
            })
            .collect();

        assert_eq!(partial_logs, vec![0, 1]);
        assert_eq!(done_markers, vec![0, 1]);
    }

    #[test]
    fn debounce_skips_second_done() {
        let mock = MockPartialIter::new(vec![
            log("x", 1),
            LogItem::UnionSomePipelineDone,
            LogItem::UnionSomePipelineDone,
        ]);
        let iter = PartialStreamIter::new(
            Box::new(mock),
            PartialStream {
                debounce: Duration::from_secs(30),
            },
        );

        let items: Vec<_> = iter.collect();

        let partial_count = items
            .iter()
            .filter(|i| matches!(i, LogItem::PartialStreamLog(..)))
            .count();
        assert_eq!(partial_count, 1);
    }
}
