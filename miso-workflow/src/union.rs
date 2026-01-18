use crossbeam_utils::Backoff;
use flume::{Receiver, RecvError, TryRecvError};
use futures_lite::future::block_on;
use futures_util::future::select_all;
use miso_workflow_types::log::{Log, LogItem, SourceId};

pub struct UnionIter {
    rxs: Vec<Receiver<Log>>,
    source_ids: Vec<SourceId>,
}

enum TryRecv {
    Item(LogItem),
    Exhausted,
    None,
}

impl UnionIter {
    pub fn new(rxs: Vec<Receiver<Log>>, source_ids: Vec<SourceId>) -> Self {
        assert_eq!(rxs.len(), source_ids.len());
        Self { rxs, source_ids }
    }

    fn try_recv(&mut self) -> TryRecv {
        for i in 0..self.rxs.len() {
            match self.rxs[i].try_recv() {
                Ok(log) => return TryRecv::Item(LogItem::Log(log)),
                Err(TryRecvError::Disconnected) => {
                    let source_id = self.source_ids.swap_remove(i);
                    self.rxs.swap_remove(i);
                    if self.rxs.is_empty() {
                        return TryRecv::Exhausted;
                    }
                    return TryRecv::Item(LogItem::SourceDone(source_id));
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
                let source_id = self.source_ids.swap_remove(i);
                self.rxs.swap_remove(i);
                (!self.rxs.is_empty()).then_some(LogItem::SourceDone(source_id))
            }
        }
    }
}
