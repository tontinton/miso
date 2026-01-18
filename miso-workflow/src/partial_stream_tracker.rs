//! Coordinates partial stream results across multiple sources in a union.
//!
//! When a `top 10` sits downstream of a union, it receives partial results from
//! multiple sources. Each source emits partial stream IDs (0, 1, 2, ...) independently.
//! Before emitting our own partial stream ID 0, we need to wait for *all* sources
//! to finish their partial stream ID 0 - otherwise we'd emit incomplete snapshots.
//!
//! Sources can progress at different speeds. If source A is on partial stream ID 2
//! and source B is on 10, we track that we're waiting for ID 3 from A and ID 11 from B.
//! When a late source appears, we register existing sources as pending for all IDs
//! they might still produce.
//!
//! Sources that don't support partial streams (like Elasticsearch) never register
//! here, so they don't block anything.

use hashbrown::{HashMap, HashSet};
use miso_workflow_types::log::{PartialStreamKey, SourceId, next_source_id};

pub struct PartialStreamTracker<S> {
    own_source_id: SourceId,

    /// Per partial stream id: the state.
    states: HashMap<usize, S>,

    /// Per partial stream id: sources that haven't finished this ID yet.
    pending: HashMap<usize, HashSet<SourceId>>,

    /// Per source: the next partial_stream_id it might produce.
    next_expected_id: HashMap<SourceId, usize>,
}

impl<S> PartialStreamTracker<S> {
    pub fn new() -> Self {
        Self {
            own_source_id: next_source_id(),
            states: HashMap::new(),
            pending: HashMap::new(),
            next_expected_id: HashMap::new(),
        }
    }

    /// Register a source's contribution to a partial stream ID and return the shared state.
    /// If this source is new, we also mark existing sources as pending for all IDs between
    /// their next expected and this one - they might still produce results for those.
    pub fn get_or_create_state<F>(&mut self, key: PartialStreamKey, create: F) -> &mut S
    where
        F: FnOnce() -> S,
    {
        let PartialStreamKey {
            partial_stream_id: pid,
            source_id,
        } = key;

        if !self.next_expected_id.contains_key(&source_id) {
            for (&other_source, &next_id) in &self.next_expected_id {
                for id in next_id..=pid {
                    self.pending.entry(id).or_default().insert(other_source);
                }
            }
            self.next_expected_id.insert(source_id, pid);
        }

        self.pending.entry(pid).or_default().insert(source_id);

        let next = self.next_expected_id.get_mut(&source_id).unwrap();
        *next = (*next).max(pid + 1);

        self.states.entry(pid).or_insert_with(create)
    }

    /// A source finished its partial stream ID. Returns the aggregated state only when
    /// all sources have finished that ID - otherwise returns None.
    pub fn mark_done(&mut self, key: PartialStreamKey) -> Option<(S, PartialStreamKey)> {
        let pending = self.pending.get_mut(&key.partial_stream_id)?;
        pending.remove(&key.source_id);

        if !pending.is_empty() {
            return None;
        }

        self.pending.remove(&key.partial_stream_id);
        let state = self.states.remove(&key.partial_stream_id)?;

        Some((
            state,
            PartialStreamKey {
                partial_stream_id: key.partial_stream_id,
                source_id: self.own_source_id,
            },
        ))
    }

    /// A source finished completely. Remove it from all pending streams and return
    /// any streams that are now complete.
    pub fn finish_source(&mut self, source_id: SourceId) -> Vec<(S, PartialStreamKey)> {
        for pending in self.pending.values_mut() {
            pending.remove(&source_id);
        }

        let own_source_id = self.own_source_id;
        let mut result = Vec::new();
        self.pending.retain(|&id, pending| {
            if !pending.is_empty() {
                return true;
            }

            if let Some(state) = self.states.remove(&id) {
                result.push((
                    state,
                    PartialStreamKey {
                        partial_stream_id: id,
                        source_id: own_source_id,
                    },
                ));
            }

            false
        });

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(partial_stream_id: usize, source_id: SourceId) -> PartialStreamKey {
        PartialStreamKey {
            partial_stream_id,
            source_id,
        }
    }

    #[test]
    fn single_source() {
        let mut t: PartialStreamTracker<u32> = PartialStreamTracker::new();
        *t.get_or_create_state(key(0, 1), || 10) += 1;
        assert_eq!(t.mark_done(key(0, 1)).unwrap().0, 11);
    }

    #[test]
    fn two_sources_waits_for_both() {
        let mut t: PartialStreamTracker<u32> = PartialStreamTracker::new();
        *t.get_or_create_state(key(0, 1), || 0) += 1;
        *t.get_or_create_state(key(0, 2), || 0) += 10;

        assert!(t.mark_done(key(0, 1)).is_none());
        assert_eq!(t.mark_done(key(0, 2)).unwrap().0, 11);
    }

    #[test]
    fn finish_source_releases_without_mark_done() {
        let mut t: PartialStreamTracker<u32> = PartialStreamTracker::new();
        *t.get_or_create_state(key(0, 1), || 0) += 1;
        *t.get_or_create_state(key(0, 2), || 0) += 10;

        assert!(t.mark_done(key(0, 1)).is_none());
        assert_eq!(t.finish_source(2), vec![(11, key(0, t.own_source_id))]);
    }

    #[test]
    fn late_source_blocks_earlier_ids() {
        let mut t: PartialStreamTracker<u32> = PartialStreamTracker::new();

        *t.get_or_create_state(key(0, 1), || 0) += 1;
        assert!(t.mark_done(key(0, 1)).is_some());

        *t.get_or_create_state(key(1, 1), || 0) += 1;
        *t.get_or_create_state(key(1, 2), || 0) += 10;

        assert!(t.mark_done(key(1, 1)).is_none());
        assert_eq!(t.mark_done(key(1, 2)).unwrap().0, 11);
    }

    #[test]
    fn finish_source_never_contributes_does_not_block() {
        let mut t: PartialStreamTracker<u32> = PartialStreamTracker::new();
        *t.get_or_create_state(key(0, 1), || 5) += 1;

        assert!(t.finish_source(2).is_empty());

        assert_eq!(t.mark_done(key(0, 1)).unwrap().0, 6);
    }

    #[test]
    fn finish_source_drains_multiple() {
        let mut t: PartialStreamTracker<u32> = PartialStreamTracker::new();
        *t.get_or_create_state(key(0, 1), || 0) = 10;
        *t.get_or_create_state(key(1, 1), || 0) = 20;
        *t.get_or_create_state(key(0, 2), || 0) += 1;
        *t.get_or_create_state(key(1, 2), || 0) += 2;

        t.mark_done(key(0, 1));
        t.mark_done(key(1, 1));

        let mut drained: Vec<_> = t.finish_source(2).into_iter().map(|(s, _)| s).collect();
        drained.sort();
        assert_eq!(drained, vec![11, 22]);
    }

    #[test]
    fn finish_source_is_idempotent() {
        let mut t: PartialStreamTracker<u32> = PartialStreamTracker::new();
        *t.get_or_create_state(key(0, 1), || 0) = 5;
        *t.get_or_create_state(key(0, 2), || 0) = 10;

        t.mark_done(key(0, 1));
        assert_eq!(t.finish_source(2).len(), 1);
        assert!(t.finish_source(2).is_empty());
        assert!(t.finish_source(2).is_empty());
    }

    #[test]
    fn interleaved_partial_stream_ids() {
        let mut t: PartialStreamTracker<u32> = PartialStreamTracker::new();

        *t.get_or_create_state(key(0, 1), || 0) = 1;
        *t.get_or_create_state(key(1, 1), || 0) = 10;
        *t.get_or_create_state(key(0, 2), || 0) += 2;
        *t.get_or_create_state(key(1, 2), || 0) += 20;

        assert!(t.mark_done(key(1, 1)).is_none());
        assert_eq!(t.mark_done(key(0, 1)), None);
        assert_eq!(t.mark_done(key(0, 2)).unwrap().0, 3);
        assert_eq!(t.mark_done(key(1, 2)).unwrap().0, 30);
    }
}
