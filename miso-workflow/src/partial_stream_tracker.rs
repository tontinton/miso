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
//! Some sources don't support partial streams (like Elasticsearch) - they only contribute
//! to the final state, which gets merged with each partial stream result. When a source
//! finishes and sends final data, any partial streams it contributed to are dropped to
//! avoid duplicates.

use hashbrown::{HashMap, HashSet};
use miso_workflow_types::log::{PartialStreamKey, SourceId, next_source_id};

pub trait Mergeable {
    fn merge(&mut self, other: &Self);
}

impl Mergeable for u64 {
    fn merge(&mut self, other: &Self) {
        *self += other;
    }
}

pub struct PartialStreamTracker<S> {
    own_source_id: SourceId,

    /// The state generated from regular (non-partial-stream) logs.
    final_state: S,

    /// Per partial stream id: the state.
    states: HashMap<usize, S>,

    /// Per partial stream id: sources that haven't finished this ID yet.
    pending: HashMap<usize, HashSet<SourceId>>,

    /// Per source: the next partial_stream_id it might produce.
    next_expected_id: HashMap<SourceId, usize>,

    /// Sources that have finished - their partial streams should be dropped.
    finished_sources: HashSet<SourceId>,

    /// Per partial stream id: all sources that contributed (not cleared on mark_done).
    contributing_sources: HashMap<usize, HashSet<SourceId>>,
}

impl<S: Mergeable> PartialStreamTracker<S> {
    pub fn new(initial: S) -> Self {
        Self {
            own_source_id: next_source_id(),
            final_state: initial,
            states: HashMap::new(),
            pending: HashMap::new(),
            next_expected_id: HashMap::new(),
            finished_sources: HashSet::new(),
            contributing_sources: HashMap::new(),
        }
    }

    pub fn update_final<F>(&mut self, f: F)
    where
        F: FnOnce(&mut S),
    {
        f(&mut self.final_state);
    }

    pub fn final_state(&self) -> &S {
        &self.final_state
    }

    pub fn into_final_state(self) -> S {
        self.final_state
    }

    fn get_merged(&self, partial_state: S) -> S {
        let mut result = partial_state;
        result.merge(&self.final_state);
        result
    }

    /// Register a source's contribution to a partial stream ID and return the shared state.
    /// If this source is new, we also mark existing sources as pending for all IDs between
    /// their next expected and this one - they might still produce results for those.
    ///
    /// If the source has already finished, we don't track it (its partial streams are
    /// superseded by final data), but still return a state for the caller to write to.
    pub fn get_or_create_state<F>(&mut self, key: PartialStreamKey, create: F) -> &mut S
    where
        F: FnOnce() -> S,
    {
        let PartialStreamKey {
            partial_stream_id: pid,
            source_id,
        } = key;

        if self.finished_sources.contains(&source_id) {
            return self.states.entry(pid).or_insert_with(create);
        }

        if !self.next_expected_id.contains_key(&source_id) {
            for (&other_source, &next_id) in &self.next_expected_id {
                for id in next_id..=pid {
                    self.pending.entry(id).or_default().insert(other_source);
                }
            }
        }

        self.pending.entry(pid).or_default().insert(source_id);
        self.contributing_sources
            .entry(pid)
            .or_default()
            .insert(source_id);
        let next = self.next_expected_id.entry(source_id).or_insert(0);
        *next = (*next).max(pid + 1);

        self.states.entry(pid).or_insert_with(create)
    }

    /// A source finished its partial stream ID. Returns the merged state only when
    /// all sources have finished that ID - otherwise returns None.
    pub fn mark_done(&mut self, key: PartialStreamKey) -> Option<(S, PartialStreamKey)> {
        let pending = self.pending.get_mut(&key.partial_stream_id)?;
        pending.remove(&key.source_id);

        if !pending.is_empty() {
            return None;
        }

        self.pending.remove(&key.partial_stream_id);
        self.contributing_sources.remove(&key.partial_stream_id);
        let state = self.states.remove(&key.partial_stream_id)?;
        Some((
            self.get_merged(state),
            PartialStreamKey {
                partial_stream_id: key.partial_stream_id,
                source_id: self.own_source_id,
            },
        ))
    }

    /// A source finished completely. Remove it from all pending streams and return
    /// any streams that are now complete (merged with final state).
    ///
    /// All partial streams this source contributed to are dropped entirely to avoid duplicates with final data.
    pub fn finish_source(&mut self, source_id: SourceId) -> Vec<(S, PartialStreamKey)> {
        self.finished_sources.insert(source_id);
        self.next_expected_id.remove(&source_id);

        let streams_to_drop: Vec<_> = self
            .contributing_sources
            .iter()
            .filter(|(_, sources)| sources.contains(&source_id))
            .map(|(&id, _)| id)
            .collect();

        for id in streams_to_drop {
            self.states.remove(&id);
            self.pending.remove(&id);
            self.contributing_sources.remove(&id);
        }

        for pending in self.pending.values_mut() {
            pending.remove(&source_id);
        }

        let completed_ids: Vec<_> = self
            .pending
            .iter()
            .filter(|(_, p)| p.is_empty())
            .map(|(&id, _)| id)
            .collect();

        let own_source_id = self.own_source_id;
        let mut result = Vec::new();

        for id in completed_ids {
            self.pending.remove(&id);
            self.contributing_sources.remove(&id);
            if let Some(state) = self.states.remove(&id) {
                result.push((
                    self.get_merged(state),
                    PartialStreamKey {
                        partial_stream_id: id,
                        source_id: own_source_id,
                    },
                ));
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl Mergeable for u32 {
        fn merge(&mut self, other: &Self) {
            *self += other;
        }
    }

    fn key(partial_stream_id: usize, source_id: SourceId) -> PartialStreamKey {
        PartialStreamKey {
            partial_stream_id,
            source_id,
        }
    }

    #[test]
    fn two_sources_waits_for_both() {
        let mut t: PartialStreamTracker<u32> = PartialStreamTracker::new(0);
        *t.get_or_create_state(key(0, 1), || 0) += 1;
        *t.get_or_create_state(key(0, 2), || 0) += 10;

        assert!(t.mark_done(key(0, 1)).is_none());
        assert_eq!(t.mark_done(key(0, 2)).unwrap().0, 11);
    }

    #[test]
    fn finish_source_drops_contributed_streams() {
        let mut t: PartialStreamTracker<u32> = PartialStreamTracker::new(0);
        *t.get_or_create_state(key(0, 1), || 0) += 1;
        *t.get_or_create_state(key(1, 1), || 0) += 10;
        *t.get_or_create_state(key(0, 2), || 0) += 100;
        *t.get_or_create_state(key(1, 2), || 0) += 1000;

        t.mark_done(key(0, 1));
        t.mark_done(key(1, 1));

        assert!(t.finish_source(2).is_empty());
        assert!(t.mark_done(key(0, 1)).is_none());
        assert!(t.mark_done(key(1, 1)).is_none());
    }

    #[test]
    fn finish_source_releases_pending_only_streams() {
        let mut t: PartialStreamTracker<u32> = PartialStreamTracker::new(0);

        *t.get_or_create_state(key(0, 1), || 0) += 5;
        *t.get_or_create_state(key(3, 2), || 0) += 10;

        t.mark_done(key(0, 1));
        t.mark_done(key(3, 2));

        let released = t.finish_source(1);
        assert_eq!(released.len(), 1);
        assert_eq!(released[0].0, 10);
    }

    #[test]
    fn late_source_blocks_earlier_ids() {
        let mut t: PartialStreamTracker<u32> = PartialStreamTracker::new(0);

        *t.get_or_create_state(key(0, 1), || 0) += 1;
        t.mark_done(key(0, 1));

        *t.get_or_create_state(key(1, 1), || 0) += 10;
        *t.get_or_create_state(key(1, 2), || 0) += 100;

        assert!(t.mark_done(key(1, 1)).is_none());
        assert_eq!(t.mark_done(key(1, 2)).unwrap().0, 110);
    }

    #[test]
    fn interleaved_partial_stream_ids() {
        let mut t: PartialStreamTracker<u32> = PartialStreamTracker::new(0);

        *t.get_or_create_state(key(0, 1), || 0) = 1;
        *t.get_or_create_state(key(1, 1), || 0) = 10;
        *t.get_or_create_state(key(0, 2), || 0) += 2;
        *t.get_or_create_state(key(1, 2), || 0) += 20;

        assert!(t.mark_done(key(1, 1)).is_none());
        assert!(t.mark_done(key(0, 1)).is_none());
        assert_eq!(t.mark_done(key(0, 2)).unwrap().0, 3);
        assert_eq!(t.mark_done(key(1, 2)).unwrap().0, 30);
    }

    #[test]
    fn merge_final_with_partial() {
        let mut t: PartialStreamTracker<u32> = PartialStreamTracker::new(0);

        t.update_final(|c| *c += 100);
        *t.get_or_create_state(key(0, 1), || 0) += 5;
        *t.get_or_create_state(key(0, 2), || 0) += 10;

        assert!(t.mark_done(key(0, 1)).is_none());
        assert_eq!(t.mark_done(key(0, 2)).unwrap().0, 115);
        assert_eq!(*t.final_state(), 100);
    }

    #[test]
    fn mark_done_after_contributor_finished_returns_none() {
        let mut t: PartialStreamTracker<u32> = PartialStreamTracker::new(0);

        *t.get_or_create_state(key(0, 1), || 0) += 5;
        *t.get_or_create_state(key(0, 2), || 0) += 10;

        t.finish_source(1);

        assert!(t.mark_done(key(0, 2)).is_none());
    }

    #[test]
    fn late_data_from_finished_source_counted_but_not_tracked() {
        let mut t: PartialStreamTracker<u32> = PartialStreamTracker::new(0);

        *t.get_or_create_state(key(0, 1), || 0) += 5;
        t.finish_source(2);
        *t.get_or_create_state(key(0, 2), || 0) += 10;

        assert_eq!(t.mark_done(key(0, 1)).unwrap().0, 15);
    }

    #[test]
    fn finished_source_not_readded_by_late_source() {
        let mut t: PartialStreamTracker<u32> = PartialStreamTracker::new(0);

        *t.get_or_create_state(key(0, 1), || 0) += 5;
        t.mark_done(key(0, 1));
        t.finish_source(1);

        *t.get_or_create_state(key(1, 2), || 0) += 10;

        assert_eq!(t.mark_done(key(1, 2)).unwrap().0, 10);
    }

    #[test]
    fn non_contributing_source_finish_does_not_block() {
        let mut t: PartialStreamTracker<u32> = PartialStreamTracker::new(0);
        *t.get_or_create_state(key(0, 1), || 0) += 5;

        t.finish_source(99);

        assert_eq!(t.mark_done(key(0, 1)).unwrap().0, 5);
    }

    #[test]
    fn no_duplicates_when_source_has_partial_and_final() {
        let mut t: PartialStreamTracker<u32> = PartialStreamTracker::new(0);

        *t.get_or_create_state(key(0, 1), || 0) += 100;
        *t.get_or_create_state(key(1, 1), || 0) += 200;
        t.update_final(|c| *c += 100);

        let released = t.finish_source(1);
        assert!(released.is_empty());
        assert!(t.states.get(&0).is_none());
        assert!(t.states.get(&1).is_none());
        assert!(t.pending.get(&0).is_none());
        assert!(t.pending.get(&1).is_none());
        assert!(t.contributing_sources.is_empty());
        assert_eq!(*t.final_state(), 100);
    }

    #[test]
    fn no_duplicates_mixed_contributing_sources() {
        let mut t: PartialStreamTracker<u32> = PartialStreamTracker::new(0);

        *t.get_or_create_state(key(0, 1), || 0) += 10;
        *t.get_or_create_state(key(0, 2), || 0) += 20;
        *t.get_or_create_state(key(1, 2), || 0) += 100;
        *t.get_or_create_state(key(1, 3), || 0) += 200;

        t.update_final(|c| *c += 50);

        t.mark_done(key(0, 1));
        t.mark_done(key(1, 2));

        let released = t.finish_source(2);
        assert!(released.is_empty());
        assert!(t.states.get(&0).is_none());
        assert!(t.states.get(&1).is_none());

        assert!(t.mark_done(key(0, 2)).is_none());
        assert!(t.mark_done(key(1, 3)).is_none());

        assert_eq!(*t.final_state(), 50);
    }

    #[test]
    fn contributed_stream_dropped_but_pending_only_stream_released() {
        let mut t: PartialStreamTracker<u32> = PartialStreamTracker::new(0);

        *t.get_or_create_state(key(0, 1), || 0) += 10;
        *t.get_or_create_state(key(2, 2), || 0) += 100;

        t.mark_done(key(0, 1));
        t.mark_done(key(2, 2));

        t.update_final(|c| *c += 1000);

        let released = t.finish_source(1);

        assert_eq!(released.len(), 1);
        assert_eq!(released[0].1.partial_stream_id, 2);
        assert_eq!(released[0].0, 1100);

        assert!(t.states.get(&0).is_none());
        assert!(t.pending.get(&0).is_none());
        assert!(t.contributing_sources.get(&0).is_none());
    }

    #[test]
    fn all_contributing_sources_finish_drops_all_partial_streams() {
        let mut t: PartialStreamTracker<u32> = PartialStreamTracker::new(0);

        *t.get_or_create_state(key(0, 1), || 0) += 1;
        *t.get_or_create_state(key(0, 2), || 0) += 2;
        *t.get_or_create_state(key(0, 3), || 0) += 4;

        t.mark_done(key(0, 1));
        t.mark_done(key(0, 2));
        t.mark_done(key(0, 3));

        t.update_final(|c| *c += 100);
        assert!(t.finish_source(1).is_empty());
        assert!(t.states.get(&0).is_none());
        assert!(t.pending.get(&0).is_none());
        assert!(t.contributing_sources.get(&0).is_none());

        *t.get_or_create_state(key(1, 2), || 0) += 10;
        *t.get_or_create_state(key(1, 3), || 0) += 20;
        t.mark_done(key(1, 2));
        t.mark_done(key(1, 3));

        t.update_final(|c| *c += 200);
        assert!(t.finish_source(2).is_empty());
        assert!(t.states.get(&1).is_none());

        *t.get_or_create_state(key(2, 3), || 0) += 1000;
        t.mark_done(key(2, 3));

        t.update_final(|c| *c += 400);
        assert!(t.finish_source(3).is_empty());
        assert!(t.states.is_empty());
        assert!(t.pending.is_empty());
        assert!(t.contributing_sources.is_empty());

        assert_eq!(*t.final_state(), 700);
    }

    #[test]
    fn state_completely_cleaned_after_contributor_finishes() {
        let mut t: PartialStreamTracker<u32> = PartialStreamTracker::new(0);

        for stream_id in 0..5 {
            *t.get_or_create_state(key(stream_id, 1), || 0) += 1;
            *t.get_or_create_state(key(stream_id, 2), || 0) += 10;
            t.mark_done(key(stream_id, 1));
        }

        t.finish_source(2);

        assert!(t.states.is_empty());
        assert!(t.pending.is_empty());
        assert!(t.contributing_sources.is_empty());
        assert!(!t.next_expected_id.contains_key(&2));
        assert!(t.finished_sources.contains(&2));

        for stream_id in 0..5 {
            assert!(t.mark_done(key(stream_id, 2)).is_none());
        }
    }

    #[test]
    fn partial_data_after_source_finished_not_tracked_for_duplicates() {
        let mut t: PartialStreamTracker<u32> = PartialStreamTracker::new(0);

        *t.get_or_create_state(key(0, 1), || 0) += 10;
        t.mark_done(key(0, 1));
        t.finish_source(1);

        *t.get_or_create_state(key(0, 2), || 0) += 100;
        *t.get_or_create_state(key(1, 2), || 0) += 200;
        t.mark_done(key(0, 2));
        t.mark_done(key(1, 2));

        assert!(
            !t.contributing_sources
                .get(&0)
                .is_some_and(|s| s.contains(&1))
        );
        assert!(
            !t.contributing_sources
                .get(&1)
                .is_some_and(|s| s.contains(&1))
        );

        t.update_final(|c| *c += 50);
        let released = t.finish_source(2);

        assert!(released.is_empty());

        assert_eq!(*t.final_state(), 50);
    }

    #[test]
    fn interleaved_finish_and_new_data_no_duplicates() {
        let mut t: PartialStreamTracker<u32> = PartialStreamTracker::new(0);

        *t.get_or_create_state(key(0, 1), || 0) += 1;
        *t.get_or_create_state(key(0, 2), || 0) += 2;
        t.mark_done(key(0, 1));

        t.update_final(|c| *c += 100);
        t.finish_source(1);

        assert!(t.states.get(&0).is_none());
        assert!(t.pending.get(&0).is_none());

        *t.get_or_create_state(key(1, 2), || 0) += 10;
        *t.get_or_create_state(key(1, 3), || 0) += 20;
        t.mark_done(key(1, 2));
        t.mark_done(key(1, 3));

        t.update_final(|c| *c += 200);
        let released = t.finish_source(2);
        assert!(released.is_empty());
        assert!(t.states.get(&1).is_none());

        *t.get_or_create_state(key(2, 3), || 0) += 1000;
        t.mark_done(key(2, 3));

        t.update_final(|c| *c += 400);
        let released = t.finish_source(3);
        assert!(released.is_empty());

        assert!(t.states.is_empty());
        assert!(t.pending.is_empty());
        assert!(t.contributing_sources.is_empty());
        assert_eq!(*t.final_state(), 700);
    }

    #[test]
    fn mark_done_releases_when_no_source_will_finish() {
        let mut t: PartialStreamTracker<u32> = PartialStreamTracker::new(0);

        *t.get_or_create_state(key(0, 1), || 0) += 10;
        *t.get_or_create_state(key(0, 2), || 0) += 20;
        *t.get_or_create_state(key(1, 1), || 0) += 100;
        *t.get_or_create_state(key(1, 2), || 0) += 200;

        t.update_final(|c| *c += 1000);

        assert!(t.mark_done(key(0, 1)).is_none());
        let r = t.mark_done(key(0, 2)).unwrap();
        assert_eq!(r.0, 1030);

        assert!(t.mark_done(key(1, 1)).is_none());
        let r = t.mark_done(key(1, 2)).unwrap();
        assert_eq!(r.0, 1300);

        assert!(t.states.is_empty());
        assert!(t.pending.is_empty());
    }

    #[test]
    fn finish_source_cascades_to_all_contaminated_streams() {
        let mut t: PartialStreamTracker<u32> = PartialStreamTracker::new(0);

        for i in 0..10 {
            *t.get_or_create_state(key(i, 1), || 0) += 1;
        }
        for i in 0..10 {
            t.mark_done(key(i, 1));
        }

        t.update_final(|c| *c += 999);

        let released = t.finish_source(1);
        assert!(released.is_empty());

        for i in 0..10 {
            assert!(t.states.get(&i).is_none());
            assert!(t.pending.get(&i).is_none());
            assert!(t.contributing_sources.get(&i).is_none());
        }

        assert_eq!(*t.final_state(), 999);
    }
}
