use hashbrown::{HashMap, HashSet};
use miso_workflow_types::log::{PartialStreamKey, SourceId, next_source_id};

pub struct PartialStreamTracker<S> {
    own_source_id: SourceId,
    states: HashMap<usize, S>,
    pending: HashMap<usize, HashSet<SourceId>>,
    seen_sources: HashSet<SourceId>,
    completed_sources: HashSet<SourceId>,
    next_expected_id: HashMap<SourceId, usize>,
}

impl<S> PartialStreamTracker<S> {
    pub fn new() -> Self {
        Self {
            own_source_id: next_source_id(),
            states: HashMap::new(),
            pending: HashMap::new(),
            seen_sources: HashSet::new(),
            completed_sources: HashSet::new(),
            next_expected_id: HashMap::new(),
        }
    }

    pub fn get_or_create_state<F>(&mut self, key: PartialStreamKey, create: F) -> &mut S
    where
        F: FnOnce() -> S,
    {
        let is_new_source = self.seen_sources.insert(key.source_id);

        if is_new_source {
            for (&source_id, &next_id) in &self.next_expected_id {
                for id in next_id..=key.partial_stream_id {
                    self.pending.entry(id).or_default().insert(source_id);
                }
            }
            self.next_expected_id
                .insert(key.source_id, key.partial_stream_id);
        }

        self.pending
            .entry(key.partial_stream_id)
            .or_default()
            .insert(key.source_id);
        self.next_expected_id
            .entry(key.source_id)
            .and_modify(|id| *id = (*id).max(key.partial_stream_id + 1))
            .or_insert(key.partial_stream_id + 1);

        self.states
            .entry(key.partial_stream_id)
            .or_insert_with(create)
    }

    pub fn mark_done(&mut self, key: PartialStreamKey) -> Option<(S, PartialStreamKey)> {
        let pending = self.pending.get_mut(&key.partial_stream_id)?;
        pending.remove(&key.source_id);

        if pending.is_empty() {
            self.pending.remove(&key.partial_stream_id);
            let state = self.states.remove(&key.partial_stream_id)?;
            let out_key = PartialStreamKey {
                partial_stream_id: key.partial_stream_id,
                source_id: self.own_source_id,
            };
            return Some((state, out_key));
        }
        None
    }

    pub fn source_completed(&mut self, source_id: SourceId) {
        if !self.completed_sources.insert(source_id) {
            return;
        }

        let ids_to_check: Vec<_> = self.pending.keys().cloned().collect();
        for id in ids_to_check {
            if let Some(pending) = self.pending.get_mut(&id) {
                pending.remove(&source_id);
            }
        }
    }

    pub fn drain_completed(&mut self) -> Vec<(S, PartialStreamKey)> {
        let mut result = Vec::new();
        let completed_ids: Vec<_> = self
            .pending
            .iter()
            .filter(|(_, pending)| pending.is_empty())
            .map(|(&id, _)| id)
            .collect();

        for id in completed_ids {
            self.pending.remove(&id);
            if let Some(state) = self.states.remove(&id) {
                let key = PartialStreamKey {
                    partial_stream_id: id,
                    source_id: self.own_source_id,
                };
                result.push((state, key));
            }
        }
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
    fn source_completed_releases_without_mark_done() {
        let mut t: PartialStreamTracker<u32> = PartialStreamTracker::new();
        *t.get_or_create_state(key(0, 1), || 0) += 1;
        *t.get_or_create_state(key(0, 2), || 0) += 10;

        assert!(t.mark_done(key(0, 1)).is_none());
        t.source_completed(2);
        assert_eq!(t.drain_completed(), vec![(11, key(0, t.own_source_id))]);
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
    fn source_never_contributes_does_not_block() {
        let mut t: PartialStreamTracker<u32> = PartialStreamTracker::new();
        *t.get_or_create_state(key(0, 1), || 5) += 1;

        t.source_completed(2);
        assert!(t.drain_completed().is_empty());

        assert_eq!(t.mark_done(key(0, 1)).unwrap().0, 6);
    }

    #[test]
    fn drain_multiple_completed() {
        let mut t: PartialStreamTracker<u32> = PartialStreamTracker::new();
        *t.get_or_create_state(key(0, 1), || 0) = 10;
        *t.get_or_create_state(key(1, 1), || 0) = 20;
        *t.get_or_create_state(key(0, 2), || 0) += 1;
        *t.get_or_create_state(key(1, 2), || 0) += 2;

        t.mark_done(key(0, 1));
        t.mark_done(key(1, 1));
        t.source_completed(2);

        let mut drained: Vec<_> = t.drain_completed().into_iter().map(|(s, _)| s).collect();
        drained.sort();
        assert_eq!(drained, vec![11, 22]);
    }

    #[test]
    fn duplicate_source_completed_is_idempotent() {
        let mut t: PartialStreamTracker<u32> = PartialStreamTracker::new();
        *t.get_or_create_state(key(0, 1), || 0) = 5;
        *t.get_or_create_state(key(0, 2), || 0) = 10;

        t.mark_done(key(0, 1));
        t.source_completed(2);
        t.source_completed(2);
        t.source_completed(2);

        assert_eq!(t.drain_completed().len(), 1);
        assert!(t.drain_completed().is_empty());
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
