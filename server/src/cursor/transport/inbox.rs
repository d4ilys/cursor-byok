//! Orders Bidi messages by append sequence number.

use std::collections::BTreeMap;

pub struct OrderedInbox<T> {
    /// `None` until the first push: a transport that is created after the
    /// client has already been appending (reconnect, interrupt recovery) must
    /// adopt the client's current sequence number instead of waiting forever
    /// for a seqno 0 that will never be sent again.
    next: Option<i64>,
    pending: BTreeMap<i64, T>,
}

impl<T> OrderedInbox<T> {
    pub fn starting_at(next: i64) -> Self {
        Self {
            next: Some(next),
            pending: BTreeMap::new(),
        }
    }

    /// Bases the inbox on the first appended seqno, for transports that are
    /// created mid-session.
    pub fn from_first_push() -> Self {
        Self {
            next: None,
            pending: BTreeMap::new(),
        }
    }

    /// The next seqno the inbox will deliver, once a base is known.
    pub fn next(&self) -> Option<i64> {
        self.next
    }

    pub fn push(&mut self, seqno: i64, value: T) -> Vec<(i64, T)> {
        if seqno < 0 {
            return Vec::new();
        }
        let next = *self.next.get_or_insert(seqno);
        if seqno < next || self.pending.contains_key(&seqno) {
            return Vec::new();
        }
        self.pending.insert(seqno, value);
        let mut ready = Vec::new();
        let mut next = next;
        while let Some(value) = self.pending.remove(&next) {
            ready.push((next, value));
            next = next.saturating_add(1);
        }
        self.next = Some(next);
        ready
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delivers_contiguous_sequences_in_order() {
        let mut inbox = OrderedInbox::starting_at(0);
        assert!(inbox.push(1, "b").is_empty());
        assert!(inbox.push(2, "c").is_empty());
        assert_eq!(inbox.push(0, "a"), vec![(0, "a"), (1, "b"), (2, "c")]);
        assert_eq!(inbox.next(), Some(3));
    }

    #[test]
    fn drops_duplicates_and_stale_sequences() {
        let mut inbox = OrderedInbox::starting_at(0);
        assert_eq!(inbox.push(0, "a"), vec![(0, "a")]);
        assert!(inbox.push(0, "again").is_empty());
        assert!(inbox.push(-1, "invalid").is_empty());
    }

    #[test]
    fn first_push_bases_the_inbox_on_the_client_sequence() {
        let mut inbox = OrderedInbox::from_first_push();
        assert_eq!(inbox.push(12, "run"), vec![(12, "run")]);
        assert_eq!(inbox.next(), Some(13));
        assert_eq!(inbox.push(13, "tools"), vec![(13, "tools")]);
        assert!(inbox.push(11, "stale").is_empty());
    }

    #[test]
    fn a_later_forward_gap_still_waits_for_the_missing_seqno() {
        // Only the *first* push is allowed to define the base.
        let mut inbox = OrderedInbox::from_first_push();
        assert_eq!(inbox.push(0, "a"), vec![(0, "a")]);
        assert!(inbox.push(2, "c").is_empty());
        assert_eq!(inbox.push(1, "b"), vec![(1, "b"), (2, "c")]);
    }
}
