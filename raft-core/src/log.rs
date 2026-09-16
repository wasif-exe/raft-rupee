use crate::message::{LogEntry, SnapshotMeta};
use crate::types::{LogIndex, Term};

/// The Raft log, using the sentinel entry convention.
///
/// `entries[0]` is always a sentinel (or snapshot base) with
/// `index == snapshot_base` and `term == snapshot_term`.
/// Real entries start at `entries[1]`.
///
/// Invariant: `entries[i].index.0 == entries[0].index.0 + i` for all i.
#[derive(Debug)]
pub struct RaftLog {
    entries: Vec<LogEntry>,
}

impl RaftLog {
    pub fn new() -> Self {
        Self {
            entries: vec![LogEntry {
                index: LogIndex::SENTINEL,
                term: Term::ZERO,
                command: Vec::new(),
            }],
        }
    }

    pub fn restore(meta: SnapshotMeta, entries: Vec<LogEntry>) -> Self {
        let sentinel = LogEntry {
            index: meta.last_included_index,
            term: meta.last_included_term,
            command: Vec::new(),
        };
        let mut log = Self {
            entries: Vec::with_capacity(1 + entries.len()),
        };
        log.entries.push(sentinel);
        log.entries.extend(entries);
        log
    }

    #[inline]
    pub fn last_index(&self) -> LogIndex {
        self.entries.last().unwrap().index
    }

    #[inline]
    pub fn last_term(&self) -> Term {
        self.entries.last().unwrap().term
    }

    /// Returns the index of the first entry currently retained (the sentinel snapshot base).
    #[inline]
    pub fn first_index(&self) -> LogIndex {
        self.entries[0].index
    }

    #[inline]
    pub fn term_at(&self, index: LogIndex) -> Option<Term> {
        let base = self.entries[0].index.0;
        let offset = index.0.checked_sub(base)? as usize;
        self.entries.get(offset).map(|e| e.term)
    }

    #[inline]
    pub fn entry_at(&self, index: LogIndex) -> Option<&LogEntry> {
        let base = self.entries[0].index.0;
        let offset = index.0.checked_sub(base)? as usize;
        self.entries.get(offset)
    }

    pub fn append(&mut self, entries: Vec<LogEntry>) {
        if entries.is_empty() {
            return;
        }
        debug_assert_eq!(
            entries[0].index,
            self.last_index().next(),
            "appended entries must be contiguous"
        );
        self.entries.extend(entries);
    }

    pub fn truncate_and_append(&mut self, from_index: LogIndex, entries: Vec<LogEntry>) {
        let base = self.entries[0].index.0;
        assert!(
            from_index.0 > base,
            "cannot truncate at or before snapshot base"
        );
        let offset = (from_index.0 - base) as usize;
        self.entries.truncate(offset);
        self.entries.extend(entries);
    }

    pub fn entries_from(&self, start_index: LogIndex) -> &[LogEntry] {
        let base = self.entries[0].index.0;
        if start_index.0 <= base {
            &self.entries[1..]
        } else {
            let offset = (start_index.0 - base) as usize;
            if offset >= self.entries.len() {
                &[]
            } else {
                &self.entries[offset..]
            }
        }
    }

    pub fn snapshot_meta(&self) -> Option<SnapshotMeta> {
        let sentinel = &self.entries[0];
        if sentinel.index == LogIndex::SENTINEL {
            None
        } else {
            Some(SnapshotMeta {
                last_included_index: sentinel.index,
                last_included_term: sentinel.term,
            })
        }
    }

    /// Compact the log by throwing away entries up to `last_included_index`.
    /// The entry at `last_included_index` is retained as the new sentinel.
    pub fn compact(&mut self, last_included_index: LogIndex, last_included_term: Term) {
        let base = self.entries[0].index.0;
        if last_included_index.0 <= base {
            return; // Already compacted past this index
        }
        let offset = (last_included_index.0 - base) as usize;
        if offset >= self.entries.len() {
            // Compacting beyond the current log. Replace everything with a clean sentinel.
            self.entries = vec![LogEntry {
                index: last_included_index,
                term: last_included_term,
                command: Vec::new(),
            }];
        } else {
            // Keep suffix starting at offset, making suffix[0] our new sentinel
            let mut suffix = self.entries.split_off(offset);
            suffix[0] = LogEntry {
                index: last_included_index,
                term: last_included_term,
                command: Vec::new(),
            };
            self.entries = suffix;
        }
    }

    pub fn install_snapshot(&mut self, meta: SnapshotMeta) {
        self.entries.clear();
        self.entries.push(LogEntry {
            index: meta.last_included_index,
            term: meta.last_included_term,
            command: Vec::new(),
        });
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.entries.len() - 1
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}