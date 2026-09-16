use raft_core::types::{LogIndex, Term, NodeId};
use raft_core::message::{LogEntry, SnapshotMeta};

#[derive(Clone)]
pub struct StableDisk {
    pub node_id: NodeId,
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub log_entries: Vec<LogEntry>,
    pub snapshot: Option<(SnapshotMeta, Vec<u8>)>,
}

impl StableDisk {
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            current_term: Term::ZERO,
            voted_for: None,
            log_entries: vec![LogEntry {
                index: LogIndex::SENTINEL,
                term: Term::ZERO,
                command: Vec::new(),
            }],
            snapshot: None,
        }
    }

    pub fn write_hard_state(&mut self, term: Term, voted_for: Option<NodeId>) {
        self.current_term = term;
        self.voted_for = voted_for;
    }

    pub fn append_entries(&mut self, entries: &[LogEntry]) {
        for entry in entries {
            let idx = entry.index.0 as usize;
            // Adjust index checks relative to snapshot point
            let base_idx = self.log_entries[0].index.0 as usize;
            if idx < base_idx {
                continue; // Skip writes that are already compacted
            }
            let offset = idx - base_idx;

            if offset < self.log_entries.len() {
                if self.log_entries[offset].term != entry.term {
                    self.log_entries.truncate(offset);
                    self.log_entries.push(entry.clone());
                }
            } else if offset == self.log_entries.len() {
                self.log_entries.push(entry.clone());
            } else {
                panic!(
                    "Non-contiguous disk write at index L{} for node {} (disk base L{}, disk len {})",
                    idx, self.node_id, base_idx, self.log_entries.len()
                );
            }
        }
    }

    pub fn truncate_and_append(&mut self, from_index: LogIndex, entries: &[LogEntry]) {
        let base_idx = self.log_entries[0].index.0;
        assert!(from_index.0 >= base_idx, "Cannot truncate before disk base");
        let offset = (from_index.0 - base_idx) as usize;
        self.log_entries.truncate(offset);
        self.append_entries(entries);
    }

    pub fn write_snapshot(&mut self, meta: SnapshotMeta, data: Vec<u8>) {
        self.snapshot = Some((meta, data));
        
        // Compact log entries on disk up to the snapshot point
        let base_idx = self.log_entries[0].index.0;
        if meta.last_included_index.0 > base_idx {
            let offset = (meta.last_included_index.0 - base_idx) as usize;
            if offset >= self.log_entries.len() {
                self.log_entries = vec![LogEntry {
                    index: meta.last_included_index,
                    term: meta.last_included_term,
                    command: Vec::new(),
                }];
            } else {
                let mut suffix = self.log_entries.split_off(offset);
                suffix[0] = LogEntry {
                    index: meta.last_included_index,
                    term: meta.last_included_term,
                    command: Vec::new(),
                };
                self.log_entries = suffix;
            }
        }
    }
}