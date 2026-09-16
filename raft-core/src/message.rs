use serde::{Serialize, Deserialize};
use crate::types::{LogIndex, NodeId, Term};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    pub from: NodeId,
    pub to: NodeId,
    pub term: Term,
    pub kind: MessageKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageKind {
    RequestVote {
        last_log_index: LogIndex,
        last_log_term: Term,
    },
    RequestVoteResponse {
        vote_granted: bool,
    },

    PreVote {
        last_log_index: LogIndex,
        last_log_term: Term,
    },
    PreVoteResponse {
        vote_granted: bool,
    },

    AppendEntries {
        prev_log_index: LogIndex,
        prev_log_term: Term,
        entries: Vec<LogEntry>,
        leader_commit: LogIndex,
    },
    AppendEntriesResponse {
        success: bool,
        conflict_term: Option<Term>,
        conflict_index: Option<LogIndex>,
    },

    InstallSnapshot {
        last_included_index: LogIndex,
        last_included_term: Term,
        offset: u64,
        data: Vec<u8>,
        done: bool,
    },
    InstallSnapshotResponse,

    // ── §6 Client Interaction ─────────────────────────────────
    ClientRequest {
        command: Vec<u8>,
    },
    ClientResponse {
        success: bool,
        leader_hint: Option<NodeId>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    pub index: LogIndex,
    pub term: Term,
    pub command: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotMeta {
    pub last_included_index: LogIndex,
    pub last_included_term: Term,
}
