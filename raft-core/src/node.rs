use std::collections::{HashMap, HashSet};

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

use crate::log::RaftLog;
use crate::message::{LogEntry, Message, MessageKind, SnapshotMeta};
use crate::types::{LogIndex, NodeId, Term};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    PreCandidate,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    Send(Message),
    AppendEntries { entries: Vec<LogEntry> },
    TruncateAndAppend { from_index: LogIndex, entries: Vec<LogEntry> },
    PersistHardState { term: Term, voted_for: Option<NodeId> },
    Apply { entries: Vec<LogEntry> },
    PersistSnapshot { last_included_index: LogIndex, last_included_term: Term, data: Vec<u8> },
}

#[derive(Debug, Clone)]
pub struct Config {
    pub id: NodeId,
    pub peers: Vec<NodeId>,
    pub election_timeout_ticks: u64,
    pub heartbeat_interval_ticks: u64,
    pub rng_seed: u64,
}

pub struct RaftNode {
    pub id: NodeId,
    peers: Vec<NodeId>,

    current_term: Term,
    voted_for: Option<NodeId>,
    log: RaftLog,

    commit_index: LogIndex,
    last_applied: LogIndex,
    role: Role,

    next_index: HashMap<NodeId, LogIndex>,
    match_index: HashMap<NodeId, LogIndex>,

    election_timeout_base: u64,
    election_timeout: u64,
    election_elapsed: u64,
    heartbeat_interval: u64,
    heartbeat_elapsed: u64,

    votes_received: HashSet<NodeId>,
    pre_votes_received: HashSet<NodeId>,
    rng: SmallRng,
    leader_id: Option<NodeId>,
}

impl RaftNode {
    pub fn new(config: Config) -> Self {
        let election_timeout = config.election_timeout_ticks;
        let heartbeat_interval = config.heartbeat_interval_ticks;
        assert!(
            heartbeat_interval < election_timeout,
            "heartbeat ({heartbeat_interval}) must be < election timeout ({election_timeout})"
        );

        let mut rng = SmallRng::seed_from_u64(config.rng_seed);
        let randomized_timeout = randomize_election_timeout(election_timeout, &mut rng);

        Self {
            id: config.id,
            peers: config.peers,
            current_term: Term::ZERO,
            voted_for: None,
            log: RaftLog::new(),
            commit_index: LogIndex::SENTINEL,
            last_applied: LogIndex::SENTINEL,
            role: Role::Follower,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            election_timeout_base: election_timeout,
            election_timeout: randomized_timeout,
            election_elapsed: 0,
            heartbeat_interval,
            heartbeat_elapsed: 0,
            votes_received: HashSet::new(),
            pre_votes_received: HashSet::new(),
            rng,
            leader_id: None,
        }
    }

    pub fn restore(
        config: Config,
        current_term: Term,
        voted_for: Option<NodeId>,
        log: RaftLog,
        commit_index: LogIndex,
        last_applied: LogIndex,
    ) -> Self {
        let mut node = Self::new(config);
        node.current_term = current_term;
        node.voted_for = voted_for;
        node.log = log;
        node.commit_index = commit_index;
        node.last_applied = last_applied;
        node
    }

    #[inline] pub fn role(&self) -> Role { self.role }
    #[inline] pub fn current_term(&self) -> Term { self.current_term }
    #[inline] pub fn voted_for(&self) -> Option<NodeId> { self.voted_for }
    #[inline] pub fn commit_index(&self) -> LogIndex { self.commit_index }
    #[inline] pub fn last_applied(&self) -> LogIndex { self.last_applied }
    #[inline] pub fn log(&self) -> &RaftLog { &self.log }
    #[inline] pub fn leader_id(&self) -> Option<NodeId> { self.leader_id }

    pub fn tick(&mut self) -> Vec<Action> {
        let mut actions = Vec::new();

        match self.role {
            Role::Leader => {
                self.heartbeat_elapsed += 1;
                if self.heartbeat_elapsed >= self.heartbeat_interval {
                    self.heartbeat_elapsed = 0;
                    self.broadcast_heartbeats(&mut actions);
                }
            }
            Role::Follower | Role::Candidate | Role::PreCandidate => {
                self.election_elapsed += 1;
                if self.election_elapsed >= self.election_timeout {
                    self.election_elapsed = 0;
                    self.start_pre_vote(&mut actions);
                }
            }
        }

        actions
    }

    pub fn step(&mut self, msg: Message) -> Vec<Action> {
        let mut actions = Vec::new();

        let is_pre_vote_msg = matches!(
            msg.kind,
            MessageKind::PreVote { .. } | MessageKind::PreVoteResponse { .. }
        );

        let is_client_msg = matches!(
            msg.kind,
            MessageKind::ClientRequest { .. } | MessageKind::ClientResponse { .. }
        );

        if msg.term > self.current_term && !is_pre_vote_msg && !is_client_msg {
            self.become_follower(msg.term, None, &mut actions);
        }

        if msg.term < self.current_term && !is_pre_vote_msg && !is_client_msg {
            return actions;
        }

        match msg.kind {
            MessageKind::ClientRequest { command } => {
                if self.role == Role::Leader {
                    let mut propose_actions = self.propose(command);
                    actions.append(&mut propose_actions);
                    actions.push(Action::Send(Message {
                        from: self.id,
                        to: msg.from,
                        term: self.current_term,
                        kind: MessageKind::ClientResponse {
                            success: true,
                            leader_hint: Some(self.id),
                        },
                    }));
                } else {
                    actions.push(Action::Send(Message {
                        from: self.id,
                        to: msg.from,
                        term: self.current_term,
                        kind: MessageKind::ClientResponse {
                            success: false,
                            leader_hint: self.leader_id,
                        },
                    }));
                }
            }
            MessageKind::ClientResponse { .. } => {}

            MessageKind::PreVote { last_log_index, last_log_term } => {
                self.handle_pre_vote(msg.from, msg.term, last_log_index, last_log_term, &mut actions);
            }
            MessageKind::PreVoteResponse { vote_granted } => {
                self.handle_pre_vote_response(msg.from, msg.term, vote_granted, &mut actions);
            }
            MessageKind::RequestVote { last_log_index, last_log_term } => {
                self.handle_request_vote(msg.from, msg.term, last_log_index, last_log_term, &mut actions);
            }
            MessageKind::RequestVoteResponse { vote_granted } => {
                self.handle_request_vote_response(msg.from, msg.term, vote_granted, &mut actions);
            }
            MessageKind::AppendEntries { prev_log_index, prev_log_term, entries, leader_commit } => {
                self.handle_append_entries(msg.from, msg.term, prev_log_index, prev_log_term, entries, leader_commit, &mut actions);
            }
            MessageKind::AppendEntriesResponse { success, conflict_term, conflict_index } => {
                self.handle_append_entries_response(msg.from, msg.term, success, conflict_term, conflict_index, &mut actions);
            }
            MessageKind::InstallSnapshot { last_included_index, last_included_term, offset: _, data, done } => {
                self.handle_install_snapshot(msg.from, msg.term, last_included_index, last_included_term, data, done, &mut actions);
            }
            MessageKind::InstallSnapshotResponse => {
                self.handle_install_snapshot_response(msg.from, msg.term, &mut actions);
            }
        }

        actions
    }

    pub fn propose(&mut self, command: Vec<u8>) -> Vec<Action> {
        let mut actions = Vec::new();

        if self.role != Role::Leader {
            return actions;
        }

        let entry = LogEntry {
            index: self.log.last_index().next(),
            term: self.current_term,
            command,
        };

        actions.push(Action::AppendEntries {
            entries: vec![entry.clone()],
        });
        self.log.append(vec![entry]);

        self.broadcast_append_entries(&mut actions);

        if self.peers.is_empty() {
            self.maybe_advance_commit_index(&mut actions);
        }

        actions
    }

    pub fn compact_logs(&mut self, last_included_index: LogIndex, last_included_term: Term, data: Vec<u8>) -> Vec<Action> {
        let mut actions = Vec::new();
        if last_included_index > self.commit_index {
            return actions;
        }
        self.log.compact(last_included_index, last_included_term);
        actions.push(Action::PersistSnapshot {
            last_included_index,
            last_included_term,
            data,
        });
        actions
    }

    fn become_follower(&mut self, term: Term, leader: Option<NodeId>, actions: &mut Vec<Action>) {
        let term_changed = term > self.current_term;
        self.current_term = term;
        self.role = Role::Follower;
        self.leader_id = leader;
        self.election_elapsed = 0;
        self.election_timeout = randomize_election_timeout(self.election_timeout_base, &mut self.rng);

        if term_changed {
            self.voted_for = None;
            actions.push(Action::PersistHardState {
                term: self.current_term,
                voted_for: None,
            });
        }
    }

    fn start_pre_vote(&mut self, actions: &mut Vec<Action>) {
        self.role = Role::PreCandidate;
        self.pre_votes_received.clear();
        self.pre_votes_received.insert(self.id);
        self.election_elapsed = 0;
        self.election_timeout = randomize_election_timeout(self.election_timeout_base, &mut self.rng);

        let last_log_index = self.log.last_index();
        let last_log_term = self.log.last_term();
        let proposed_term = self.current_term.next();

        for &peer in &self.peers {
            actions.push(Action::Send(Message {
                from: self.id,
                to: peer,
                term: proposed_term,
                kind: MessageKind::PreVote {
                    last_log_index,
                    last_log_term,
                },
            }));
        }

        if self.peers.is_empty() {
            self.start_real_election(actions);
        }
    }

    fn start_real_election(&mut self, actions: &mut Vec<Action>) {
        self.current_term = self.current_term.next();
        self.role = Role::Candidate;
        self.voted_for = Some(self.id);
        self.votes_received.clear();
        self.votes_received.insert(self.id);
        self.election_elapsed = 0;
        self.election_timeout = randomize_election_timeout(self.election_timeout_base, &mut self.rng);

        actions.push(Action::PersistHardState {
            term: self.current_term,
            voted_for: Some(self.id),
        });

        let last_log_index = self.log.last_index();
        let last_log_term = self.log.last_term();

        for &peer in &self.peers {
            actions.push(Action::Send(Message {
                from: self.id,
                to: peer,
                term: self.current_term,
                kind: MessageKind::RequestVote {
                    last_log_index,
                    last_log_term,
                },
            }));
        }

        if self.peers.is_empty() {
            self.become_leader(actions);
        }
    }

    fn become_leader(&mut self, actions: &mut Vec<Action>) {
        self.role = Role::Leader;
        self.leader_id = Some(self.id);
        self.heartbeat_elapsed = 0;

        let last_index = self.log.last_index();
        self.next_index.clear();
        self.match_index.clear();
        for &peer in &self.peers {
            self.next_index.insert(peer, last_index.next());
            self.match_index.insert(peer, LogIndex::SENTINEL);
        }

        let noop = LogEntry {
            index: last_index.next(),
            term: self.current_term,
            command: Vec::new(),
        };
        actions.push(Action::AppendEntries {
            entries: vec![noop.clone()],
        });
        self.log.append(vec![noop]);

        self.broadcast_heartbeats(actions);
    }

    fn handle_pre_vote(
        &mut self,
        from: NodeId,
        proposed_term: Term,
        last_log_index: LogIndex,
        last_log_term: Term,
        actions: &mut Vec<Action>,
    ) {
        let log_ok = self.is_log_up_to_date(last_log_index, last_log_term);
        let leader_lease_active = self.leader_id.is_some() && self.election_elapsed < self.election_timeout_base;
        let grant = proposed_term >= self.current_term && log_ok && !leader_lease_active;

        actions.push(Action::Send(Message {
            from: self.id,
            to: from,
            term: self.current_term,
            kind: MessageKind::PreVoteResponse { vote_granted: grant },
        }));
    }

    fn handle_pre_vote_response(&mut self, from: NodeId, msg_term: Term, vote_granted: bool, actions: &mut Vec<Action>) {
        if self.role != Role::PreCandidate || msg_term != self.current_term {
            return;
        }

        if vote_granted {
            self.pre_votes_received.insert(from);
            let majority = (self.peers.len() + 1) / 2 + 1;
            if self.pre_votes_received.len() >= majority {
                self.start_real_election(actions);
            }
        }
    }

    fn handle_request_vote(
        &mut self,
        from: NodeId,
        _term: Term,
        last_log_index: LogIndex,
        last_log_term: Term,
        actions: &mut Vec<Action>,
    ) {
        let leader_lease_active = self.leader_id.is_some() && self.election_elapsed < self.election_timeout_base;
        let can_vote = self.voted_for.is_none() || self.voted_for == Some(from);
        let log_ok = self.is_log_up_to_date(last_log_index, last_log_term);
        let grant = can_vote && log_ok && !leader_lease_active;

        if grant {
            self.voted_for = Some(from);
            self.election_elapsed = 0;
            actions.push(Action::PersistHardState {
                term: self.current_term,
                voted_for: Some(from),
            });
        }

        actions.push(Action::Send(Message {
            from: self.id,
            to: from,
            term: self.current_term,
            kind: MessageKind::RequestVoteResponse { vote_granted: grant },
        }));
    }

    fn handle_request_vote_response(
        &mut self,
        from: NodeId,
        msg_term: Term,
        vote_granted: bool,
        actions: &mut Vec<Action>,
    ) {
        if self.role != Role::Candidate || msg_term != self.current_term {
            return;
        }

        if vote_granted {
            self.votes_received.insert(from);
            let majority = (self.peers.len() + 1) / 2 + 1;
            if self.votes_received.len() >= majority {
                self.become_leader(actions);
            }
        }
    }

    fn handle_append_entries(
        &mut self,
        from: NodeId,
        term: Term,
        prev_log_index: LogIndex,
        prev_log_term: Term,
        entries: Vec<LogEntry>,
        leader_commit: LogIndex,
        actions: &mut Vec<Action>,
    ) {
        if term < self.current_term {
            actions.push(Action::Send(Message {
                from: self.id,
                to: from,
                term: self.current_term,
                kind: MessageKind::AppendEntriesResponse {
                    success: false,
                    conflict_term: None,
                    conflict_index: None,
                },
            }));
            return;
        }

        self.become_follower(term, Some(from), actions);

        let local_term = self.log.term_at(prev_log_index);
        match local_term {
            None => {
                let last_idx = self.log.last_index();
                actions.push(Action::Send(Message {
                    from: self.id,
                    to: from,
                    term: self.current_term,
                    kind: MessageKind::AppendEntriesResponse {
                        success: false,
                        conflict_term: None,
                        conflict_index: Some(last_idx.next()),
                    },
                }));
                return;
            }
            Some(t) if t != prev_log_term => {
                let mut first_idx = prev_log_index;
                while first_idx.0 > self.log.first_index().0 {
                    let prev = first_idx.prev();
                    if self.log.term_at(prev) == Some(t) {
                        first_idx = prev;
                    } else {
                        break;
                    }
                }
                actions.push(Action::Send(Message {
                    from: self.id,
                    to: from,
                    term: self.current_term,
                    kind: MessageKind::AppendEntriesResponse {
                        success: false,
                        conflict_term: Some(t),
                        conflict_index: Some(first_idx),
                    },
                }));
                return;
            }
            _ => {}
        }

        if !entries.is_empty() {
            let mut attach_idx = entries[0].index;
            let mut entries_to_append = entries;

            for e in &entries_to_append {
                match self.log.term_at(e.index) {
                    Some(local_t) if local_t == e.term => {
                        attach_idx = e.index.next();
                    }
                    Some(_) => {
                        break;
                    }
                    None => {
                        break;
                    }
                }
            }

            let start_offset = (attach_idx.0 - entries_to_append[0].index.0) as usize;
            if start_offset < entries_to_append.len() {
                let suffix = entries_to_append.split_off(start_offset);
                actions.push(Action::TruncateAndAppend {
                    from_index: attach_idx,
                    entries: suffix.clone(),
                });
                self.log.truncate_and_append(attach_idx, suffix);
            }
        }

        if leader_commit > self.commit_index {
            let last_new_entry_idx = self.log.last_index();
            let new_commit = std::cmp::min(leader_commit, last_new_entry_idx);
            if new_commit > self.commit_index {
                self.commit_index = new_commit;
                self.apply_committed_entries(actions);
            }
        }

        actions.push(Action::Send(Message {
            from: self.id,
            to: from,
            term: self.current_term,
            kind: MessageKind::AppendEntriesResponse {
                success: true,
                conflict_term: None,
                conflict_index: None,
            },
        }));
    }

    fn handle_append_entries_response(
        &mut self,
        from: NodeId,
        term: Term,
        success: bool,
        conflict_term: Option<Term>,
        conflict_index: Option<LogIndex>,
        actions: &mut Vec<Action>,
    ) {
        if self.role != Role::Leader || term != self.current_term {
            return;
        }

        if success {
            let next = self.log.last_index().next();
            let match_idx = self.log.last_index();
            self.next_index.insert(from, next);
            self.match_index.insert(from, match_idx);

            self.maybe_advance_commit_index(actions);
        } else {
            let mut next = self.next_index.get(&from).copied().unwrap_or(LogIndex(1));

            if let Some(c_term) = conflict_term {
                let mut last_index_of_term = None;
                let last_log_idx = self.log.last_index();
                for idx in (self.log.first_index().0..=last_log_idx.0).rev() {
                    let idx = LogIndex(idx);
                    if self.log.term_at(idx) == Some(c_term) {
                        last_index_of_term = Some(idx);
                        break;
                    }
                }

                if let Some(last_idx) = last_index_of_term {
                    next = last_idx.next();
                } else {
                    next = conflict_index.unwrap_or(LogIndex(1));
                }
            } else if let Some(c_index) = conflict_index {
                next = c_index;
            } else {
                next = next.prev();
            }

            if next.0 == 0 {
                next = LogIndex(1);
            }
            self.next_index.insert(from, next);

            self.replicate_to_peer(from, actions);
        }
    }

    fn handle_install_snapshot(
        &mut self,
        from: NodeId,
        term: Term,
        last_included_index: LogIndex,
        last_included_term: Term,
        data: Vec<u8>,
        done: bool,
        actions: &mut Vec<Action>,
    ) {
        if term < self.current_term {
            actions.push(Action::Send(Message {
                from: self.id,
                to: from,
                term: self.current_term,
                kind: MessageKind::InstallSnapshotResponse,
            }));
            return;
        }

        self.become_follower(term, Some(from), actions);

        if last_included_index <= self.commit_index {
            actions.push(Action::Send(Message {
                from: self.id,
                to: from,
                term: self.current_term,
                kind: MessageKind::InstallSnapshotResponse,
            }));
            return;
        }

        if done {
            self.log.install_snapshot(SnapshotMeta {
                last_included_index,
                last_included_term,
            });

            self.commit_index = last_included_index;
            self.last_applied = last_included_index;

            actions.push(Action::PersistSnapshot {
                last_included_index,
                last_included_term,
                data,
            });
        }

        actions.push(Action::Send(Message {
            from: self.id,
            to: from,
            term: self.current_term,
            kind: MessageKind::InstallSnapshotResponse,
        }));
    }

    fn handle_install_snapshot_response(&mut self, from: NodeId, term: Term, actions: &mut Vec<Action>) {
        if self.role != Role::Leader || term != self.current_term {
            return;
        }

        let first_idx = self.log.first_index();
        self.match_index.insert(from, first_idx);
        self.next_index.insert(from, first_idx.next());

        self.maybe_advance_commit_index(actions);
    }

    fn is_log_up_to_date(&self, candidate_last_index: LogIndex, candidate_last_term: Term) -> bool {
        let my_last_term = self.log.last_term();
        let my_last_index = self.log.last_index();

        if candidate_last_term != my_last_term {
            candidate_last_term > my_last_term
        } else {
            candidate_last_index >= my_last_index
        }
    }

    fn broadcast_heartbeats(&self, actions: &mut Vec<Action>) {
        for &peer in &self.peers {
            self.replicate_to_peer(peer, actions);
        }
    }

    fn broadcast_append_entries(&self, actions: &mut Vec<Action>) {
        for &peer in &self.peers {
            self.replicate_to_peer(peer, actions);
        }
    }

    fn replicate_to_peer(&self, peer: NodeId, actions: &mut Vec<Action>) {
        let next = self.next_index.get(&peer).copied().unwrap_or(LogIndex(1));
        let first = self.log.first_index();

        if next <= first {
            let meta = self.log.snapshot_meta().unwrap_or(SnapshotMeta {
                last_included_index: LogIndex::SENTINEL,
                last_included_term: Term::ZERO,
            });
            actions.push(Action::Send(Message {
                from: self.id,
                to: peer,
                term: self.current_term,
                kind: MessageKind::InstallSnapshot {
                    last_included_index: meta.last_included_index,
                    last_included_term: meta.last_included_term,
                    offset: 0,
                    data: b"simulated-compacted-snapshot".to_vec(),
                    done: true,
                },
            }));
        } else {
            let prev_log_index = next.prev();
            let prev_log_term = self.log.term_at(prev_log_index).unwrap_or(Term::ZERO);
            let entries = self.log.entries_from(next).to_vec();

            actions.push(Action::Send(Message {
                from: self.id,
                to: peer,
                term: self.current_term,
                kind: MessageKind::AppendEntries {
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit: self.commit_index,
                },
            }));
        }
    }

    fn maybe_advance_commit_index(&mut self, actions: &mut Vec<Action>) {
        let last = self.log.last_index();
        let mut new_commit = self.commit_index;

        for n in (self.commit_index.0 + 1)..=last.0 {
            let n = LogIndex(n);
            if self.log.term_at(n) != Some(self.current_term) {
                continue;
            }

            let mut count = 1;
            for &peer in &self.peers {
                if self.match_index.get(&peer).copied().unwrap_or(LogIndex::SENTINEL) >= n {
                    count += 1;
                }
            }

            let majority = (self.peers.len() + 1) / 2 + 1;
            if count >= majority {
                new_commit = n;
            }
        }

        if new_commit > self.commit_index {
            self.commit_index = new_commit;
            self.apply_committed_entries(actions);
        }
    }

    fn apply_committed_entries(&mut self, actions: &mut Vec<Action>) {
        if self.commit_index > self.last_applied {
            let start = self.last_applied.next();
            let entries = self.log.entries_from(start).to_vec();
            let to_apply: Vec<LogEntry> = entries
                .into_iter()
                .take_while(|e| e.index <= self.commit_index)
                .collect();

            if !to_apply.is_empty() {
                self.last_applied = to_apply.last().unwrap().index;
                actions.push(Action::Apply { entries: to_apply });
            }
        }
    }
}

fn randomize_election_timeout(base: u64, rng: &mut SmallRng) -> u64 {
    base + rng.gen_range(0..base)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_node_elects_itself() {
        let config = Config {
            id: NodeId(1),
            peers: vec![],
            election_timeout_ticks: 10,
            heartbeat_interval_ticks: 3,
            rng_seed: 42,
        };
        let mut node = RaftNode::new(config);

        let mut elected = false;
        for _ in 0..30 {
            let actions = node.tick();
            if node.role() == Role::Leader {
                elected = true;
                assert!(actions.iter().any(|a| matches!(a, Action::PersistHardState { .. })));
                break;
            }
        }
        assert!(elected, "single node should elect itself");
    }

    #[test]
    fn three_nodes_elect_one_leader() {
        let ids = [NodeId(1), NodeId(2), NodeId(3)];
        let mut nodes: Vec<RaftNode> = ids
            .iter()
            .map(|&id| {
                RaftNode::new(Config {
                    id,
                    peers: ids.iter().copied().filter(|&p| p != id).collect(),
                    election_timeout_ticks: 10,
                    heartbeat_interval_ticks: 3,
                    rng_seed: id.0 * 1000,
                })
            })
            .collect();

        let mut messages: Vec<Message> = Vec::new();

        for _tick in 0..100 {
            for node in &mut nodes {
                let actions = node.tick();
                for action in actions {
                    if let Action::Send(msg) = action {
                        messages.push(msg);
                    }
                }
            }

            let pending = std::mem::take(&mut messages);
            for msg in pending {
                let target = nodes.iter_mut().find(|n| n.id == msg.to).unwrap();
                let actions = target.step(msg);
                for action in actions {
                    if let Action::Send(m) = action {
                        messages.push(m);
                    }
                }
            }

            let leaders: Vec<_> = nodes
                .iter()
                .filter(|n| n.role() == Role::Leader)
                .collect();
            if leaders.len() > 1 {
                let terms: Vec<_> = leaders.iter().map(|n| n.current_term()).collect();
                let unique_terms: std::collections::HashSet<_> = terms.iter().collect();
                assert_eq!(
                    unique_terms.len(),
                    terms.len(),
                    "Election Safety violated: two leaders in the same term!"
                );
            }
        }

        let leaders: Vec<_> = nodes
            .iter()
            .filter(|n| n.role() == Role::Leader)
            .collect();
        assert_eq!(leaders.len(), 1, "exactly one leader should be elected");
    }

    #[test]
    fn log_replication_happy_path() {
        let ids = [NodeId(1), NodeId(2), NodeId(3)];
        let mut nodes: Vec<RaftNode> = ids
            .iter()
            .map(|&id| {
                RaftNode::new(Config {
                    id,
                    peers: ids.iter().copied().filter(|&p| p != id).collect(),
                    election_timeout_ticks: 10,
                    heartbeat_interval_ticks: 3,
                    rng_seed: id.0 * 1000,
                })
            })
            .collect();

        let mut messages: Vec<Message> = Vec::new();

        let mut leader_id = None;
        for _tick in 0..50 {
            for node in &mut nodes {
                let actions = node.tick();
                for action in actions {
                    if let Action::Send(msg) = action {
                        messages.push(msg);
                    }
                }
            }

            let pending = std::mem::take(&mut messages);
            for msg in pending {
                let target = nodes.iter_mut().find(|n| n.id == msg.to).unwrap();
                let actions = target.step(msg);
                for action in actions {
                    if let Action::Send(m) = action {
                        messages.push(m);
                    }
                }
            }

            if let Some(leader) = nodes.iter().find(|n| n.role() == Role::Leader) {
                leader_id = Some(leader.id);
            }
        }

        let leader_id = leader_id.expect("leader should have been elected");

        let leader_node = nodes.iter_mut().find(|n| n.id == leader_id).unwrap();
        let cmd = b"set x = 42".to_vec();
        let actions = leader_node.propose(cmd.clone());

        for action in actions {
            if let Action::Send(msg) = action {
                messages.push(msg);
            }
        }

        for _tick in 0..20 {
            for node in &mut nodes {
                let actions = node.tick();
                for action in actions {
                    if let Action::Send(msg) = action {
                        messages.push(msg);
                    }
                }
            }

            let pending = std::mem::take(&mut messages);
            for msg in pending {
                let target = nodes.iter_mut().find(|n| n.id == msg.to).unwrap();
                let actions = target.step(msg);
                for action in actions {
                    if let Action::Send(m) = action {
                        messages.push(m);
                    }
                }
            }
        }

        for node in &nodes {
            assert_eq!(node.commit_index(), LogIndex(2), "node {} failed to commit index 2", node.id);
            assert_eq!(node.last_applied(), LogIndex(2), "node {} failed to apply index 2", node.id);
            
            let entry = node.log().entry_at(LogIndex(2)).unwrap();
            assert_eq!(entry.command, cmd);
        }
    }
}
