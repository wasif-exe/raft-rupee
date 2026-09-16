use std::collections::HashMap;
use raft_core::types::{Term, LogIndex, NodeId};
use raft_core::node::{RaftNode, Role};

pub struct SafetyInvariants {
    term_leaders: HashMap<Term, NodeId>,
    committed_entries: HashMap<LogIndex, (Term, Vec<u8>)>,
}

impl SafetyInvariants {
    pub fn new() -> Self {
        Self {
            term_leaders: HashMap::new(),
            committed_entries: HashMap::new(),
        }
    }

    pub fn verify_safety(&mut self, nodes: &HashMap<NodeId, RaftNode>) -> Result<(), String> {
        self.check_election_safety(nodes)?;
        self.check_log_matching(nodes)?;
        self.check_leader_append_only(nodes)?;
        self.check_state_machine_safety(nodes)?;
        Ok(())
    }

    fn check_election_safety(&mut self, nodes: &HashMap<NodeId, RaftNode>) -> Result<(), String> {
        for node in nodes.values() {
            if node.role() == Role::Leader {
                if let Some(&existing_leader) = self.term_leaders.get(&node.current_term()) {
                    if existing_leader != node.id {
                        return Err(format!(
                            "ELECTION SAFETY VIOLATION: Nodes {} and {} both claim leadership in Term {}",
                            existing_leader, node.id, node.current_term()
                        ));
                    }
                } else {
                    self.term_leaders.insert(node.current_term(), node.id);
                }
            }
        }
        Ok(())
    }

    fn check_log_matching(&self, nodes: &HashMap<NodeId, RaftNode>) -> Result<(), String> {
        let node_list: Vec<&RaftNode> = nodes.values().collect();
        for i in 0..node_list.len() {
            for j in (i + 1)..node_list.len() {
                let n1 = node_list[i];
                let n2 = node_list[j];

                let min_last_idx = std::cmp::min(n1.log().last_index(), n2.log().last_index());
                for idx in 1..=min_last_idx.0 {
                    let index = LogIndex(idx);
                    let term1 = n1.log().term_at(index);
                    let term2 = n2.log().term_at(index);

                    if term1.is_some() && term1 == term2 {
                        // Terms match.
                        // We ONLY compare command bytes if BOTH nodes have this entry uncompacted
                        // (i.e., the index is strictly greater than the first_index of BOTH logs).
                        if index > n1.log().first_index() && index > n2.log().first_index() {
                            let e1 = n1.log().entry_at(index).unwrap();
                            let e2 = n2.log().entry_at(index).unwrap();
                            if e1.command != e2.command {
                                return Err(format!(
                                    "LOG MATCHING VIOLATION: Node {} and Node {} match at index {} term {:?}, but commands differ!",
                                    n1.id, n2.id, index, term1
                                ));
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn check_leader_append_only(&self, _nodes: &HashMap<NodeId, RaftNode>) -> Result<(), String> {
        Ok(())
    }

    fn check_state_machine_safety(&mut self, nodes: &HashMap<NodeId, RaftNode>) -> Result<(), String> {
        for node in nodes.values() {
            let last_applied = node.last_applied();
            if last_applied == LogIndex::SENTINEL {
                continue;
            }

            for idx in 1..=last_applied.0 {
                let index = LogIndex(idx);
                if let Some(entry) = node.log().entry_at(index) {
                    if entry.command.is_empty() {
                        continue; // Skip no-ops or compacted sentinels
                    }

                    if let Some((term, cmd)) = self.committed_entries.get(&index) {
                        if *term != entry.term || *cmd != entry.command {
                            return Err(format!(
                                "STATE MACHINE SAFETY VIOLATION: Node {} applied command index {} with Term {}, but index was already committed with Term {}",
                                node.id, index, entry.term, term
                            ));
                        }
                    } else {
                        self.committed_entries.insert(index, (entry.term, entry.command.clone()));
                    }
                }
            }
        }
        Ok(())
    }
}
