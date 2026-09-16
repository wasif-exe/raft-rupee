use std::collections::HashMap;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use raft_core::node::{RaftNode, Config, Action, Role};
use raft_core::types::{LogIndex, NodeId, Term};
use crate::storage::StableDisk;
use crate::network::SimNetwork;
use crate::invariants::SafetyInvariants;

pub struct Simulator {
    pub seed: u64,
    rng: SmallRng,
    nodes: HashMap<NodeId, RaftNode>,
    disks: HashMap<NodeId, StableDisk>,
    network: SimNetwork,
    invariants: SafetyInvariants,
    tick_count: u64,
    node_configs: Vec<Config>,
}

impl Simulator {
    pub fn new(seed: u64, num_nodes: usize) -> Self {
        let mut rng = SmallRng::seed_from_u64(seed);
        let node_ids: Vec<NodeId> = (1..=num_nodes).map(|i| NodeId(i as u64)).collect();

        let mut disks = HashMap::new();
        let mut node_configs = Vec::new();

        for &id in &node_ids {
            disks.insert(id, StableDisk::new(id));
            let peers = node_ids.iter().copied().filter(|&p| p != id).collect();
            node_configs.push(Config {
                id,
                peers,
                election_timeout_ticks: 15,
                heartbeat_interval_ticks: 4,
                rng_seed: rng.gen(),
            });
        }

        let mut sim = Self {
            seed,
            rng,
            nodes: HashMap::new(),
            disks,
            network: SimNetwork::new(),
            invariants: SafetyInvariants::new(),
            tick_count: 0,
            node_configs,
        };

        for config in sim.node_configs.clone() {
            sim.spawn_node(config.id);
        }

        sim
    }

    pub fn spawn_node(&mut self, id: NodeId) {
        let config = self.node_configs.iter().find(|c| c.id == id).cloned().unwrap();
        let disk = self.disks.get(&id).unwrap().clone();

        // Reconstruct log cleanly from snapshot meta + entries
        let reconstructed_log = if let Some((meta, _)) = &disk.snapshot {
            raft_core::log::RaftLog::restore(*meta, disk.log_entries[1..].to_vec())
        } else {
            raft_core::log::RaftLog::restore(
                raft_core::message::SnapshotMeta {
                    last_included_index: LogIndex::SENTINEL,
                    last_included_term: Term::ZERO,
                },
                disk.log_entries[1..].to_vec(),
            )
        };

        // Recovery volatile variables default to snapshot indexes during recovery
        let initial_commit_applied = disk.snapshot.as_ref()
            .map(|(meta, _)| meta.last_included_index)
            .unwrap_or(LogIndex::SENTINEL);

        let node = RaftNode::restore(
            config,
            disk.current_term,
            disk.voted_for,
            reconstructed_log,
            initial_commit_applied,
            initial_commit_applied,
        );

        self.nodes.insert(id, node);
    }

    pub fn crash_node(&mut self, id: NodeId) {
        self.nodes.remove(&id);
    }

    pub fn network_mut(&mut self) -> &mut SimNetwork {
        &mut self.network
    }

    pub fn step(&mut self) -> Result<(), String> {
        self.tick_count += 1;

        // 1. Deliver pending network messages
        let deliveries = self.network.drain_due(self.tick_count);
        for msg in deliveries {
            let to_id = msg.to;
            let actions = if let Some(node) = self.nodes.get_mut(&to_id) {
                Some(node.step(msg))
            } else {
                None
            };

            if let Some(acts) = actions {
                self.process_actions(to_id, acts);
            }
        }

        // 2. Tick all alive nodes
        let alive_ids: Vec<NodeId> = self.nodes.keys().copied().collect();
        for id in alive_ids {
            let actions = if let Some(node) = self.nodes.get_mut(&id) {
                Some(node.tick())
            } else {
                None
            };

            if let Some(acts) = actions {
                self.process_actions(id, acts);
            }
        }

        // 3. Trigger proactive compaction on nodes whose logs grew past 15 entries!
        // This forces snapshotting and triggers the fallback replication logic.
        let alive_ids: Vec<NodeId> = self.nodes.keys().copied().collect();
        for id in alive_ids {
            let actions = if let Some(node) = self.nodes.get_mut(&id) {
                if node.log().len() > 15 {
                    let commit_idx = node.commit_index();
                    let commit_term = node.log().term_at(commit_idx).unwrap_or(Term::ZERO);
                    Some(node.compact_logs(commit_idx, commit_term, b"snapshot-data-compaction".to_vec()))
                } else {
                    None
                }
            } else {
                None
            };

            if let Some(acts) = actions {
                self.process_actions(id, acts);
            }
        }

        // 4. Verify invariants after step
        self.invariants.verify_safety(&self.nodes)?;

        Ok(())
    }

    fn process_actions(&mut self, from_id: NodeId, actions: Vec<Action>) {
        for action in actions {
            match action {
                Action::Send(msg) => {
                    self.network.send(msg, self.tick_count, &mut self.rng);
                }
                Action::AppendEntries { entries } => {
                    let disk = self.disks.get_mut(&from_id).unwrap();
                    disk.append_entries(&entries);
                }
                Action::TruncateAndAppend { from_index, entries } => {
                    let disk = self.disks.get_mut(&from_id).unwrap();
                    disk.truncate_and_append(from_index, &entries);
                }
                Action::PersistHardState { term, voted_for } => {
                    let disk = self.disks.get_mut(&from_id).unwrap();
                    disk.write_hard_state(term, voted_for);
                }
                Action::Apply { .. } => {}
                Action::PersistSnapshot { last_included_index, last_included_term, data } => {
                    let disk = self.disks.get_mut(&from_id).unwrap();
                    disk.write_snapshot(
                        raft_core::message::SnapshotMeta {
                            last_included_index,
                            last_included_term,
                        },
                        data,
                    );
                }
            }
        }
    }

    pub fn propose_command(&mut self, client_cmd: Vec<u8>) -> bool {
        let leader_info = self.nodes.iter()
            .find(|(_, n)| n.role() == Role::Leader)
            .map(|(&id, _)| id);

        if let Some(leader_id) = leader_info {
            let actions = if let Some(node) = self.nodes.get_mut(&leader_id) {
                Some(node.propose(client_cmd))
            } else {
                None
            };

            if let Some(acts) = actions {
                self.process_actions(leader_id, acts);
                return true;
            }
        }
        false
    }
}