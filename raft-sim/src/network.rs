use std::collections::{BinaryHeap, HashSet};
use std::cmp::Ordering;
use rand::rngs::SmallRng;
use rand::Rng;
use raft_core::message::Message;
use raft_core::types::NodeId;

#[derive(Debug, Clone)]
pub struct ScheduledMessage {
    pub delivery_tick: u64,
    pub msg: Message,
}

impl PartialEq for ScheduledMessage {
    fn eq(&self, other: &Self) -> bool {
        self.delivery_tick == other.delivery_tick
    }
}
impl Eq for ScheduledMessage {}

impl Ord for ScheduledMessage {
    fn cmp(&self, other: &Self) -> Ordering {
        other.delivery_tick.cmp(&self.delivery_tick)
    }
}

impl PartialOrd for ScheduledMessage {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub struct SimNetwork {
    in_flight: BinaryHeap<ScheduledMessage>,
    partitions: HashSet<(NodeId, NodeId)>,
    pub drop_rate: f64,
    pub duplicate_rate: f64,
    pub max_delay_ticks: u64,
}

impl SimNetwork {
    pub fn new() -> Self {
        Self {
            in_flight: BinaryHeap::new(),
            partitions: HashSet::new(),
            drop_rate: 0.0,
            duplicate_rate: 0.0,
            max_delay_ticks: 3,
        }
    }

    pub fn partition(&mut self, from: NodeId, to: NodeId) {
        self.partitions.insert((from, to));
    }

    pub fn heal_all(&mut self) {
        self.partitions.clear();
    }

    pub fn send(&mut self, msg: Message, current_tick: u64, rng: &mut SmallRng) {
        if self.partitions.contains(&(msg.from, msg.to)) {
            return;
        }

        if rng.gen_bool(self.drop_rate) {
            return;
        }

        let delay = if self.max_delay_ticks == 0 {
            0
        } else {
            rng.gen_range(0..=self.max_delay_ticks)
        };

        let delivery_tick = current_tick + delay;
        self.in_flight.push(ScheduledMessage {
            delivery_tick,
            msg: msg.clone(),
        });

        if rng.gen_bool(self.duplicate_rate) {
            let extra_delay = delay + rng.gen_range(1..=3);
            self.in_flight.push(ScheduledMessage {
                delivery_tick: current_tick + extra_delay,
                msg,
            });
        }
    }

    pub fn drain_due(&mut self, current_tick: u64) -> Vec<Message> {
        let mut due = Vec::new();
        while let Some(msg) = self.in_flight.peek() {
            if msg.delivery_tick <= current_tick {
                due.push(self.in_flight.pop().unwrap().msg);
            } else {
                break;
            }
        }
        due
    }
}
