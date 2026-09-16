use raft_sim::simulator::Simulator;
use raft_core::types::NodeId;
use rand::{Rng, SeedableRng};
use rand::rngs::SmallRng;
use std::collections::HashSet;

#[test]
fn test_chaos_deterministic_sim() {
    for seed in 0..100 {
        let mut test_rng = SmallRng::seed_from_u64(seed);
        let mut sim = Simulator::new(seed, 5);
        
        {
            let network = sim.network_mut();
            network.drop_rate = 0.15;
            network.duplicate_rate = 0.05;
            network.max_delay_ticks = 4;
        }

        let mut crashed_nodes = HashSet::new();

        for tick in 0..1000 {
            if tick % 100 == 0 {
                let network = sim.network_mut();
                network.heal_all();
                
                network.partition(NodeId(1), NodeId(3));
                network.partition(NodeId(1), NodeId(4));
                network.partition(NodeId(1), NodeId(5));
                network.partition(NodeId(2), NodeId(3));
                network.partition(NodeId(2), NodeId(4));
                network.partition(NodeId(2), NodeId(5));
            }

            if tick % 30 == 0 && test_rng.gen_bool(0.3) {
                let victim_id = NodeId(test_rng.gen_range(1..=5));
                if crashed_nodes.contains(&victim_id) {
                    sim.spawn_node(victim_id);
                    crashed_nodes.remove(&victim_id);
                } else {
                    if crashed_nodes.len() < 2 {
                        sim.crash_node(victim_id);
                        crashed_nodes.insert(victim_id);
                    }
                }
            }

            if tick % 15 == 0 {
                let cmd = format!("val-{}", tick).into_bytes();
                sim.propose_command(cmd);
            }

            if let Err(violation) = sim.step() {
                panic!(
                    "SAFETY INVARIANT BROKEN on Seed 0x{:016X} at tick {}: {}", 
                    seed, tick, violation
                );
            }
        }
    }
}

#[test]
fn test_pre_vote_disruptive_server_prevention() {
    let mut sim = Simulator::new(42, 3);
    
    {
        let network = sim.network_mut();
        network.drop_rate = 0.0;
        network.duplicate_rate = 0.0;
        network.max_delay_ticks = 0;
    }

    for _ in 0..40 {
        sim.step().unwrap();
    }

    sim.propose_command(b"initial-write".to_vec());
    for _ in 0..10 {
        sim.step().unwrap();
    }

    // Partition Node 3 (victim) completely from Node 1 and Node 2
    {
        let network = sim.network_mut();
        network.partition(NodeId(3), NodeId(1));
        network.partition(NodeId(3), NodeId(2));
        network.partition(NodeId(1), NodeId(3));
        network.partition(NodeId(2), NodeId(3));
    }

    // Run isolated for 150 ticks
    for _ in 0..150 {
        sim.step().unwrap();
    }

    // Heal the partition completely
    {
        let network = sim.network_mut();
        network.heal_all();
    }

    // Run the cluster for another 30 ticks
    for _ in 0..30 {
        sim.step().unwrap();
    }

    let write_succeeded = sim.propose_command(b"disruption-free-write".to_vec());
    assert!(write_succeeded, "Pre-vote failed to prevent disruptive partitioned server from hijacking election term!");
}