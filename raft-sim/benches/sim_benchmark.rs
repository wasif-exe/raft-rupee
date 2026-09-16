use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use raft_sim::simulator::Simulator;
use raft_core::types::NodeId;

fn bench_simulation_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("deterministic_simulation");
    group.throughput(Throughput::Elements(1000));

    group.bench_function("sim_1000_ticks_with_invariants", |b| {
        b.iter(|| {
            let mut sim = Simulator::new(black_box(0xDEADBEEF), 5);
            {
                let network = sim.network_mut();
                network.drop_rate = 0.05;
                network.max_delay_ticks = 2;
                network.partition(NodeId(1), NodeId(5));
            }

            for tick in 0..1000 {
                if tick % 10 == 0 {
                    sim.propose_command(b"bench_cmd".to_vec());
                }
                let _ = sim.step();
            }
        });
    });

    group.finish();
}

criterion_group!(benches, bench_simulation_throughput);
criterion_main!(benches);
