pub mod storage;
pub mod codec;

use std::collections::HashMap;
use std::net::{TcpListener, TcpStream};
use std::time::{Duration, Instant};
use std::path::PathBuf;
use std::env;

use raft_core::node::{RaftNode, Config, Action};
use raft_core::types::{NodeId, LogIndex, Term};
use raft_core::message::Message;
use crate::storage::DurableDisk;
use crate::codec::{send_framed_msg, recv_framed_msg};

fn main() -> std::io::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 4 {
        eprintln!("Usage: raft-server <node_id> <base_dir> <peer_id_1=addr_1> <peer_id_2=addr_2> ...");
        std::process::exit(1);
    }

    let node_id = NodeId(args[1].parse::<u64>().unwrap());
    let base_dir = PathBuf::from(&args[2]);

    let mut peer_addresses = HashMap::new();
    for peer_arg in &args[3..] {
        let parts: Vec<&str> = peer_arg.split('=').collect();
        let pid = NodeId(parts[0].parse::<u64>().unwrap());
        let addr = parts[1].to_string();
        peer_addresses.insert(pid, addr);
    }

    println!("Starting Raft-₹ Node {}, Disk Directory: {:?}", node_id, base_dir);

    let mut disk = DurableDisk::open(&base_dir)?;
    
    let (recovered_term, recovered_vote) = disk.read_hard_state()?
        .unwrap_or((Term::ZERO, None));
    
    let replayed_entries = disk.replay_wal()?;
    println!("DURABILITY RECOVERY: Replayed {} entries from WAL logs", replayed_entries.len());

    let reconstructed_log = raft_core::log::RaftLog::restore(
        raft_core::message::SnapshotMeta {
            last_included_index: LogIndex::SENTINEL,
            last_included_term: Term::ZERO,
        },
        replayed_entries,
    );

    let config = Config {
        id: node_id,
        peers: peer_addresses.keys().copied().collect(),
        election_timeout_ticks: 15,
        heartbeat_interval_ticks: 3,
        rng_seed: rand::random(),
    };

    let mut node = RaftNode::restore(
        config,
        recovered_term,
        recovered_vote,
        reconstructed_log,
        LogIndex::SENTINEL,
        LogIndex::SENTINEL,
    );

    let self_addr = peer_addresses.remove(&node_id)
        .expect("Self address configuration must be present inside peers list");
    let listener = TcpListener::bind(&self_addr)?;
    listener.set_nonblocking(true)?;

    println!("Server bound to {}, listening for connections...", self_addr);

    let mut peer_connections: HashMap<NodeId, TcpStream> = HashMap::new();
    let mut client_connections: Vec<TcpStream> = Vec::new();
    let mut last_tick = Instant::now();

    loop {
        // 1. Accept incoming streams
        if let Ok((stream, _)) = listener.accept() {
            stream.set_nonblocking(true).unwrap();
            client_connections.push(stream);
        }

        // 2. Drive the logical clock ticks
        if last_tick.elapsed() >= Duration::from_millis(50) {
            let actions = node.tick();
            process_actions(&mut peer_connections, &peer_addresses, &mut disk, actions, None);
            last_tick = Instant::now();
        }

        // 3. Connect to unconnected peers
        for (&peer_id, addr) in &peer_addresses {
            if !peer_connections.contains_key(&peer_id) {
                if let Ok(stream) = TcpStream::connect(addr) {
                    stream.set_nonblocking(true).unwrap();
                    peer_connections.insert(peer_id, stream);
                    println!("NET: Successfully established connection to Peer {}", peer_id);
                }
            }
        }

        // 4. Poll incoming messages from connected peers
        let peer_ids: Vec<NodeId> = peer_connections.keys().copied().collect();
        for peer_id in peer_ids {
            let mut disconnected = false;
            if let Some(stream) = peer_connections.get_mut(&peer_id) {
                match recv_framed_msg(stream) {
                    Ok(msg) => {
                        let actions = node.step(msg);
                        process_actions(&mut peer_connections, &peer_addresses, &mut disk, actions, None);
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                    Err(_) => {
                        disconnected = true;
                    }
                }
            }
            if disconnected {
                peer_connections.remove(&peer_id);
                println!("NET: Connection lost to Peer {}", peer_id);
            }
        }

        // 5. Poll incoming messages from client connections
        let mut closed_clients = Vec::new();
        for (idx, stream) in client_connections.iter_mut().enumerate() {
            match recv_framed_msg(stream) {
                Ok(msg) => {
                    let actions = node.step(msg);
                    process_actions(&mut peer_connections, &peer_addresses, &mut disk, actions, Some(stream));
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(_) => {
                    closed_clients.push(idx);
                }
            }
        }
        for idx in closed_clients.into_iter().rev() {
            client_connections.swap_remove(idx);
        }

        std::thread::sleep(Duration::from_millis(5));
    }
}

fn process_actions(
    conns: &mut HashMap<NodeId, TcpStream>,
    peer_addrs: &HashMap<NodeId, String>,
    disk: &mut DurableDisk,
    actions: Vec<Action>,
    current_client: Option<&mut TcpStream>,
) {
    let mut client_stream = current_client;

    for action in actions {
        match action {
            Action::Send(msg) => {
                let to_id = msg.to;
                // If destination is a client response (NodeId 999), reply on the active client stream
                if to_id == NodeId(999) {
                    if let Some(ref mut c_stream) = client_stream {
                        let _ = send_framed_msg(c_stream, &msg);
                    }
                } else if let Some(stream) = conns.get_mut(&to_id) {
                    let _ = send_framed_msg(stream, &msg);
                } else if let Some(addr) = peer_addrs.get(&to_id) {
                    if let Ok(mut stream) = TcpStream::connect(addr) {
                        stream.set_nonblocking(true).unwrap();
                        let _ = send_framed_msg(&mut stream, &msg);
                        conns.insert(to_id, stream);
                    }
                }
            }
            Action::AppendEntries { entries } => {
                disk.append_log_entries(&entries).unwrap();
            }
            Action::TruncateAndAppend { from_index, entries } => {
                disk.truncate_and_append(from_index, &entries).unwrap();
            }
            Action::PersistHardState { term, voted_for } => {
                disk.write_hard_state(term, voted_for).unwrap();
            }
            Action::Apply { entries } => {
                for entry in entries {
                    if !entry.command.is_empty() {
                        println!("STATE MACHINE APPLY: Committing write at Index L{}", entry.index);
                    }
                }
            }
            Action::PersistSnapshot { .. } => {}
        }
    }
}
