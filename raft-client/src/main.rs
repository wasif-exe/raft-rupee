use std::net::TcpStream;
use std::io::{Read, Write};
use std::env;
use std::time::Duration;
use raft_core::message::{Message, MessageKind};
use raft_core::types::{NodeId, Term};

fn send_framed(stream: &mut TcpStream, msg: &Message) -> std::io::Result<()> {
    let payload = bincode::serialize(msg)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let len = payload.len() as u32;
    stream.write_all(&len.to_be_bytes())?;
    stream.write_all(&payload)?;
    stream.flush()?;
    Ok(())
}

fn recv_framed(stream: &mut TcpStream) -> std::io::Result<Message> {
    let mut len_bytes = [0u8; 4];
    stream.read_exact(&mut len_bytes)?;
    let len = u32::from_be_bytes(len_bytes) as usize;
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload)?;
    let msg = bincode::deserialize(&payload)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    Ok(msg)
}

fn main() -> std::io::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: raft-client <server_address> <command_payload>");
        std::process::exit(1);
    }

    let server_addr = &args[1];
    let payload = args[2].as_bytes().to_vec();

    println!("Connecting to Raft-₹ Node at {}...", server_addr);
    let mut stream = TcpStream::connect(server_addr)?;
    stream.set_read_timeout(Some(Duration::from_secs(3)))?;

    let req = Message {
        from: NodeId(999), // Client identifier
        to: NodeId(0),     // Addressed to cluster
        term: Term::ZERO,
        kind: MessageKind::ClientRequest {
            command: payload.clone(),
        },
    };

    send_framed(&mut stream, &req)?;

    match recv_framed(&mut stream) {
        Ok(Message { kind: MessageKind::ClientResponse { success, leader_hint }, .. }) => {
            if success {
                println!("✅ TRANSACTION COMMITTED: Payload '{}' committed by Leader!", String::from_utf8_lossy(&payload));
            } else {
                println!("⚠️ REDIRECT: Node was not leader. Leader Hint: {:?}", leader_hint);
            }
        }
        Ok(other) => {
            println!("Received unexpected response: {:?}", other);
        }
        Err(e) => {
            println!("Response read timeout or error: {:?}", e);
        }
    }

    Ok(())
}
