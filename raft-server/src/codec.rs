use std::io::{Read, Write};
use std::net::TcpStream;
use raft_core::message::Message;

/// Write a serialized Message framed with a big-endian u32 length prefix.
pub fn send_framed_msg(stream: &mut TcpStream, msg: &Message) -> std::io::Result<()> {
    let payload = bincode::serialize(msg)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    
    let len = payload.len() as u32;
    stream.write_all(&len.to_be_bytes())?;
    stream.write_all(&payload)?;
    stream.flush()?;
    Ok(())
}

/// Read a framed Message from the stream.
pub fn recv_framed_msg(stream: &mut TcpStream) -> std::io::Result<Message> {
    let mut len_bytes = [0u8; 4];
    stream.read_exact(&mut len_bytes)?;
    let len = u32::from_be_bytes(len_bytes) as usize;

    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload)?;

    let msg = bincode::deserialize(&payload)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    Ok(msg)
}