#!/bin/bash
set -e

# 1. Clean up old runs and directories
echo "🧹 Cleaning up previous run state..."
rm -rf /tmp/raft-node-1 /tmp/raft-node-2 /tmp/raft-node-3
mkdir -p /tmp/raft-node-1 /tmp/raft-node-2 /tmp/raft-node-3

# 2. Build the workspace binaries
echo "⚙️ Building Raft-₹ binaries..."
cargo build --workspace

# Peer connection arguments
PEERS="1=127.0.0.1:8001 2=127.0.0.1:8002 3=127.0.0.1:8003"

# 3. Spin up the 3 nodes as background processes
echo "🚀 Booting physical local cluster..."
./target/debug/raft-server 1 /tmp/raft-node-1 $PEERS > /tmp/node1.log 2>&1 &
PID1=$!
./target/debug/raft-server 2 /tmp/raft-node-2 $PEERS > /tmp/node2.log 2>&1 &
PID2=$!
./target/debug/raft-server 3 /tmp/raft-node-3 $PEERS > /tmp/node3.log 2>&1 &
PID3=$!

# Trap script exits to kill the server processes automatically
cleanup() {
    echo "Stopping Raft-Server processes (PIDs: $PID1, $PID2, $PID3)..."
    kill $PID1 $PID2 $PID3 || true
    exit
}
trap cleanup EXIT INT TERM

echo "⏳ Waiting 3 seconds for leader election to stabilize..."
sleep 3

# 4. Propose a write command through the Client
echo "✍️ Submitting transaction 'set_rupee=1000' through Client to Node 1..."
./target/debug/raft-client 127.0.0.1:8001 "set_rupee=1000"

echo "⏳ Waiting 2 seconds for log replication to complete..."
sleep 2

# 5. Output server log files to see the network action
echo -e "\n=== Node 1 Logs ==="
tail -n 10 /tmp/node1.log || true

echo -e "\n=== Node 2 Logs ==="
tail -n 10 /tmp/node2.log || true

# 6. Check the physical storage files
echo -e "\n=== Durable Storage Files Generated ==="
ls -l /tmp/raft-node-1/

echo -e "\n=== Hexadecimal Dump of physical O_DIRECT WAL Log (proving hardware alignment) ==="
hexdump -C /tmp/raft-node-1/wal.log | head -n 20 || true

