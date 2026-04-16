# Jackfruit: Distributed Collaborative Canvas

## Project Overview

**Jackfruit** is a distributed, fault-tolerant collaborative drawing application built on top of the **RAFT consensus algorithm**. It allows multiple users to draw on a shared canvas in real-time, with guaranteed consistency even if some servers fail.

The system consists of:
- **3 RAFT replica nodes** — maintain a replicated, consistent log of drawing strokes
- **1 WebSocket gateway** — manages client connections and routes requests to the RAFT leader
- **Browser frontend** — a real-time collaborative canvas UI

---

## High-Level Architecture

```
┌──────────────────────────────────────┐
│  Browser 1    Browser 2   Browser 3  │
│  (WebSocket)  (WebSocket) (WebSocket)│
└────────────────┬────────────────┬────┘
                 │                │
        ╔════════╩════════════════╩═════════╗
        ║  Gateway (Port 8080)              ║
        ║  - Accepts WS connections         ║
        ║  - Tracks leader                  ║
        ║  - Forwards strokes → leader      ║
        ║  - Broadcasts committed strokes   ║
        ╚════════╤════════════════╤═════════╝
                 │                │
    ┌────────────┼────────────────┼────────────┐
    │            │                │            │
    ▼            ▼                ▼            ▼
┌────────┐  ┌────────┐      ┌────────┐  ┌────────┐
│Replica1│  │Replica2│  ─── │Replica3│  │(spare) │
│ RAFT   │  │ RAFT   │      │ RAFT   │  │        │
│Leader  │  │Follow. │      │Follow. │  │        │
│(5001)  │  │(5002)  │      │(5003)  │  │        │
└────────┘  └────────┘      └────────┘  └────────┘
```

---

## System Components

### 1. **Gateway** (`gateway/main.py`)

The gateway is the entry point for all client connections. It runs a FastAPI server with WebSocket support.

**Responsibilities:**
- Accept WebSocket connections from browser clients (`/ws` endpoint)
- Run a **background leader-discovery loop** that continuously polls replicas to find the current leader
- Forward incoming strokes from clients to the RAFT leader's `/stroke` endpoint
- Broadcast committed strokes to all connected clients
- Handle automatic failover — if the leader changes, the gateway updates its target

**Key Endpoints:**
- `GET /` — Serves the frontend HTML
- `GET /health` — Reports gateway state (leader URL, number of connected clients)
- `WebSocket /ws` — Client connection endpoint
- `POST /stroke` — Internal (not directly called by clients)

**Client Flow:**
```
Browser stroke → WS /ws → Gateway → POST /stroke on leader → 
→ Leader commits → Gateway /append-entries broadcasts → WS send to all clients
```

**Leader Discovery:**
- Every 1 second, the gateway calls `/health` on all 3 replicas
- It checks which one has `"role": "leader"`
- If a new leader is elected, the gateway updates `gw.leader_url`
- If no leader is found (election in progress), it waits and retries

---

### 2. **Replica Nodes** (`replica/main.py`, `replica1/`, `replica2/`, `replica3/`)

Each replica is an independent RAFT node that manages a distributed log. All three replicas run the same code (from `./replica/`) but in separate containers with their own volumes (`./replica1/`, `./replica2/`, `./replica3/`). This allows hot-reloading: edit `replica1/main.py` and only that container restarts.

**RAFT State Machine:**

Each replica maintains:
- **Persistent state** — survives across restarts
  - `current_term` — the RAFT term (election epoch)
  - `voted_for` — which candidate we voted for in this term
  
- **Volatile state** — lost on crash, rebuilt from log
  - `role` — FOLLOWER, CANDIDATE, or LEADER
  - `leader_id` — the ID of the current leader
  - `log[]` — list of log entries (strokes)
  - `commit_index` — the index of the last committed entry

**RAFT Roles:**

1. **Follower** (initial state)
   - Receives heartbeats from the leader
   - Resets election timer when heartbeat arrives
   - If no heartbeat for election_timeout (500–800 ms), becomes CANDIDATE

2. **Candidate** (during election)
   - Increments `current_term`
   - Votes for itself
   - Sends `RequestVote` RPC to all peers
   - If it receives votes from a majority, becomes LEADER
   - If it receives `AppendEntries` from a higher term, steps down to FOLLOWER

3. **Leader**
   - Sends `AppendEntries` (hearbeat) to all followers every 150 ms
   - When a stroke arrives at `/stroke`, appends it to its own log
   - Replicates the entry to all followers
   - Commits the entry when a majority acknowledges
   - Returns the committed entry so gateway can broadcast it

**Key Endpoints:**

- `GET /health` — Returns role, term, leader_id (used by gateway for discovery)
- `GET /status` — Full state dump (for debugging)
- `POST /request-vote` — RPC for leader election
- `POST /append-entries` — RPC for heartbeats and log replication
- `POST /stroke` — Gateway sends stroke here (leader only)
- `POST /sync-log` — Used by followers to catch up if they fall behind

**RAFT Message Flow:**

```
ELECTION (500–800 ms timeout):
  Follower → RequestVote RPC → Peers
  Peers evaluate: "Is candidate's log at least as up-to-date as mine?"
  If yes → grant vote
  Candidate collects votes → if majority → becomes LEADER

HEARTBEAT & REPLICATION (150 ms interval):
  Leader → AppendEntries RPC → Followers
  If entries are included: Followers append → send back match_index
  Leader collects ACKs → if majority → advances commit_index
```

---

### 3. **Frontend** (`gateway/static/index.html`)

A modern, responsive web UI for collaborative drawing.

**UI Elements:**
- **Header** — Shows connection status (connecting/connected/error) with animated dot
- **Toolbar** — Color picker (5 colors), brush size (S/M/L), clear canvas button
- **Canvas** — Full-screen drawing area, supports mouse and touch
- **Log panel** — Bottom-right, shows connection/sync events

**How Drawing Works:**
1. User draws on canvas with mouse or touch
2. For each stroke segment (line between two points):
   - Draw immediately on local canvas (instant feedback)
   - Send stroke JSON to gateway via WebSocket
3. When stroke commits on the leader:
   - Gateway receives the committed entry
   - Broadcasts to all connected clients
   - Each client renders the stroke
4. If connection drops:
   - Overlay appears ("reconnecting to cluster...")
   - Canvas is cleared (local changes are discarded)
   - Auto-reconnects with exponential backoff (1s → 8s)

**WebSocket Messages:**

Client → Gateway:
```json
{
  "x0": 100,
  "y0": 200,
  "x1": 120,
  "y1": 220,
  "color": "#00ffcc",
  "lineWidth": 3
}
```

Gateway → All Clients:
```json
{
  "type": "stroke",
  "entry": {
    "index": 5,
    "term": 2,
    "stroke": {
      "x0": 100,
      "y0": 200,
      "x1": 120,
      "y1": 220,
      "color": "#00ffcc",
      "lineWidth": 3
    }
  }
}
```

---

## Data Flow: A Complete Stroke

Here's what happens when a user draws a stroke:

```
1. USER DRAWS on canvas
   └─→ Browser JS: drawLine() + sendStroke()

2. BROWSER SENDS WEBSOCKET MESSAGE to Gateway
   └─→ Gateway receives JSON with x0, y0, x1, y1, color, lineWidth

3. GATEWAY DISCOVERS LEADER (1s loop)
   └─→ If no leader cached: polls /health on all 3 replicas
   └─→ Finds the one with role="leader"

4. GATEWAY FORWARDS STROKE TO LEADER
   POST /stroke { "stroke": {...} }
   └─→ Leader appends to its log
   └─→ Leader returns entry with index, term

5. LEADER REPLICATES TO FOLLOWERS
   └─→ Parallel POST /append-entries to replica2, replica3
   └─→ Followers append entry to their logs
   └─→ Each follower responds with success=true + match_index

6. LEADER COMMITS WHEN MAJORITY ACK
   └─→ Leader has ACK from itself (1) + at least 1 follower (total 2 of 3)
   └─→ Leader advances commit_index
   └─→ Leader returns committed entry to gateway

7. GATEWAY BROADCASTS TO ALL CLIENTS
   POST /ws send_json { "type": "stroke", "entry": {...} }
   └─→ Each connected browser receives the stroke
   └─→ Canvas renders the remote stroke

8. ALL CLIENTS NOW HAVE IDENTICAL STATE
   └─→ Every connected user sees the same drawing
```

---

## How RAFT Ensures Consistency

### 1. **Leader Election**
- If a follower doesn't hear from the leader for 500–800 ms, it becomes a candidate
- Candidate increments term and requests votes from peers
- Peers grant votes only if candidate's log is as up-to-date as theirs
- Candidate wins if it gets votes from a majority (2 out of 3)
- Only one leader per term is possible (safety violation returns 403)

### 2. **Log Replication**
- Leader appends new entries to its log
- Leader sends `AppendEntries` RPC with entries to all followers
- Follower only appends if consistency check passes (previous entry's term matches)
- Leader waits for a majority of ACKs before committing
- Committed entries are applied to the state machine (drawing canvas)

### 3. **Crash Recovery**
- If a non-leader crashes and restarts, it's a follower at an older term
- Leader's `AppendEntries` brings it up-to-date with consistency checks
- If a leader crashes, replicas elect a new leader within 500–800 ms
- Uncommitted entries are not visible to clients

### 4. **Safety Guarantees**
- **Election safety**: At most one leader per term
- **Log matching**: If entries match at an index, all entries before match too
- **Leader completeness**: Leader has all committed entries
- **State machine safety**: Same entries applied in same order → identical state

---

## Deployment & Docker Compose

**Service Topology** (defined in `docker-compose.yml`):

```yaml
gateway:
  - Listens on port 8080
  - Depends on all 3 replicas being healthy
  - Environment: REPLICA_URLS=http://replica1:5000,...

replica1, replica2, replica3:
  - Each listens on port 5000 (internally), exposed on 5001, 5002, 5003
  - Each has a unique REPLICA_ID (1, 2, 3)
  - Each knows its peers: PEERS=http://replica2:5000,http://replica3:5000
  - Healthcheck: curl /health every 5 seconds
  - Volume-mounted for hot reload
```

**Network:**
- All services on `raft-network` bridge network
- Gateway reaches replicas by service name: `http://replica1:5000`

**Startup Order:**
1. Docker Compose starts all services
2. Replicas perform health checks (initially fail, then pass)
3. Gateway waits for all replicas to pass health checks
4. Gateway starts and begins leader discovery loop
5. First client connects, draws, and triggers leader election if needed

---

## Setup & Running

### Setup Script (`setup.sh`)
Copies the shared `./replica` directory to `./replica1`, `./replica2`, `./replica3`:
```bash
bash setup.sh
```
This scaffolds the individual directories. After this, edits to `replica1/main.py` only affect that container.

### Running Locally
```bash
docker-compose up --build
```
Then open browser to `http://localhost:8080`

### Scale to More Replicas
To add additional replicas, modify `docker-compose.yml`:
```yaml
replica4:
  build: ./replica
  ports: ["5004:5000"]
  environment:
    - REPLICA_ID=4
    - PEERS=http://replica1:5000,http://replica2:5000,http://replica3:5000
  volumes: ["./replica4:/usr/src/app"]
  healthcheck: [...]
```

Update gateway `REPLICA_URLS` and run `mkdir replica4 && cp -r replica replica4`.

---

## Fault Tolerance Scenarios

### Scenario 1: Follower Crashes
- Other 2 replicas elect a leader
- Crashed replica restarts as follower
- Leader replicates missing entries, catches it up
- **Result**: No data loss, system continues

### Scenario 2: Leader Crashes
- Remaining 2 followers detect missing heartbeat
- One becomes candidate, wins election
- Become new leader, continue accepting strokes
- Old leader restarts as follower
- **Result**: ~500–800 ms pause, then recovery

### Scenario 3: Majority Fails (2 out of 3 down)
- Remaining 1 replica cannot form quorum
- System stops accepting new strokes (403 Not Leader)
- When any replica restarts, new leader is elected
- **Result**: Read-only until majority recovers

### Scenario 4: Network Partition
If replicas A & B separate from C:
- A & B partition: can elect leader from {A, B} (no majority of {A, B, C})
- C partition: cannot elect leader
- Clients connected to A & B: keep drawing (committed with 2-of-2 quorum)
- Clients connected to C: get 503 / 403
- When partition heals: higher-term leader wins, C catches up by log replication
- **Result**: Temporarily divergent but consistent after healing

---

## Performance Characteristics

| Metric | Value | Notes |
|--------|-------|-------|
| **Leader Detection** | ~1 second | Polling interval |
| **Leader Election** | 500–800 ms | Timeout + RPC RTT |
| **Stroke Latency** | ~50–200 ms | Network + leader processing |
| **Replication** | Parallel | All followers replicated simultaneously |
| **Commit Threshold** | 2 out of 3 | Majority quorum |
| **Heartbeat Interval** | 150 ms | Keeps followers alive |

---

## Key Files & Responsibilities

| File | Purpose | Key Functions |
|------|---------|---|
| `docker-compose.yml` | Service orchestration | Defines gateway, replica1, replica2, replica3 |
| `setup.sh` | One-time setup | Creates replica1/, replica2/, replica3/ dirs |
| `gateway/main.py` | Client hub & RPC client | WebSocket server, leader discovery, forwarding |
| `replica/main.py` | RAFT node | State machine, RPCs, election, replication |
| `gateway/static/index.html` | Frontend UI & canvas | Drawing canvas, WebSocket client, rendering |
| `gateway/requirements.txt` | Python deps | FastAPI, uvicorn, httpx, websockets |

---

## Development Tips

### Debugging
```bash
# Watch a replica's state
curl http://localhost:5001/status | jq .

# Check gateway status
curl http://localhost:8080/health | jq .

# Check leader
curl http://localhost:5001/health | jq '.role'
```

### Hot Reload
Edit `replica1/main.py` → uvicorn auto-reloads only replica1 container (thanks to volume mount).

### Logs
```bash
docker-compose logs -f gateway
docker-compose logs -f replica1
```

### Scale Down to 1 or 2 Replicas
For testing, modify `docker-compose.yml` to run fewer services. RAFT still works (quorum = 1 of 1, or 1 of 2).

---

## Future Enhancements

- **Persistence** — WAL (Write-Ahead Log) to disk for durability
- **Snapshotting** — Compress old log entries into snapshots
- **Dynamic Membership** — Add/remove replicas without restart
- **Leader Stickiness** — Prefer elected leader for faster failover
- **Client Sessions** — Track which strokes came from which user
- **Canvas History** — Undo/redo backed by log replication
- **Multi-Canvas** — Multiple independent drawing boards, each with own RAFT cluster

---

## Architecture Summary

**Consensus**: RAFT algorithm ensures all replicas maintain identical stroke logs.  
**Gateway**: Routes clients to current leader, broadcasts committed strokes.  
**Frontend**: Real-time collaborative canvas with auto-reconnect.  
**Fault Tolerance**: Survives crash of any 1 node, continues operation while majority is healthy.  
**Consistency**: Stronger than eventual consistency — all committed strokes are visible to all clients in order.

---

## References

- RAFT Consensus Algorithm: https://raft.io/
- FastAPI: https://fastapi.tiangolo.com/
- Docker Compose: https://docs.docker.com/compose/
