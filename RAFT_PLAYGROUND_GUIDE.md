# RAFT Playground Guide

## Overview

The Jackfruit application is now a full **RAFT consensus playground** where you can:
- Watch elections happen in real-time
- Crash and recover replicas to observe RAFT behavior
- See term advancement and leader changes
- Monitor log replication across the cluster
- Understand RAFT consensus in action

## Quick Start

### 1. Start the Application

```bash
cd ~/Downloads/Jackfruit/Jackfruit
docker-compose up --build
```

Open http://localhost:8080 in your browser.

### 2. Open Network Monitoring Tab

Click the **Network** button in the toolbar to open the RAFT playground dashboard.

### 3. Watch the RAFT Cluster

You'll see all 3 replicas with:
- **Replica ID** and current **Role** (Leader/Candidate/Follower)
- **Term** (current election term)
- **Leader ID** (which replica is the leader)
- **Log Entries** (total entries in the log)
- **Commit Index** (highest committed index)
- **Health Status** (green dot = healthy, red = offline/crashed)
- **Updated** timestamp (last health check)

---

## RAFT Playground Features

### Configuration File (.env)

Edit `.env` to change deployment URLs:

```env
GATEWAY_URL=http://localhost:8080
REPLICA_1_URL=http://localhost:5001
REPLICA_2_URL=http://localhost:5002
REPLICA_3_URL=http://localhost:5003
```

### Draw on the Canvas

1. Use the drawing tools (colors, brush size) to draw strokes
2. Strokes are **automatically streamed** to the RAFT leader
3. The leader **replicates** them to followers
4. When **majority commits**, strokes appear on all clients
5. Watch the **Commit Index** increase in real-time on Network tab

### Crash a Replica

Click the **🔴 Crash** button on any replica card to:
- Simulate a node crash (network partition)
- The replica stops responding to requests
- Watch the leader **detect the failure** and continue with N-1 replicas
- If the **leader crashes**, followers will trigger a **new election**

**What to observe:**
- Replica shows **"CRASHED"** status (red dot)
- Other replicas may become **candidate** (starting election)
- A new **leader is elected** from remaining replicas
- Strokes can still be committed with N-1 replicas

### Recover a Replica

Click the **🟢 Recover** button to bring a crashed replica back online:
- Replica **rejoins the cluster**
- It loads its **persistent MongoDB log**
- Updates its **term and voted_for state**
- Resumes normal operation as **follower**
- Catches up on **missed log entries** from leader

**What to observe:**
- Replica shows **Healthy** status (green dot)
- Its **role changes** to Follower
- **Commit index updates** as it catches up

---

## Key RAFT Concepts Visible in Playground

### 1. Leader Election (Term Advancement)

Trigger by crashing the leader:

```
Initial State:
  Replica 1: Leader, Term 5
  Replica 2: Follower, Term 5
  Replica 3: Follower, Term 5

After crashing Replica 1:
  Replica 2: Candidate, Term 6 (starts election)
  Replica 3: Follower, Term 6
  
After election:
  Replica 2: Candidate, Term 7 (new leader elected)
  Replica 1: Offline (crashed)
  Replica 3: Follower, Term 7
```

**Observed in UI:**
- Watch **Term increment** in real-time
- See replica role change **follower → candidate → leader**
- Network log shows **"Election: Replica X became leader"**

### 2. Log Replication

Draw while monitoring:

```
Canvas Stroke → Leader (Replica 1)
  ↓
AppendEntries RPC to Followers (Replica 2, 3)
  ↓
Followers replicate to their MongoDB
  ↓
Leader waits for majority ACK (2 out of 3)
  ↓
Commit when majority responds
  ↓
Commit Index increments
  ↓
All replicas apply stroke to their logs
```

**Observed in UI:**
- **Log Entries** count increases on leader first
- Followers catch up shortly after
- **Commit Index** advances when majority ACK
- Stroke appears on all connected clients

### 3. Majority Rule

Test the consensus:

1. **Crash 1 replica** → Cluster still works (2/3 replicas alive)
2. **Crash another replica** → Draw a stroke
   - Nothing happens (need majority of 2 alive replicas)
   - New strokes fail to commit
3. **Recover any crashed replica** → Strokes resume committing

**Observed in UI:**
- Strokes commit with **2 out of 3 replicas**
- Strokes fail with **only 1 out of 3 replicas**
- **Leader still processes** but can't commit without majority

### 4. Split Brain Prevention

RAFT prevents dangling **"leader of one"** scenario:

1. **Crash leader** → New election starts
2. **Immediately crash the new leader** → Another election
3. The **remaining single replica** becomes **candidate** but never **leader**
   - It needs **votes from at least 2 replicas** to become leader
   - With only itself, it can never reach majority

**Observed in UI:**
- Single replica stays in **CANDIDATE role**
- **Leader ID remains the old leader**
- No split-brain leadership

---

## API Endpoints Added

### Replica Endpoints

**GET `/health`** - Health status (used by frontend polling)
```json
{
  "replica_id": 1,
  "role": "leader",
  "status": "healthy",
  "term": 5,
  "leader_id": 1,
  "log_length": 42,
  "commit_index": 40
}
```

**POST `/crash`** - Simulate crash (playground feature)
```json
{
  "status": "crashed",
  "replica_id": 1,
  "message": "Replica 1 is now unresponsive (simulated crash)"
}
```

**POST `/recover`** - Recover from crash (playground feature)
```json
{
  "status": "recovered",
  "replica_id": 1,
  "role": "follower",
  "term": 5
}
```

### Gateway Endpoints

**GET `/config`** - Playground configuration (new)
```json
{
  "replica_public_urls": {
    "1": "http://localhost:5001",
    "2": "http://localhost:5002",
    "3": "http://localhost:5003"
  },
  "polling_interval_ms": 1000,
  "election_timeout_min_ms": 500,
  "election_timeout_max_ms": 800
}
```

---

## Monitoring Features

### Real-Time Polling

- **Polls every 1 second** (configurable in .env)
- Fetches `/health` from all 3 replicas
- Updates replica cards in-place
- Shows **live status changes**

### Election Detection

Automatically logs:
- When replica changes role (follower → candidate → leader)
- Timestamp of role change
- Up to 10 most recent election events per replica

### Live Commit Updates

- When you draw a stroke while monitoring
- **Commit index updates** in real-time
- Shows which replica committed the stroke
- Displays **last commit** value

---

## Testing Scenarios

### Scenario 1: Normal Operation + Election

1. Open Network tab
2. Draw 3 strokes (watch them commit)
3. Crash the **leader** (red replica)
4. Watch **new election** (term advances, new leader elected)
5. Draw another stroke (commits with new leader)

### Scenario 2: Majority Rule

1. Draw 2 strokes (all replicas commit)
2. Crash **Replica 2**
3. Draw 1 stroke (still commits on 2/3 replicas)
4. Crash **Replica 3**
5. Try to draw a stroke (stuck waiting for votes)
6. Recover **Replica 3** → Stroke commits

### Scenario 3: Catch-Up After Offline

1. Crash **Replica 2**
2. Draw 5 strokes while it's offline (committed on 2 replicas)
3. Recover **Replica 2**
4. Watch it catch up (log entries sent to it)
5. Verify it has same commit index as leader

### Scenario 4: Network Partition

1. Suppose **Replica 1** is leader
2. Crash **Replica 1**
3. As Replicas 2 & 3 elect new leader (say Replica 2)
4. Recover Replica 1 → It sees higher term, steps down to follower
5. Verify all 3 replicas agree on latest term and leader

---

## Environment Variables

Edit `.env` file:

```env
# Browser-accessible URLs (for frontend)
GATEWAY_URL=http://localhost:8080
REPLICA_1_URL=http://localhost:5001
REPLICA_2_URL=http://localhost:5002
REPLICA_3_URL=http://localhost:5003

# Docker-compose internal URLs (for server-to-server)
REPLICA_URLS=http://replica1:5000,http://replica2:5000,http://replica3:5000

# Timeout configuration (milliseconds)
POLLING_INTERVAL_MS=1000           # How often to poll replicas
ELECTION_TIMEOUT_MIN_MS=500        # Min time before follower starts election
ELECTION_TIMEOUT_MAX_MS=800        # Max time before follower starts election
HEARTBEAT_INTERVAL_MS=150          # How often leader sends heartbeats
```

### Deployment Changes

For production deployment:

1. Update `REPLICA_X_URL` to point to actual replica servers
2. Update `REPLICA_URLS` in `docker-compose.yml` environment
3. Keep local `.env` for reference

---

## Persistent Storage

All strokes are stored in **MongoDB**:
- **mongo1** (port 27017) → replica1_db
- **mongo2** (port 27018) → replica2_db
- **mongo3** (port 27019) → replica3_db

### Session Restore

Click **"Restore Session"** to:
1. Fetch all **committed strokes** from leader
2. Redraw them on canvas
3. Useful after browser refresh or cluster recovery

### Clear All

Click **"Clear Local"** to:
1. Clear **MongoDB on all 3 replicas**
2. Reset all strokes across cluster
3. Implemented as distributed transaction in RAFT

---

## Troubleshooting

### No replicas appear in Network tab
- Verify docker-compose is running: `docker-compose ps`
- Check gateway logs: `docker-compose logs gateway`
- Ensure `.env` URLs are correct

### Strokes not replicating
- Make sure **at least 2 replicas are healthy**
- Check if any replica is **crashed**
- Verify MongoDB services are running: `docker-compose ps`

### Election stuck
- You need **majority of replicas alive** for election
- 1 replica cannot become leader (needs 2 votes minimum)
- Recover a crashed replica to proceed

### URLs not updating after .env change
- Restart docker-compose: `docker-compose restart`
- Refresh browser after restart
- Clear browser cache if needed

---

## Architecture

```
Browser (Frontend)
    ↓
Gateway (http://localhost:8080)
    ├─→ Replica 1 (http://localhost:5001) + MongoDB 1
    ├─→ Replica 2 (http://localhost:5002) + MongoDB 2
    └─→ Replica 3 (http://localhost:5003) + MongoDB 3
    
User draws stroke:
1. Goes through WebSocket to Gateway
2. Gateway sends to Leader via POST /stroke
3. Leader replicates to Followers via AppendEntries RPC
4. Followers store in MongoDB
5. Commits when majority responds
6. Gateway broadcasts committed stroke to all clients
```

---

## What's New in This Release

✅ **Configuration File (.env)** - Easily change deployment URLs
✅ **Crash/Recover Endpoints** - Simulate node failures for testing
✅ **Real-Time Monitoring** - Watch elections and consensus in action
✅ **Network Playground** - Full RAFT dashboard with replica control
✅ **Config API** - Gateway serves configuration to frontend
✅ **Crash Detection** - Replicas show "CRASHED" status in UI
✅ **Live Election Logging** - Automatic election event tracking
✅ **Action Buttons** - Quick crash/recover controls per replica

---

## Next Steps

1. **Run the playground**: `docker-compose up --build`
2. **Open the dashboard**: Click "Network" button
3. **Draw some strokes**: Watch them replicate in real-time
4. **Crash the leader**: See election happen automatically
5. **Crash another node**: Verify majority rule works
6. **Recover nodes**: Watch catch-up and consensus

Enjoy exploring RAFT consensus!

🚀 **Happy RAFTing!**
