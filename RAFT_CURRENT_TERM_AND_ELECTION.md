# RAFT: current_term and Leader Election

## Part 1: Understanding `current_term`

### What is `current_term`?

`current_term` is a **monotonically increasing logical clock** that divides time into electoral epochs called "terms" or "periods". It's one of the most critical components of the RAFT consensus algorithm.

### Key Characteristics

1. **Persistent State** 
   - Survives server crashes (should be written to disk in production)
   - Not lost when a server restarts

2. **Monotonically Increasing**
   - Always increases, never decreases or resets
   - Starts at 0 when a replica initializes
   - Incremented only when transitioning to Candidate

3. **Election Heartbeat**
   - Each term represents one potential leader election period
   - Only one leader can exist per term
   - When a term ends (leader dies/network partition), a new election begins

4. **Log Entry Versioning**
   - Every log entry stores the term it was created in
   - Entries with the same term came from the same leader
   - Used for consistency verification

### RAFT State Initialization

```python
class RaftState:
    def __init__(self):
        # Persistent state
        self.current_term: int = 0              # Atomic clock, starts at 0
        self.voted_for: Optional[int] = None    # Which candidate we voted for this term

        # Volatile state
        self.role: Role = Role.FOLLOWER         # Initial role
        self.leader_id: Optional[int] = None    # Current leader's ID
        
        # Log state
        self.log: list[dict] = []               # Replicated log
        self.commit_index: int = -1             # Last committed entry index
```

### Three Core Uses of `current_term`

#### 1. **Leader Election**

When a Follower's election timer fires:
- Transitions to Candidate
- **Increments `current_term`** (e.g., 0 → 1)
- Votes for itself
- Sends `RequestVote` RPC with the new `current_term` to all peers
- Peers see the higher term and accept the request

```python
async def _start_election():
    state.role          = Role.CANDIDATE
    state.current_term += 1          # INCREMENT HERE
    state.voted_for     = REPLICA_ID  # Vote for self
    state.leader_id     = None
```

#### 2. **Stale Message Detection**

Every RPC includes the sender's `current_term`. When a replica receives an RPC:
- If `received_term < current_term` → the message is **stale**, reject it
- If `received_term > current_term` → the sender has newer information, **step down** and update term
- If `received_term == current_term` → accept (both in same epoch)

```python
@app.post("/request-vote", response_model=VoteResponse)
async def request_vote(req: VoteRequest):
    if req.term < state.current_term:
        # Reject: candidate's term is stale
        return VoteResponse(term=state.current_term, vote_granted=False)
    
    if req.term > state.current_term:
        # Higher term found: step down and update
        _step_down(req.term)
        # Then continue processing...
```

#### 3. **Log Entry Versioning**

Each log entry is tagged with the term it was created in:

```python
log_entry = {
    "index"  : new_index,
    "term"   : state.current_term,     # Term this entry was created in
    "stroke" : entry.stroke,
}
```

This allows RAFT to verify log consistency:
- If two logs have an entry at the same index and term, **all entries before that point are identical**
- This property is verified during `AppendEntries` RPC

### The `_step_down` Function

When a replica sees a higher term, it must step down:

```python
def _step_down(new_term: int):
    """Revert to follower state when we see a higher term."""
    log.info(f"Stepping down: term {state.current_term} → {new_term}")
    state.current_term = new_term    # Always update to higher term
    state.role         = Role.FOLLOWER
    state.voted_for    = None        # Reset vote (can vote in new term)
    state.leader_id    = None        # Forget old leader
    state.reset_election_timer()     # Reset timer for new term
```

### Example Timeline

```
┌─────────────────────────────────────────────────────────────────┐
│ REPLICA 1               │ REPLICA 2               │ REPLICA 3   │
├─────────────────────────┼─────────────────────────┼─────────────┤
│ Term 0                  │ Term 0                  │ Term 0      │
│ LEADER                  │ FOLLOWER                │ FOLLOWER    │
│ current_term=0          │ current_term=0          │ current_term=0
│ Sending heartbeats...   │ ← Heartbeat term=0      │ ← Heartbeat │
│                         │ Reset election timer    │   Reset timer
│                         │                         │              │
│ (Leader crashes)        │ (timeout: 500-800ms)    │ (timeout)   │
│ (no more heartbeats)    │                         │              │
│                         │ current_term: 0 → 1     │              │
│                         │ role: FOLLOWER → CANDI. │              │
│                         │ voted_for = 2           │              │
│                         │ Send RequestVote(t=1)   │              │
│                         │ ─────────────────────→  │              │
│                         │                         │ Receive req. │
│                         │                         │ (term 1 > 0)│
│                         │                         │ Step down:   │
│                         │                         │ t: 0 → 1    │
│                         │                         │ Grant vote  │
│                         │ ← Vote granted ✓        │              │
│                         │ Collect majority votes  │              │
│                         │ (2 votes: self + rep3)  │              │
│                         │ role: CANDIDATE → LEA.  │              │
│                         │ 🏆 BECOMES LEADER t=1   │              │
│                         │ Send heartbeat(t=1)     │              │
│                         │ ─────────────────────→  │              │
│                         │                         │ Reset timer  │
│                         │                         │ FOLLOWER t=1 │
└─────────────────────────┴─────────────────────────┴─────────────┘
```

---

## Part 2: How Election Works in RAFT Replicas

### Overview

RAFT leader election is a **distributed, fault-tolerant voting mechanism** that ensures:
- Only **one leader per term** exists
- A **majority** of servers must agree on the leader
- **Automatic failover** when the leader dies or becomes unreachable

### The Three Roles

#### 1. **Follower** (Initial State)
- Listens for heartbeats from the leader
- Resets election timer on every heartbeat
- If timer fires (no heartbeat for 500–800ms), becomes Candidate

#### 2. **Candidate** (During Election)
- Increments `current_term`
- Votes for itself
- Sends `RequestVote` RPC to all peers
- Waits for majority votes
- If wins → becomes Leader
- If loses or higher term seen → steps down to Follower

#### 3. **Leader** (Elected)
- Sends periodic heartbeats (empty `AppendEntries`) every 150ms
- Accepts new entries from the gateway
- Replicates entries to followers
- Commits entries when majority acknowledges

### Election Phase 1: Timeout Detection

Each replica maintains an **election timer**:

```python
ELECTION_TIMEOUT_MIN = 0.5           # 500 ms
ELECTION_TIMEOUT_MAX = 0.8           # 800 ms

def reset_election_timer(self):
    self.last_heartbeat = time.time()
    self.election_timeout = self._new_timeout()  # Random 500-800ms

def _new_timeout(self) -> float:
    return random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
```

**Why randomized?**
- Prevents simultaneous elections (only one candidate wins each term)
- With fixed timeouts, all followers would become candidates at once
- With randomization, one candidate gets to vote first

**Main RAFT Loop:**

```python
async def _raft_loop():
    """
    Main RAFT background loop every 50 ms tick.
    """
    while True:
        await asyncio.sleep(0.05)   # Tick every 50 ms

        if state.role == Role.LEADER:
            # Leader: send heartbeats every 150 ms
            await _send_heartbeats()
            await asyncio.sleep(HEARTBEAT_INTERVAL - 0.05)

        elif state.role == Role.FOLLOWER:
            # Follower: check if election timeout has elapsed
            elapsed = time.time() - state.last_heartbeat
            if elapsed >= state.election_timeout:
                log.info(f"Election timeout after {elapsed:.2f}s → start election")
                await _start_election()
```

### Election Phase 2: Candidate Initialization

When a Follower times out:

```python
async def _start_election():
    """Transition to candidate and request votes from all peers."""
    
    # Transition to candidate
    state.role          = Role.CANDIDATE
    state.current_term += 1              # Increment term
    state.voted_for     = REPLICA_ID     # Vote for ourselves
    state.leader_id     = None           # Clear old leader
    state.reset_election_timer()         # Reset timer (for new term)

    log.info(f"Starting election for term {state.current_term}")
```

**State Changes:**
```
Before:  role=FOLLOWER, term=0, leader_id=1
After:   role=CANDIDATE, term=1, leader_id=None, voted_for=SELF
```

### Election Phase 3: Request Votes

The candidate sends `RequestVote` RPC to all peers with:

```python
vote_request = {
    "term"           : state.current_term,      # Our new term
    "candidate_id"   : REPLICA_ID,              # Who's asking
    "last_log_index" : state.last_log_index(),  # Our log length
    "last_log_term"  : state.last_log_term(),   # Term of our last entry
}
```

**Why send log info?**
- Voters want to ensure the candidate has an **up-to-date log**
- A candidate with stale or missing entries shouldn't become leader
- This prevents losing committed data

**Voter's Decision Logic:**

```python
@app.post("/request-vote", response_model=VoteResponse)
async def request_vote(req: VoteRequest):
    """
    Rules for granting a vote:
      1. Reject if candidate's term < our term (stale candidate)
      2. If candidate's term > our term → step down and update
      3. Grant vote if: haven't voted yet AND candidate's log is up-to-date
    """
    
    # Rule 1: Reject stale terms
    if req.term < state.current_term:
        return VoteResponse(term=state.current_term, vote_granted=False)

    # Rule 2: Step down if we see a higher term
    if req.term > state.current_term:
        _step_down(req.term)

    # Rule 3: Check if we can grant vote
    already_voted = (state.voted_for is not None and state.voted_for != req.candidate_id)
    
    # Is candidate's log at least as up-to-date as ours?
    log_ok = (
        req.last_log_term > state.last_log_term()
        or (req.last_log_term == state.last_log_term() 
            and req.last_log_index >= state.last_log_index())
    )

    if already_voted or not log_ok:
        return VoteResponse(term=state.current_term, vote_granted=False)

    # Grant the vote
    state.voted_for = req.candidate_id
    state.reset_election_timer()
    return VoteResponse(term=state.current_term, vote_granted=True)
```

**Vote Granting Rules:**
```
Grant vote if:
  • candidate_term >= current_term  (not stale)
  • haven't voted yet in this term   (one vote per follower, per term)
  • candidate's log is at least      (prevent log loss)
    as up-to-date as ours
```

### Election Phase 4: Tally Votes

After sending `RequestVote` to all peers, the candidate collects responses:

```python
votes = 1   # Start with our own vote
async with httpx.AsyncClient(timeout=0.5) as client:
    tasks = [client.post(f"{peer}/request-vote", json=vote_request) 
             for peer in PEER_URLS]
    results = await asyncio.gather(*tasks, return_exceptions=True)

for peer, result in zip(PEER_URLS, results):
    if isinstance(result, Exception):
        log.warning(f"Vote request to {peer} failed: {result}")
        continue
    
    if result.status_code == 200:
        data = result.json()
        
        # If voter saw a higher term, step down
        if data.get("term", 0) > state.current_term:
            _step_down(data["term"])
            return  # Election failed
        
        # Count the vote
        if data.get("vote_granted"):
            votes += 1
            log.info(f"Got vote from {peer} — total {votes}")
```

### Election Phase 5: Majority Check

A candidate needs **majority votes** to become leader:

```python
majority = (len(PEER_URLS) + 1) // 2 + 1
# For 3 replicas: majority = (3 + 1) // 2 + 1 = 3
# For 5 replicas: majority = (5 + 1) // 2 + 1 = 4
```

**Majority Examples:**
```
1 replica:  1 vote needed (just itself)
3 replicas: 2 votes needed (itself + 1 other)
5 replicas: 3 votes needed (itself + 2 others)
```

### Election Phase 6: Outcome

#### ✅ Won Election (Got Majority)

```python
if state.role == Role.CANDIDATE and votes >= majority:
    state.role      = Role.LEADER
    state.leader_id = REPLICA_ID
    log.info(f"🏆 Became LEADER for term {state.current_term} with {votes} votes")
    # Continue in _raft_loop as LEADER
```

Immediately after becoming leader:
- **Sends initial heartbeat** to establish authority
- Resets each follower's election timer
- Can now accept new entries from the gateway

#### ❌ Lost Election

```python
else:
    log.info(f"Election failed ({votes} votes) — reverting to follower")
    _step_down(state.current_term)
```

Reasons for losing:
- Didn't get majority votes within the RPC timeout
- Received higher-term `RequestVote` from another candidate
- Received valid `AppendEntries` from a higher-term leader

### Leader's Heartbeat Loop

Once a replica becomes leader, it enters heartbeat mode:

```python
async def _send_heartbeats():
    """Leader sends empty AppendEntries to all peers every HEARTBEAT_INTERVAL."""
    payload = {
        "term"           : state.current_term,      # Current term
        "leader_id"      : REPLICA_ID,              # Who's leading
        "prev_log_index" : state.last_log_index(),
        "prev_log_term"  : state.last_log_term(),
        "entries"        : [],                      # Empty = heartbeat
        "leader_commit"  : state.commit_index,
    }
    
    async with httpx.AsyncClient(timeout=0.5) as client:
        tasks = [client.post(f"{peer}/append-entries", json=payload) 
                 for peer in PEER_URLS]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    for peer, result in zip(PEER_URLS, results):
        if result.status_code == 200:
            data = result.json()
            if data.get("term", 0) > state.current_term:
                _step_down(data["term"])  # Higher term elsewhere, step down
```

**Heartbeat Benefits:**
- Prevents follower election timeouts (resets their timer)
- Allows leader to detect if it's been partitioned
- Carries `leader_commit` to advance follower commit indices

---

## Complete Election Scenario

### Scenario: Three-Replica Cluster, Leader Crashes

```
INITIAL STATE:
┌──────────────┬──────────────┬──────────────┐
│ Replica 1    │ Replica 2    │ Replica 3    │
├──────────────┼──────────────┼──────────────┤
│ LEADER       │ FOLLOWER     │ FOLLOWER     │
│ term=1       │ term=1       │ term=1       │
│ Heartbeat... │ ← HB (t=1)   │ ← HB (t=1)   │
│              │ Timer reset  │ Timer reset  │
└──────────────┴──────────────┴──────────────┘

TIME PASSES: 200ms (no heartbeat received)
┌──────────────┬──────────────┬──────────────┐
│ Replica 1    │ Replica 2    │ Replica 3    │
├──────────────┼──────────────┼──────────────┤
│ LEADER       │ FOLLOWER     │ FOLLOWER     │
│ 🔴 CRASHED   │ elapsed=200ms│ elapsed=200ms│
│              │ timeout=750ms│ timeout=680ms│
│              │ Still waiting│ Still waiting│
└──────────────┴──────────────┴──────────────┘

TIME PASSES: 500ms (Replica 3 times out first due to randomization)
┌──────────────┬──────────────┬──────────────┐
│ Replica 1    │ Replica 2    │ Replica 3    │
├──────────────┼──────────────┼──────────────┤
│ CRASHED      │ FOLLOWER     │ CANDIDATE    │
│ ✗            │ elapsed=500ms│ elapsed=500ms│
│              │ timeout=750ms│ 🗳️ START ELECTION
│              │              │ term: 1 → 2  │
│              │              │ voted_for=3  │
│              │              │ Send RequestVote(t=2)
│              │              │    ─────→ to Rep 1 & 2
│              │ ← Vote Req   │              │
│              │ (term 2 > 1) │              │
│              │ step_down:   │              │
│              │ t: 1 → 2     │              │
│              │ Grant vote! ✓│              │
│              │ Set voted_for=3
│              │ 👍 vote back to Replica 3
└──────────────┴──────────────┴──────────────┘

REPLICA 3 TALLIES VOTES:
  Round responses:
  - Itself:        1 vote ✓
  - Replica 2:     1 vote ✓ (total = 2)
  - Replica 1:     timeout (no response, crashed)
  
  Majority = 2 of 3 = 2 votes
  Status: 2 >= 2 → MAJORITY REACHED!

┌──────────────┬──────────────┬──────────────┐
│ Replica 1    │ Replica 2    │ Replica 3    │
├──────────────┼──────────────┼──────────────┤
│ CRASHED      │ FOLLOWER     │ LEADER       │
│ ✗            │ term=2       │ 🏆 NEW LEADER
│              │ voted_for=3  │ term=2       │
│              │              │ leader_id=3  │
│              │ ← Heartbeat  │ Send HB(t=2) │
│              │ (term 2)     │ ──────────→  │
│              │ Reset timer  │              │
│              │ leader_id=3  │              │
└──────────────┴──────────────┴──────────────┘

✅ ELECTION COMPLETE
   Replica 3 is the new leader
   Cluster accepts new strokes from gateway
   Replica 1 stays crashed until restarted
```

---

## Safety Properties Maintained by Elections

### 1. **Election Safety**
- At most **one leader** per term can exist
- Once a server votes for a candidate, it can't vote for another in that term

### 2. **Leader Completeness**
- New leader has **all previously committed entries**
- Required because voters only vote for candidates with up-to-date logs

### 3. **Fast Failover**
- When leader crashes, **new election in 500–800ms**
- Randomized timeouts prevent "thundering herd" of simultaneous candidates
- Only one candidate wins per term

### 4. **No Split Brain**
- `current_term` prevents servers from recognizing two leaders in the same term
- Higher term always wins (forces higher-term leader to be recognized)

---

## Key Takeaways

| Concept | Purpose | Example |
|---------|---------|---------|
| **current_term** | Logical clock, electoral epoch | Term 0 → 1 → 2... |
| **Randomized timeout** | Prevent simultaneous elections | 500–800ms randomized |
| **Majority vote** | Ensure single leader | 2 of 3 replicas vote "yes" |
| **Log checks** | Prevent data loss | Voter: "Is your log up-to-date?" |
| **Step down** | Recover from split brain | See higher term → become follower |
| **Heartbeat** | Suppress elections | Leader sends every 150ms |

---

## References

- RAFT Paper: https://raft.io/raft.pdf
- Visualization: https://raft.github.io/raftscope/index.html
- Jackfruit Implementation: `replica/main.py` in this project
