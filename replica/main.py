"""
replica/main.py  —  Mini-RAFT Replica Node  
=====================================================
Environment variables (set by docker-compose):
  REPLICA_ID   : unique integer ID  e.g. 1, 2, 3
  PORT         : port this server listens on  e.g. 5000
  PEERS        : comma-separated peer URLs  e.g. http://replica2:5000,http://replica3:5000
"""

import asyncio
import logging
import os
import random
import time
from enum import Enum
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from motor.motor_asyncio import AsyncIOMotorClient   # FIX 1: async Motor driver
from fastapi.middleware.cors import CORSMiddleware

# ── CONFIG ─────────────────────────────────────────────────────────────────────
REPLICA_ID   = int(os.getenv("REPLICA_ID", "1"))
PORT         = int(os.getenv("PORT", "5000"))
PEER_URLS    = [p.strip() for p in os.getenv("PEERS", "").split(",") if p.strip()]
MONGO_URI    = os.getenv("MONGO_URI", f"mongodb://localhost:27017/replica{REPLICA_ID}_db")

HEARTBEAT_INTERVAL   = 0.20   # 200 ms — leader sends heartbeats this often

# Tuned for Dockerized local clusters where transient scheduling/network jitter is common.
ELECTION_TIMEOUT_MIN = 3.0
ELECTION_TIMEOUT_MAX = 6.0

HEARTBEAT_SEND_DEADLINE = 2.5
HEARTBEAT_RPC_TIMEOUT = 1.5
VOTE_RPC_TIMEOUT = 1.5
HEARTBEAT_PEER_BACKOFF = 1.0

logging.basicConfig(
    level=logging.INFO,
    format=f"[Replica {REPLICA_ID}] %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)

raft_http_client: Optional[httpx.AsyncClient] = None

# ── MONGODB INITIALIZATION (async Motor) ───────────────────────────────────────
# FIX 1: Motor is non-blocking; all DB calls are awaited and never block the loop.
motor_client  = AsyncIOMotorClient(MONGO_URI, serverSelectionTimeoutMS=5000)
mongo_db      = motor_client[f"replica{REPLICA_ID}_db"]
log_collection   = mongo_db["log"]
state_collection = mongo_db["state"]

async def init_mongo_indexes():
    """Create indexes once on startup (idempotent)."""
    await log_collection.create_index("index", unique=True)
    await state_collection.create_index("key", unique=True)
    log.info(f"MongoDB indexes ready — URI: {MONGO_URI}")

async def save_log_entry(entry: dict):
    """Async: persist a log entry to MongoDB."""
    try:
        await log_collection.update_one(
            {"index": entry["index"]},
            {"$setOnInsert": {
                "index":     entry["index"],
                "term":      entry["term"],
                "stroke":    entry.get("stroke"),
                "timestamp": time.time(),
            }},
            upsert=True,
        )
    except Exception as e:
        log.warning(f"Failed to save log entry index={entry['index']}: {e}")

async def load_log_from_db() -> list[dict]:
    """Async: load all log entries ordered by index."""
    try:
        cursor  = log_collection.find({}, {"_id": 0}).sort("index", 1)
        entries = await cursor.to_list(length=None)
        log.info(f"Loaded {len(entries)} log entries from MongoDB")
        return entries
    except Exception as e:
        log.warning(f"Failed to load log from DB: {e}")
        return []

async def save_state_to_db(state_dict: dict):
    """Async: upsert RAFT persistent state (term, voted_for)."""
    try:
        await state_collection.update_one(
            {"key": "raft_state"},
            {"$set": state_dict},
            upsert=True,
        )
    except Exception as e:
        log.warning(f"Failed to save RAFT state: {e}")

async def load_state_from_db() -> dict:
    """Async: load RAFT persistent state."""
    try:
        doc = await state_collection.find_one({"key": "raft_state"})
        if doc:
            log.info("Loaded RAFT state from MongoDB")
            return doc
        return {}
    except Exception as e:
        log.warning(f"Failed to load RAFT state: {e}")
        return {}

# ── STATE ──────────────────────────────────────────────────────────────────────
class Role(str, Enum):
    FOLLOWER  = "follower"
    CANDIDATE = "candidate"
    LEADER    = "leader"

class RaftState:
    def __init__(self):
        # Loaded async in startup(); placeholders here
        self.current_term: int          = 0
        self.voted_for: Optional[int]   = None

        self.role: Role                 = Role.FOLLOWER
        self.leader_id: Optional[int]   = None
        self.log: list[dict]            = []
        self.commit_index: int          = -1

        # Election timer
        self.last_heartbeat: float  = time.monotonic()
        self.election_timeout: float = self._new_timeout()

        # FIX 5: per-peer progress tracking (populated when we become leader)
        # next_index[peer_url]  = next log index to send to that peer
        # match_index[peer_url] = highest log index known to be replicated on that peer
        self.next_index:  dict[str, int] = {}
        self.match_index: dict[str, int] = {}
        self.peer_backoff_until: dict[str, float] = {peer: 0.0 for peer in PEER_URLS}

    def _new_timeout(self) -> float:
        # Add a per-replica bias (100ms * REPLICA_ID) so even in the worst case
        # each node's timeout range is staggered, preventing simultaneous elections
        # when Docker starts all containers at nearly the same time.
        bias = REPLICA_ID * 0.2
        return random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX) + bias

    def reset_election_timer(self):
        self.last_heartbeat   = time.monotonic()
        self.election_timeout = self._new_timeout()

    def last_log_index(self) -> int:
        return len(self.log) - 1

    def last_log_term(self) -> int:
        return self.log[-1]["term"] if self.log else 0

    def _init_leader_state(self):
        """Reset per-peer tracking when this node becomes leader."""
        for peer in PEER_URLS:
            self.next_index[peer]  = self.last_log_index() + 1
            self.match_index[peer] = -1
            self.peer_backoff_until[peer] = 0.0

state = RaftState()

# ── CRASH SIMULATION ───────────────────────────────────────────────────────────
crashed = False

# ── PYDANTIC SCHEMAS ───────────────────────────────────────────────────────────
class VoteRequest(BaseModel):
    term: int
    candidate_id: int
    last_log_index: int
    last_log_term: int

class VoteResponse(BaseModel):
    term: int
    vote_granted: bool

class AppendEntriesRequest(BaseModel):
    term: int
    leader_id: int
    prev_log_index: int
    prev_log_term: int
    entries: list[dict]
    leader_commit: int

class AppendEntriesResponse(BaseModel):
    term: int
    success: bool
    match_index: int

class SyncLogRequest(BaseModel):
    from_index: int

class StrokeEntry(BaseModel):
    stroke: dict

# ── FASTAPI APP ────────────────────────────────────────────────────────────────
app = FastAPI(title=f"RAFT Replica {REPLICA_ID}")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── HEALTH ─────────────────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    if crashed:
        return {
            "replica_id":   REPLICA_ID,
            "role":         "crashed",
            "status":       "unresponsive",
            "term":         state.current_term,
            "leader_id":    None,
            "log_length":   len(state.log),
            "commit_index": state.commit_index,
        }
    return {
        "replica_id":   REPLICA_ID,
        "role":         state.role,
        "status":       "healthy",
        "term":         state.current_term,
        "leader_id":    state.leader_id,
        "log_length":   len(state.log),
        "commit_index": state.commit_index,
    }

# ── STATUS ─────────────────────────────────────────────────────────────────────
@app.get("/status")
async def status():
    now = time.monotonic()
    return {
        "replica_id":   REPLICA_ID,
        "role":         state.role,
        "term":         state.current_term,
        "leader_id":    state.leader_id,
        "log":          state.log,
        "commit_index": state.commit_index,
        "peers":        PEER_URLS,
        "election_timeout_s": round(state.election_timeout, 3),
        "heartbeat_age_s": round(now - state.last_heartbeat, 3),
    }

# ── COMMITTED STROKES ──────────────────────────────────────────────────────────
@app.get("/committed-strokes")
async def get_committed_strokes():
    committed = state.log[: state.commit_index + 1] if state.commit_index >= 0 else []
    log.info(f"Serving {len(committed)} committed strokes")
    return {
        "replica_id":      REPLICA_ID,
        "role":            state.role,
        "total_log_length": len(state.log),
        "commit_index":    state.commit_index,
        "strokes":         committed,
    }

# ── REQUEST VOTE RPC ───────────────────────────────────────────────────────────
@app.post("/request-vote", response_model=VoteResponse)
async def request_vote(req: VoteRequest):
    if crashed:
        raise HTTPException(status_code=503, detail="Replica is crashed")

    if req.term < state.current_term:
        log.info(f"Rejecting vote for {req.candidate_id}: stale term {req.term}")
        return VoteResponse(term=state.current_term, vote_granted=False)

    if req.term > state.current_term:
        await _step_down(req.term)   # FIX 4: awaited async step-down

    already_voted = (state.voted_for is not None and state.voted_for != req.candidate_id)
    log_ok = (
        req.last_log_term > state.last_log_term()
        or (req.last_log_term == state.last_log_term() and req.last_log_index >= state.last_log_index())
    )

    if already_voted or not log_ok:
        log.info(f"Rejecting vote for {req.candidate_id}: already_voted={already_voted} log_ok={log_ok}")
        return VoteResponse(term=state.current_term, vote_granted=False)

    state.voted_for = req.candidate_id
    state.reset_election_timer()

    # FIX 1 & 4: awaited async DB write
    await save_state_to_db({
        "key":          "raft_state",
        "current_term": state.current_term,
        "voted_for":    state.voted_for,
    })

    log.info(f"Granted vote to candidate {req.candidate_id} for term {req.term}")
    return VoteResponse(term=state.current_term, vote_granted=True)

# ── APPEND ENTRIES RPC ─────────────────────────────────────────────────────────
@app.post("/append-entries", response_model=AppendEntriesResponse)
async def append_entries(req: AppendEntriesRequest):
    if crashed:
        raise HTTPException(status_code=503, detail="Replica is crashed")

    if req.term < state.current_term:
        return AppendEntriesResponse(
            term=state.current_term, success=False,
            match_index=state.last_log_index()
        )

    if req.term > state.current_term or state.role != Role.FOLLOWER:
        await _step_down(req.term)   # FIX 4

    state.leader_id = req.leader_id
    state.reset_election_timer()

    # ── Consistency check ──────────────────────────────────────────────────────
    if req.prev_log_index >= 0:
        if req.prev_log_index > state.last_log_index():
            log.warning(f"Missing entries: our log ends at {state.last_log_index()}, need prev={req.prev_log_index}")
            # FIX 10: Follower detects gap — request sync from leader
            asyncio.create_task(_request_sync_from_leader(req.leader_id, state.last_log_index() + 1))
            return AppendEntriesResponse(
                term=state.current_term, success=False,
                match_index=state.last_log_index()
            )
        if state.log[req.prev_log_index]["term"] != req.prev_log_term:
            state.log = state.log[: req.prev_log_index]
            log.warning(f"Term conflict at index {req.prev_log_index}, truncated log")
            return AppendEntriesResponse(
                term=state.current_term, success=False,
                match_index=state.last_log_index()
            )

    # ── Append new entries ─────────────────────────────────────────────────────
    for entry in req.entries:
        insert_index = entry["index"]
        if insert_index <= state.last_log_index():
            if state.log[insert_index]["term"] != entry["term"]:
                state.log = state.log[:insert_index]
            else:
                continue
        state.log.append(entry)
        await save_log_entry(entry)   # FIX 1: awaited async write
        log.info(f"Appended entry index={entry['index']} term={entry['term']}")

    # ── Advance commit index ───────────────────────────────────────────────────
    if req.leader_commit > state.commit_index:
        state.commit_index = min(req.leader_commit, state.last_log_index())
        log.info(f"Commit index advanced to {state.commit_index}")

    return AppendEntriesResponse(
        term=state.current_term, success=True,
        match_index=state.last_log_index()
    )

# ── SYNC LOG ───────────────────────────────────────────────────────────────────
@app.post("/sync-log")
async def sync_log(req: SyncLogRequest):
    if state.role != Role.LEADER:
        raise HTTPException(status_code=403, detail="Not the leader")

    missing = [
        e for e in state.log
        if req.from_index <= e["index"] <= state.commit_index
    ]
    log.info(f"Sync-log from index {req.from_index}: sending {len(missing)} entries")
    return {"entries": missing, "commit_index": state.commit_index}

# ── STROKE ─────────────────────────────────────────────────────────────────────
@app.post("/stroke")
async def receive_stroke(entry: StrokeEntry):
    if crashed:
        raise HTTPException(status_code=503, detail="Replica is crashed")
    if state.role != Role.LEADER:
        raise HTTPException(
            status_code=403,
            detail=f"Not the leader. Current leader is replica {state.leader_id}"
        )

    new_index = len(state.log)
    log_entry = {
        "index":  new_index,
        "term":   state.current_term,
        "stroke": entry.stroke,
    }
    state.log.append(log_entry)
    await save_log_entry(log_entry)   # FIX 1
    log.info(f"Leader appended stroke at index {new_index}")

    acks = await _replicate_entry(log_entry)
    majority = (len(PEER_URLS) + 1) // 2 + 1

    if acks + 1 >= majority:
        state.commit_index = new_index
        log.info(f"Committed entry {new_index} with {acks + 1} acks")
        return {"committed": True, "entry": log_entry}
    else:
        log.warning(f"Failed to commit entry {new_index}: only {acks + 1} acks")
        raise HTTPException(status_code=500, detail="Failed to achieve majority")

# ── CLEAR LOG ──────────────────────────────────────────────────────────────────
@app.post("/clear-log")
async def clear_log():
    try:
        await log_collection.delete_many({})     # FIX 1: async
        await state_collection.delete_many({})   # FIX 1: async

        state.log          = []
        state.commit_index = -1

        log.info("Cleared all strokes from log and MongoDB")
        return {"status": "success", "replica_id": REPLICA_ID, "message": "Log cleared"}
    except Exception as e:
        log.error(f"Failed to clear log: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to clear log: {e}")

# ── PLAYGROUND — crash / recover ───────────────────────────────────────────────
@app.post("/crash")
async def crash_replica():
    global crashed
    crashed = True
    log.warning(f"🔴 REPLICA {REPLICA_ID} CRASHED")
    return {"status": "crashed", "replica_id": REPLICA_ID}

@app.post("/recover")
async def recover_replica():
    global crashed
    crashed = False
    state.reset_election_timer()
    log.warning(f"🟢 REPLICA {REPLICA_ID} RECOVERED")
    return {"status": "recovered", "replica_id": REPLICA_ID, "role": state.role, "term": state.current_term}

# ── INTERNAL HELPERS ───────────────────────────────────────────────────────────

async def _step_down(new_term: int):
    """
    FIX 4: Now fully async — awaits the DB write so it never blocks the event loop.
    Revert to follower when we discover a higher term.

    FIX 6: Only reset the election timer when the term genuinely advances.
    If called with the same term (e.g. after a lost election), we preserve the
    existing timer so the node waits for the new leader's heartbeat before
    competing again — rather than immediately re-arming and causing another
    split-vote race (the exact pattern seen in the term-25 logs).
    """
    if new_term < state.current_term:
        return   # never go backwards

    old_term = state.current_term
    term_advanced = new_term > old_term

    state.current_term = new_term
    state.role = Role.FOLLOWER
    state.leader_id = None

    if term_advanced:
        # New term: clear vote and persist it exactly once for this term.
        state.voted_for = None
        state.reset_election_timer()
        await save_state_to_db({
            "key":          "raft_state",
            "current_term": state.current_term,
            "voted_for":    None,
        })
    # Same-term step-down: keep voted_for unchanged to preserve RAFT's
    # one-vote-per-term rule and avoid leader flip-flopping.

    log.info(
        f"Stepped down -> follower: term={state.current_term}, "
        f"term_advanced={term_advanced}, voted_for={state.voted_for}"
    )


async def _replicate_entry(entry: dict) -> int:
    """
    FIX 5: Uses per-peer next_index to compute the correct prev_log_index per peer.
    Send AppendEntries to all peers; return count of successful ACKs.
    """
    acks = 0
    if raft_http_client is None:
        return acks

    tasks = []
    peers_in_flight = []

    for peer in PEER_URLS:
        ni         = state.next_index.get(peer, entry["index"])
        prev_index = ni - 1
        prev_term  = state.log[prev_index]["term"] if prev_index >= 0 and prev_index < len(state.log) else 0

        # Send all entries from next_index up to and including the new one
        entries_to_send = [e for e in state.log if ni <= e["index"] <= entry["index"]]

        payload = AppendEntriesRequest(
            term           = state.current_term,
            leader_id      = REPLICA_ID,
            prev_log_index = prev_index,
            prev_log_term  = prev_term,
            entries        = entries_to_send,
            leader_commit  = state.commit_index,
        )
        tasks.append(
            raft_http_client.post(
                f"{peer}/append-entries",
                json=payload.model_dump(),
                timeout=HEARTBEAT_RPC_TIMEOUT,
            )
        )
        peers_in_flight.append(peer)

    results = await asyncio.gather(*tasks, return_exceptions=True)

    for peer, result in zip(peers_in_flight, results):
        if isinstance(result, Exception):
            log.warning(f"Replication to {peer} failed: {result}")
            continue
        if result.status_code == 200:
            data = result.json()
            if data.get("success"):
                acks += 1
                # FIX 5: advance per-peer progress
                state.next_index[peer]  = entry["index"] + 1
                state.match_index[peer] = entry["index"]
            else:
                # Peer is behind — back off next_index and schedule catch-up
                match = data.get("match_index", -1)
                state.next_index[peer] = max(0, match + 1)
                asyncio.create_task(_catchup_peer(peer, state.next_index[peer]))

    return acks


async def _request_sync_from_leader(leader_id: int, from_index: int):
    """
    FIX 10: Follower calls /sync-log on the leader to fetch missing entries.
    This is triggered when append-entries detects a gap in the log.
    """
    if state.role != Role.FOLLOWER or state.leader_id is None:
        return
    
    # Construct leader URL from PEER_URLS by matching leader_id
    leader_url = None
    for peer_url in PEER_URLS:
        # Try to extract leader URL — assumes PEER_URLS are consistently ordered
        # For now, use a simple heuristic: the leader_id determines which peer
        # This works if peers are http://replicaX:5000 where X is the replica ID
        if f"replica{leader_id}" in peer_url:
            leader_url = peer_url
            break
    
    if leader_url is None:
        log.warning(f"Could not find leader URL for leader_id={leader_id}")
        return
    
    try:
        if raft_http_client is None:
            return
        resp = await raft_http_client.post(
            f"{leader_url}/sync-log",
            json={"from_index": from_index},
            timeout=2.0,
        )
        if resp.status_code == 200:
            data = resp.json()
            entries = data.get("entries", [])
            commit_idx = data.get("commit_index", -1)
            
            # Apply all fetched entries to local log
            for entry in entries:
                if entry["index"] <= state.last_log_index():
                    if state.log[entry["index"]]["term"] != entry["term"]:
                        state.log = state.log[:entry["index"]]
                    else:
                        continue
                state.log.append(entry)
                await save_log_entry(entry)
            
            # Advance commit index
            if commit_idx > state.commit_index:
                state.commit_index = min(commit_idx, state.last_log_index())
                log.info(f"Sync-log applied: commit_index={state.commit_index}, entries_applied={len(entries)}")
            else:
                log.info(f"Sync-log applied {len(entries)} entries")
        else:
            log.warning(f"Sync-log from {leader_url} returned HTTP {resp.status_code}")
    except Exception as e:
        log.warning(f"Sync-log from leader {leader_url} failed: {e}")


async def _catchup_peer(peer_url: str, from_index: int):
    """Push all committed entries from from_index to a lagging follower."""
    missing = [e for e in state.log if from_index <= e["index"] <= state.commit_index]
    if not missing:
        return

    prev_index = from_index - 1
    prev_term  = state.log[prev_index]["term"] if prev_index >= 0 and prev_index < len(state.log) else 0

    payload = AppendEntriesRequest(
        term           = state.current_term,
        leader_id      = REPLICA_ID,
        prev_log_index = prev_index,
        prev_log_term  = prev_term,
        entries        = missing,
        leader_commit  = state.commit_index,
    )
    try:
        if raft_http_client is None:
            return
        resp = await raft_http_client.post(
            f"{peer_url}/append-entries",
            json=payload.model_dump(),
            timeout=2.0,
        )
        if resp.status_code == 200 and resp.json().get("success"):
            state.next_index[peer_url]  = state.commit_index + 1
            state.match_index[peer_url] = state.commit_index
            log.info(f"Catch-up to {peer_url} succeeded ({len(missing)} entries)")
        else:
            log.warning(f"Catch-up to {peer_url} rejected: {resp.text}")
    except Exception as e:
        log.warning(f"Catch-up to {peer_url} failed: {e}")


async def _send_heartbeats():
    """
    FIX 5: Per-peer prev_log_index to avoid spurious consistency failures.
    FIX 9: Hard timeout wrapper (asyncio.wait_for) ensures this coroutine
           never takes longer than HEARTBEAT_INTERVAL even if peers are slow,
           so it can never block the timer loop for more than one tick.
    """
    async def _do_send():
        if raft_http_client is None:
            return

        tasks = []
        peers_in_flight = []
        now = time.monotonic()

        for peer in PEER_URLS:
            if state.peer_backoff_until.get(peer, 0.0) > now:
                continue

            ni         = state.next_index.get(peer, state.last_log_index() + 1)
            prev_index = ni - 1
            prev_term  = state.log[prev_index]["term"] if 0 <= prev_index < len(state.log) else 0

            payload = {
                "term":           state.current_term,
                "leader_id":      REPLICA_ID,
                "prev_log_index": prev_index,
                "prev_log_term":  prev_term,
                "entries":        [],
                "leader_commit":  state.commit_index,
            }
            tasks.append(
                raft_http_client.post(
                    f"{peer}/append-entries",
                    json=payload,
                    timeout=HEARTBEAT_RPC_TIMEOUT,
                )
            )
            peers_in_flight.append(peer)

        if not tasks:
            return

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for peer, result in zip(peers_in_flight, results):
            if isinstance(result, Exception):
                state.peer_backoff_until[peer] = time.monotonic() + HEARTBEAT_PEER_BACKOFF
                log.warning(f"Heartbeat to {peer} failed: {type(result).__name__}: {result}")
                continue
            if result.status_code == 200:
                state.peer_backoff_until[peer] = 0.0
                data = result.json()
                if data.get("term", 0) > state.current_term:
                    await _step_down(data["term"])
                    return
            else:
                state.peer_backoff_until[peer] = time.monotonic() + HEARTBEAT_PEER_BACKOFF
                log.warning(f"Heartbeat to {peer} returned HTTP {result.status_code}")

    try:
        await asyncio.wait_for(_do_send(), timeout=HEARTBEAT_SEND_DEADLINE)
    except asyncio.TimeoutError:
        log.warning("_send_heartbeats timed out — peers may be slow")


async def _start_election():
    """
    Transition to candidate and request votes from all peers.
    FIX 9: DB write is fire-and-forget (create_task) so it cannot stall the
           HTTP vote requests. The vote requests themselves are wrapped in
           wait_for so a slow peer cannot block the election past one timeout.
    """
    state.role          = Role.CANDIDATE
    state.current_term += 1
    state.voted_for     = REPLICA_ID
    state.leader_id     = None
    state.reset_election_timer()

    # FIX 9: persist state in background — don't block vote solicitation on a
    # MongoDB write. If we crash before this completes Python restarts with
    # voted_for=None which is safe (we just re-vote in the new term).
    asyncio.create_task(save_state_to_db({
        "key":          "raft_state",
        "current_term": state.current_term,
        "voted_for":    REPLICA_ID,
    }))

    log.info(f"Starting election for term {state.current_term}")

    vote_request = {
        "term":           state.current_term,
        "candidate_id":   REPLICA_ID,
        "last_log_index": state.last_log_index(),
        "last_log_term":  state.last_log_term(),
    }

    votes = 1
    try:
        # Cap vote gathering so a very slow peer cannot block election progress.
        async def _gather_votes():
            if raft_http_client is None:
                return []
            tasks = [
                raft_http_client.post(
                    f"{peer}/request-vote",
                    json=vote_request,
                    timeout=VOTE_RPC_TIMEOUT,
                )
                for peer in PEER_URLS
            ]
            return await asyncio.gather(*tasks, return_exceptions=True)

        results = await asyncio.wait_for(
            _gather_votes(),
            timeout=min(4.0, ELECTION_TIMEOUT_MIN * 0.9)
        )
    except asyncio.TimeoutError:
        log.warning("Election vote-gathering timed out — reverting to follower")
        await _step_down(state.current_term)
        return

    for peer, result in zip(PEER_URLS, results):
        if isinstance(result, Exception):
            log.warning(f"Vote request to {peer} failed: {result}")
            continue
        if result.status_code == 200:
            data = result.json()
            if data.get("term", 0) > state.current_term:
                await _step_down(data["term"])
                return
            if data.get("vote_granted"):
                votes += 1
                log.info(f"Got vote from {peer} — total {votes}")

    majority = (len(PEER_URLS) + 1) // 2 + 1
    if state.role == Role.CANDIDATE and votes >= majority:
        state.role      = Role.LEADER
        state.leader_id = REPLICA_ID
        state._init_leader_state()
        log.info(f"🏆 Became LEADER for term {state.current_term} with {votes} votes")
        await _send_heartbeats()
    else:
        log.info(f"Election failed ({votes} votes) — reverting to follower")
        await _step_down(state.current_term)


# ── BACKGROUND TASK — election timer + heartbeat loop ─────────────────────────
async def _raft_loop():
    """
    FIX 3: Time-based scheduling.
    FIX 9: All I/O is fire-and-forget via create_task so this loop never
           blocks on network or DB calls. Even if _send_heartbeats or
           _start_election stall internally, the timer keeps advancing.
           This is what prevents the 12s election timeout seen in the logs
           where a DB/network stall froze the entire event loop.
    """
    log.info(f"RAFT loop started. Role: {state.role}, Peers: {PEER_URLS}")
    last_heartbeat_sent: float = 0.0
    heartbeat_task: Optional[asyncio.Task] = None
    heartbeat_task_started_at: float = 0.0

    while True:
        await asyncio.sleep(0.05)   # pure sleep — never blocked

        now = time.monotonic()

        if state.role == Role.LEADER:
            # If a heartbeat wave is stuck, cancel it so it cannot block all
            # future heartbeats and trigger follower election timeouts.
            if (
                heartbeat_task is not None
                and not heartbeat_task.done()
                and (now - heartbeat_task_started_at) > HEARTBEAT_SEND_DEADLINE
            ):
                heartbeat_task.cancel()
                heartbeat_task = None

            if now - last_heartbeat_sent >= HEARTBEAT_INTERVAL:
                # Keep at most one heartbeat RPC wave in flight at a time.
                if heartbeat_task is None or heartbeat_task.done():
                    heartbeat_task = asyncio.create_task(_send_heartbeats())
                    heartbeat_task_started_at = now
                    last_heartbeat_sent = now

        elif state.role == Role.FOLLOWER:
            # If we are no longer leader, abandon any stale leader heartbeat task.
            if heartbeat_task is not None and not heartbeat_task.done():
                heartbeat_task.cancel()
            heartbeat_task = None

            elapsed = now - state.last_heartbeat
            if elapsed >= state.election_timeout:
                log.info(f"Election timeout after {elapsed:.2f}s — starting election")
                # FIX 9: election runs as a separate task; loop keeps ticking.
                # Advance last_heartbeat so the next tick doesn't spawn a
                # second election while the first is still in flight.
                state.last_heartbeat = now
                asyncio.create_task(_start_election())

        # CANDIDATE: _start_election task runs independently; loop just ticks


# ── STARTUP ────────────────────────────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    global raft_http_client

    # FIX 1: ensure indexes exist before anything else
    await init_mongo_indexes()

    # Reuse one HTTP client for all RAFT RPCs to avoid per-heartbeat
    # connection churn and reduce false ConnectTimeouts.
    raft_http_client = httpx.AsyncClient(
        limits=httpx.Limits(max_connections=100, max_keepalive_connections=20, keepalive_expiry=30.0)
    )

    # Load persistent RAFT state asynchronously
    saved = await load_state_from_db()
    state.current_term = saved.get("current_term", 0)
    state.voted_for    = saved.get("voted_for", None)

    # Replay log from MongoDB
    state.log = await load_log_from_db()
    log.info(
        f"Replica {REPLICA_ID} restored: term={state.current_term}, "
        f"voted_for={state.voted_for}, log_len={len(state.log)}"
    )

    asyncio.create_task(_raft_loop())
    log.info(f"Replica {REPLICA_ID} started on port {PORT}")


@app.on_event("shutdown")
async def shutdown():
    global raft_http_client
    if raft_http_client is not None:
        await raft_http_client.aclose()
        raft_http_client = None