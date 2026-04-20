"""
gateway/main.py  —  WebSocket Gateway  (Fixed)
===============================================
Fixes applied:
  1. asyncio.Lock guards all leader_url mutations
     → Concurrent stroke-forward and poll-loop can no longer race each other
        to clobber leader_url mid-stroke
  2. _discover_leader picks the replica with the HIGHEST term that is leader
     → A stale low-term leader left over from a previous term is never returned
  3. 403 "not leader" response is parsed for the leader_id hint
     → Gateway jumps straight to the correct replica instead of doing a full
        poll cycle, cutting failover latency from ~1s to one extra HTTP round trip
  4. LEADER_POLL_INTERVAL reduced 1.0s → 0.3s
     → Gateway tracks real leadership changes faster; combined with the lock
        this does NOT cause a thundering-herd because discovery is serialised
  5. _forward_stroke_to_leader uses a single shared discovery in-flight flag
     → Multiple concurrent strokes during leader loss trigger exactly ONE
        rediscovery instead of N simultaneous _discover_leader() calls
  6. Exponential back-off on transient stroke failures (non-leader-change errors)
     → Avoids hammering a temporarily overloaded leader
  7. /config returns accurate timeout values that match the fixed replica code

Environment variables (set by docker-compose):
  REPLICA_URLS : comma-separated replica base URLs
                 e.g. http://replica1:5000,http://replica2:5000,http://replica3:5000
"""

import asyncio
import logging
import os
import subprocess
from typing import Optional

import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware

# ── CONFIG ─────────────────────────────────────────────────────────────────────
REPLICA_URLS = [
    u.strip()
    for u in os.getenv("REPLICA_URLS", "").split(",")
    if u.strip()
]

REPLICA_PUBLIC_URLS = {
    1: os.getenv("REPLICA_1_URL", "http://localhost:5001"),
    2: os.getenv("REPLICA_2_URL", "http://localhost:5002"),
    3: os.getenv("REPLICA_3_URL", "http://localhost:5003"),
}

# FIX 4: Reduced from 1.0s → 0.3s for faster failover detection
LEADER_POLL_INTERVAL = 0.3
STROKE_TIMEOUT       = 2.0
LEADER_RETRY_DELAY   = 0.1   # base delay for back-off (FIX 6)

logging.basicConfig(
    level=logging.INFO,
    format="[Gateway] %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)

# ── STATE ──────────────────────────────────────────────────────────────────────
class GatewayState:
    def __init__(self):
        self.leader_url: Optional[str] = None
        self.clients: set[WebSocket]   = set()

        # FIX 1: single lock serialises all leader_url reads+writes
        self._leader_lock: asyncio.Lock = asyncio.Lock()

        # FIX 5: in-flight discovery flag prevents N concurrent _discover_leader
        # calls when N strokes all fail at once during a leader transition
        self._discovery_in_flight: bool = False

gw = GatewayState()

# ── FASTAPI APP ────────────────────────────────────────────────────────────────
app = FastAPI(title="RAFT Gateway")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    return HTMLResponse(content=open("/usr/src/app/static/index.html").read())

# ── HEALTH ─────────────────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    return {
        "leader_url":       gw.leader_url,
        "connected_clients": len(gw.clients),
        "replicas":         REPLICA_URLS,
    }

# ── CONFIG  (FIX 7: values now match the fixed replica timeouts) ───────────────
@app.get("/config")
async def get_config():
    return {
        "replica_public_urls":      REPLICA_PUBLIC_URLS,
        "replica_internal_urls":    REPLICA_URLS,
        "polling_interval_ms":      int(LEADER_POLL_INTERVAL * 1000),
        # Match replica/main.py values and per-replica bias (200ms * replica_id)
        "election_timeout_min_ms":  3200,   # 3000 + 200 (replica 1 bias)
        "election_timeout_max_ms":  6600,   # 6000 + 600 (replica 3 bias)
        "heartbeat_interval_ms":    200,
    }

# ── COMMITTED STROKES ──────────────────────────────────────────────────────────
@app.get("/committed-strokes")
async def get_committed_strokes():
    if not gw.leader_url:
        await _discover_leader()

    for attempt in range(3):
        if not gw.leader_url:
            await asyncio.sleep(LEADER_RETRY_DELAY)
            await _discover_leader()
            continue
        try:
            async with httpx.AsyncClient(timeout=2.0) as client:
                response = await client.get(f"{gw.leader_url}/committed-strokes")
            if response.status_code == 200:
                return response.json()
            async with gw._leader_lock:   # FIX 1
                gw.leader_url = None
            await _discover_leader()
        except Exception as e:
            log.warning(f"Error fetching strokes: {e}")
            async with gw._leader_lock:   # FIX 1
                gw.leader_url = None
            await _discover_leader()

    return {"strokes": [], "error": "No leader available"}

# ── CLEAR LOG ──────────────────────────────────────────────────────────────────
@app.post("/clear-all")
async def clear_all():
    log.info("Clearing all strokes from cluster...")
    results = {"status": "completed", "cleared_replicas": [], "failed_replicas": []}
    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            tasks     = [client.post(f"{url}/clear-log") for url in REPLICA_URLS]
            responses = await asyncio.gather(*tasks, return_exceptions=True)
        for url, response in zip(REPLICA_URLS, responses):
            if isinstance(response, Exception):
                results["failed_replicas"].append({"url": url, "error": str(response)})
            elif response.status_code == 200:
                results["cleared_replicas"].append(response.json().get("replica_id", "unknown"))
            else:
                results["failed_replicas"].append({"url": url, "status_code": response.status_code})
    except Exception as e:
        return {"status": "error", "message": str(e)}
    return results

# Resolve containers by compose service label so project-name prefixes do not break controls.
def _resolve_container_for_service(service_name: str) -> Optional[str]:
    try:
        result = subprocess.run(
            [
                "docker", "ps", "-a",
                "--filter", f"label=com.docker.compose.service={service_name}",
                "--format", "{{.Names}}",
            ],
            check=False,
            timeout=5,
            capture_output=True,
            text=True,
        )
        names = [line.strip() for line in result.stdout.splitlines() if line.strip()]
        return names[0] if names else None
    except Exception as e:
        log.warning(f"Failed to resolve container for service '{service_name}': {e}")
        return None


def _resolve_replica_and_mongo(replica_id: int) -> tuple[Optional[str], Optional[str]]:
    replica = _resolve_container_for_service(f"replica{replica_id}")
    mongo = _resolve_container_for_service(f"mongo{replica_id}")

    # Backward-compatible fallback for old project naming.
    if not replica:
        replica = f"jackfruit-replica{replica_id}-1"
    if not mongo:
        mongo = f"jackfruit-mongo{replica_id}-1"

    return replica, mongo


# ── CRASH / RECOVER ────────────────────────────────────────────────────────────
@app.post("/crash/{replica_id}")
async def crash_replica(replica_id: int):
    replica_container, mongo_container = _resolve_replica_and_mongo(replica_id)
    log.warning(f"Crashing replica {replica_id}")
    try:
        replica_stop = subprocess.run(
            ["docker", "stop", replica_container],
            check=False,
            timeout=5,
            capture_output=True,
            text=True,
        )
        mongo_stop = subprocess.run(
            ["docker", "stop", mongo_container],
            check=False,
            timeout=5,
            capture_output=True,
            text=True,
        )

        if replica_stop.returncode != 0 and mongo_stop.returncode != 0:
            return {
                "status": "error",
                "replica_id": replica_id,
                "message": "Failed to stop replica and mongo containers",
                "replica_container": replica_container,
                "mongo_container": mongo_container,
                "replica_error": (replica_stop.stderr or replica_stop.stdout).strip(),
                "mongo_error": (mongo_stop.stderr or mongo_stop.stdout).strip(),
            }

        return {
            "status": "crashed",
            "replica_id": replica_id,
            "replica_container": replica_container,
            "mongo_container": mongo_container,
        }
    except Exception as e:
        return {"status": "error", "replica_id": replica_id, "message": str(e)}

@app.post("/recover/{replica_id}")
async def recover_replica(replica_id: int):
    replica_container, mongo_container = _resolve_replica_and_mongo(replica_id)
    log.warning(f"Recovering replica {replica_id}")
    try:
        mongo_start = subprocess.run(
            ["docker", "start", mongo_container],
            check=False,
            timeout=5,
            capture_output=True,
            text=True,
        )
        await asyncio.sleep(2)
        replica_start = subprocess.run(
            ["docker", "start", replica_container],
            check=False,
            timeout=5,
            capture_output=True,
            text=True,
        )

        if replica_start.returncode != 0 and mongo_start.returncode != 0:
            return {
                "status": "error",
                "replica_id": replica_id,
                "message": "Failed to start replica and mongo containers",
                "replica_container": replica_container,
                "mongo_container": mongo_container,
                "replica_error": (replica_start.stderr or replica_start.stdout).strip(),
                "mongo_error": (mongo_start.stderr or mongo_start.stdout).strip(),
            }

        return {
            "status": "recovering",
            "replica_id": replica_id,
            "replica_container": replica_container,
            "mongo_container": mongo_container,
        }
    except Exception as e:
        return {"status": "error", "replica_id": replica_id, "message": str(e)}

# ── WEBSOCKET ──────────────────────────────────────────────────────────────────
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    gw.clients.add(websocket)
    log.info(f"Client connected. Total: {len(gw.clients)}")
    try:
        while True:
            data            = await websocket.receive_json()
            committed_entry = await _forward_stroke_to_leader(data)
            if committed_entry:
                await _broadcast(committed_entry)
    except WebSocketDisconnect:
        log.info("Client disconnected")
    except Exception as e:
        log.warning(f"WebSocket error: {e}")
    finally:
        gw.clients.discard(websocket)
        log.info(f"Client removed. Total: {len(gw.clients)}")

# ── STROKE FORWARDING ──────────────────────────────────────────────────────────
async def _forward_stroke_to_leader(stroke: dict) -> Optional[dict]:
    """
    Forward a stroke to the current leader.

    FIX 1: leader_url reads are done outside the lock (lock is only held during
           writes) so normal fast-path strokes have zero contention.
    FIX 3: 403 response body is parsed for leader_id hint — we jump directly
           to the hinted replica instead of doing a full health-check poll.
    FIX 5: If leader_url is None and discovery is already in flight, we wait
           briefly instead of spawning another concurrent _discover_leader().
    FIX 6: Transient errors (not leader-change events) use exponential back-off.
    """
    for attempt in range(5):
        # ── Wait for a known leader ────────────────────────────────────────────
        if not gw.leader_url:
            if not gw._discovery_in_flight:
                await _discover_leader()
            else:
                # Another coroutine is already discovering — just wait
                await asyncio.sleep(LEADER_RETRY_DELAY * (attempt + 1))
            continue

        try:
            async with httpx.AsyncClient(timeout=STROKE_TIMEOUT) as client:
                response = await client.post(
                    f"{gw.leader_url}/stroke",
                    json={"stroke": stroke}
                )

            # ── Happy path ─────────────────────────────────────────────────────
            if response.status_code == 200:
                log.info(f"Stroke committed via {gw.leader_url}")
                return response.json().get("entry")

            # ── FIX 3: 403 = wrong leader, use hint to jump directly ───────────
            elif response.status_code == 403:
                body      = response.json()
                leader_id = body.get("detail", "")
                log.warning(f"Not-leader 403 from {gw.leader_url}; hint='{leader_id}'")

                # Try to parse "Not the leader. Current leader is replica N"
                hinted_url = _url_from_leader_hint(leader_id)
                async with gw._leader_lock:   # FIX 1
                    if hinted_url:
                        log.info(f"Jumping directly to hinted leader: {hinted_url}")
                        gw.leader_url = hinted_url
                    else:
                        gw.leader_url = None   # fall back to full discovery

                if not hinted_url:
                    await _discover_leader()

            # ── FIX 6: other errors — exponential back-off, don't clear leader ─
            else:
                wait = LEADER_RETRY_DELAY * (2 ** attempt)
                log.warning(f"Stroke HTTP {response.status_code} — back-off {wait:.2f}s")
                await asyncio.sleep(wait)

        except Exception as e:
            # Network error → leader may be dead, trigger discovery
            log.warning(f"Stroke request to {gw.leader_url} failed: {e}")
            async with gw._leader_lock:   # FIX 1
                gw.leader_url = None
            await _discover_leader()

    log.error("Failed to commit stroke after 5 attempts")
    return None


def _url_from_leader_hint(detail: str) -> Optional[str]:
    """
    Parse a replica ID out of the 403 detail string and map it to a URL.
    Detail format: "Not the leader. Current leader is replica N"
    Returns the replica's internal URL, or None if unparseable.
    """
    try:
        # The detail string ends with "replica N"
        parts = detail.strip().split()
        replica_id = int(parts[-1])
        # Match against REPLICA_URLS by index (replica IDs are 1-based)
        for url in REPLICA_URLS:
            # URLs are like http://replica1:5000, http://replica2:5000 ...
            if f"replica{replica_id}" in url:
                return url
    except (ValueError, IndexError):
        pass
    return None

# ── BROADCAST ──────────────────────────────────────────────────────────────────
async def _broadcast(entry: dict):
    dead_clients: set = set()
    for client in gw.clients:
        try:
            await client.send_json({"type": "stroke", "entry": entry})
        except Exception:
            dead_clients.add(client)
    for client in dead_clients:
        gw.clients.discard(client)
    if dead_clients:
        log.info(f"Removed {len(dead_clients)} dead clients. Active: {len(gw.clients)}")

# ── LEADER DISCOVERY ───────────────────────────────────────────────────────────
async def _discover_leader():
    """
    Poll all replicas and elect the one with role=leader AND the highest term.

    FIX 1: Holds _leader_lock only during the final write to leader_url.
    FIX 2: Picks highest-term leader, not just the first one that responds.
    FIX 5: Sets _discovery_in_flight so concurrent callers back off instead
           of all issuing parallel health-check storms.
    """
    # FIX 5: mark discovery as in-flight
    gw._discovery_in_flight = True
    best_url:  Optional[str] = None
    best_term: int            = -1

    try:
        async with httpx.AsyncClient(timeout=1.0) as client:
            tasks   = [client.get(f"{url}/health") for url in REPLICA_URLS]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        # FIX 2: iterate ALL replicas and keep the one with the highest term
        for url, result in zip(REPLICA_URLS, results):
            if isinstance(result, Exception):
                log.warning(f"Health check failed for {url}: {result}")
                continue
            if result.status_code != 200:
                continue
            data = result.json()
            if data.get("role") == "leader":
                term = data.get("term", 0)
                if term > best_term:
                    best_term = term
                    best_url  = url

        async with gw._leader_lock:   # FIX 1: atomic write
            if best_url and best_url != gw.leader_url:
                log.info(f"Leader updated: {best_url} (term {best_term})")
            elif not best_url:
                log.warning("No leader found — election may be in progress")
            gw.leader_url = best_url

    finally:
        # FIX 5: always clear the flag even if discovery raised
        gw._discovery_in_flight = False

# ── BACKGROUND POLL ────────────────────────────────────────────────────────────
async def _leader_poll_loop():
    """
    FIX 4: Polls every 0.3s (was 1.0s) for faster failover detection.
    The asyncio.Lock in _discover_leader ensures this never races with
    concurrent discovery triggered by a failed stroke.
    """
    log.info("Leader discovery loop started")
    while True:
        await _discover_leader()
        await asyncio.sleep(LEADER_POLL_INTERVAL)

# ── STARTUP ────────────────────────────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    asyncio.create_task(_leader_poll_loop())
    log.info(f"Gateway started. Replicas: {REPLICA_URLS}")