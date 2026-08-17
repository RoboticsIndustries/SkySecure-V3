"""
api/main.py
────────────
FastAPI application with L1 multi-source position cross-validation.

No physical receivers exist yet, so this uses independent live ADS-B
aggregator networks (OpenSky, adsb.lol, adsb.fi) as stand-ins for true
TDOA baselines — see processing/cross_source_validator.py for the full
explanation and processing/tdoa_validator.py for the real-TDOA path once
receiver hardware is deployed.

Key pieces:
- L1 cross-validator, rate-limit-aware (bounded + cached per broadcast cycle)
- Enhanced anomaly detector (L2/L3 kinematic + integrity scoring)
- Endpoints: /api/l1/sources, /api/l1/validate
- L1 status folded into WebSocket broadcasts
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional, List, Dict, Any

import aiohttp
import orjson
import redis.asyncio as aioredis
from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse
from contextlib import asynccontextmanager

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from models import StateVector, RiskBand, Classification
from config import settings

# L1 imports — real multi-source cross-validation (no receiver hardware yet;
# see processing/cross_source_validator.py for why this replaces the old
# simulated TDOA path, and processing/tdoa_validator.py for the true-TDOA
# path once physical receivers exist)
try:
    from processing.cross_source_validator import CrossSourceValidator
    from anomaly.enhanced_detector import EnhancedAnomalyDetector
    TDOA_AVAILABLE = True
except ImportError:
    TDOA_AVAILABLE = False
    logging.warning("L1 cross-validation module not available - running without it")

log = logging.getLogger(__name__)

# ─── Global state ──────────────────────────────────────────────────────────────

redis_client: Optional[aioredis.Redis] = None
_ws_clients: set[WebSocket] = set()
_track_snapshot: List[Dict[str, Any]] = []

# L1 validator instances
cross_validator: Optional[CrossSourceValidator] = None
anomaly_detector: Optional[EnhancedAnomalyDetector] = None

# Live aircraft cache
_live_cache: Dict[str, Any] = {
    "ts":       0,
    "aircraft": [],
}
LIVE_CACHE_TTL = 30   # seconds

HEADERS = {
    "User-Agent": "SkySecure/2.0 (airspace research)",
    "Accept":     "application/json",
}


# ─── L1 Helper Functions (real, live-data cross-validation) ──────────────────

# Per-ICAO result cache so we don't re-query the same aircraft every
# broadcast tick. External free-tier APIs will rate-limit/ban aggressive
# per-aircraft polling, so this is not optional.
_L1_CACHE: Dict[str, tuple] = {}
_L1_CACHE_TTL = 60  # seconds

# Hard cap on live cross-validation calls per broadcast cycle. The global
# feed carries thousands of aircraft; adsb.lol/adsb.fi/OpenSky cannot take
# a per-aircraft hit at that volume. Bump this only if you have paid/higher
# rate limit tiers.
_L1_MAX_PER_CYCLE = 20


def _select_l1_candidates(aircraft_list: List[dict]) -> List[dict]:
    """Pick which aircraft get a live cross-validation check this cycle:
    anything already flagged risky by other layers first, then fill the
    remaining budget so coverage rotates rather than always hitting the
    same first N aircraft in the list."""
    now = time.time()
    fresh_candidates = [
        ac for ac in aircraft_list
        if ac.get("icao") and ac.get("lat") is not None and ac.get("lon") is not None
        and (ac["icao"] not in _L1_CACHE or now - _L1_CACHE[ac["icao"]][0] > _L1_CACHE_TTL)
    ]
    fresh_candidates.sort(key=lambda ac: ac.get("risk", 0), reverse=True)
    return fresh_candidates[:_L1_MAX_PER_CYCLE]


async def run_l1_cross_validation(aircraft_list: List[dict]) -> None:
    """
    Runs real multi-source cross-validation (see processing/cross_source_validator.py)
    on a bounded, rate-limit-respecting sample of the current aircraft list,
    mutating each aircraft dict in place with 'l1' results. Cached results
    from previous cycles are also applied to any aircraft still in cache.
    """
    if not TDOA_AVAILABLE or not cross_validator:
        return

    now = time.time()
    candidates = _select_l1_candidates(aircraft_list)

    if candidates:
        results = await asyncio.gather(
            *[
                cross_validator.validate_aircraft(ac["icao"], ac["lat"], ac["lon"])
                for ac in candidates
            ],
            return_exceptions=True,
        )
        for ac, result in zip(candidates, results):
            if isinstance(result, Exception):
                log.warning(f"L1 cross-validation failed for {ac.get('icao')}: {result}")
                continue
            _L1_CACHE[ac["icao"]] = (now, result.to_dict())

    # Apply cache (fresh this cycle or still within TTL) to every aircraft
    for ac in aircraft_list:
        icao = ac.get("icao")
        cached = _L1_CACHE.get(icao)
        if not cached or now - cached[0] > _L1_CACHE_TTL:
            continue
        result = cached[1]
        ac["l1"] = {
            "validated": result["is_valid"],
            "verdict": result["verdict"],
            "disagreement_m": result["max_disagreement_m"],
            "confidence": result["confidence"],
            "sources": result["sources_used"],
        }
        if result["verdict"] == "SPOOFED":
            ac["risk"] = max(ac.get("risk", 0), 80)
            ac["band"] = "HIGH"
            ac.setdefault("anoms", []).append({
                "type": "L1_POSITION_DISAGREEMENT",
                "description": f"Independent networks disagree by {result['max_disagreement_m']:.0f}m "
                                f"({'/'.join(result['sources_used'])})",
            })


# ─── Lifespan ─────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client, cross_validator, anomaly_detector
    
    redis_client = aioredis.from_url(settings.REDIS_URL, decode_responses=False)
    
    # Initialize L1 cross-source validator (real live-network comparison,
    # not simulated TDOA — see processing/cross_source_validator.py)
    if TDOA_AVAILABLE:
        try:
            cross_validator = CrossSourceValidator()
            await cross_validator.__aenter__()
            anomaly_detector = EnhancedAnomalyDetector()
            log.info("✅ L1 cross-source validator initialized (OpenSky/adsb.lol/adsb.fi)")
        except Exception as e:
            log.error(f"Failed to initialize L1 cross-validator: {e}")
            cross_validator = None
            anomaly_detector = None
    
    asyncio.create_task(broadcast_loop())
    asyncio.create_task(alert_consumer_loop())
    
    yield
    
    await redis_client.close()
    if cross_validator:
        await cross_validator.__aexit__(None, None, None)


# ─── App ──────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="SkySecure V2 API with TDOA",
    version="2.1.0",
    default_response_class=ORJSONResponse,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── Live aircraft fetching ───────────────────────────────────────────────────

async def _fetch_live_aircraft() -> List[dict]:
    """
    Fetch from OpenSky and apply TDOA validation to each aircraft.
    """
    try:
        connector = aiohttp.TCPConnector(ssl=True)
        async with aiohttp.ClientSession(connector=connector, headers=HEADERS) as session:
            async with session.get(
                "https://opensky-network.org/api/states/all",
                timeout=aiohttp.ClientTimeout(total=25),
            ) as resp:
                if resp.status != 200:
                    log.warning("OpenSky returned HTTP %d", resp.status)
                    return []
                data = await resp.json(content_type=None)
                states = data.get("states") or []

        aircraft = []
        for s in states:
            if not s or s[0] is None or s[5] is None or s[6] is None:
                continue
            try:
                lat, lon = float(s[6]), float(s[5])
            except (TypeError, ValueError):
                continue
            if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                continue
            icao = s[0].upper().strip()
            if len(icao) != 6:
                continue

            def ft(m):
                try: return int(float(m) * 3.28084) if m else None
                except: return None
            def kts(ms):
                try: return int(float(ms) * 1.944) if ms else None
                except: return None

            ac = {
                "icao": icao,
                "cs":   (s[1] or "").strip() or None,
                "lat":  lat, "lon": lon,
                "alt":  ft(s[7]), "vel": kts(s[9]),
                "hdg":  float(s[10]) if s[10] else None,
                "vr":   int(float(s[11]) * 196.85) if s[11] else None,
                "gnd":  bool(s[8]), "src": "opensky",
                "risk": 0, "anoms": [], "cls": "CIVILIAN",
                "conf": 0.85, "mil": 0.0, "band": "NORMAL", "trail": [],
            }
            
            aircraft.append(ac)

        # L1 cross-validation runs once on the whole batch (bounded/cached
        # internally) rather than per-aircraft, to respect rate limits
        if TDOA_AVAILABLE and cross_validator:
            await run_l1_cross_validation(aircraft)

        log.info(f"OpenSky: {len(aircraft)} aircraft (L1: {'enabled' if TDOA_AVAILABLE else 'disabled'})")
        return aircraft

    except Exception as e:
        log.error("OpenSky fetch failed: %s", e)
        return []


# ─── REST Endpoints ───────────────────────────────────────────────────────────

@app.get("/api/live-aircraft")
async def get_live_aircraft():
    """
    Server-side proxy for OpenSky with TDOA validation.
    Returns all globally tracked aircraft with spoofing detection.
    Cached for 30 seconds.
    """
    global _live_cache

    now = time.time()
    if now - _live_cache["ts"] < LIVE_CACHE_TTL and _live_cache["aircraft"]:
        return {
            "count":    len(_live_cache["aircraft"]),
            "source":   "cache",
            "aircraft": _live_cache["aircraft"],
            "tdoa_enabled": TDOA_AVAILABLE,
        }

    aircraft = await _fetch_live_aircraft()

    if aircraft:
        _live_cache = {"ts": now, "aircraft": aircraft}

    return {
        "count":    len(aircraft),
        "source":   "live",
        "aircraft": aircraft,
        "tdoa_enabled": TDOA_AVAILABLE,
    }


@app.get("/api/aircraft")
async def get_all_aircraft(
    limit: int = Query(5000, le=20000),
    min_risk: int = Query(0, ge=0, le=100),
):
    """
    Return state vectors from the Redis fusion pipeline with TDOA validation.
    """
    keys = await redis_client.keys("sv:*")
    results = []

    if keys:
        pipe = redis_client.pipeline()
        for k in keys:
            pipe.get(k)
        raw_values = await pipe.execute()

        for raw in raw_values:
            if not raw:
                continue
            try:
                sv = StateVector.from_bytes(raw)
                if sv.risk_score >= min_risk:
                    ac = sv.to_api_dict()
                    results.append(ac)
            except Exception:
                continue

    if TDOA_AVAILABLE and cross_validator:
        await run_l1_cross_validation(results)

    return {
        "count": len(results),
        "timestamp": time.time(),
        "aircraft": results[:limit],
        "tdoa_enabled": TDOA_AVAILABLE,
    }


@app.get("/api/alerts")
async def get_alerts(limit: int = Query(100, le=1000), min_score: int = Query(50)):
    keys = await redis_client.keys("sv:*")
    alerts = []
    if keys:
        pipe = redis_client.pipeline()
        for k in keys:
            pipe.get(k)
        for raw in await pipe.execute():
            if not raw:
                continue
            try:
                sv = StateVector.from_bytes(raw)
                if sv.risk_score >= min_score and sv.anomalies:
                    alert = {
                        "icao24":         sv.icao24,
                        "callsign":       sv.callsign,
                        "risk_score":     sv.risk_score,
                        "risk_band":      sv.risk_band.value,
                        "classification": sv.classification.value,
                        "anomalies": [{"type": a.anomaly_type.value, "description": a.description} for a in sv.anomalies],
                        "lat":            sv.lat,
                        "lon":            sv.lon,
                        "last_seen":      sv.last_seen,
                    }
                    
                    # Add L1 cross-validation status if available. This is
                    # an already-small alert list, so a direct per-item call
                    # (not the batch/rate-limit path used for the full feed) is fine.
                    if TDOA_AVAILABLE and cross_validator and sv.lat and sv.lon:
                        try:
                            l1_result = await cross_validator.validate_aircraft(sv.icao24, sv.lat, sv.lon)
                            alert['l1'] = {
                                'validated': l1_result.is_valid,
                                'verdict': l1_result.verdict,
                                'disagreement_m': round(l1_result.max_disagreement_m, 1),
                                'confidence': round(l1_result.confidence, 3),
                            }
                        except Exception as e:
                            log.warning(f"L1 validation failed for {sv.icao24}: {e}")
                    
                    alerts.append(alert)
            except Exception:
                continue
    alerts.sort(key=lambda x: x["risk_score"], reverse=True)
    return {"count": len(alerts), "alerts": alerts[:limit]}


@app.get("/api/stats")
async def get_stats():
    keys = await redis_client.keys("sv:*")
    total = len(keys) if keys else 0
    classifications = {c.value: 0 for c in Classification}
    risk_bands = {b.value: 0 for b in RiskBand}
    
    tdoa_stats = {"validated": 0, "spoofed": 0, "uncertain": 0}
    integrity_tracks = 0
    
    if keys:
        pipe = redis_client.pipeline()
        for k in keys:
            pipe.get(k)
        for raw in await pipe.execute():
            if not raw:
                continue
            try:
                sv = StateVector.from_bytes(raw)
                classifications[sv.classification.value] += 1
                risk_bands[sv.risk_band.value] += 1
                if sv.nic is not None or sv.nac_p is not None:
                    integrity_tracks += 1
            except Exception:
                continue
    
    return {
        "timestamp":       time.time(),
        "total_tracks":    total,
        "classifications": classifications,
        "risk_bands":      risk_bands,
        "ws_clients":      len(_ws_clients),
        "tdoa_enabled":    TDOA_AVAILABLE,
        "tdoa_stats":      tdoa_stats,
        "integrity_tracks": integrity_tracks,
    }


# ─── L1 cross-validation endpoints ────────────────────────────────────────────
# Note: no physical receivers exist yet, so there is no "receiver network
# status" endpoint anymore — that concept only applies once real hardware
# (see processing/tdoa_validator.py) is deployed. What exists today is a
# live-network cross-check, exposed below.

@app.get("/api/l1/sources")
async def get_l1_sources():
    """Which independent live networks L1 cross-validation is currently using."""
    if not TDOA_AVAILABLE or not cross_validator:
        return {"error": "L1 cross-validation not available", "sources": []}
    from processing.cross_source_validator import SOURCES
    return {"count": len(SOURCES), "sources": list(SOURCES.keys())}


@app.post("/api/l1/validate")
async def validate_position_l1(
    icao: str,
    lat: float,
    lon: float,
):
    """Manually cross-validate an aircraft's position against independent live networks."""
    if not TDOA_AVAILABLE or not cross_validator:
        return {"error": "L1 cross-validation not available"}
    
    try:
        result = await cross_validator.validate_aircraft(icao, lat, lon)
        return {
            "icao": icao,
            "position": {"lat": lat, "lon": lon},
            "l1_result": result.to_dict(),
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/healthz")
async def healthz():
    return {
        "status": "ok",
        "time": time.time(),
        "l1_enabled": TDOA_AVAILABLE,
        "l1_sources_active": 3 if cross_validator else 0,
    }


# ─── WebSocket ────────────────────────────────────────────────────────────────

@app.websocket("/ws/tracks")
async def ws_tracks(websocket: WebSocket):
    await websocket.accept()
    _ws_clients.add(websocket)
    try:
        # Send initial snapshot with TDOA data
        payload = orjson.dumps({
            "type":     "snapshot",
            "ts":       time.time(),
            "count":    len(_track_snapshot),
            "aircraft": _track_snapshot,
            "tdoa_enabled": TDOA_AVAILABLE,
        })
        await websocket.send_bytes(payload)

        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
                if data == "ping":
                    await websocket.send_text("pong")
            except asyncio.TimeoutError:
                await websocket.send_text("ping")
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        _ws_clients.discard(websocket)


# ─── Background: broadcast loop ───────────────────────────────────────────────

async def broadcast_loop() -> None:
    """Broadcast all aircraft with TDOA validation to WebSocket clients."""
    global _track_snapshot

    while True:
        await asyncio.sleep(settings.WS_BROADCAST_INTERVAL)
        try:
            keys = await redis_client.keys("ac:*")
            fused_keys = await redis_client.keys("sv:*")

            all_keys = list(set(keys + fused_keys))
            tracks = []

            if all_keys:
                pipe = redis_client.pipeline()
                for k in all_keys:
                    pipe.get(k)
                for raw in await pipe.execute():
                    if not raw:
                        continue
                    try:
                        import orjson as _oj
                        ac = _oj.loads(raw)
                        if isinstance(ac, dict) and ac.get("icao"):
                            tracks.append(ac)
                            continue
                    except Exception:
                        pass
                    try:
                        sv = StateVector.from_bytes(raw)
                        ac = sv.to_api_dict()
                        tracks.append(ac)
                    except Exception:
                        continue

            # L1 cross-validation runs once per broadcast tick on the whole
            # snapshot (internally bounded/cached), not per-track
            if TDOA_AVAILABLE and cross_validator:
                await run_l1_cross_validation(tracks)

            _track_snapshot = tracks

            if not _ws_clients:
                continue

            payload = orjson.dumps({
                "type":     "snapshot",
                "ts":       time.time(),
                "count":    len(tracks),
                "aircraft": tracks,
                "tdoa_enabled": TDOA_AVAILABLE,
            })

            dead = set()
            for ws in _ws_clients:
                try:
                    await ws.send_bytes(payload)
                except Exception:
                    dead.add(ws)
            for ws in dead:
                _ws_clients.discard(ws)

        except Exception as e:
            log.error("Broadcast loop error: %s", e)


# ─── Background: alert consumer ───────────────────────────────────────────────

async def alert_consumer_loop() -> None:
    consumer = AIOKafkaConsumer(
        settings.TOPIC_ALERTS_ANOMALY,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP,
        group_id=f"{settings.KAFKA_GROUP_PREFIX}.api-alerts",
        value_deserializer=lambda v: v,
        auto_offset_reset="latest",
    )
    await consumer.start()
    try:
        async for msg in consumer:
            if not _ws_clients:
                continue
            try:
                sv = StateVector.from_bytes(msg.value)
                ac = sv.to_api_dict()
                
                # Apply L1 cross-validation to this single alert (one item
                # at a time off the Kafka topic, so a direct call is fine —
                # the rate-limit concern is about the full global feed)
                if TDOA_AVAILABLE and cross_validator and ac.get("lat") and ac.get("lon"):
                    try:
                        l1_result = await cross_validator.validate_aircraft(ac["icao"], ac["lat"], ac["lon"])
                        ac["l1"] = {
                            "validated": l1_result.is_valid,
                            "verdict": l1_result.verdict,
                            "disagreement_m": round(l1_result.max_disagreement_m, 1),
                        }
                    except Exception as e:
                        log.warning(f"L1 validation failed in alert loop: {e}")
                
                alert_payload = orjson.dumps({
                    "type":     "alert",
                    "ts":       time.time(),
                    "aircraft": ac,
                    "anomalies": [{"type": a.anomaly_type.value, "description": a.description} for a in sv.anomalies],
                })
                dead = set()
                for ws in _ws_clients:
                    try:
                        await ws.send_bytes(alert_payload)
                    except Exception:
                        dead.add(ws)
                for ws in dead:
                    _ws_clients.discard(ws)
            except Exception as e:
                log.error("Alert push error: %s", e)
    finally:
        await consumer.stop()
