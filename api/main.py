"""
api/main.py
────────────
FastAPI application with L1 multi-source position cross-validation.

No physical receivers exist yet. OpenSky, adsb.lol, and adsb.fi are treated
as separate aggregator reports, not independent physical measurements and
not substitutes for TDOA. See processing/cross_source_validator.py for scope
and processing/tdoa_validator.py for the hardware-backed path.

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
import asyncpg
import orjson
import redis.asyncio as aioredis
from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse
from contextlib import asynccontextmanager

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from models import StateVector, RiskBand, Classification, DetectionLayer, LayerStatus
from config import settings, KAFKA_CONSUMER_STABILITY
from coverage_area import (
    COVERAGE_LOCK_KEY,
    COVERAGE_RATE_KEY,
    CoverageArea,
    coverage_url,
    default_coverage_area,
    load_coverage_area,
    load_coverage_area_record,
    save_coverage_area,
    within_coverage_area,
)

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
    "coverage_token": b"",
}
_live_fetch_lock = asyncio.Lock()

_layer_vector_cache: Dict[str, Any] = {
    "client_id": None,
    "ts": 0.0,
    "vectors": [],
}
_layer_vector_cache_lock = asyncio.Lock()
LIVE_CACHE_TTL = 30   # seconds


def _track_in_coverage(track: Dict[str, Any], area: CoverageArea) -> bool:
    try:
        return within_coverage_area(float(track["lat"]), float(track["lon"]), area)
    except (KeyError, TypeError, ValueError):
        return False


def _websocket_origin_allowed(websocket: WebSocket) -> bool:
    origin = websocket.headers.get("origin")
    if origin is None:
        return True  # Non-browser monitoring clients do not send Origin.
    allowed = {value.rstrip("/") for value in settings.API_CORS_ORIGINS}
    return origin.rstrip("/") in allowed


HEADERS = {
    "User-Agent": "SkySecure/2.0 (airspace research)",
    "Accept":     "application/json",
}

def _parse_adsb_lol_aircraft(data: dict) -> List[dict]:
    """Normalize an adsb.lol point-feed response to the public API shape."""
    aircraft = []
    for raw in data.get("ac") or []:
        icao = str(raw.get("hex") or "").upper().lstrip("~")
        lat, lon = raw.get("lat"), raw.get("lon")
        if len(icao) != 6 or lat is None or lon is None:
            continue
        try:
            lat, lon = float(lat), float(lon)
        except (TypeError, ValueError):
            continue
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            continue

        def number(value, *, integer=False):
            try:
                parsed = float(value)
                return int(parsed) if integer else parsed
            except (TypeError, ValueError):
                return None

        altitude = number(raw.get("alt_baro"), integer=True)
        aircraft.append({
            "icao": icao,
            "cs": str(raw.get("flight") or raw.get("r") or "").strip() or None,
            "lat": lat,
            "lon": lon,
            "alt": altitude,
            "vel": number(raw.get("gs"), integer=True),
            "hdg": number(raw.get("track")),
            "vr": number(raw.get("baro_rate"), integer=True),
            "nic": number(raw.get("nic"), integer=True),
            "nac_p": number(raw.get("nac_p"), integer=True),
            "gnd": raw.get("alt_baro") == "ground",
            "src": "adsb_lol",
            "risk": 0,
            "anoms": [],
            "cls": "CIVILIAN",
            "conf": 0.75,
            "mil": 0.0,
            "band": "NORMAL",
            "trail": [],
        })
    return aircraft


# ─── L1 Helper Functions (real, live-data cross-validation) ──────────────────

# Per-ICAO-and-claim-source result cache so we don't re-query the same aircraft every
# broadcast tick. External free-tier APIs will rate-limit/ban aggressive
# per-aircraft polling, so this is not optional.
_L1_CACHE: Dict[tuple[str, str], tuple] = {}
_L1_CACHE_TTL = 60  # seconds

# Hard cap on live cross-validation calls per broadcast cycle. The global
# feed carries thousands of aircraft; adsb.lol/adsb.fi/OpenSky cannot take
# a per-aircraft hit at that volume. Bump this only if you have paid/higher
# rate limit tiers.
_L1_MAX_PER_CYCLE = 20


def _l1_cache_key(ac: dict) -> tuple[str, str]:
    return ac["icao"], str(ac.get("src") or "unknown").lower()


def _select_l1_candidates(aircraft_list: List[dict]) -> List[dict]:
    """Pick which aircraft get a live cross-validation check this cycle:
    anything already flagged risky by other layers first, then fill the
    remaining budget so coverage rotates rather than always hitting the
    same first N aircraft in the list."""
    now = time.time()
    fresh_candidates = [
        ac for ac in aircraft_list
        if ac.get("icao") and ac.get("lat") is not None and ac.get("lon") is not None
        and str(ac.get("src") or "").lower() in {"opensky", "adsb_lol", "adsb_fi"}
        and (_l1_cache_key(ac) not in _L1_CACHE
             or now - _L1_CACHE[_l1_cache_key(ac)][0] > _L1_CACHE_TTL)
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
                cross_validator.validate_aircraft(
                    ac["icao"], ac["lat"], ac["lon"],
                    claimed_source=ac.get("src"),
                )
                for ac in candidates
            ],
            return_exceptions=True,
        )
        for ac, result in zip(candidates, results):
            if isinstance(result, BaseException):
                log.warning(f"L1 cross-validation failed for {ac.get('icao')}: {result}")
                continue
            _L1_CACHE[_l1_cache_key(ac)] = (now, result.to_dict())

    # Apply cache (fresh this cycle or still within TTL) to every aircraft
    for ac in aircraft_list:
        cached = _L1_CACHE.get(_l1_cache_key(ac))
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
                "description": f"Aggregator reports disagree by {result['max_disagreement_m']:.0f}m "
                                f"({'/'.join(result['sources_used'])})",
            })


_THREAT_TO_RISK = {
    "UNKNOWN": 0, "LOW": 0, "MEDIUM": 45, "HIGH": 75, "CRITICAL": 95,
}
_THREAT_TO_BAND = {
    "UNKNOWN": "NORMAL", "LOW": "NORMAL", "MEDIUM": "ELEVATED",
    "HIGH": "HIGH", "CRITICAL": "HIGH",
}


def run_l2_l3_detection(aircraft_list: List[dict]) -> None:
    """Attach local kinematic/integrity assessment to live aircraft records."""
    if not anomaly_detector:
        return

    now = time.time()
    for ac in aircraft_list:
        icao = ac.get("icao")
        if not icao or ac.get("lat") is None or ac.get("lon") is None:
            continue
        cached_l1 = _L1_CACHE.get(_l1_cache_key(ac))
        l1_result = None
        if cached_l1 and now - cached_l1[0] <= _L1_CACHE_TTL:
            l1_result = cached_l1[1]
        try:
            assessment = anomaly_detector.assess(
                icao=icao, lat=ac["lat"], lon=ac["lon"],
                alt_baro=ac.get("alt"), alt_geo=ac.get("alt_geo"),
                velocity=ac.get("vel"), vertical_rate=ac.get("vr"),
                heading=ac.get("hdg"), nic=ac.get("nic"),
                nac_p=ac.get("nac_p"), l1_result=l1_result,
                observed_at=now,
            )
        except Exception as exc:
            log.warning("L2/L3 assessment failed for %s: %s", icao, exc)
            continue

        ac["fused"] = assessment.to_dict()
        fused_risk = _THREAT_TO_RISK.get(assessment.threat_level, 0)
        if fused_risk > ac.get("risk", 0):
            ac["risk"] = fused_risk
            ac["band"] = _THREAT_TO_BAND.get(assessment.threat_level, "NORMAL")

        existing_types = {
            item.get("type") for item in ac.setdefault("anoms", [])
            if isinstance(item, dict)
        }
        for key, label in (
            ("l2_kinematic", "L2_KINEMATIC"),
            ("l3_integrity", "L3_INTEGRITY"),
        ):
            layer = assessment.layers.get(key)
            if (layer and layer.available
                    and layer.score >= anomaly_detector.ASSERT_THRESHOLD
                    and label not in existing_types):
                ac["anoms"].append({"type": label, "description": layer.reason})
        if (assessment.corroborated
                and "MULTI_LAYER_CORROBORATION" not in existing_types):
            ac["anoms"].append({
                "type": "MULTI_LAYER_CORROBORATION",
                "description": "; ".join(assessment.notes[:1]),
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
    allow_origins=settings.API_CORS_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── Live aircraft fetching ───────────────────────────────────────────────────

async def _fetch_live_aircraft(area: Optional[CoverageArea] = None) -> List[dict]:
    """
    Fetch from OpenSky and apply TDOA validation to each aircraft.
    """
    try:
        if area is None:
            area = await load_coverage_area(redis_client) if redis_client else default_coverage_area()
        connector = aiohttp.TCPConnector(ssl=True)
        async with aiohttp.ClientSession(connector=connector, headers=HEADERS) as session:
            async with session.get(
                "https://opensky-network.org/api/states/all",
                timeout=aiohttp.ClientTimeout(total=25),
            ) as resp:
                if resp.status != 200:
                    log.warning("OpenSky returned HTTP %d", resp.status)
                    if resp.status == 429:
                        async with session.get(
                            coverage_url(area),
                            timeout=aiohttp.ClientTimeout(total=20),
                        ) as fallback_resp:
                            if fallback_resp.status == 200:
                                fallback_data = await fallback_resp.json(content_type=None)
                                fallback_aircraft = _parse_adsb_lol_aircraft(fallback_data)
                                if TDOA_AVAILABLE and cross_validator:
                                    await run_l1_cross_validation(fallback_aircraft)
                                run_l2_l3_detection(fallback_aircraft)
                                log.info("adsb.lol fallback: %d aircraft", len(fallback_aircraft))
                                return fallback_aircraft
                            log.warning("adsb.lol fallback returned HTTP %d", fallback_resp.status)
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
            if not within_coverage_area(lat, lon, area):
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
        run_l2_l3_detection(aircraft)

        log.info(f"OpenSky: {len(aircraft)} aircraft (L1: {'enabled' if TDOA_AVAILABLE else 'disabled'})")
        return aircraft

    except Exception as e:
        log.error("OpenSky fetch failed: %s", e)
        return []


# ─── REST Endpoints ───────────────────────────────────────────────────────────

@app.get("/api/coverage")
async def get_coverage_area_config():
    area = await load_coverage_area(redis_client) if redis_client else default_coverage_area()
    return {"coverage": area.model_dump()}


@app.put("/api/coverage")
async def update_coverage_area_config(area: CoverageArea):
    global _live_cache
    if redis_client is None:
        raise HTTPException(status_code=503, detail="Redis unavailable")
    lock = redis_client.lock(COVERAGE_LOCK_KEY, timeout=30, blocking_timeout=5)
    async with lock:
        if await redis_client.get(COVERAGE_RATE_KEY):
            raise HTTPException(status_code=429, detail="Coverage may be changed once every 2 seconds")
        await save_coverage_area(redis_client, area)
        await redis_client.set(COVERAGE_RATE_KEY, "1", ex=2)
    _live_cache = {"ts": 0, "aircraft": [], "coverage_token": b""}
    return {"coverage": area.model_dump(), "status": "updated"}

@app.get("/api/live-aircraft")
async def get_live_aircraft():
    """
    Server-side proxy for the selected live coverage area with L1 validation.
    Returns aircraft within the runtime-configured center and radius.
    Cached for 30 seconds.
    """
    global _live_cache

    async with _live_fetch_lock:
        if redis_client is None:
            raise HTTPException(status_code=503, detail="Redis unavailable")

        coverage_lock = redis_client.lock(COVERAGE_LOCK_KEY, timeout=30, blocking_timeout=5)
        async with coverage_lock:
            area, coverage_token = await load_coverage_area_record(redis_client)
            now = time.time()
            if (
                now - _live_cache["ts"] < LIVE_CACHE_TTL
                and _live_cache["aircraft"]
                and _live_cache.get("coverage_token") == coverage_token
            ):
                return {
                    "count": len(_live_cache["aircraft"]),
                    "source": "cache",
                    "aircraft": _live_cache["aircraft"],
                    "tdoa_enabled": TDOA_AVAILABLE,
                }

        for _ in range(5):
            aircraft = await _fetch_live_aircraft(area)
            coverage_lock = redis_client.lock(COVERAGE_LOCK_KEY, timeout=30, blocking_timeout=5)
            async with coverage_lock:
                current_area, current_token = await load_coverage_area_record(redis_client)
                if current_token == coverage_token:
                    _live_cache = {
                        "ts": time.time(),
                        "aircraft": aircraft,
                        "coverage_token": coverage_token,
                    }
                    return {
                        "count": len(aircraft),
                        "source": "live",
                        "aircraft": aircraft,
                        "tdoa_enabled": TDOA_AVAILABLE,
                    }
            # The operator switched during the fetch. The lock guarantees the
            # token and response decision are atomic with PUT.
            area, coverage_token = current_area, current_token

        raise HTTPException(status_code=409, detail="Coverage changed repeatedly; retry the request")


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
    area = await load_coverage_area(redis_client)

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
                    if _track_in_coverage(ac, area):
                        results.append(ac)
            except Exception:
                continue

    if TDOA_AVAILABLE and cross_validator:
        await run_l1_cross_validation(results)
    run_l2_l3_detection(results)
    area = await load_coverage_area(redis_client)
    results = [result for result in results if _track_in_coverage(result, area)]

    return {
        "count": len(results),
        "timestamp": time.time(),
        "aircraft": results[:limit],
        "tdoa_enabled": TDOA_AVAILABLE,
    }


@app.get("/api/alerts")
async def get_alerts(limit: int = Query(100, le=1000), min_score: int = Query(50)):
    alerts = []
    area = await load_coverage_area(redis_client)
    for sv in await _load_state_vectors():
            try:
                if sv.risk_score >= min_score and sv.anomalies:
                    alert = {
                        "icao24":         sv.icao24,
                        "callsign":       sv.callsign,
                        "risk_score":     sv.risk_score,
                        "risk_band":      sv.risk_band.value,
                        "classification": sv.classification.value,
                        "anomalies": [a.to_api_dict() for a in sv.anomalies],
                        "lat":            sv.lat,
                        "lon":            sv.lon,
                        "last_seen":      sv.last_seen,
                    }
                    if not _track_in_coverage(alert, area):
                        continue
                    
                    # Add L1 cross-validation status if available. This is
                    # an already-small alert list, so a direct per-item call
                    # (not the batch/rate-limit path used for the full feed) is fine.
                    if (TDOA_AVAILABLE and cross_validator
                            and sv.lat is not None and sv.lon is not None):
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
    area = await load_coverage_area(redis_client)
    alerts = [alert for alert in alerts if _track_in_coverage(alert, area)]
    return {"count": len(alerts), "alerts": alerts[:limit]}


@app.get("/api/stats")
async def get_stats():
    if redis_client is None:
        raise HTTPException(status_code=503, detail="Redis unavailable")
    lock = redis_client.lock(COVERAGE_LOCK_KEY, timeout=30, blocking_timeout=5)
    async with lock:
        keys = await redis_client.keys("sv:*")
        total = 0
        area = await load_coverage_area(redis_client)
        classifications = {c.value: 0 for c in Classification}
        risk_bands = {b.value: 0 for b in RiskBand}
        tdoa_stats = {"validated": 0, "spoofed": 0, "uncertain": 0}

        if keys:
            pipe = redis_client.pipeline()
            for key in keys:
                pipe.get(key)
            for raw in await pipe.execute():
                if not raw:
                    continue
                try:
                    sv = StateVector.from_bytes(raw)
                    if not _track_in_coverage(sv.to_api_dict(), area):
                        continue
                    total += 1
                    classifications[sv.classification.value] += 1
                    risk_bands[sv.risk_band.value] += 1
                except Exception:
                    continue

        return {
            "timestamp": time.time(),
            "total_tracks": total,
            "classifications": classifications,
            "risk_bands": risk_bands,
            "ws_clients": len(_ws_clients),
            "tdoa_enabled": TDOA_AVAILABLE,
            "tdoa_stats": tdoa_stats,
        }


async def _load_state_vectors() -> List[StateVector]:
    """Load current enriched tracks with non-blocking discovery and a short cache."""
    if redis_client is None:
        return []
    async with _layer_vector_cache_lock:
        now = time.time()
        if (
            _layer_vector_cache["client_id"] == id(redis_client)
            and now - _layer_vector_cache["ts"] < 5.0
        ):
            return _layer_vector_cache["vectors"]

        keys = []
        async for key in redis_client.scan_iter(match="sv:*", count=500):
            keys.append(key)
        if not keys:
            _layer_vector_cache.update(
                client_id=id(redis_client), ts=now, vectors=[]
            )
            return []
        pipe = redis_client.pipeline()
        for key in keys:
            pipe.get(key)
        vectors = []
        for raw in await pipe.execute():
            if not raw:
                continue
            try:
                vectors.append(StateVector.from_bytes(raw))
            except Exception:
                continue
        _layer_vector_cache.update(
            client_id=id(redis_client), ts=now, vectors=vectors
        )
        return vectors


def _empty_layer_summary() -> Dict[str, Dict[str, Any]]:
    descriptions = {
        "L1": "Position and source cross-validation",
        "L2": "Kinematic and behavioral anomaly detection",
        "L3": "Learned trajectory models",
        "L4": "Multi-sensor fusion",
        "L5": "Identity and threat intelligence",
    }
    return {
        layer.value: {
            "description": descriptions[layer.value],
            "evaluated": 0,
            "triggered": 0,
            "skipped": 0,
            "trigger_count": 0,
            "detectors": {},
            "skipped_reasons": {},
        }
        for layer in DetectionLayer
    }


@app.get("/api/layers")
async def get_layer_summary():
    """Return evaluated/skipped/triggered counts for every canonical layer."""
    layers = _empty_layer_summary()
    vectors = await _load_state_vectors()
    now = time.time()

    for sv in vectors:
        for evaluation in sv.layer_evaluations.values():
            bucket = layers[evaluation.layer.value]
            if (
                evaluation.layer == DetectionLayer.L4
                and evaluation.status == LayerStatus.TRIGGERED
                and now - evaluation.timestamp > settings.FUSION_TRIGGER_TTL_SEC
            ):
                bucket["skipped"] += 1
                reason = "L4 trigger evidence expired"
                bucket["skipped_reasons"][reason] = (
                    bucket["skipped_reasons"].get(reason, 0) + 1
                )
                continue
            if evaluation.status == LayerStatus.SKIPPED:
                bucket["skipped"] += 1
                reason = evaluation.skipped_reason or "Unspecified"
                bucket["skipped_reasons"][reason] = (
                    bucket["skipped_reasons"].get(reason, 0) + 1
                )
            else:
                bucket["evaluated"] += 1
            if evaluation.status == LayerStatus.TRIGGERED:
                bucket["triggered"] += 1

        for flag in sv.anomalies:
            if (
                flag.layer == DetectionLayer.L4
                and now - flag.timestamp > settings.FUSION_TRIGGER_TTL_SEC
            ):
                continue
            bucket = layers[flag.layer.value]
            bucket["trigger_count"] += 1
            bucket["detectors"][flag.detector] = bucket["detectors"].get(flag.detector, 0) + 1

    # L1 runs in the API cross-source snapshot rather than the Kafka anomaly service.
    for aircraft in _track_snapshot:
        result = aircraft.get("l1")
        if not result:
            continue
        layers["L1"]["evaluated"] += 1
        if result.get("verdict") == "SPOOFED":
            layers["L1"]["triggered"] += 1
            layers["L1"]["trigger_count"] += 1
            layers["L1"]["detectors"]["cross_source_position"] = (
                layers["L1"]["detectors"].get("cross_source_position", 0) + 1
            )

    return {"timestamp": time.time(), "tracks": len(vectors), "layers": layers}


@app.get("/api/layers/{layer}/triggers")
async def get_layer_triggers(layer: str, limit: int = Query(100, ge=1, le=1000)):
    """Return recent trigger evidence for one canonical detection layer."""
    try:
        selected = DetectionLayer(layer.upper())
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=f"Unknown layer: {layer}") from exc

    triggers = []
    now = time.time()
    for sv in await _load_state_vectors():
        for flag in sv.anomalies:
            if flag.layer != selected:
                continue
            if (
                flag.layer == DetectionLayer.L4
                and now - flag.timestamp > settings.FUSION_TRIGGER_TTL_SEC
            ):
                continue
            triggers.append({
                "aircraft_id": sv.icao24,
                "callsign": sv.callsign,
                "layer": flag.layer.value,
                "detector": flag.detector,
                "type": flag.anomaly_type.value,
                "score_delta": flag.score_delta,
                "description": flag.description,
                "evidence": flag.meta,
                "timestamp": flag.timestamp,
            })

    if selected == DetectionLayer.L1:
        for aircraft in _track_snapshot:
            result = aircraft.get("l1") or {}
            if result.get("verdict") != "SPOOFED":
                continue
            triggers.append({
                "aircraft_id": aircraft.get("icao"),
                "callsign": aircraft.get("cs"),
                "layer": "L1",
                "detector": "cross_source_position",
                "type": "POSITION_DISAGREEMENT",
                "score_delta": 80,
                "description": "Independent source positions disagree",
                "evidence": result,
                "timestamp": aircraft.get("ts", time.time()),
            })

    triggers.sort(key=lambda item: item["timestamp"], reverse=True)
    return {"layer": selected.value, "count": len(triggers), "triggers": triggers[:limit]}


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
    dependencies = {"redis": "error", "postgres": "error", "kafka": "error"}
    try:
        if redis_client is None:
            raise ConnectionError("Redis client is not initialized")
        await redis_client.ping()
        dependencies["redis"] = "ok"

        postgres = await asyncpg.connect(settings.POSTGRES_DSN, timeout=2)
        try:
            await postgres.fetchval("SELECT 1")
            dependencies["postgres"] = "ok"
        finally:
            await postgres.close()

        bootstrap = settings.KAFKA_BOOTSTRAP.split(",", 1)[0]
        kafka_host, kafka_port = bootstrap.rsplit(":", 1)
        _, writer = await asyncio.wait_for(
            asyncio.open_connection(kafka_host, int(kafka_port)),
            timeout=2,
        )
        writer.close()
        await writer.wait_closed()
        dependencies["kafka"] = "ok"
    except Exception:
        detail = {
            "status": "degraded",
            "time": time.time(),
            "dependencies": dependencies,
        }
        raise HTTPException(status_code=503, detail=detail)

    return {
        "status": "ok",
        "time": time.time(),
        "dependencies": dependencies,
        "l1_enabled": TDOA_AVAILABLE,
        "l1_validator_ready": cross_validator is not None,
    }


# ─── WebSocket ────────────────────────────────────────────────────────────────

@app.websocket("/ws/tracks")
async def ws_tracks(websocket: WebSocket):
    if not _websocket_origin_allowed(websocket):
        await websocket.close(code=1008, reason="Origin not allowed")
        return
    await websocket.accept()
    _ws_clients.add(websocket)
    try:
        # Send initial snapshot under the same lock used by coverage updates.
        if redis_client is None:
            raise RuntimeError("Redis unavailable")
        lock = redis_client.lock(COVERAGE_LOCK_KEY, timeout=10, blocking_timeout=5)
        async with lock:
            area = await load_coverage_area(redis_client)
            initial_tracks = [track for track in _track_snapshot if _track_in_coverage(track, area)]
            payload = orjson.dumps({
                "type":     "snapshot",
                "ts":       time.time(),
                "count":    len(initial_tracks),
                "aircraft": initial_tracks,
                "tdoa_enabled": TDOA_AVAILABLE,
            })
            await asyncio.wait_for(websocket.send_bytes(payload), timeout=1.0)

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

async def _publish_track_snapshot(tracks: List[Dict[str, Any]]) -> None:
    """Atomically publish only tracks belonging to the active coverage area."""
    global _track_snapshot
    if redis_client is None:
        return
    lock = redis_client.lock(COVERAGE_LOCK_KEY, timeout=10, blocking_timeout=5)
    async with lock:
        area = await load_coverage_area(redis_client)
        tracks = [track for track in tracks if _track_in_coverage(track, area)]
        _track_snapshot = tracks
        if not _ws_clients:
            return
        payload = orjson.dumps({
            "type": "snapshot",
            "ts": time.time(),
            "count": len(tracks),
            "aircraft": tracks,
            "tdoa_enabled": TDOA_AVAILABLE,
        })
        clients = list(_ws_clients)
        await lock.extend(10, replace_ttl=True)
        results = await asyncio.gather(
            *(asyncio.wait_for(ws.send_bytes(payload), timeout=1.0) for ws in clients),
            return_exceptions=True,
        )
        for ws, result in zip(clients, results):
            if isinstance(result, BaseException):
                _ws_clients.discard(ws)


async def _publish_alert(ac: Dict[str, Any], anomalies: List[Dict[str, Any]]) -> None:
    """Publish an alert while holding the same coverage lock used by PUT."""
    if redis_client is None:
        return
    lock = redis_client.lock(COVERAGE_LOCK_KEY, timeout=10, blocking_timeout=5)
    async with lock:
        area = await load_coverage_area(redis_client)
        if not _track_in_coverage(ac, area):
            return
        payload = orjson.dumps({
            "type": "alert",
            "ts": time.time(),
            "aircraft": ac,
            "anomalies": anomalies,
        })
        clients = list(_ws_clients)
        await lock.extend(10, replace_ttl=True)
        results = await asyncio.gather(
            *(asyncio.wait_for(ws.send_bytes(payload), timeout=1.0) for ws in clients),
            return_exceptions=True,
        )
        for ws, result in zip(clients, results):
            if isinstance(result, BaseException):
                _ws_clients.discard(ws)

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

            area = await load_coverage_area(redis_client)
            tracks = [track for track in tracks if _track_in_coverage(track, area)]

            # L1 cross-validation runs once per broadcast tick on the whole
            # snapshot (internally bounded/cached), not per-track
            if TDOA_AVAILABLE and cross_validator:
                await run_l1_cross_validation(tracks)
            run_l2_l3_detection(tracks)

            await _publish_track_snapshot(tracks)

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
        **KAFKA_CONSUMER_STABILITY,
    )
    await consumer.start()
    try:
        async for msg in consumer:
            if not _ws_clients:
                await consumer.commit()
                continue
            try:
                sv = StateVector.from_bytes(msg.value)
                ac = sv.to_api_dict()
                area = await load_coverage_area(redis_client)
                if not _track_in_coverage(ac, area):
                    await consumer.commit()
                    continue
                
                # Apply L1 cross-validation to this single alert (one item
                # at a time off the Kafka topic, so a direct call is fine —
                # the rate-limit concern is about the full global feed)
                if (TDOA_AVAILABLE and cross_validator
                        and ac.get("lat") is not None and ac.get("lon") is not None):
                    try:
                        l1_result = await cross_validator.validate_aircraft(ac["icao"], ac["lat"], ac["lon"])
                        ac["l1"] = {
                            "validated": l1_result.is_valid,
                            "verdict": l1_result.verdict,
                            "disagreement_m": round(l1_result.max_disagreement_m, 1),
                        }
                    except Exception as e:
                        log.warning(f"L1 validation failed in alert loop: {e}")
                
                await _publish_alert(ac, [a.to_api_dict() for a in sv.anomalies])
                await consumer.commit()
            except Exception as e:
                log.error("Alert push error: %s", e)
                raise
    finally:
        await consumer.stop()
