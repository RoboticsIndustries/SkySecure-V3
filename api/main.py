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
import hashlib
import logging
import math
import re
import secrets
import time
from collections.abc import Mapping
from typing import Optional, List, Dict, Any

import aiohttp
import asyncpg
import orjson
import redis.asyncio as aioredis
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import Depends, FastAPI, Header, HTTPException, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse
from contextlib import asynccontextmanager

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from models import RawADSBMessage, StateVector, RiskBand, Classification, DetectionLayer, LayerStatus, normalize_icao24
from config import settings, KAFKA_CONSUMER_STABILITY
from kafka_offsets import commit_record
from receiver_auth import validate_receiver_credentials as validate_receiver_key_map
from processing.mlat_solver import (
    geodetic_to_ecef, physical_reception_key, receiver_geometry_is_safe,
    validate_receiver_configuration,
)
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
    from processing.cross_source_validator import (
        CrossSourceValidator,
        DISAGREEMENT_SPOOFED_M,
        DISAGREEMENT_UNCERTAIN_M,
    )
    from anomaly.enhanced_detector import EnhancedAnomalyDetector
    TDOA_AVAILABLE = True
except ImportError:
    DISAGREEMENT_UNCERTAIN_M = 1500.0
    DISAGREEMENT_SPOOFED_M = 5000.0
    TDOA_AVAILABLE = False
    logging.warning("L1 cross-validation module not available - running without it")

log = logging.getLogger(__name__)

# ─── Global state ──────────────────────────────────────────────────────────────

redis_client: Optional[aioredis.Redis] = None
mlat_reception_producer: Optional[AIOKafkaProducer] = None
_ws_clients: set[WebSocket] = set()
_track_snapshot: List[Dict[str, Any]] = []
_SCAN_CURSORS: Dict[str, int] = {}
_SCAN_SNAPSHOTS: Dict[str, Dict[Any, None]] = {}
_SCAN_BUILDING: Dict[str, Dict[Any, None]] = {}
_SCAN_LOCKS: Dict[str, asyncio.Lock] = {}


async def scan_key_batch(
    pattern: str, *, count: int = 500, max_batches: int = 4, max_keys: int = 10_000
):
    """Build and publish bounded full-cycle Redis key snapshots via SCAN."""
    if redis_client is None:
        return []
    lock = _SCAN_LOCKS.setdefault(pattern, asyncio.Lock())
    async with lock:
        cursor = _SCAN_CURSORS.get(pattern, 0)
        building = _SCAN_BUILDING.setdefault(pattern, {})
        for _ in range(max_batches):
            cursor, batch = await redis_client.scan(
                cursor, match=pattern, count=count
            )
            for key in batch:
                if len(building) < max_keys:
                    building[key] = None
            if cursor == 0:
                _SCAN_SNAPSHOTS[pattern] = building
                _SCAN_BUILDING[pattern] = {}
                break
        _SCAN_CURSORS[pattern] = int(cursor)
        snapshot = _SCAN_SNAPSHOTS.get(pattern) or building
        return list(snapshot)[:max_keys]

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
    """Normalize adsb.lol records without inventing source event timestamps."""
    aircraft = []
    received_at = time.time()
    for raw in data.get("ac") or []:
        if not isinstance(raw, dict):
            continue
        icao = _l1_normalized_icao(str(raw.get("hex") or "").upper().lstrip("~"))
        lat, lon = raw.get("lat"), raw.get("lon")
        if icao is None or lat is None or lon is None:
            continue
        try:
            lat, lon = float(lat), float(lon)
        except (TypeError, ValueError):
            continue
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            continue

        def number(value, *, integer=False):
            return _safe_public_number(value, integer=integer)

        altitude = number(raw.get("alt_baro"), integer=True)
        record = {
            "icao": icao,
            "cs": _safe_public_callsign(raw.get("flight") or raw.get("r")),
            "lat": lat, "lon": lon, "alt": altitude,
            "vel": number(raw.get("gs"), integer=True),
            "hdg": number(raw.get("track")),
            "vr": number(raw.get("baro_rate"), integer=True),
            "nic": number(raw.get("nic"), integer=True),
            "nac_p": number(raw.get("nac_p"), integer=True),
            "gnd": raw.get("alt_baro") == "ground", "src": "adsb_lol",
            "risk": 0, "anoms": [], "cls": "CIVILIAN", "conf": 0.75,
            "mil": 0.0, "band": "NORMAL", "trail": [],
        }
        seen_pos = _safe_public_number(raw.get("seen_pos"), minimum=0.0)
        if seen_pos is not None:
            record["ts"] = received_at - seen_pos
        aircraft.append(record)
    return aircraft


# ─── L1 Helper Functions (real, live-data cross-validation) ──────────────────

# Per-claim result cache so we don't re-query the same aircraft every
# broadcast tick. External free-tier APIs will rate-limit/ban aggressive
# per-aircraft polling, so this is not optional.
_L1_CACHE: Dict[tuple[str, str], tuple] = {}
_L1_CACHE_TTL = 60  # seconds
_L1_ALLOWED_SOURCES = {"opensky", "adsb_lol", "adsb_fi"}
_L1_ALLOWED_VERDICTS = {
    "LEGITIMATE", "UNCERTAIN", "SPOOFED", "INSUFFICIENT_SOURCES",
}
# Hard cap on live cross-validation calls per broadcast cycle. The global
# feed carries thousands of aircraft; adsb.lol/adsb.fi/OpenSky cannot take
# a per-aircraft hit at that volume. Bump this only if you have paid/higher
# rate limit tiers.
_L1_MAX_PER_CYCLE = 20


def _l1_cache_key(
    ac: dict,
    now: Optional[float] = None,
) -> tuple[str, str]:
    """Bind a verdict to a source claim without defeating its insertion TTL.

    Validated coordinates are stored in the cache entry and a discontinuous
    position jump invalidates it early. Normal motion remains covered by the
    insertion TTL instead of creating a new external request every second.
    ``now`` is retained for call-site compatibility and deterministic tests.
    """
    return (
        str(ac["icao"]).strip().upper(),
        str(ac.get("src") or "unknown").lower(),
    )


def _l1_coordinate(value: Any, *, latitude: bool) -> Optional[float]:
    """Return a finite in-range coordinate, or ``None`` for invalid input."""
    if isinstance(value, bool):
        return None
    try:
        coordinate = float(value)
    except Exception:
        return None
    limit = 90.0 if latitude else 180.0
    if not math.isfinite(coordinate) or not -limit <= coordinate <= limit:
        return None
    return coordinate


def _l1_normalized_icao(value: Any) -> Optional[str]:
    """Normalize a six-hex-character ICAO address."""
    if not isinstance(value, str):
        return None
    normalized = value.strip().upper()
    if len(normalized) != 6 or any(char not in "0123456789ABCDEF" for char in normalized):
        return None
    return normalized


def _safe_public_number(
    value: Any,
    *,
    minimum: Optional[float] = None,
    maximum: Optional[float] = None,
    integer: bool = False,
) -> Optional[float]:
    """Normalize an untrusted optional display number to a finite value."""
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(parsed):
        return None
    if minimum is not None:
        parsed = max(minimum, parsed)
    if maximum is not None:
        parsed = min(maximum, parsed)
    return int(parsed) if integer else parsed


def _safe_public_callsign(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    normalized = value.strip().upper()
    allowed = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 -")
    if not 1 <= len(normalized) <= 16 or any(char not in allowed for char in normalized):
        return None
    return normalized


def _raw_claim_matches_redis_key(key_text: str, claim: Any) -> bool:
    """Validate an ``ac:<ICAO>`` key and its untrusted embedded raw claim."""
    if not isinstance(claim, dict) or not isinstance(key_text, str):
        return False
    namespace, separator, suffix = key_text.partition(":")
    key_icao = _l1_normalized_icao(suffix) if separator and namespace == "ac" else None
    claim_icao = _l1_normalized_icao(claim.get("icao"))
    source = claim.get("src")
    return bool(
        key_icao is not None
        and claim_icao == key_icao
        and isinstance(source, str)
        and source.lower() in _L1_ALLOWED_SOURCES
        and _l1_coordinate(claim.get("lat"), latitude=True) is not None
        and _l1_coordinate(claim.get("lon"), latitude=False) is not None
    )


def _decode_redis_track(
    namespace: str,
    key_text: str,
    raw: bytes,
) -> Optional[tuple[str, dict]]:
    """Decode one Redis record without allowing namespace or key bypasses."""
    key_namespace, separator, _ = key_text.partition(":")
    if not separator or key_namespace != namespace:
        return None
    if namespace == "ac":
        try:
            claim = orjson.loads(raw)
        except Exception:
            return None
        if not _raw_claim_matches_redis_key(key_text, claim):
            return None
        normalized = {
            "icao": _l1_normalized_icao(claim.get("icao")),
            "src": str(claim.get("src")).lower(),
            "lat": float(claim["lat"]),
            "lon": float(claim["lon"]),
            "cs": _safe_public_callsign(claim.get("cs")),
            "alt": _safe_public_number(claim.get("alt"), integer=True),
            "vel": _safe_public_number(claim.get("vel")),
            "hdg": _safe_public_number(claim.get("hdg"), minimum=0.0, maximum=360.0),
            "vr": _safe_public_number(claim.get("vr"), integer=True),
            "nic": _safe_public_number(claim.get("nic"), minimum=0.0, maximum=15.0, integer=True),
            "nac_p": _safe_public_number(claim.get("nac_p"), minimum=0.0, maximum=15.0, integer=True),
            "gnd": claim.get("gnd") is True,
            "cls": claim.get("cls") if claim.get("cls") in {
                "CIVILIAN", "LIKELY_MILITARY", "CONFIRMED_MILITARY",
                "DARK_AIRCRAFT", "UNKNOWN",
            } else "UNKNOWN",
            "conf": _safe_public_number(claim.get("conf"), minimum=0.0, maximum=1.0) or 0.0,
            "mil": _safe_public_number(claim.get("mil"), minimum=0.0, maximum=1.0) or 0.0,
            "band": claim.get("band") if claim.get("band") in {
                "NORMAL", "ELEVATED", "HIGH", "CRITICAL",
            } else "NORMAL",
            "trail": [],
        }
        try:
            risk = float(claim.get("risk", 0.0))
            if not math.isfinite(risk):
                raise ValueError
        except (TypeError, ValueError):
            risk = 0.0
        normalized["risk"] = min(100.0, max(0.0, risk))
        raw_anomalies = claim.get("anoms", [])
        safe_anomalies = []
        if isinstance(raw_anomalies, list):
            allowed_chars = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_:-")
            for item in raw_anomalies[:32]:
                anomaly_type = item.get("type") if isinstance(item, dict) else item
                if not isinstance(anomaly_type, str):
                    continue
                anomaly_type = anomaly_type.strip().upper()
                if (
                    1 <= len(anomaly_type) <= 64
                    and all(char in allowed_chars for char in anomaly_type)
                ):
                    safe_anomalies.append(anomaly_type)
        normalized["anoms"] = safe_anomalies
        try:
            timestamp = float(claim["ts"])
            if not math.isfinite(timestamp) or timestamp < 0:
                raise ValueError
            normalized["ts"] = timestamp
        except (KeyError, TypeError, ValueError):
            normalized.pop("ts", None)
        return namespace, normalized

    if namespace == "sv":
        try:
            state = StateVector.from_bytes(raw)
            aircraft = state.to_api_dict()
        except Exception:
            return None
        _, separator, suffix = key_text.partition(":")
        key_icao = _l1_normalized_icao(suffix) if separator else None
        state_icao = _l1_normalized_icao(aircraft.get("icao"))
        if (
            key_icao is None
            or state_icao != key_icao
            or _l1_coordinate(aircraft.get("lat"), latitude=True) is None
            or _l1_coordinate(aircraft.get("lon"), latitude=False) is None
        ):
            return None
        return namespace, aircraft
    return None


def _l1_projection_fingerprint(ac: Mapping[str, Any]) -> tuple[Optional[float], ...]:
    """Normalize every claim input used by time-projected L1 validation."""
    values = []
    for field in ("ts", "vel", "hdg"):
        try:
            value = float(ac[field])
            values.append(value if math.isfinite(value) else None)
        except (KeyError, TypeError, ValueError):
            values.append(None)
    return tuple(values)


def _l1_cache_entry_valid(
    cached: Any,
    now: float,
    expected_icao: Optional[str] = None,
    expected_source: Optional[str] = None,
) -> bool:
    """Validate the complete safety-relevant cache representation."""
    if not isinstance(cached, tuple) or len(cached) != 5:
        return False
    timestamp, result, cached_lat, cached_lon, raw_disagreement = cached
    if any(isinstance(value, bool) for value in (timestamp, now, raw_disagreement)):
        return False
    try:
        timestamp = float(timestamp)
        now = float(now)
        raw_disagreement = float(raw_disagreement)
    except Exception:
        return False
    if not isinstance(result, Mapping):
        return False
    try:
        display_value = result["max_disagreement_m"]
        confidence_value = result["confidence"]
        if isinstance(display_value, bool) or isinstance(confidence_value, bool):
            return False
        display_disagreement = float(display_value)
        confidence = float(confidence_value)
        is_valid = result["is_valid"]
        verdict = result["verdict"]
        sources = result["sources_used"]
        result_icao = _l1_normalized_icao(result["icao"])
    except Exception:
        return False
    age = now - timestamp
    if (
        not math.isfinite(now)
        or not math.isfinite(timestamp)
        or not math.isfinite(age)
        or age < 0.0
        or age > _L1_CACHE_TTL
        or not math.isfinite(display_disagreement)
        or display_disagreement < 0.0
        or not math.isfinite(confidence)
        or not 0.0 <= confidence <= 1.0
        or not isinstance(is_valid, bool)
        or not isinstance(verdict, str)
        or verdict not in _L1_ALLOWED_VERDICTS
        or not isinstance(sources, (list, tuple))
        or not all(isinstance(source, str) for source in sources)
        or not math.isfinite(raw_disagreement)
        or raw_disagreement < 0.0
        or _l1_coordinate(cached_lat, latitude=True) is None
        or _l1_coordinate(cached_lon, latitude=False) is None
        or result_icao is None
        or (expected_icao is not None and result_icao != _l1_normalized_icao(expected_icao))
        or display_disagreement != round(raw_disagreement, 1)
        or is_valid != (verdict == "LEGITIMATE")
    ):
        return False
    if verdict == "INSUFFICIENT_SOURCES":
        return raw_disagreement == 0.0 and not sources
    normalized_sources = [source.lower() for source in sources]
    normalized_expected_source = (
        expected_source.lower() if isinstance(expected_source, str) else None
    )
    if (
        not sources
        or any(source not in _L1_ALLOWED_SOURCES for source in normalized_sources)
        or normalized_expected_source in normalized_sources
    ):
        return False
    expected_verdict = (
        "LEGITIMATE"
        if raw_disagreement < DISAGREEMENT_UNCERTAIN_M
        else "UNCERTAIN"
        if raw_disagreement < DISAGREEMENT_SPOOFED_M
        else "SPOOFED"
    )
    if verdict != expected_verdict:
        return False
    return True


def _l1_claim_displacement_m(ac: dict, cached: tuple) -> float:
    """Great-circle distance from the coordinates validated in a cache entry."""
    current_lat = _l1_coordinate(ac.get("lat"), latitude=True)
    current_lon = _l1_coordinate(ac.get("lon"), latitude=False)
    cached_lat = _l1_coordinate(cached[2], latitude=True)
    cached_lon = _l1_coordinate(cached[3], latitude=False)
    if None in (current_lat, current_lon, cached_lat, cached_lon):
        return float("inf")
    assert current_lat is not None and current_lon is not None
    assert cached_lat is not None and cached_lon is not None
    lat1, lon1 = map(math.radians, (current_lat, current_lon))
    lat2, lon2 = map(math.radians, (cached_lat, cached_lon))
    dlat, dlon = lat2 - lat1, lon2 - lon1
    value = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 6_371_000 * 2 * math.atan2(math.sqrt(value), math.sqrt(max(0.0, 1 - value)))


def _l1_cache_is_fresh(ac: dict, now: float) -> bool:
    key = _l1_cache_key(ac, now)
    cached = _L1_CACHE.get(key)
    if (
        _l1_coordinate(ac.get("lat"), latitude=True) is None
        or _l1_coordinate(ac.get("lon"), latitude=False) is None
        or not _l1_cache_entry_valid(cached, now, key[0], key[1])
    ):
        _L1_CACHE.pop(key, None)
        return False
    assert isinstance(cached, tuple)
    cached_fingerprint = cached[1].get("_claim_fingerprint")
    current_fingerprint = _l1_projection_fingerprint(ac)
    if cached_fingerprint is None:
        # Legacy/test entries are reusable only when no projection inputs exist.
        if any(value is not None for value in current_fingerprint):
            _L1_CACHE.pop(key, None)
            return False
    elif (
        not isinstance(cached_fingerprint, (list, tuple))
        or tuple(cached_fingerprint) != current_fingerprint
    ):
        _L1_CACHE.pop(key, None)
        return False
    try:
        disagreement = float(cached[4])
    except Exception:
        _L1_CACHE.pop(key, None)
        return False
    # By the triangle inequality, moving the claim by m can change its maximum
    # disagreement by at most m. Reuse is safe only while movement is strictly
    # below the nearest classification boundary. At exactly 1500 m or 5000 m
    # the margin is zero, so even an unchanged claim is conservatively checked.
    margin = min(
        abs(disagreement - DISAGREEMENT_UNCERTAIN_M),
        abs(disagreement - DISAGREEMENT_SPOOFED_M),
    )
    return _l1_claim_displacement_m(ac, cached) < margin


def _select_l1_candidates(
    aircraft_list: List[dict],
    now: Optional[float] = None,
) -> List[dict]:
    """Pick which aircraft get a live cross-validation check this cycle:
    anything already flagged risky by other layers first, then fill the
    remaining budget so coverage rotates rather than always hitting the
    same first N aircraft in the list."""
    now = time.time() if now is None else now
    fresh_candidates = [
        ac for ac in aircraft_list
        if ac.get("icao") and ac.get("lat") is not None and ac.get("lon") is not None
        and str(ac.get("src") or "").lower() in _L1_ALLOWED_SOURCES
        and not _l1_cache_is_fresh(ac, now)
    ]
    def safe_risk(ac: dict) -> float:
        try:
            risk = float(ac.get("risk", 0.0))
            return risk if math.isfinite(risk) else 0.0
        except (TypeError, ValueError):
            return 0.0

    fresh_candidates.sort(key=safe_risk, reverse=True)
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
    for key, cached in list(_L1_CACHE.items()):
        if not _l1_cache_entry_valid(cached, now, key[0], key[1]):
            _L1_CACHE.pop(key, None)
    candidates = _select_l1_candidates(aircraft_list, now)
    validated_keys = set()

    if candidates:
        results = await asyncio.gather(
            *[
                cross_validator.validate_aircraft(
                    ac["icao"], ac["lat"], ac["lon"],
                    claimed_velocity_kts=ac.get("vel"),
                    claimed_heading_deg=ac.get("hdg"),
                    claimed_observed_at=ac.get("ts"),
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
            key = _l1_cache_key(ac, now)
            cached_result = result.to_dict()
            cached_result["_claim_fingerprint"] = list(
                _l1_projection_fingerprint(ac)
            )
            candidate_entry = (
                now,
                cached_result,
                float(ac["lat"]),
                float(ac["lon"]),
                float(result.max_disagreement_m),
            )
            if not _l1_cache_entry_valid(candidate_entry, now, key[0], key[1]):
                log.warning("Rejected inconsistent L1 result for %s", ac.get("icao"))
                _L1_CACHE.pop(key, None)
                continue
            _L1_CACHE[key] = candidate_entry
            validated_keys.add(key)

    # Apply cache (fresh this cycle or still within TTL) to every aircraft
    for ac in aircraft_list:
        key = _l1_cache_key(ac, now)
        cached = _L1_CACHE.get(key)
        if cached is None:
            ac.pop("l1", None)
            continue
        # A result produced this cycle is authoritative even when it lies
        # exactly on a boundary. It must simply never be reused next cycle.
        if key not in validated_keys and not _l1_cache_is_fresh(ac, now):
            ac.pop("l1", None)
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
            anomalies = ac.setdefault("anoms", [])
            if not any(
                isinstance(item, dict) and item.get("type") == "L1_POSITION_DISAGREEMENT"
                for item in anomalies
            ):
                anomalies.append({
                    "type": "L1_POSITION_DISAGREEMENT",
                    "description": f"Aggregator reports disagree by {result['max_disagreement_m']:.0f}m "
                                   f"({'/'.join(result['sources_used'])})",
                })


async def _validate_raw_l1_claim(icao: str) -> Optional[dict]:
    """Validate the originating raw aggregator claim, never a canonical fusion output."""
    requested_icao = _l1_normalized_icao(icao)
    if redis_client is None or cross_validator is None or requested_icao is None:
        return None
    raw = await redis_client.get(f"ac:{requested_icao}")
    if not raw:
        return None
    try:
        claim = orjson.loads(raw)
    except Exception:
        return None
    if not isinstance(claim, dict):
        return None
    claim_icao = _l1_normalized_icao(claim.get("icao"))
    source = claim.get("src")
    lat = _l1_coordinate(claim.get("lat"), latitude=True)
    lon = _l1_coordinate(claim.get("lon"), latitude=False)
    if (
        claim_icao != requested_icao
        or not isinstance(source, str)
        or source.lower() not in _L1_ALLOWED_SOURCES
        or lat is None
        or lon is None
    ):
        return None
    claim["icao"] = claim_icao
    claim["src"] = source.lower()
    claim["lat"] = lat
    claim["lon"] = lon
    claims = _select_l1_claim_records([("ac", claim)])
    if not claims:
        return None
    await run_l1_cross_validation(claims)
    return claims[0].get("l1")


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
        # Canonical state vectors already carry L2/L3 output from the anomaly
        # service. Re-sampling them in every WebSocket snapshot would distort
        # stateful histories; only raw live-feed records are assessed here.
        if ac.get("layer_evaluations"):
            continue
        cached_l1 = _L1_CACHE.get(_l1_cache_key(ac, now))
        l1_result = None
        if cached_l1 and _l1_cache_is_fresh(ac, now):
            l1_result = cached_l1[1]
        observed_at = ac.get("last_seen", ac.get("obs_ts", ac.get("ts")))
        try:
            observed_at = float(observed_at)
            if not math.isfinite(observed_at) or observed_at < 0:
                raise ValueError
        except (TypeError, ValueError):
            log.warning("Skipping timestamp-less raw observation for %s", icao)
            continue
        try:
            assessment = anomaly_detector.assess(
                icao=icao, lat=ac["lat"], lon=ac["lon"],
                alt_baro=ac.get("alt"), alt_geo=ac.get("alt_geo"),
                velocity=ac.get("vel"), vertical_rate=ac.get("vr"),
                heading=ac.get("hdg"), nic=ac.get("nic"),
                nac_p=ac.get("nac_p"), l1_result=l1_result,
                observed_at=observed_at,
                observation_sequence=int(ac.get("update_count") or 0),
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
    global redis_client, mlat_reception_producer, cross_validator, anomaly_detector

    tasks: List[asyncio.Task] = []
    validator = None
    reception_producer = None
    body_error: Optional[BaseException] = None
    critical_errors: List[BaseException] = []
    shutdown_started = False
    owner_task = asyncio.current_task()
    redis_client = None
    mlat_reception_producer = None
    cross_validator = None
    anomaly_detector = None
    try:
        validate_receiver_configuration()
        redis_client = aioredis.from_url(settings.REDIS_URL, decode_responses=False)
        reception_producer = AIOKafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP,
            compression_type="lz4",
        )
        await reception_producer.start()
        mlat_reception_producer = reception_producer

        # Initialize L1 cross-source validator (real live-network comparison,
        # not simulated TDOA — see processing/cross_source_validator.py)
        if TDOA_AVAILABLE:
            try:
                validator = CrossSourceValidator()
                cross_validator = await validator.__aenter__()
                anomaly_detector = EnhancedAnomalyDetector()
                log.info("✅ L1 cross-source validator initialized (OpenSky/adsb.lol/adsb.fi)")
            except Exception as e:
                log.error("Failed to initialize L1 cross-validator: %s", e)
                cross_validator = None
                anomaly_detector = None

        for background_loop in (broadcast_loop, alert_consumer_loop):
            coroutine = background_loop()
            try:
                task = asyncio.create_task(coroutine)
                tasks.append(task)

                def surface_critical_exit(done_task: asyncio.Task) -> None:
                    if shutdown_started or done_task.cancelled():
                        return
                    try:
                        error = done_task.exception()
                    except BaseException as exc:
                        error = exc
                    if error is None:
                        error = RuntimeError("critical background task exited unexpectedly")
                    critical_errors.append(error)
                    if owner_task is not None and not owner_task.done():
                        owner_task.cancel(str(error))

                task.add_done_callback(surface_critical_exit)
            except BaseException:
                coroutine.close()
                raise
        try:
            yield
        except BaseException as exc:
            body_error = exc
            raise
    finally:
        shutdown_started = True
        for task in tasks:
            if not task.done():
                task.cancel()
        results = await asyncio.gather(*tasks, return_exceptions=True) if tasks else []

        critical_error = critical_errors[0] if critical_errors else None
        for result in results:
            if not isinstance(result, asyncio.CancelledError):
                if critical_error is None:
                    critical_error = (
                        result
                        if isinstance(result, BaseException)
                        else RuntimeError("critical background task exited unexpectedly")
                    )
                break

        # Background work must no longer be able to touch these resources.
        cleanup_error = None
        try:
            if reception_producer is not None:
                await reception_producer.stop()
        except BaseException as exc:
            cleanup_error = exc
        try:
            if redis_client is not None:
                await redis_client.close()
        except BaseException as exc:
            cleanup_error = exc
        try:
            if validator is not None:
                await validator.__aexit__(None, None, None)
        except BaseException as exc:
            if cleanup_error is None:
                cleanup_error = exc
        redis_client = None
        mlat_reception_producer = None
        cross_validator = None
        anomaly_detector = None
        if body_error is None:
            if critical_error is not None:
                raise critical_error
            if cleanup_error is not None:
                raise cleanup_error


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
                states = data.get("states") or [] if isinstance(data, dict) else []
                if not isinstance(states, list):
                    states = []

        aircraft = []
        for s in states:
            if not isinstance(s, (list, tuple)) or len(s) < 12:
                continue
            if not isinstance(s[0], str) or s[5] is None or s[6] is None:
                continue
            try:
                lat, lon = float(s[6]), float(s[5])
            except (TypeError, ValueError):
                continue
            if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                continue
            if not within_coverage_area(lat, lon, area):
                continue
            icao = _l1_normalized_icao(s[0].upper().strip())
            if icao is None:
                continue

            altitude_m = _safe_public_number(s[7])
            velocity_ms = _safe_public_number(s[9])
            timestamp = _safe_public_number(s[4])
            if timestamp is None:
                timestamp = _safe_public_number(s[3])

            ac = {
                "icao": icao,
                "cs":   _safe_public_callsign(s[1]),
                "lat":  lat, "lon": lon,
                "alt":  int(altitude_m * 3.28084) if altitude_m is not None else None,
                "vel":  int(velocity_ms * 1.944) if velocity_ms is not None else None,
                "hdg":  _safe_public_number(s[10], minimum=0.0, maximum=360.0),
                "vr":   (int(vertical_rate * 196.85)
                         if (vertical_rate := _safe_public_number(s[11])) is not None else None),
                "gnd":  bool(s[8]), "src": "opensky",
                "risk": 0, "anoms": [], "cls": "CIVILIAN",
                "conf": 0.85, "mil": 0.0, "band": "NORMAL", "trail": [],
            }
            if timestamp is not None:
                ac["ts"] = timestamp
            
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

L1_MANUAL_RATE_KEY = "l1:manual:rate"


async def require_operator(
    operator_key: Optional[str] = Header(default=None, alias="X-SkySecure-Operator-Key"),
) -> None:
    configured = settings.OPERATOR_API_KEY
    if not configured:
        raise HTTPException(status_code=503, detail="Operator API access is not configured")
    if operator_key is None or not secrets.compare_digest(operator_key, configured):
        raise HTTPException(status_code=403, detail="Operator authorization required")


def validate_receiver_credentials() -> Dict[str, str]:
    return validate_receiver_key_map(
        settings.MLAT_RECEIVER_LOCATIONS, settings.MLAT_RECEIVER_API_KEYS
    )


async def require_mlat_receiver(
    receiver_key: Optional[str] = Header(default=None, alias="X-SkySecure-Receiver-Key"),
) -> str:
    try:
        configured = validate_receiver_credentials()
    except ValueError:
        raise HTTPException(status_code=503, detail="Physical receiver access is not configured")
    if receiver_key is not None:
        for receiver_id, expected in configured.items():
            if expected and secrets.compare_digest(receiver_key, expected):
                return receiver_id
    raise HTTPException(status_code=403, detail="Physical receiver authorization required")


@app.post("/api/mlat/receptions")
async def ingest_mlat_reception(
    reception: RawADSBMessage,
    authenticated_receiver_id: str = Depends(require_mlat_receiver),
):
    if reception.receiver_id != authenticated_receiver_id:
        raise HTTPException(status_code=403, detail="Receiver identity does not match credential")
    if reception.receiver_id not in settings.MLAT_RECEIVER_LOCATIONS:
        raise HTTPException(status_code=422, detail="Unknown physical receiver identity")
    if (
        reception.msg_type not in (11, 17, 18, 20, 21)
        or re.fullmatch(r"(?:[0-9A-F]{14}|[0-9A-F]{28})", reception.raw_message) is None
    ):
        raise HTTPException(status_code=422, detail="Unsupported physical Mode-S reception")
    now = time.time()
    if (
        reception.recv_time < now - settings.SOURCE_EVENT_MAX_AGE_SEC
        or reception.recv_time > now + settings.SOURCE_EVENT_FUTURE_SKEW_SEC
    ):
        raise HTTPException(status_code=422, detail="Reception timestamp is stale or future-dated")
    if mlat_reception_producer is None:
        raise HTTPException(status_code=503, detail="Physical receiver publisher is not initialized")
    if redis_client is None:
        raise HTTPException(status_code=503, detail="Redis unavailable")
    reception_bytes = reception.to_bytes()
    receiver_secret = validate_receiver_credentials()[authenticated_receiver_id]
    await mlat_reception_producer.send_and_wait(
        topic=settings.TOPIC_MLAT_RECEPTIONS,
        key=physical_reception_key(reception, receiver_secret),
        value=reception_bytes,
    )
    await redis_client.setex(
        f"mlat:receiver:last_seen:{reception.receiver_id}",
        settings.RECEIVER_TIMEOUT_SEC * 2,
        str(reception.recv_time).encode(),
    )
    return {"status": "accepted"}

@app.get("/api/coverage")
async def get_coverage_area_config():
    area = await load_coverage_area(redis_client) if redis_client else default_coverage_area()
    return {"coverage": area.model_dump()}


@app.put("/api/coverage", dependencies=[Depends(require_operator)])
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
    keys = await scan_key_batch("sv:*")
    results = []
    area = await load_coverage_area(redis_client)

    if keys:
        pipe = redis_client.pipeline()
        for k in keys:
            pipe.get(k)
        raw_values = await pipe.execute()

        for key, raw in zip(keys, raw_values):
            if not raw:
                continue
            try:
                sv = StateVector.from_bytes(raw)
                key_text = key.decode() if isinstance(key, bytes) else str(key)
                if sv.icao24 != normalize_icao24(key_text.removeprefix("sv:")):
                    continue
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
                    if TDOA_AVAILABLE and cross_validator:
                        try:
                            l1 = await _validate_raw_l1_claim(sv.icao24)
                            if l1:
                                alert["l1"] = l1
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
        keys = await scan_key_batch("sv:*")
        total = 0
        area = await load_coverage_area(redis_client)
        classifications = {c.value: 0 for c in Classification}
        risk_bands = {b.value: 0 for b in RiskBand}
        tdoa_stats = {"validated": 0, "spoofed": 0, "uncertain": 0}

        if keys:
            pipe = redis_client.pipeline()
            for key in keys:
                pipe.get(key)
            for key, raw in zip(keys, await pipe.execute()):
                if not raw:
                    continue
                try:
                    sv = StateVector.from_bytes(raw)
                    key_text = key.decode() if isinstance(key, bytes) else str(key)
                    if sv.icao24 != normalize_icao24(key_text.removeprefix("sv:")):
                        continue
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

        keys = await scan_key_batch("sv:*")
        if not keys:
            _layer_vector_cache.update(
                client_id=id(redis_client), ts=now, vectors=[]
            )
            return []
        vectors = []
        for start in range(0, len(keys), 500):
            chunk = keys[start:start + 500]
            pipe = redis_client.pipeline()
            for key in chunk:
                pipe.get(key)
            for key, raw in zip(chunk, await pipe.execute()):
                if not raw:
                    continue
                try:
                    sv = StateVector.from_bytes(raw)
                    key_text = key.decode() if isinstance(key, bytes) else str(key)
                    if sv.icao24 != normalize_icao24(key_text.removeprefix("sv:")):
                        continue
                    vectors.append(sv)
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
        "L3": "Trajectory models and ADS-B NIC/NACp integrity",
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


@app.post("/api/l1/validate", dependencies=[Depends(require_operator)])
async def validate_position_l1(
    icao: str,
    lat: float,
    lon: float,
):
    """Manually cross-validate an aircraft's position against independent live networks."""
    normalized_icao = _l1_normalized_icao(icao)
    if normalized_icao is None:
        raise HTTPException(status_code=422, detail="ICAO must be exactly six hexadecimal characters")
    if not TDOA_AVAILABLE or not cross_validator:
        return {"error": "L1 cross-validation not available"}
    if redis_client is None:
        raise HTTPException(status_code=503, detail="Redis unavailable")
    permitted = await redis_client.set(L1_MANUAL_RATE_KEY, "1", ex=5, nx=True)
    if not permitted:
        raise HTTPException(status_code=429, detail="Manual L1 validation is rate limited")
    
    try:
        result = await cross_validator.validate_aircraft(normalized_icao, lat, lon)
        return {
            "icao": normalized_icao,
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


@app.get("/api/mlat/readiness")
async def mlat_readiness():
    if redis_client is None or mlat_reception_producer is None:
        raise HTTPException(status_code=503, detail="MLAT intake dependencies unavailable")
    try:
        validate_receiver_credentials()
        positions = [
            geodetic_to_ecef(*location)
            for location in settings.MLAT_RECEIVER_LOCATIONS.values()
        ]
    except (TypeError, ValueError):
        raise HTTPException(status_code=503, detail="Invalid MLAT receiver configuration")
    if not receiver_geometry_is_safe(positions):
        raise HTTPException(status_code=503, detail="Unsafe MLAT receiver geometry")
    receiver_ids = list(settings.MLAT_RECEIVER_LOCATIONS)
    values = await redis_client.mget([
        f"mlat:receiver:last_seen:{receiver_id}" for receiver_id in receiver_ids
    ])
    now = time.time()
    active = []
    for receiver_id, value in zip(receiver_ids, values):
        try:
            timestamp = float(value)
        except (TypeError, ValueError):
            continue
        if 0 <= now - timestamp <= settings.RECEIVER_TIMEOUT_SEC:
            active.append(receiver_id)
    if len(active) < settings.MLAT_MIN_RECEIVERS:
        raise HTTPException(status_code=503, detail={
            "status": "not_ready",
            "active_receivers": len(active),
            "required_receivers": settings.MLAT_MIN_RECEIVERS,
        })
    return {
        "status": "ready",
        "active_receivers": len(active),
        "required_receivers": settings.MLAT_MIN_RECEIVERS,
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
        if clients and all(isinstance(result, BaseException) for result in results):
            raise RuntimeError("alert delivery failed for every connected client")

def _deduplicate_track_records(records: List[tuple[str, dict]]) -> List[dict]:
    """Return one track per ICAO, preferring canonical fused state vectors."""
    selected: Dict[str, tuple[str, dict]] = {}
    for namespace, track in records:
        icao = str(track.get("icao") or "").upper()
        if not icao:
            continue
        existing = selected.get(icao)
        if existing is None or (namespace == "sv" and existing[0] != "sv"):
            selected[icao] = (namespace, track)
    return [record[1] for record in selected.values()]


def _select_l1_claim_records(records: List[tuple[str, dict]]) -> List[dict]:
    """Keep one raw aggregator claim per ICAO for external L1 validation."""
    selected: Dict[str, dict] = {}
    allowed_sources = {"opensky", "adsb_lol", "adsb_fi"}
    for namespace, track in records:
        icao = str(track.get("icao") or "").upper()
        source = str(track.get("src") or "").lower()
        if (
            namespace != "ac" or not icao or source not in allowed_sources
            or track.get("lat") is None or track.get("lon") is None
        ):
            continue
        existing = selected.get(icao)
        if existing is None or float(track.get("ts") or 0) >= float(existing.get("ts") or 0):
            selected[icao] = track
    return list(selected.values())


def _merge_l1_results(tracks: List[dict], claims: List[dict]) -> None:
    """Transfer raw-claim L1 evidence onto the preferred canonical record."""
    targets = {str(track.get("icao") or "").upper(): track for track in tracks}
    for claim in claims:
        target = targets.get(str(claim.get("icao") or "").upper())
        if target is None or "l1" not in claim:
            continue
        target["l1"] = dict(claim["l1"])
        if claim.get("risk", 0) > target.get("risk", 0):
            target["risk"] = claim["risk"]
            target["band"] = claim.get("band", target.get("band"))
        anomalies = target.setdefault("anoms", [])
        existing_types = {
            item.get("type") if isinstance(item, dict) else item for item in anomalies
        }
        for item in claim.get("anoms", []):
            anomaly_type = item.get("type") if isinstance(item, dict) else item
            if anomaly_type and anomaly_type not in existing_types:
                anomalies.append(anomaly_type)
                existing_types.add(anomaly_type)


async def broadcast_loop() -> None:
    """Broadcast all aircraft with TDOA validation to WebSocket clients."""
    global _track_snapshot

    while True:
        await asyncio.sleep(settings.WS_BROADCAST_INTERVAL)
        try:
            keys = await scan_key_batch("ac:*")
            fused_keys = await scan_key_batch("sv:*")

            all_keys = list(set(keys + fused_keys))
            records: List[tuple[str, dict]] = []

            if all_keys:
                pipe = redis_client.pipeline()
                for k in all_keys:
                    pipe.get(k)
                for key, raw in zip(all_keys, await pipe.execute()):
                    if not raw:
                        continue
                    key_text = key.decode() if isinstance(key, bytes) else str(key)
                    namespace = key_text.split(":", 1)[0]
                    decoded = _decode_redis_track(namespace, key_text, raw)
                    if decoded is None:
                        log.warning("Discarding invalid Redis track %s", key_text)
                        continue
                    records.append(decoded)

            tracks = _deduplicate_track_records(records)
            l1_claims = _select_l1_claim_records(records)

            area = await load_coverage_area(redis_client)
            tracks = [track for track in tracks if _track_in_coverage(track, area)]
            l1_claims = [claim for claim in l1_claims if _track_in_coverage(claim, area)]

            # L1 cross-validation runs once per broadcast tick on the whole
            # snapshot (internally bounded/cached), not per-track
            if TDOA_AVAILABLE and cross_validator:
                await run_l1_cross_validation(l1_claims)
                _merge_l1_results(tracks, l1_claims)
            run_l2_l3_detection(tracks)

            await _publish_track_snapshot(tracks)

        except Exception as e:
            log.error("Broadcast loop error: %s", e)


# ─── Background: alert consumer ───────────────────────────────────────────────

def _alert_event_identity(msg) -> bytes:
    """Derive stable identity from immutable payload bytes, never an untrusted header."""
    value = getattr(msg, "value", b"")
    if not isinstance(value, bytes):
        value = bytes(value)
    return hashlib.sha256(value).hexdigest().encode()


def _alert_effect_keys(msg) -> tuple[str, str]:
    digest = hashlib.sha256(_alert_event_identity(msg)).hexdigest()
    return f"completed:api-alert:{digest}", f"processing:api-alert:{digest}"


async def _reserve_alert_effect(msg, *, timeout: float = 2.0) -> Optional[str]:
    """Atomically acquire a short lease, or return None for a completed replay."""
    if redis_client is None:
        raise RuntimeError("Redis unavailable for alert idempotency")
    completed_key, lease_key = _alert_effect_keys(msg)
    token = secrets.token_hex(16)
    script = """
    if redis.call('EXISTS', KEYS[1]) == 1 then return 2 end
    if redis.call('SET', KEYS[2], ARGV[1], 'NX', 'EX', ARGV[2]) then return 1 end
    return 0
    """
    status = await asyncio.wait_for(
        redis_client.eval(script, 2, completed_key, lease_key, token, 30),
        timeout=timeout,
    )
    if status == 2:
        return None
    if status == 1:
        return token
    raise RuntimeError("alert effect is already being processed")


async def _complete_alert_effect(msg, token: str, *, timeout: float = 2.0) -> None:
    """Atomically mark completion only while this worker still owns the lease."""
    if redis_client is None:
        raise RuntimeError("Redis unavailable for alert idempotency")
    completed_key, lease_key = _alert_effect_keys(msg)
    script = """
    if redis.call('GET', KEYS[2]) ~= ARGV[1] then return 0 end
    redis.call('SET', KEYS[1], '1', 'EX', ARGV[2])
    redis.call('DEL', KEYS[2])
    return 1
    """
    completed = await asyncio.wait_for(
        redis_client.eval(script, 2, completed_key, lease_key, token, 7 * 86_400),
        timeout=timeout,
    )
    if completed != 1:
        raise RuntimeError("lost ownership of alert effect lease")


async def _release_alert_effect(msg, token: str, *, timeout: float = 2.0) -> None:
    """Release an unfinished effect lease without disturbing another owner."""
    if redis_client is None:
        raise RuntimeError("Redis unavailable for alert idempotency")
    _, lease_key = _alert_effect_keys(msg)
    script = """
    if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
    return redis.call('DEL', KEYS[1])
    """
    await asyncio.wait_for(
        redis_client.eval(script, 1, lease_key, token), timeout=timeout,
    )


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
                await commit_record(consumer, msg)
                continue
            try:
                sv = StateVector.from_bytes(msg.value)
            except Exception as exc:
                log.error("Skipping malformed anomaly-alert record: %s", exc)
                await commit_record(consumer, msg)
                continue

            # Retry the same record before consuming a later offset. Committing
            # a later message after skipping this one would also commit past the
            # failed offset on that partition and silently lose the alert.
            for attempt in range(2):
                token = None
                completed = False
                try:
                    ac = sv.to_api_dict()
                    area = await load_coverage_area(redis_client)
                    if not _track_in_coverage(ac, area):
                        await commit_record(consumer, msg)
                        break
                    token = await _reserve_alert_effect(msg)
                    if token is None:
                        await commit_record(consumer, msg)
                        break

                    # Apply L1 cross-validation to this single alert (one item
                    # at a time off the Kafka topic, so a direct call is fine.
                    if TDOA_AVAILABLE and cross_validator:
                        try:
                            l1 = await _validate_raw_l1_claim(ac["icao"])
                            if l1:
                                ac["l1"] = l1
                        except Exception as e:
                            log.warning("L1 validation failed in alert loop: %s", e)

                    await _publish_alert(ac, [a.to_api_dict() for a in sv.anomalies])
                    await _complete_alert_effect(msg, token)
                    completed = True
                    await commit_record(consumer, msg)
                    break
                except BaseException as e:
                    if token is not None and not completed:
                        try:
                            await _release_alert_effect(msg, token)
                        except Exception as release_error:
                            log.error("Alert lease release error: %s", release_error)
                    if not isinstance(e, Exception):
                        raise
                    log.error("Alert push attempt %d failed: %s", attempt + 1, e)
                    if attempt == 1:
                        # Do not advance to another offset. Restarting the
                        # consumer preserves at-least-once replay for this one.
                        raise
                    await asyncio.sleep(0)
    finally:
        await consumer.stop()
