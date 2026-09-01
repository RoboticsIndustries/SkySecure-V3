"""processing/watch_monitor.py
─────────────────────────────
Watch service: transponder-shutoff and extraordinary military-activity detection.

This service is the sole owner of watch analysis. It periodically scans the
fused live state written by the fusion engine (fusion:sv:*) and emits durable,
deduplicated events:

  TRANSPONDER_OFF    — an airborne aircraft vanished from public feeds while
                       enough nearby peers were still being received to prove
                       the feed was alive at that position (coverage-liveness
                       proof). Without peer proof, no event is emitted: an
                       aggregator or coverage dropout is not evidence of a
                       deliberate shutoff. This is fail-closed by design.
  EMERGENCY_SQUAWK   — 7500/7600/7700 observed on a live track.
  MIL_CONCENTRATION  — a sustained cluster of military-scored aircraft.
                       Multi-aircraft, deduplicated, sustained, and recent:
                       a cell emits once when it forms and clears only after
                       the hold window passes with no members.
  MIL_HIGH_PERFORMANCE — a military-scored aircraft flying a profile outside
                       ordinary transport behavior (very fast and low, or an
                       extreme climb/descent rate).

Events are appended to a bounded Redis recent list for the live feed and
inserted into the watch_events PostgreSQL table for recoverable history.
Single-aircraft detections are events; concentrations are the hotspot form.

Honesty contract: TRANSPONDER_OFF means "disappeared under proven-live
coverage" — consistent with a deliberate shutoff, not proof of one. Residual
explanations (descent below receiver line-of-sight, provider filtering) are
recorded in event metadata.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import asyncpg
import redis.asyncio as aioredis

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from models import StateVector
from config import settings
from coverage_area import load_coverage_area, within_coverage_area

log = logging.getLogger(__name__)

# ─── Redis keys ───────────────────────────────────────────────────────────────
LEDGER_KEY = "watch:ledger"                  # hash: icao -> last airborne snapshot
RECENT_EVENTS_KEY = "watch:events:recent"    # list: newest-first JSON events
SNAPSHOT_KEY = "watch:snapshot"              # JSON summary for the API
CONCENTRATION_KEY = "watch:conc:active"      # hash: cell -> active concentration
DEDUP_PREFIX = "watch:dedup"                 # watch:dedup:<kind>:<id> (SETEX)
SV_SCAN_MATCH = "fusion:sv:*"

MAX_VALUE_BYTES = 1_048_576          # skip oversized Redis values (DoS guard)
SCAN_BATCHES_PER_CYCLE = 4
SCAN_COUNT = 500
MAX_TRACKS_PER_CYCLE = 10_000

EMERGENCY_SQUAWKS = {"7500": "HIJACK", "7600": "RADIO FAILURE", "7700": "EMERGENCY"}


def haversine_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 3440.065
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.asin(min(1.0, math.sqrt(a)))


# ─── Event model ──────────────────────────────────────────────────────────────

@dataclass
class WatchEvent:
    kind: str
    icao24: Optional[str]
    callsign: Optional[str]
    severity: int                    # 1 (info) .. 5 (critical)
    summary: str
    lat: Optional[float]
    lon: Optional[float]
    meta: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)

    @property
    def event_id(self) -> str:
        basis = f"{self.kind}|{self.icao24 or '-'}|{int(self.timestamp)}|{self.summary[:64]}"
        return hashlib.sha256(basis.encode()).hexdigest()[:32]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "time": self.timestamp,
            "kind": self.kind,
            "icao24": self.icao24,
            "callsign": self.callsign,
            "severity": self.severity,
            "summary": self.summary,
            "lat": self.lat,
            "lon": self.lon,
            "meta": self.meta,
        }


# ─── Pure detection logic (unit-testable) ─────────────────────────────────────

def snapshot_from_sv(sv: StateVector) -> Dict[str, Any]:
    """Minimal ledger snapshot of a live airborne track."""
    return {
        "icao24": sv.icao24,
        "callsign": sv.callsign,
        "lat": sv.lat,
        "lon": sv.lon,
        "alt": sv.altitude_baro,
        "vel": sv.velocity,
        "hdg": sv.heading,
        "vr": sv.vertical_rate,
        "squawk": sv.squawk,
        "mil": round(sv.military_score, 3),
        "cls": sv.classification.value,
        "last_seen": sv.last_seen,
        "updates": sv.update_count,
    }


def is_disappearance_candidate(entry: Dict[str, Any], now: float) -> bool:
    """A ledger track whose loss could mean a deliberate shutoff.

    Excludes the ordinary explanations: on ground, too low (landing), too slow
    (taxi/park), or too briefly tracked to characterize.
    """
    if entry.get("lat") is None or entry.get("lon") is None:
        return False
    if (entry.get("alt") or 0) < settings.WATCH_MIN_ALT_FT:
        return False
    if (entry.get("vel") or 0) < settings.WATCH_MIN_SPEED_KTS:
        return False
    if (entry.get("updates") or 0) < settings.WATCH_MIN_UPDATES:
        return False
    last_seen = float(entry.get("last_seen") or 0.0)
    return now - last_seen >= settings.WATCH_SILENCE_SEC


def count_live_peers(
    entry: Dict[str, Any], fresh: List[StateVector], now: float
) -> int:
    """Coverage-liveness proof: live aircraft currently received near the loss point."""
    peers = 0
    for sv in fresh:
        if sv.icao24 == entry.get("icao24"):
            continue
        if sv.lat is None or sv.lon is None:
            continue
        if haversine_nm(entry["lat"], entry["lon"], sv.lat, sv.lon) <= settings.WATCH_PEER_RADIUS_NM:
            peers += 1
    return peers


def build_transponder_off_event(
    entry: Dict[str, Any], peers: int, now: float
) -> WatchEvent:
    silence = now - float(entry["last_seen"])
    mil = float(entry.get("mil") or 0.0)
    is_mil = mil >= settings.WATCH_MIL_SCORE_MIN
    ident = entry.get("callsign") or entry.get("icao24") or "UNKNOWN"
    severity = 4 if is_mil else 3
    return WatchEvent(
        kind="TRANSPONDER_OFF",
        icao24=entry.get("icao24"),
        callsign=entry.get("callsign"),
        severity=severity,
        summary=(
            f"{ident} vanished from public feeds for {int(silence)}s at "
            f"{entry.get('alt')} ft while {peers} nearby aircraft were still "
            f"being received — consistent with deliberate transponder shutoff"
        ),
        lat=entry.get("lat"),
        lon=entry.get("lon"),
        timestamp=float(entry["last_seen"]),
        meta={
            "silence_sec": int(silence),
            "last_alt_ft": entry.get("alt"),
            "last_speed_kts": entry.get("vel"),
            "last_heading": entry.get("hdg"),
            "last_vertical_rate": entry.get("vr"),
            "squawk": entry.get("squawk"),
            "military_score": mil,
            "classification": entry.get("cls"),
            "live_peers_within_nm": {"count": peers, "radius_nm": settings.WATCH_PEER_RADIUS_NM},
            "caveats": [
                "descent below receiver line-of-sight",
                "provider-side filtering of the airframe",
            ],
            "confidence": "SUSPECTED",
        },
    )


def build_squawk_event(sv: StateVector, now: float) -> Optional[WatchEvent]:
    if not sv.squawk or sv.squawk not in EMERGENCY_SQUAWKS:
        return None
    meaning = EMERGENCY_SQUAWKS[sv.squawk]
    ident = sv.callsign or sv.icao24
    return WatchEvent(
        kind="EMERGENCY_SQUAWK",
        icao24=sv.icao24,
        callsign=sv.callsign,
        severity=5 if sv.squawk in ("7500", "7700") else 4,
        summary=f"{ident} squawking {sv.squawk} — {meaning}",
        lat=sv.lat,
        lon=sv.lon,
        timestamp=now,
        meta={
            "squawk": sv.squawk,
            "meaning": meaning,
            "alt_ft": sv.altitude_baro,
            "speed_kts": sv.velocity,
            "military_score": round(sv.military_score, 3),
            "confidence": "OBSERVED",
        },
    )


def build_high_performance_event(sv: StateVector, now: float) -> Optional[WatchEvent]:
    """Military-scored aircraft flying well outside ordinary transport profiles."""
    if sv.military_score < settings.WATCH_MIL_SCORE_MIN or sv.on_ground:
        return None
    alt = sv.altitude_baro
    vel = sv.velocity
    vr = sv.vertical_rate
    reasons = []
    if (
        vel is not None and alt is not None
        and vel >= settings.WATCH_HIGH_PERF_SPEED_KTS
        and alt <= settings.WATCH_HIGH_PERF_MAX_ALT_FT
    ):
        reasons.append(f"{int(vel)} kts at {alt} ft")
    if (
        vr is not None and alt is not None
        and abs(vr) >= settings.WATCH_HIGH_PERF_VRATE_FPM
        and alt <= 30_000
    ):
        reasons.append(f"vertical rate {int(vr)} fpm at {alt} ft")
    if not reasons:
        return None
    ident = sv.callsign or sv.icao24
    return WatchEvent(
        kind="MIL_HIGH_PERFORMANCE",
        icao24=sv.icao24,
        callsign=sv.callsign,
        severity=3,
        summary=f"{ident} (mil P={sv.military_score:.2f}) flying unusual profile: {'; '.join(reasons)}",
        lat=sv.lat,
        lon=sv.lon,
        timestamp=now,
        meta={
            "reasons": reasons,
            "military_score": round(sv.military_score, 3),
            "classification": sv.classification.value,
            "alt_ft": alt,
            "speed_kts": vel,
            "vertical_rate": vr,
            "confidence": "OBSERVED",
        },
    )


def cluster_military(
    aircraft: List[StateVector], radius_nm: float
) -> List[List[StateVector]]:
    """Greedy single-linkage clustering of military-scored aircraft."""
    mil = [
        sv for sv in aircraft
        if sv.military_score >= settings.WATCH_MIL_SCORE_MIN
        and sv.lat is not None and sv.lon is not None
    ]
    clusters: List[List[StateVector]] = []
    for sv in mil:
        placed = False
        for cluster in clusters:
            if any(
                haversine_nm(sv.lat, sv.lon, other.lat, other.lon) <= radius_nm
                for other in cluster
            ):
                cluster.append(sv)
                placed = True
                break
        if not placed:
            clusters.append([sv])
    # Merge clusters that became linked through later members.
    merged = True
    while merged:
        merged = False
        for i in range(len(clusters)):
            for j in range(i + 1, len(clusters)):
                if any(
                    haversine_nm(a.lat, a.lon, b.lat, b.lon) <= radius_nm
                    for a in clusters[i] for b in clusters[j]
                ):
                    clusters[i].extend(clusters[j])
                    del clusters[j]
                    merged = True
                    break
            if merged:
                break
    return clusters


def concentration_cell(cluster: List[StateVector]) -> str:
    """Stable cell identity for a cluster (rounded centroid)."""
    clat = sum(sv.lat for sv in cluster) / len(cluster)
    clon = sum(sv.lon for sv in cluster) / len(cluster)
    return f"{clat:.1f},{clon:.1f}"


def build_concentration_event(
    cluster: List[StateVector], cell: str, now: float
) -> WatchEvent:
    clat = sum(sv.lat for sv in cluster) / len(cluster)
    clon = sum(sv.lon for sv in cluster) / len(cluster)
    members = sorted(sv.icao24 for sv in cluster)
    callsigns = sorted({sv.callsign for sv in cluster if sv.callsign})
    return WatchEvent(
        kind="MIL_CONCENTRATION",
        icao24=None,
        callsign=None,
        severity=4 if len(cluster) >= 8 else 3,
        summary=(
            f"{len(cluster)} military aircraft concentrated near "
            f"{clat:.2f},{clon:.2f} ({', '.join(callsigns[:6]) or 'no callsigns'})"
        ),
        lat=clat,
        lon=clon,
        timestamp=now,
        meta={
            "cell": cell,
            "aircraft_count": len(cluster),
            "members": members,
            "callsigns": callsigns,
            "radius_nm": settings.WATCH_MIL_CONCENTRATION_RADIUS_NM,
            "confidence": "OBSERVED",
        },
    )


# ─── Service ──────────────────────────────────────────────────────────────────

class WatchMonitor:
    def __init__(self) -> None:
        self.redis: Optional[aioredis.Redis] = None
        self.pg: Optional[asyncpg.Pool] = None
        self._scan_cursor = 0
        self._scan_building: Dict[bytes, None] = {}

    async def start(self) -> None:
        self.redis = aioredis.from_url(settings.REDIS_URL, decode_responses=False)
        try:
            self.pg = await asyncpg.create_pool(
                settings.POSTGRES_DSN, min_size=1, max_size=2, command_timeout=10,
            )
        except Exception as exc:
            # Live Redis feed still works; history persistence is reported absent.
            log.error("watch_monitor: PostgreSQL unavailable, history disabled: %s", exc)
            self.pg = None

    # ── bounded state-vector loading ──────────────────────────────────────
    async def _load_state_vectors(self) -> List[StateVector]:
        assert self.redis is not None
        keys: List[bytes] = []
        for _ in range(SCAN_BATCHES_PER_CYCLE):
            self._scan_cursor, batch = await self.redis.scan(
                self._scan_cursor, match=SV_SCAN_MATCH, count=SCAN_COUNT,
            )
            for key in batch:
                if len(self._scan_building) < MAX_TRACKS_PER_CYCLE:
                    self._scan_building[key] = None
            if self._scan_cursor == 0:
                keys = list(self._scan_building)
                self._scan_building = {}
                break
        else:
            keys = list(self._scan_building)
        if not keys:
            return []
        vectors: List[StateVector] = []
        for start in range(0, len(keys), 500):
            chunk = keys[start:start + 500]
            pipe = self.redis.pipeline()
            for key in chunk:
                pipe.strlen(key)
            lengths = await pipe.execute()
            eligible = [
                key for key, length in zip(chunk, lengths)
                if isinstance(length, int) and 0 < length <= MAX_VALUE_BYTES
            ]
            if not eligible:
                continue
            pipe = self.redis.pipeline()
            for key in eligible:
                pipe.get(key)
            for raw in await pipe.execute():
                if not raw:
                    continue
                try:
                    vectors.append(StateVector.from_bytes(raw))
                except Exception:
                    continue
        return vectors

    # ── ledger ────────────────────────────────────────────────────────────
    async def _load_ledger(self) -> Dict[str, Dict[str, Any]]:
        assert self.redis is not None
        raw = await self.redis.hgetall(LEDGER_KEY)
        ledger: Dict[str, Dict[str, Any]] = {}
        for field_b, payload in raw.items():
            if len(payload) > MAX_VALUE_BYTES:
                continue
            try:
                entry = json.loads(payload)
                ledger[field_b.decode()] = entry
            except (ValueError, UnicodeDecodeError):
                continue
        return ledger

    async def _save_ledger(self, ledger: Dict[str, Dict[str, Any]], now: float) -> None:
        assert self.redis is not None
        pipe = self.redis.pipeline()
        survivors = 0
        for icao, entry in ledger.items():
            last_seen = float(entry.get("last_seen") or 0.0)
            if now - last_seen > settings.WATCH_LEDGER_TTL_SEC:
                pipe.hdel(LEDGER_KEY, icao)
                continue
            if survivors >= settings.WATCH_LEDGER_MAX:
                pipe.hdel(LEDGER_KEY, icao)
                continue
            survivors += 1
            pipe.hset(LEDGER_KEY, icao, json.dumps(entry))
        pipe.expire(LEDGER_KEY, settings.WATCH_LEDGER_TTL_SEC * 2)
        await pipe.execute()

    # ── event emission ────────────────────────────────────────────────────
    async def _emit(self, event: WatchEvent) -> None:
        assert self.redis is not None
        persisted = "redis-only"
        if self.pg is not None:
            try:
                await self.pg.execute(
                    """
                    INSERT INTO watch_events (
                        event_id, time, kind, icao24, callsign, severity,
                        summary, lat, lon, meta
                    ) VALUES (
                        $1, to_timestamp($2), $3, $4, $5, $6, $7, $8, $9, $10::jsonb
                    )
                    ON CONFLICT (event_id) DO NOTHING
                    """,
                    event.event_id, event.timestamp, event.kind, event.icao24,
                    event.callsign, event.severity, event.summary,
                    event.lat, event.lon, json.dumps(event.meta),
                )
                persisted = "postgres"
            except Exception as exc:
                log.error("watch_monitor: PostgreSQL insert failed: %s", exc)
        event.meta["persistence"] = persisted
        payload = json.dumps(event.to_dict())
        pipe = self.redis.pipeline()
        pipe.lpush(RECENT_EVENTS_KEY, payload)
        pipe.ltrim(RECENT_EVENTS_KEY, 0, settings.WATCH_RECENT_EVENTS_MAX - 1)
        await pipe.execute()
        log.info("WATCH %-20s %s", event.kind, event.summary)

    async def _dedup_claim(self, kind: str, ident: str, ttl: int) -> bool:
        """True if this detection may emit (first inside the dedup window)."""
        assert self.redis is not None
        return bool(await self.redis.set(
            f"{DEDUP_PREFIX}:{kind}:{ident}", b"1", ex=ttl, nx=True,
        ))

    # ── one analysis cycle ────────────────────────────────────────────────
    async def run_cycle(self, now: Optional[float] = None) -> Dict[str, int]:
        now = time.time() if now is None else now
        stats = {"tracks": 0, "fresh": 0, "events": 0, "concentrations": 0}
        vectors = await self._load_state_vectors()
        stats["tracks"] = len(vectors)
        area = await load_coverage_area(self.redis)
        fresh = [
            sv for sv in vectors
            if sv.last_seen and now - sv.last_seen <= settings.WATCH_FRESH_SEC
        ]
        stats["fresh"] = len(fresh)

        ledger = await self._load_ledger()
        seen_now = set()

        # Update ledger from live airborne traffic; a re-seen aircraft clears
        # any reported disappearance so a later loss is a new event.
        for sv in fresh:
            if sv.lat is None or sv.lon is None or sv.on_ground:
                continue
            seen_now.add(sv.icao24)
            entry = snapshot_from_sv(sv)
            prior = ledger.get(sv.icao24)
            if prior and prior.get("gone_reported"):
                entry["gone_reported"] = False
            elif prior:
                entry["gone_reported"] = prior.get("gone_reported", False)
                entry["updates"] = max(entry["updates"], int(prior.get("updates") or 0) + 1)
            ledger[sv.icao24] = entry

        # ── Transponder-off: ledger tracks gone silent under live coverage ──
        for icao, entry in ledger.items():
            if icao in seen_now or entry.get("gone_reported"):
                continue
            if not is_disappearance_candidate(entry, now):
                continue
            if not within_coverage_area(entry["lat"], entry["lon"], area):
                # Coverage rotated away from the last fix — not evidence.
                continue
            peers = count_live_peers(entry, fresh, now)
            if peers < settings.WATCH_MIN_PEERS:
                # Fail closed: cannot rule out feed/coverage outage.
                continue
            await self._emit(build_transponder_off_event(entry, peers, now))
            entry["gone_reported"] = True
            stats["events"] += 1

        # ── Per-aircraft military/squawk detections over live traffic ─────
        for sv in fresh:
            event = build_squawk_event(sv, now)
            if event and await self._dedup_claim(
                "sqk", f"{sv.icao24}:{sv.squawk}", settings.WATCH_EVENT_DEDUP_SEC,
            ):
                await self._emit(event)
                stats["events"] += 1
            event = build_high_performance_event(sv, now)
            if event and await self._dedup_claim(
                "hiperf", sv.icao24, settings.WATCH_EVENT_DEDUP_SEC,
            ):
                await self._emit(event)
                stats["events"] += 1

        # ── Military concentrations: multi-aircraft, deduped, sustained ───
        active_raw = await self.redis.hgetall(CONCENTRATION_KEY)
        active: Dict[str, Dict[str, Any]] = {}
        for field_b, payload in active_raw.items():
            try:
                active[field_b.decode()] = json.loads(payload)
            except (ValueError, UnicodeDecodeError):
                continue
        observed_cells = set()
        for cluster in cluster_military(fresh, settings.WATCH_MIL_CONCENTRATION_RADIUS_NM):
            if len(cluster) < settings.WATCH_MIL_CONCENTRATION_MIN:
                continue
            cell = concentration_cell(cluster)
            observed_cells.add(cell)
            record = active.get(cell)
            if record is None:
                event = build_concentration_event(cluster, cell, now)
                await self._emit(event)
                stats["events"] += 1
                active[cell] = {
                    "started": now, "last_seen": now,
                    "peak": len(cluster), "members": sorted(sv.icao24 for sv in cluster),
                }
            else:
                record["last_seen"] = now
                record["peak"] = max(int(record.get("peak") or 0), len(cluster))
                record["members"] = sorted(sv.icao24 for sv in cluster)
        stats["concentrations"] = len(observed_cells)

        # Clear cells not sustained within the hold window.
        pipe = self.redis.pipeline()
        for cell, record in active.items():
            if cell in observed_cells:
                pipe.hset(CONCENTRATION_KEY, cell, json.dumps(record))
            elif now - float(record.get("last_seen") or 0.0) > settings.WATCH_MIL_CONCENTRATION_HOLD_SEC:
                pipe.hdel(CONCENTRATION_KEY, cell)
            else:
                pipe.hset(CONCENTRATION_KEY, cell, json.dumps(record))
        pipe.expire(CONCENTRATION_KEY, settings.WATCH_LEDGER_TTL_SEC)
        await pipe.execute()

        await self._save_ledger(ledger, now)

        # ── Snapshot for the API ──────────────────────────────────────────
        snapshot = {
            "generated_at": now,
            "analysis_available": True,
            "tracks_observed": stats["tracks"],
            "tracks_fresh": stats["fresh"],
            "ledger_size": len(ledger),
            "active_concentrations": [
                {"cell": cell, **record}
                for cell, record in active.items()
                if cell in observed_cells
            ],
            "history_persistence": self.pg is not None,
            "thresholds": {
                "silence_sec": settings.WATCH_SILENCE_SEC,
                "min_alt_ft": settings.WATCH_MIN_ALT_FT,
                "peer_radius_nm": settings.WATCH_PEER_RADIUS_NM,
                "min_peers": settings.WATCH_MIN_PEERS,
                "mil_concentration_min": settings.WATCH_MIL_CONCENTRATION_MIN,
            },
        }
        await self.redis.set(SNAPSHOT_KEY, json.dumps(snapshot))
        return stats

    async def run(self) -> None:
        await self.start()
        log.info("watch_monitor: started (interval %ss)", settings.WATCH_SCAN_INTERVAL_SEC)
        while True:
            t0 = time.time()
            try:
                stats = await self.run_cycle(now=t0)
                log.debug("watch cycle: %s", stats)
            except Exception as exc:
                log.error("watch_monitor cycle failed: %s", exc)
            await asyncio.sleep(max(2.0, settings.WATCH_SCAN_INTERVAL_SEC - (time.time() - t0)))


async def _main() -> None:
    logging.basicConfig(
        level=settings.LOG_LEVEL,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    await WatchMonitor().run()


if __name__ == "__main__":
    asyncio.run(_main())
