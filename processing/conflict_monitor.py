"""Single-owner passive conflict monitor input and publication boundary."""
from __future__ import annotations

import asyncio
import logging
import math
import secrets
import time
from collections import Counter
from typing import Any

import orjson
import redis.asyncio as aioredis

from config import settings
from models import DataSource, SourceReport, StateVector
from processing.conflict_engine import analyze_conflicts
from processing.conflict_schema import unavailable_conflict_snapshot

# Stable per-process identity for the single-owner monitor lease.
_PROCESS_OWNER_TOKEN = secrets.token_hex(16)

MAX_SOURCE_REPORT_AGE_SECONDS = 15.0
MAX_FUTURE_SOURCE_SKEW_SECONDS = 5.0
CONFLICT_SNAPSHOT_KEY = "conflict:latest"
CONFLICT_LIFECYCLE_KEY = "conflict:lifecycle"
CONFLICT_OWNER_KEY = "conflict:monitor:owner"
CONFLICT_SNAPSHOT_TTL_SECONDS = 30
CONFLICT_LIFECYCLE_TTL_SECONDS = 120
CONFLICT_OWNER_TTL_SECONDS = 60
MONITOR_INTERVAL_SECONDS = 5.0
MAX_FUSION_STATE_KEYS = 10_000
MAX_SCAN_BATCHES = 100
MAX_LIFECYCLE_PAIRS = 500
MAX_FUSION_STATE_BYTES = 1_000_000
MAX_FUSION_MGET_BATCH_KEYS = 16
MAX_FUSION_TOTAL_BYTES = 64 * 1024 * 1024
MAX_LIFECYCLE_BYTES = 500_000
FENCED_PUBLISH_SCRIPT = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
  return 0
end
redis.call('SET', KEYS[2], ARGV[2], 'EX', ARGV[4])
redis.call('SET', KEYS[3], ARGV[3], 'EX', ARGV[5])
redis.call('EXPIRE', KEYS[1], ARGV[6])
return 1
"""
REFRESH_OWNER_SCRIPT = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('EXPIRE', KEYS[1], ARGV[2])
end
return 0
"""
log = logging.getLogger("conflict-monitor")


def _complete_adsb_report(report: SourceReport) -> bool:
    if report.source != DataSource.ADSB or report.on_ground is None:
        return False
    values = (
        report.lat,
        report.lon,
        report.altitude,
        report.velocity,
        report.heading,
        report.vertical_rate,
        report.timestamp,
    )
    if any(value is None for value in values):
        return False
    try:
        numeric = [float(value) for value in values if value is not None]
    except (TypeError, ValueError, OverflowError):
        return False
    return all(math.isfinite(value) for value in numeric)


def coherent_adsb_track(
    state: StateVector, *, now: float,
) -> tuple[dict[str, Any] | None, str | None]:
    """Select one coherent complete ADS-B report; never mix top-level field ages."""
    complete = [report for report in state.source_reports if _complete_adsb_report(report)]
    if not complete:
        return None, "no_complete_source_report"
    report = max(complete, key=lambda item: item.timestamp)
    age = float(now) - float(report.timestamp)
    if age > MAX_SOURCE_REPORT_AGE_SECONDS:
        return None, "stale_complete_source_report"
    if age < -MAX_FUTURE_SOURCE_SKEW_SECONDS:
        return None, "future_source_report"
    if report.on_ground:
        return None, "on_ground"
    return {
        "icao": state.icao24,
        "lat": report.lat,
        "lon": report.lon,
        "alt": report.altitude,
        "vel": report.velocity,
        "hdg": report.heading,
        "vr": report.vertical_rate,
        "ts": report.timestamp,
        "gnd": report.on_ground,
        "src": report.source.value,
        "receiver_id": report.receiver_id,
    }, None


def build_conflict_snapshot(
    states: list[StateVector], *, now: float,
) -> dict[str, Any]:
    tracks: list[dict[str, Any]] = []
    skipped: Counter[str] = Counter()
    for state in states:
        track, reason = coherent_adsb_track(state, now=now)
        if track is None:
            skipped[reason or "invalid_state"] += 1
            continue
        tracks.append(track)
    if not tracks:
        snapshot = _unavailable_snapshot(now, "no_eligible_conflict_tracks")
        snapshot["evaluated_aircraft"] = []
        snapshot["monitor_input_states"] = len(states)
        snapshot["monitor_skipped_tracks"] = dict(sorted(skipped.items()))
        return snapshot
    snapshot = analyze_conflicts(tracks, now=now)
    snapshot["evidence_scope"] = "coherent_adsb_source_reports"
    snapshot.setdefault("analysis_available", True)
    snapshot["evaluated_aircraft"] = sorted(track["icao"] for track in tracks)
    snapshot["monitor_input_states"] = len(states)
    snapshot["monitor_skipped_tracks"] = dict(sorted(skipped.items()))
    return snapshot


def apply_conflict_lifecycle(
    snapshot: dict[str, Any], prior: dict[str, Any], *, now: float,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Apply bounded confirmation/hysteresis without inventing clear on outage."""
    result = dict(snapshot)
    prior = prior if isinstance(prior, dict) else {}
    if not snapshot.get("analysis_available", False):
        held = []
        for record in list(prior.values())[:MAX_LIFECYCLE_PAIRS]:
            if isinstance(record, dict) and record.get("active") and isinstance(record.get("conflict"), dict):
                conflict = dict(record["conflict"])
                conflict["lifecycle"] = "STALE_HOLD"
                conflict["data_status"] = "INSUFFICIENT_DATA"
                held.append(conflict)
        result["conflicts"] = held
        result["active_conflict_count"] = len(held)
        return result, prior

    current = {
        str(conflict.get("conflict_id")): conflict
        for conflict in snapshot.get("conflicts", [])
        if isinstance(conflict, dict) and conflict.get("conflict_id")
    }
    evaluated_aircraft = {
        str(icao).upper()
        for icao in snapshot.get("evaluated_aircraft", [])
        if isinstance(icao, str)
    }
    next_state: dict[str, Any] = {}
    active_conflicts: list[dict[str, Any]] = []
    for pair_id, conflict in list(current.items())[:MAX_LIFECYCLE_PAIRS]:
        previous_value = prior.get(pair_id)
        previous = previous_value if isinstance(previous_value, dict) else {}
        hits = int(previous.get("hits", 0)) + 1
        immediate = float(conflict.get("time_to_threshold_seconds", 999.0)) <= 20.0
        active = bool(previous.get("active")) or hits >= 2 or immediate
        rendered = dict(conflict)
        rendered["lifecycle"] = "ACTIVE" if active else "PENDING"
        next_state[pair_id] = {
            "hits": hits,
            "misses": 0,
            "active": active,
            "last_seen": float(now),
            "conflict": rendered,
        }
        if active:
            active_conflicts.append(rendered)

    for pair_id, previous in prior.items():
        if pair_id in current or len(next_state) >= MAX_LIFECYCLE_PAIRS:
            continue
        if not isinstance(previous, dict) or not previous.get("active"):
            continue
        previous_conflict_value = previous.get("conflict")
        if not isinstance(previous_conflict_value, dict):
            continue
        previous_conflict: dict[str, Any] = previous_conflict_value
        pair = previous_conflict.get("pair")
        pair_was_evaluated = (
            isinstance(pair, list)
            and len(pair) == 2
            and all(str(icao).upper() in evaluated_aircraft for icao in pair)
        )
        if not pair_was_evaluated:
            rendered = dict(previous_conflict)
            rendered["lifecycle"] = "STALE_HOLD"
            rendered["data_status"] = "INSUFFICIENT_DATA"
            next_state[pair_id] = {
                **previous,
                "conflict": rendered,
            }
            active_conflicts.append(rendered)
            continue
        misses = int(previous.get("misses", 0)) + 1
        if misses >= 3:
            continue
        rendered = dict(previous_conflict)
        rendered["lifecycle"] = "CLEARING"
        rendered["data_status"] = "AWAITING_CLEAR_CONFIRMATION"
        next_state[pair_id] = {
            **previous,
            "misses": misses,
            "conflict": rendered,
        }
        active_conflicts.append(rendered)

    result["raw_conflict_count"] = len(current)
    result["conflicts"] = active_conflicts[:MAX_LIFECYCLE_PAIRS]
    result["active_conflict_count"] = len(result["conflicts"])
    return result, next_state


async def publish_conflict_snapshot(redis_client, snapshot: dict[str, Any]) -> None:
    await redis_client.set(
        CONFLICT_SNAPSHOT_KEY,
        orjson.dumps(snapshot),
        ex=CONFLICT_SNAPSHOT_TTL_SECONDS,
    )


async def _acquire_monitor_lease(redis_client, owner_token: str) -> bool:
    acquired = await redis_client.set(
        CONFLICT_OWNER_KEY,
        owner_token,
        ex=CONFLICT_OWNER_TTL_SECONDS,
        nx=True,
    )
    if acquired:
        return True
    refreshed = await redis_client.eval(
        REFRESH_OWNER_SCRIPT,
        1,
        CONFLICT_OWNER_KEY,
        owner_token,
        CONFLICT_OWNER_TTL_SECONDS,
    )
    return bool(refreshed)


async def _publish_with_lifecycle(
    redis_client, snapshot: dict[str, Any], *, now: float, owner_token: str,
) -> dict[str, Any]:
    prior: dict[str, Any] = {}
    raw = await redis_client.get(CONFLICT_LIFECYCLE_KEY)
    if raw and len(raw) <= MAX_LIFECYCLE_BYTES:
        try:
            decoded = orjson.loads(raw)
            if isinstance(decoded, dict) and len(decoded) <= MAX_LIFECYCLE_PAIRS:
                prior = decoded
        except Exception:
            prior = {}
    rendered, state = apply_conflict_lifecycle(snapshot, prior, now=now)
    published = await redis_client.eval(
        FENCED_PUBLISH_SCRIPT,
        3,
        CONFLICT_OWNER_KEY,
        CONFLICT_LIFECYCLE_KEY,
        CONFLICT_SNAPSHOT_KEY,
        owner_token,
        orjson.dumps(state),
        orjson.dumps(rendered),
        CONFLICT_LIFECYCLE_TTL_SECONDS,
        CONFLICT_SNAPSHOT_TTL_SECONDS,
        CONFLICT_OWNER_TTL_SECONDS,
    )
    if not published:
        return _unavailable_snapshot(now, "monitor_ownership_lost")
    return rendered


def _unavailable_snapshot(now: float, reason: str) -> dict[str, Any]:
    return unavailable_conflict_snapshot(now, reason)


async def run_cycle(redis_client, *, now: float | None = None) -> dict[str, Any]:
    assessed_at = time.time() if now is None else float(now)
    if not await _acquire_monitor_lease(redis_client, _PROCESS_OWNER_TOKEN):
        return _unavailable_snapshot(assessed_at, "monitor_ownership_lost")
    cursor = 0
    keys: dict[bytes | str, None] = {}
    for _ in range(MAX_SCAN_BATCHES):
        cursor, batch = await redis_client.scan(
            cursor, match="fusion:sv:*", count=500,
        )
        for key in batch:
            if len(keys) >= MAX_FUSION_STATE_KEYS:
                snapshot = _unavailable_snapshot(
                    assessed_at, "fusion_state_limit_exceeded",
                )
                return await _publish_with_lifecycle(
                    redis_client, snapshot, now=assessed_at,
                    owner_token=_PROCESS_OWNER_TOKEN,
                )
            keys[key] = None
        if int(cursor) == 0:
            break
    if int(cursor) != 0:
        snapshot = _unavailable_snapshot(assessed_at, "fusion_scan_budget_exceeded")
        return await _publish_with_lifecycle(
            redis_client, snapshot, now=assessed_at,
            owner_token=_PROCESS_OWNER_TOKEN,
        )

    ordered_keys = sorted(keys, key=lambda value: value if isinstance(value, bytes) else value.encode())
    if not ordered_keys:
        snapshot = _unavailable_snapshot(assessed_at, "no_fusion_states")
        return await _publish_with_lifecycle(
            redis_client, snapshot, now=assessed_at,
            owner_token=_PROCESS_OWNER_TOKEN,
        )
    states: list[StateVector] = []
    malformed = 0
    total_raw_bytes = 0
    for offset in range(0, len(ordered_keys), MAX_FUSION_MGET_BATCH_KEYS):
        batch_keys = ordered_keys[offset:offset + MAX_FUSION_MGET_BATCH_KEYS]
        raw_states = await redis_client.mget(batch_keys)
        if len(raw_states) != len(batch_keys):
            malformed += len(batch_keys)
            continue
        for key, raw in zip(batch_keys, raw_states):
            if not raw or len(raw) > MAX_FUSION_STATE_BYTES:
                malformed += 1
                continue
            total_raw_bytes += len(raw)
            if total_raw_bytes > MAX_FUSION_TOTAL_BYTES:
                snapshot = _unavailable_snapshot(
                    assessed_at, "fusion_state_bytes_limit_exceeded",
                )
                return await _publish_with_lifecycle(
                    redis_client, snapshot, now=assessed_at,
                    owner_token=_PROCESS_OWNER_TOKEN,
                )
            try:
                state = StateVector.from_bytes(raw)
                key_text = key.decode() if isinstance(key, bytes) else str(key)
                if key_text != f"fusion:sv:{state.icao24}":
                    raise ValueError("fusion key identity mismatch")
                states.append(state)
            except Exception:
                malformed += 1
    if malformed:
        snapshot = _unavailable_snapshot(assessed_at, "malformed_fusion_states")
        snapshot["malformed_fusion_states"] = malformed
        return await _publish_with_lifecycle(
            redis_client, snapshot, now=assessed_at,
            owner_token=_PROCESS_OWNER_TOKEN,
        )
    snapshot = build_conflict_snapshot(states, now=assessed_at)
    snapshot["malformed_fusion_states"] = malformed
    return await _publish_with_lifecycle(
        redis_client, snapshot, now=assessed_at,
        owner_token=_PROCESS_OWNER_TOKEN,
    )


async def run() -> None:
    logging.basicConfig(level=settings.LOG_LEVEL)
    redis_client = aioredis.from_url(settings.REDIS_URL, decode_responses=False)
    try:
        while True:
            try:
                snapshot = await run_cycle(redis_client)
                log.info(
                    "Conflict cycle: available=%s tracks=%d pairs=%d conflicts=%d",
                    snapshot.get("analysis_available"),
                    snapshot.get("evaluated_tracks", 0),
                    snapshot.get("candidate_pairs", 0),
                    len(snapshot.get("conflicts", [])),
                )
            except Exception:
                log.exception("Conflict monitor cycle failed")
            await asyncio.sleep(MONITOR_INTERVAL_SECONDS)
    finally:
        await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(run())
