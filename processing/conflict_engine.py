"""Passive aircraft-pair conflict analytics from public state vectors.

This module is not TCAS/ACAS and never generates pilot resolution commands.
It projects reported ground tracks to identify potential loss of separation for
research and situational awareness only.
"""
from __future__ import annotations

import hashlib
import math
import re
from collections import defaultdict
from itertools import product
from typing import Any, Sequence

from processing.conflict_schema import (
    DEFAULT_MAX_CANDIDATE_PAIRS,
    DEFAULT_MAX_CONFLICTS,
)

EARTH_RADIUS_M = 6_371_000.0
METERS_PER_NM = 1_852.0
KNOT_TO_MPS = METERS_PER_NM / 3_600.0
LOOKAHEAD_SECONDS = 120.0
MAX_TRACK_AGE_SECONDS = 30.0
MAX_FUTURE_SKEW_SECONDS = 5.0
MAX_PAIR_TIME_SKEW_SECONDS = 5.0
CANDIDATE_RADIUS_M = 100.0 * METERS_PER_NM
MAX_TRACK_SPEED_KTS = 1_200.0
ICAO_PATTERN = re.compile(r"^[0-9A-F]{6}$")
SEVERITY_PRIORITY = {
    "MONITOR": 1,
    "TRAFFIC_CONFLICT": 2,
    "PREDICTED_LOSS_OF_SEPARATION": 3,
}


def _finite(value: Any) -> float | None:
    try:
        value = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return value if math.isfinite(value) else None


def _ecef(track: dict[str, Any]) -> tuple[float, float, float]:
    lat = math.radians(float(track["lat"]))
    lon = math.radians(float(track["lon"]))
    radius = EARTH_RADIUS_M
    return (
        radius * math.cos(lat) * math.cos(lon),
        radius * math.cos(lat) * math.sin(lon),
        radius * math.sin(lat),
    )


def _ecef_velocity(track: dict[str, Any]) -> tuple[float, float, float]:
    lat = math.radians(float(track["lat"]))
    lon = math.radians(float(track["lon"]))
    speed = float(track["vel"]) * KNOT_TO_MPS
    heading = math.radians(float(track["hdg"]))
    east_speed = speed * math.sin(heading)
    north_speed = speed * math.cos(heading)
    east_unit = (-math.sin(lon), math.cos(lon), 0.0)
    north_unit = (
        -math.sin(lat) * math.cos(lon),
        -math.sin(lat) * math.sin(lon),
        math.cos(lat),
    )
    return (
        east_speed * east_unit[0] + north_speed * north_unit[0],
        east_speed * east_unit[1] + north_speed * north_unit[1],
        east_speed * east_unit[2] + north_speed * north_unit[2],
    )


def _projected_ecef(track: dict[str, Any], target_time: float) -> tuple[float, float, float]:
    position = _ecef(track)
    velocity = _ecef_velocity(track)
    delta = max(0.0, target_time - float(track["ts"]))
    return (
        position[0] + velocity[0] * delta,
        position[1] + velocity[1] * delta,
        position[2] + velocity[2] * delta,
    )


def _arc_distance_m(chord_m: float) -> float:
    ratio = min(1.0, max(0.0, chord_m / (2.0 * EARTH_RADIUS_M)))
    return 2.0 * EARTH_RADIUS_M * math.asin(ratio)


def _horizontal_interval(
    relative_position: Sequence[float], relative_velocity: Sequence[float],
    radius_nm: float,
) -> tuple[float, float] | None:
    arc_m = radius_nm * METERS_PER_NM
    radius_m = 2.0 * EARTH_RADIUS_M * math.sin(arc_m / (2.0 * EARTH_RADIUS_M))
    quadratic = sum(component * component for component in relative_velocity)
    linear = 2.0 * sum(
        relative_position[index] * relative_velocity[index]
        for index in range(len(relative_position))
    )
    constant = sum(component * component for component in relative_position) - radius_m * radius_m
    if quadratic <= 1e-9:
        return (0.0, LOOKAHEAD_SECONDS) if constant <= 0.0 else None
    discriminant = linear * linear - 4.0 * quadratic * constant
    if discriminant < 0.0:
        return None
    root = math.sqrt(max(0.0, discriminant))
    start = max(0.0, (-linear - root) / (2.0 * quadratic))
    end = min(LOOKAHEAD_SECONDS, (-linear + root) / (2.0 * quadratic))
    return (start, end) if start <= end else None


def _vertical_interval(
    vertical_ft: float, rel_vertical_fps: float, limit_ft: float,
) -> tuple[float, float] | None:
    if abs(rel_vertical_fps) <= 1e-9:
        return (0.0, LOOKAHEAD_SECONDS) if abs(vertical_ft) <= limit_ft else None
    first = (-limit_ft - vertical_ft) / rel_vertical_fps
    second = (limit_ft - vertical_ft) / rel_vertical_fps
    start = max(0.0, min(first, second))
    end = min(LOOKAHEAD_SECONDS, max(first, second))
    return (start, end) if start <= end else None


def _threshold_entry(
    relative_position: Sequence[float], relative_velocity: Sequence[float],
    vertical_ft: float, rel_vertical_fps: float,
    horizontal_nm: float, vertical_limit_ft: float,
) -> float | None:
    horizontal = _horizontal_interval(
        relative_position, relative_velocity, horizontal_nm,
    )
    vertical = _vertical_interval(vertical_ft, rel_vertical_fps, vertical_limit_ft)
    if horizontal is None or vertical is None:
        return None
    entry = max(horizontal[0], vertical[0])
    exit_time = min(horizontal[1], vertical[1])
    return entry if entry <= exit_time else None


def _spatial_candidates(
    tracks: list[dict[str, Any]], max_pairs: int,
) -> tuple[list[tuple[dict[str, Any], dict[str, Any]]], int, bool]:
    """Return nearby pairs using global ECEF buckets, including dateline/poles."""
    buckets: dict[tuple[int, int, int], list[tuple[dict[str, Any], tuple[float, float, float]]]] = defaultdict(list)
    pairs: list[tuple[dict[str, Any], dict[str, Any]]] = []
    comparisons = 0
    for track in tracks:
        xyz = _ecef(track)
        bucket = (
            math.floor(xyz[0] / CANDIDATE_RADIUS_M),
            math.floor(xyz[1] / CANDIDATE_RADIUS_M),
            math.floor(xyz[2] / CANDIDATE_RADIUS_M),
        )
        for delta in product((-1, 0, 1), repeat=3):
            neighbor = (
                bucket[0] + delta[0],
                bucket[1] + delta[1],
                bucket[2] + delta[2],
            )
            for other, other_xyz in buckets.get(neighbor, ()):
                comparisons += 1
                chord = math.sqrt(sum((xyz[index] - other_xyz[index]) ** 2 for index in range(3)))
                if chord <= CANDIDATE_RADIUS_M:
                    if len(pairs) >= max_pairs:
                        return pairs, comparisons, True
                    pairs.append((other, track))
        buckets[bucket].append((track, xyz))
    return pairs, comparisons, False


def _conflict_for_pair(
    a: dict[str, Any], b: dict[str, Any], *, now: float,
) -> dict[str, Any] | None:
    input_tracks = (a, b)
    input_time_skew = abs(float(a["ts"]) - float(b["ts"]))
    evaluation_time = max(float(a["ts"]), float(b["ts"]))
    a_position = _projected_ecef(a, evaluation_time)
    b_position = _projected_ecef(b, evaluation_time)
    a_velocity = _ecef_velocity(a)
    b_velocity = _ecef_velocity(b)
    relative_position = tuple(
        b_position[index] - a_position[index] for index in range(3)
    )
    relative_velocity = tuple(
        b_velocity[index] - a_velocity[index] for index in range(3)
    )
    speed_squared = sum(component * component for component in relative_velocity)
    closing_dot = sum(
        relative_position[index] * relative_velocity[index]
        for index in range(3)
    )
    if speed_squared > 1e-9:
        raw_tcpa = -closing_dot / speed_squared
        tcpa = max(0.0, min(LOOKAHEAD_SECONDS, raw_tcpa))
    else:
        raw_tcpa = 0.0
        tcpa = 0.0

    predicted_position = tuple(
        relative_position[index] + relative_velocity[index] * tcpa
        for index in range(3)
    )
    current_chord = math.sqrt(sum(component * component for component in relative_position))
    predicted_chord = math.sqrt(sum(component * component for component in predicted_position))
    current_horizontal_nm = _arc_distance_m(current_chord) / METERS_PER_NM
    predicted_horizontal_nm = _arc_distance_m(predicted_chord) / METERS_PER_NM

    a_altitude = float(a["alt"]) + float(a["vr"]) * (
        evaluation_time - float(a["ts"])
    ) / 60.0
    b_altitude = float(b["alt"]) + float(b["vr"]) * (
        evaluation_time - float(b["ts"])
    ) / 60.0
    vertical_now = b_altitude - a_altitude
    rel_vertical_fps = (float(b.get("vr") or 0.0) - float(a.get("vr") or 0.0)) / 60.0
    predicted_vertical_ft = abs(vertical_now + rel_vertical_fps * tcpa)

    thresholds = (
        ("PREDICTED_LOSS_OF_SEPARATION", 3.0, 1_000.0),
        ("TRAFFIC_CONFLICT", 5.0, 1_000.0),
        ("MONITOR", 10.0, 2_000.0),
    )
    severity = None
    time_to_threshold = None
    for candidate_severity, horizontal_nm, vertical_limit_ft in thresholds:
        entry = _threshold_entry(
            relative_position, relative_velocity,
            vertical_now, rel_vertical_fps,
            horizontal_nm, vertical_limit_ft,
        )
        if entry is None:
            continue
        if candidate_severity == "MONITOR" and raw_tcpa <= 0.0 and not (
            current_horizontal_nm <= 5.0 and abs(vertical_now) <= 1_000.0
        ):
            continue
        severity = candidate_severity
        time_to_threshold = entry
        break
    if severity is None or time_to_threshold is None:
        return None

    aircraft = sorted((str(a["icao"]).upper(), str(b["icao"]).upper()))
    conflict_id = hashlib.sha256(":".join(aircraft).encode()).hexdigest()[:16]
    observations = []
    for track in sorted(input_tracks, key=lambda item: str(item["icao"]).upper()):
        source = track.get("src", track.get("primary_source", "UNKNOWN"))
        source = getattr(source, "value", source)
        observations.append({
            "icao": str(track["icao"]).upper(),
            "source": str(source),
            "observed_at": float(track["ts"]),
            "age_seconds": round(float(now) - float(track["ts"]), 1),
        })
    return {
        "conflict_id": conflict_id,
        "aircraft": aircraft,
        "pair": aircraft,
        "observations": observations,
        "severity": severity,
        "tcpa_seconds": round(tcpa, 1),
        "current_horizontal_nm": round(current_horizontal_nm, 3),
        "current_vertical_ft": round(abs(vertical_now), 1),
        "predicted_horizontal_nm": round(predicted_horizontal_nm, 3),
        "predicted_vertical_ft": round(predicted_vertical_ft, 1),
        "time_to_threshold_seconds": round(time_to_threshold, 1),
        "evaluation_time": evaluation_time,
        "input_time_skew_seconds": round(input_time_skew, 1),
        "operational_advisory": False,
        "basis": "public_state_vector_projection",
    }


def _conflict_priority(conflict: dict[str, Any]) -> tuple[float, float, float]:
    return (
        float(SEVERITY_PRIORITY.get(str(conflict.get("severity")), 0)),
        -float(conflict.get("tcpa_seconds", LOOKAHEAD_SECONDS)),
        -float(conflict.get("predicted_horizontal_nm", CANDIDATE_RADIUS_M / METERS_PER_NM)),
    )


def analyze_conflicts(
    tracks: list[dict[str, Any]], *, now: float,
    max_candidate_pairs: int = DEFAULT_MAX_CANDIDATE_PAIRS,
    max_conflicts: int = DEFAULT_MAX_CONFLICTS,
) -> dict[str, Any]:
    """Analyze a snapshot of public state vectors for potential conflicts."""
    required = ("lat", "lon", "alt", "vel", "hdg", "vr", "ts")
    valid: list[dict[str, Any]] = []
    skipped = {
        "stale": 0,
        "on_ground": 0,
        "incomplete": 0,
        "invalid_identity": 0,
    }
    for track in tracks:
        if not isinstance(track, dict):
            skipped["incomplete"] += 1
            continue
        icao = str(track.get("icao") or "").upper()
        if not ICAO_PATTERN.fullmatch(icao):
            skipped["invalid_identity"] += 1
            continue
        values = [_finite(track.get(field)) for field in required]
        if any(value is None for value in values):
            skipped["incomplete"] += 1
            continue
        lat, lon, _alt, speed, heading, _vertical_rate, timestamp = (
            float(value) for value in values if value is not None
        )
        if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
            skipped["incomplete"] += 1
            continue
        if not (0.0 <= speed <= MAX_TRACK_SPEED_KTS and 0.0 <= heading < 360.0):
            skipped["incomplete"] += 1
            continue
        if track.get("gnd") is True:
            skipped["on_ground"] += 1
            continue
        age = float(now) - timestamp
        if age > MAX_TRACK_AGE_SECONDS or age < -MAX_FUTURE_SKEW_SECONDS:
            skipped["stale"] += 1
            continue
        valid.append(track)

    max_candidate_pairs = max(1, int(max_candidate_pairs))
    max_conflicts = max(1, int(max_conflicts))
    conflicts = []
    pairs, spatial_comparisons, pairs_truncated = _spatial_candidates(
        valid, max_candidate_pairs
    )
    total_possible_pairs = len(valid) * (len(valid) - 1) // 2
    if pairs_truncated:
        return {
            "generated_at": float(now),
            "mode": "PASSIVE_NON_OPERATIONAL",
            "layer": "L2",
            "detector": "aircraft_conflict_projection",
            "evidence_scope": "coherent_adsb_source_reports",
            "analysis_available": False,
            "unavailable_reason": "candidate_pair_limit_exceeded",
            "evaluated_tracks": len(valid),
            "candidate_pairs": len(pairs),
            "total_possible_pairs": total_possible_pairs,
            "spatial_comparisons": spatial_comparisons,
            "truncated": True,
            "limits": {
                "max_candidate_pairs": max_candidate_pairs,
                "max_conflicts": max_conflicts,
            },
            "skipped_tracks": {key: count for key, count in skipped.items() if count},
            "skipped_pairs": {},
            "conflicts": [],
        }
    conflict_output_truncated = False
    skipped_pairs = {"time_skew": 0}
    for first, second in pairs:
        if str(first["icao"]).upper() == str(second["icao"]).upper():
            continue
        if abs(float(first["ts"]) - float(second["ts"])) > MAX_PAIR_TIME_SKEW_SECONDS:
            skipped_pairs["time_skew"] += 1
            continue
        conflict = _conflict_for_pair(first, second, now=now)
        if conflict is not None:
            if len(conflicts) < max_conflicts:
                conflicts.append(conflict)
            else:
                conflict_output_truncated = True
                least_important = min(
                    range(len(conflicts)), key=lambda index: _conflict_priority(conflicts[index])
                )
                if _conflict_priority(conflict) > _conflict_priority(conflicts[least_important]):
                    conflicts[least_important] = conflict

    conflicts.sort(key=_conflict_priority, reverse=True)

    total_possible_pairs = len(valid) * (len(valid) - 1) // 2
    candidate_pairs = len(pairs)

    return {
        "generated_at": float(now),
        "mode": "PASSIVE_NON_OPERATIONAL",
        "layer": "L2",
        "detector": "aircraft_conflict_projection",
        "evidence_scope": "public_state_vectors",
        "evaluated_tracks": len(valid),
        "candidate_pairs": candidate_pairs,
        "total_possible_pairs": total_possible_pairs,
        "spatial_comparisons": spatial_comparisons,
        "truncated": pairs_truncated or conflict_output_truncated,
        "limits": {
            "max_candidate_pairs": max_candidate_pairs,
            "max_conflicts": max_conflicts,
        },
        "skipped_tracks": {key: count for key, count in skipped.items() if count},
        "skipped_pairs": {key: count for key, count in skipped_pairs.items() if count},
        "conflicts": conflicts,
    }
