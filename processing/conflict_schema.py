"""Canonical shared contract for passive conflict-analysis snapshots."""
from __future__ import annotations

import math
import re
from typing import Any

DEFAULT_MAX_CANDIDATE_PAIRS = 100_000
DEFAULT_MAX_CONFLICTS = 500
CONFLICT_MODE = "PASSIVE_NON_OPERATIONAL"
CONFLICT_LAYER = "L2"
CONFLICT_DETECTOR = "aircraft_conflict_projection"
CONFLICT_EVIDENCE_SCOPE = "coherent_adsb_source_reports"
REQUIRED_CONFLICT_SNAPSHOT_FIELDS = frozenset({
    "generated_at",
    "mode",
    "layer",
    "detector",
    "evidence_scope",
    "analysis_available",
    "evaluated_tracks",
    "evaluated_aircraft",
    "candidate_pairs",
    "total_possible_pairs",
    "spatial_comparisons",
    "truncated",
    "limits",
    "skipped_tracks",
    "skipped_pairs",
    "conflicts",
})


def unavailable_conflict_snapshot(
    now: float, reason: str,
) -> dict[str, Any]:
    """Return the complete fail-closed shared snapshot schema."""
    return {
        "generated_at": float(now),
        "mode": CONFLICT_MODE,
        "layer": CONFLICT_LAYER,
        "detector": CONFLICT_DETECTOR,
        "evidence_scope": CONFLICT_EVIDENCE_SCOPE,
        "analysis_available": False,
        "unavailable_reason": str(reason),
        "evaluated_tracks": 0,
        "evaluated_aircraft": [],
        "candidate_pairs": 0,
        "total_possible_pairs": 0,
        "spatial_comparisons": 0,
        "truncated": False,
        "limits": {
            "max_candidate_pairs": DEFAULT_MAX_CANDIDATE_PAIRS,
            "max_conflicts": DEFAULT_MAX_CONFLICTS,
        },
        "skipped_tracks": {},
        "skipped_pairs": {},
        "conflicts": [],
    }


def conflict_snapshot_has_required_fields(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and REQUIRED_CONFLICT_SNAPSHOT_FIELDS.issubset(value)
    )


def _nonnegative_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _counter_map(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and all(
            isinstance(key, str) and key and _nonnegative_int(count)
            for key, count in value.items()
        )
    )


def _finite_nonnegative_number(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
        and float(value) >= 0.0
    )


def _conflict_record_is_valid(
    conflict: Any, evaluated_aircraft: set[str], generated_at: float,
) -> bool:
    if not isinstance(conflict, dict):
        return False
    conflict_id = conflict.get("conflict_id")
    pair = conflict.get("pair")
    lifecycle = conflict.get("lifecycle")
    if (
        not isinstance(conflict_id, str)
        or re.fullmatch(r"[0-9a-f]{16}", conflict_id) is None
        or not isinstance(pair, list)
        or len(pair) != 2
        or pair != sorted(pair)
        or any(
            not isinstance(identity, str)
            or re.fullmatch(r"[0-9A-F]{6}", identity) is None
            for identity in pair
        )
        or pair[0] == pair[1]
        or conflict.get("aircraft") != pair
        or lifecycle not in {"ACTIVE", "CONFIRMED", "CLEARING", "STALE_HOLD"}
        or (
            lifecycle != "STALE_HOLD"
            and not set(pair).issubset(evaluated_aircraft)
        )
    ):
        return False
    if lifecycle == "STALE_HOLD":
        stale_reason = conflict.get("stale_reason")
        if not isinstance(stale_reason, str) or not stale_reason:
            return False
    observations = conflict.get("observations")
    if (
        not isinstance(observations, list)
        or len(observations) != 2
        or {item.get("icao") for item in observations if isinstance(item, dict)}
        != set(pair)
    ):
        return False
    for observation in observations:
        if (
            not isinstance(observation, dict)
            or observation.get("source") != "ADSB"
            or not _finite_nonnegative_number(observation.get("observed_at"))
            or float(observation["observed_at"]) > generated_at + 5.0
            or not _finite_nonnegative_number(observation.get("age_seconds"))
        ):
            return False
    if conflict.get("severity") not in {
        "MONITOR", "TRAFFIC_CONFLICT", "PREDICTED_LOSS_OF_SEPARATION",
    }:
        return False
    metric_names = (
        "tcpa_seconds",
        "current_horizontal_nm",
        "current_vertical_ft",
        "predicted_horizontal_nm",
        "predicted_vertical_ft",
        "time_to_threshold_seconds",
        "evaluation_time",
        "input_time_skew_seconds",
    )
    if not all(
        _finite_nonnegative_number(conflict.get(name)) for name in metric_names
    ):
        return False
    if (
        float(conflict["tcpa_seconds"]) > 120.0
        or float(conflict["time_to_threshold_seconds"]) > 120.0
        or float(conflict["input_time_skew_seconds"]) > 5.0
        or float(conflict["evaluation_time"]) > generated_at + 5.0
        or conflict.get("operational_advisory") is not False
        or conflict.get("basis") != "public_state_vector_projection"
    ):
        return False
    return True


def conflict_snapshot_schema_is_valid(value: Any) -> bool:
    if not conflict_snapshot_has_required_fields(value):
        return False
    if (
        value.get("mode") != CONFLICT_MODE
        or value.get("layer") != CONFLICT_LAYER
        or value.get("detector") != CONFLICT_DETECTOR
        or value.get("evidence_scope") != CONFLICT_EVIDENCE_SCOPE
        or not isinstance(value.get("analysis_available"), bool)
        or not isinstance(value.get("truncated"), bool)
    ):
        return False
    generated_at = value.get("generated_at")
    if (
        not isinstance(generated_at, (int, float))
        or isinstance(generated_at, bool)
        or not math.isfinite(float(generated_at))
        or float(generated_at) < 0.0
    ):
        return False
    counter_names = (
        "evaluated_tracks",
        "candidate_pairs",
        "total_possible_pairs",
        "spatial_comparisons",
    )
    if not all(_nonnegative_int(value.get(name)) for name in counter_names):
        return False
    identities = value.get("evaluated_aircraft")
    if (
        not isinstance(identities, list)
        or len(identities) != value["evaluated_tracks"]
        or len(identities) > 10_000
        or len(set(identities)) != len(identities)
        or any(
            not isinstance(identity, str)
            or re.fullmatch(r"[0-9A-F]{6}", identity) is None
            for identity in identities
        )
    ):
        return False
    if value["candidate_pairs"] > value["total_possible_pairs"]:
        return False
    if value.get("limits") != {
        "max_candidate_pairs": DEFAULT_MAX_CANDIDATE_PAIRS,
        "max_conflicts": DEFAULT_MAX_CONFLICTS,
    }:
        return False
    if not _counter_map(value.get("skipped_tracks")):
        return False
    if not _counter_map(value.get("skipped_pairs")):
        return False
    conflicts = value.get("conflicts")
    if (
        not isinstance(conflicts, list)
        or len(conflicts) > DEFAULT_MAX_CONFLICTS
        or any(
            not _conflict_record_is_valid(
                conflict, set(identities), float(generated_at)
            )
            for conflict in conflicts
        )
    ):
        return False
    if not value["analysis_available"]:
        reason = value.get("unavailable_reason")
        if not isinstance(reason, str) or not reason:
            return False
    return True
