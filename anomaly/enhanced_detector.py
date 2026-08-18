"""
anomaly/enhanced_detector.py
============================
SkySecure v3 — L2 (Kinematic) + L3 (Integrity) detection, and the L1/L2/L3
fusion step.

LAYER NUMBERING (authoritative — matches README and the JSHS paper):

    L1  Multi-source position cross-validation   processing/cross_source_validator.py
    L2  Kinematic anomaly detection              THIS FILE
    L3  NIC/NACp integrity clustering            THIS FILE
    L4  Multi-sensor fusion                      processing/fusion_engine.py
    L5  Identity and threat intelligence         upstream canonical pipeline

An earlier version of this file used its own conflicting internal numbering
(physics=L1, behavioral=L2, integrity=L3, TDOA=L4, ensemble=L5). That is gone.
Physics and behavioral checks are both *components of L2*, not layers.

DEGRADATION CONTRACT
Every layer reports whether it actually had the data it needs. A layer with no
usable input returns `available=False` and is dropped from the weighted sum,
with the remaining weights renormalized. A layer never contributes a nonzero
score on the basis of absent data — missing input is not evidence of spoofing.
This matters concretely: OpenSky's /states/all carries no NIC/NACp fields, so
on that feed L3 is unavailable for every aircraft. The previous implementation
returned 0.35 in that case, which would have applied a standing spoofing
penalty to the entire sky.
"""

from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

import numpy as np

logger = logging.getLogger(__name__)

EARTH_RADIUS_M = 6371000.0

# L2 kinematic bounds. Civil transport envelope with margin; military and
# some bizjets legitimately exceed parts of this, which is why L2 alone
# never produces a verdict — it feeds fusion.
MAX_PLAUSIBLE_SPEED_KTS = 700.0      # sustained ground speed
ABSURD_SPEED_KTS = 1500.0            # score saturates here
MAX_CLIMB_FPM = 8000.0
ABSURD_CLIMB_FPM = 20000.0
MAX_BARO_GEO_DELTA_FT = 600.0        # normal baro/geo spread is well under this
ABSURD_BARO_GEO_DELTA_FT = 3000.0
MAX_TURN_RATE_DEG_S = 12.0           # a hard fighter turn; airliners ~3 deg/s

# Below this dt, two reports are effectively the same sample and implied-speed
# arithmetic explodes on noise. Skip rather than flag.
MIN_DT_S = 1.0
# Above this, the aircraft may legitimately have manoeuvred out of any
# straight-line prediction; treat as a fresh track.
MAX_DT_S = 300.0


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in metres. Replaces the previous
    sqrt(dlat^2+dlon^2)*111 approximation, which understated longitude
    distance by cos(latitude) — a ~50% error at 60 deg N."""
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * EARTH_RADIUS_M * math.asin(math.sqrt(a))


def _ramp(value: float, floor: float, ceiling: float) -> float:
    """0.0 at or below floor, 1.0 at or above ceiling, linear between."""
    if ceiling <= floor:
        return 0.0
    return float(np.clip((value - floor) / (ceiling - floor), 0.0, 1.0))


def _heading_delta(h1: float, h2: float) -> float:
    """Smallest absolute angular difference in degrees (0-180)."""
    d = abs(h1 - h2) % 360.0
    return d if d <= 180.0 else 360.0 - d


@dataclass
class LayerResult:
    """One layer's contribution. `available` is the degradation flag."""
    name: str
    score: float                 # 0-1, only meaningful when available
    available: bool
    reason: str = ""             # why unavailable, or what drove the score
    detail: Dict[str, float] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "score": round(self.score, 3) if self.available else None,
            "available": self.available,
            "reason": self.reason,
            "detail": {k: round(v, 3) for k, v in self.detail.items()},
        }


@dataclass
class FusedAssessment:
    """Combined L1/L2/L3 verdict for one aircraft at one instant."""
    icao: str
    timestamp: datetime
    layers: Dict[str, LayerResult]
    overall_score: float
    threat_level: str
    layers_available: List[str]
    corroborated: bool           # 2+ independent layers agree something is wrong
    notes: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "icao": self.icao,
            "timestamp": self.timestamp.isoformat(),
            "layers": {k: v.to_dict() for k, v in self.layers.items()},
            "overall_score": round(self.overall_score, 3),
            "threat_level": self.threat_level,
            "layers_available": self.layers_available,
            "corroborated": self.corroborated,
            "notes": self.notes,
        }


class EnhancedAnomalyDetector:
    """
    L2 + L3 detection and L1/L2/L3 fusion.

    Usage from the live path:

        det = EnhancedAnomalyDetector()
        assessment = det.assess(
            icao=..., lat=..., lon=..., alt_baro=..., alt_geo=...,
            velocity=..., vertical_rate=..., heading=...,
            nic=..., nac_p=...,
            l1_result=<CrossValidationResult.to_dict() or None>,
            observed_at=<unix seconds>,
        )

    L1 is consumed as a plain dict, so this class has no import dependency on
    CrossSourceValidator and no dependency at all on the retired simulated
    TDOA validator.
    """

    # Base weights. Renormalized over whichever layers are actually available.
    BASE_WEIGHTS = {
        "l1_cross_source": 0.45,
        "l2_kinematic": 0.35,
        "l3_integrity": 0.20,
    }

    L1_VERDICT_SCORE = {
        "LEGITIMATE": 0.0,
        "UNCERTAIN": 0.5,
        "SPOOFED": 1.0,
    }

    # A layer at or above this is "asserting" an anomaly, for corroboration.
    ASSERT_THRESHOLD = 0.5

    def __init__(self, history_len: int = 20):
        self.previous_states: Dict[str, dict] = {}
        self.integrity_history: Dict[str, List[tuple]] = {}
        self._integrity_results: Dict[str, tuple[tuple[float, int], LayerResult]] = {}
        self.history_len = history_len

    # ── L2: Kinematic ────────────────────────────────────────────────────────

    def check_kinematics(
        self,
        icao: str,
        lat: float,
        lon: float,
        alt_baro: Optional[float],
        alt_geo: Optional[float],
        velocity: Optional[float],
        vertical_rate: Optional[float],
        heading: Optional[float],
        observed_at: Optional[float] = None,
        observation_sequence: int = 0,
    ) -> LayerResult:
        """
        L2 — kinematic plausibility.

        Two families of check:
          (a) instantaneous — is this single report self-consistent with the
              flight envelope (speed, climb rate, baro/geo agreement)?
          (b) differential — is this report consistent with the *previous*
              report for the same aircraft, given the real elapsed time?

        The differential checks are the ones that catch position spoofing, and
        they were previously broken: the old code hardcoded `time_elapsed = 1.0`
        second regardless of actual poll interval. On the live path aircraft are
        polled every ~30s, so every normal aircraft appeared to move ~30x too
        fast and any aircraft moving >0.5 km in a cycle was scored as
        "teleportation". That check now uses the true dt.
        """
        now = observed_at if observed_at is not None else time.time()
        detail: Dict[str, float] = {}
        drivers: List[str] = []

        # (a) Instantaneous checks — each only runs if its input exists.
        if velocity is not None:
            s = _ramp(velocity, MAX_PLAUSIBLE_SPEED_KTS, ABSURD_SPEED_KTS)
            detail["impossible_speed"] = s
            if s > 0:
                drivers.append(f"ground speed {velocity:.0f}kt")

        if vertical_rate is not None:
            s = _ramp(abs(vertical_rate), MAX_CLIMB_FPM, ABSURD_CLIMB_FPM)
            detail["impossible_climb"] = s
            if s > 0:
                drivers.append(f"vertical rate {vertical_rate:.0f}fpm")

        if alt_baro is not None and alt_geo is not None:
            delta = abs(alt_baro - alt_geo)
            s = _ramp(delta, MAX_BARO_GEO_DELTA_FT, ABSURD_BARO_GEO_DELTA_FT)
            detail["altitude_conflict"] = s
            if s > 0:
                drivers.append(f"baro/geo split {delta:.0f}ft")

        # (b) Differential checks against previous state.
        prev = self.previous_states.get(icao)
        event_order = (now, observation_sequence)
        if prev is not None and event_order > (
            float(prev["t"]), int(prev.get("seq", 0))
        ):
            dt = now - prev["t"]
            if MIN_DT_S <= dt <= MAX_DT_S:
                dist_m = haversine_m(prev["lat"], prev["lon"], lat, lon)
                implied_kts = (dist_m / dt) * 1.94384

                # Compare against the aircraft's own reported speed where we
                # have it — a jump inconsistent with self-reported velocity is
                # a stronger signal than one merely above a global bound.
                bound = MAX_PLAUSIBLE_SPEED_KTS
                if velocity is not None and velocity > 0:
                    bound = max(bound, velocity * 2.5)
                s = _ramp(implied_kts, bound, max(bound * 3.0, ABSURD_SPEED_KTS))
                detail["position_jump"] = s
                detail["implied_speed_kts"] = implied_kts
                if s > 0:
                    drivers.append(
                        f"implied speed {implied_kts:.0f}kt over {dt:.0f}s"
                    )

                if heading is not None and prev.get("hdg") is not None:
                    turn_rate = _heading_delta(heading, prev["hdg"]) / dt
                    s_turn = _ramp(turn_rate, MAX_TURN_RATE_DEG_S, MAX_TURN_RATE_DEG_S * 3)
                    detail["impossible_turn"] = s_turn
                    detail["turn_rate_deg_s"] = turn_rate
                    if s_turn > 0:
                        drivers.append(f"turn rate {turn_rate:.1f}deg/s")

        # Replayed or delayed snapshots must not move detector state backward.
        if prev is None or event_order > (
            float(prev["t"]), int(prev.get("seq", 0))
        ):
            self.previous_states[icao] = {
                "lat": lat, "lon": lon, "hdg": heading,
                "vel": velocity, "t": now, "seq": observation_sequence,
            }

        component_scores = [
            v for k, v in detail.items()
            if k not in ("implied_speed_kts", "turn_rate_deg_s")
        ]
        if not component_scores:
            return LayerResult(
                name="l2_kinematic", score=0.0, available=False,
                reason="no kinematic fields present in this report",
            )

        # Max, not mean: one impossible quantity is not diluted by several
        # normal ones. Averaging here would let a 1500kt report score 0.2.
        score = max(component_scores)
        reason = "; ".join(drivers) if drivers else "within kinematic envelope"
        return LayerResult(
            name="l2_kinematic", score=score, available=True,
            reason=reason, detail=detail,
        )

    # ── L3: Integrity metadata ───────────────────────────────────────────────

    def check_integrity(
        self,
        icao: str,
        nic: Optional[int],
        nac_p: Optional[int],
        observed_at: Optional[float] = None,
        observation_sequence: int = 0,
    ) -> LayerResult:
        """
        L3 — NIC/NACp integrity clustering.

        Detects two things: chronically low integrity values, and integrity
        that departs from the aircraft's own established pattern (a transmitter
        substitution or a synthetic emitter that doesn't replicate the target's
        metadata signature).

        If neither field is present, the layer is UNAVAILABLE — not zero, and
        emphatically not the 0.35 default the previous implementation used.
        OpenSky /states/all does not carry these fields; adsb.lol and adsb.fi
        do, so populating L3 on the live path means sourcing metadata from the
        L1 fetch rather than from OpenSky.
        """
        now = observed_at if observed_at is not None else time.time()
        event_order = (now, observation_sequence)
        prior = self._integrity_results.get(icao)
        if prior is not None and event_order <= prior[0]:
            return prior[1]

        if nic is None and nac_p is None:
            result = LayerResult(
                name="l3_integrity", score=0.0, available=False,
                reason="no NIC/NACp in feed (OpenSky /states/all omits these)",
            )
            self._integrity_results[icao] = (event_order, result)
            return result

        history = self.integrity_history.setdefault(icao, [])
        history.append((nic, nac_p))
        del history[:-self.history_len]

        detail: Dict[str, float] = {}
        if nic is not None:
            detail["nic"] = float(nic)
        if nac_p is not None:
            detail["nac_p"] = float(nac_p)

        # Absolute component: NIC/NACp are 0-11ish scales where higher is
        # better. Persistently low values are independently suspicious.
        present = [float(v) for v in (nic, nac_p) if v is not None]
        absolute = _ramp(-(sum(present) / len(present)), -8.0, -2.0)
        detail["low_integrity"] = absolute

        # Deviation component: needs enough history to have a baseline.
        complete = [p for p in history[:-1] if p[0] is not None and p[1] is not None]
        deviation = 0.0
        if len(complete) >= 3 and nic is not None and nac_p is not None:
            nic_vals = [float(p[0]) for p in complete]
            nac_vals = [float(p[1]) for p in complete]
            nic_med, nac_med = float(np.median(nic_vals)), float(np.median(nac_vals))
            delta = abs(nic - nic_med) + abs(nac_p - nac_med)
            spread = max(1.0, float(np.std(nic_vals) + np.std(nac_vals)))
            deviation = float(np.clip(delta / (spread * 4.0), 0.0, 1.0))
            detail["deviation"] = deviation
            # Integrity improving beyond baseline is not a threat signal.
            if nic >= nic_med and nac_p >= nac_med:
                deviation *= 0.3
        else:
            detail["deviation"] = 0.0

        score = float(np.clip(max(absolute, deviation), 0.0, 1.0))
        if score >= self.ASSERT_THRESHOLD:
            reason = f"integrity anomaly (NIC={nic}, NACp={nac_p})"
        else:
            reason = f"integrity nominal (NIC={nic}, NACp={nac_p})"
        result = LayerResult(
            name="l3_integrity", score=score, available=True,
            reason=reason, detail=detail,
        )
        self._integrity_results[icao] = (event_order, result)
        return result

    # ── L1 adapter ───────────────────────────────────────────────────────────

    def adapt_l1(self, l1_result: Optional[dict]) -> LayerResult:
        """
        Convert a CrossValidationResult.to_dict() into a LayerResult so L1 can
        participate in fusion. This is the adapter that was missing — it is why
        L1 and L2/L3 could not previously share a verdict.

        INSUFFICIENT_SOURCES means only one network saw the aircraft, so there
        was nothing to cross-check. That is unavailable, not innocent.
        """
        if not l1_result:
            return LayerResult(
                name="l1_cross_source", score=0.0, available=False,
                reason="no L1 result for this aircraft this cycle",
            )

        verdict = l1_result.get("verdict")
        if verdict not in self.L1_VERDICT_SCORE:
            return LayerResult(
                name="l1_cross_source", score=0.0, available=False,
                reason=f"L1 inconclusive ({verdict})",
            )

        score = self.L1_VERDICT_SCORE[verdict]
        # L1's own confidence scales how far it moves the fused score. A
        # low-confidence SPOOFED (two sources, marginal disagreement) should
        # not alone drive the aircraft to CRITICAL.
        confidence = float(l1_result.get("confidence") or 0.0)
        sources = l1_result.get("sources_used") or []
        detail = {
            "raw_score": score,
            "confidence": confidence,
            "max_disagreement_m": float(l1_result.get("max_disagreement_m") or 0.0),
            "n_sources": float(len(sources)),
        }
        return LayerResult(
            name="l1_cross_source",
            score=score * max(confidence, 0.5) if score > 0 else 0.0,
            available=True,
            reason=f"{verdict} across {'/'.join(sources) or 'unknown sources'}",
            detail=detail,
        )

    # ── Fusion ───────────────────────────────────────────────────────────────

    def assess(
        self,
        icao: str,
        lat: float,
        lon: float,
        alt_baro: Optional[float] = None,
        alt_geo: Optional[float] = None,
        velocity: Optional[float] = None,
        vertical_rate: Optional[float] = None,
        heading: Optional[float] = None,
        nic: Optional[int] = None,
        nac_p: Optional[int] = None,
        l1_result: Optional[dict] = None,
        observed_at: Optional[float] = None,
        observation_sequence: int = 0,
    ) -> FusedAssessment:
        """
        Run L2 and L3, fold in L1, and produce a single fused assessment.

        Weighted sum over available layers only, with weights renormalized, plus
        a corroboration bonus: two independent layers each asserting an anomaly
        is stronger evidence than one layer asserting it loudly, because the
        layers fail in uncorrelated ways (L1 fails on network latency, L2 on
        unusual-but-real manoeuvres, L3 on avionics quirks).
        """
        layers = {
            "l1_cross_source": self.adapt_l1(l1_result),
            "l2_kinematic": self.check_kinematics(
                icao, lat, lon, alt_baro, alt_geo,
                velocity, vertical_rate, heading, observed_at,
                observation_sequence,
            ),
            "l3_integrity": self.check_integrity(
                icao, nic, nac_p, observed_at, observation_sequence
            ),
        }

        notes: List[str] = []
        available = {k: v for k, v in layers.items() if v.available}

        if not available:
            notes.append("no layer had usable input; no assessment possible")
            return FusedAssessment(
                icao=icao, timestamp=datetime.now(timezone.utc), layers=layers,
                overall_score=0.0, threat_level="UNKNOWN",
                layers_available=[], corroborated=False, notes=notes,
            )

        total_weight = sum(self.BASE_WEIGHTS[k] for k in available)
        overall = sum(
            v.score * (self.BASE_WEIGHTS[k] / total_weight)
            for k, v in available.items()
        )

        asserting = [k for k, v in available.items() if v.score >= self.ASSERT_THRESHOLD]
        corroborated = len(asserting) >= 2
        if corroborated:
            overall = min(1.0, overall * 1.25)
            notes.append(
                "corroborated by independent layers: " + ", ".join(asserting)
            )

        # Honesty note: with only one layer reporting, the score is that layer's
        # opinion alone and shouldn't be presented as multi-layer detection.
        if len(available) == 1:
            notes.append(
                f"single-layer assessment ({next(iter(available))}); "
                "not corroborated"
            )
        missing = [k for k, v in layers.items() if not v.available]
        if missing:
            notes.append("unavailable layers: " + ", ".join(missing))
        notes.append("L4/L5 are evaluated by the upstream canonical pipeline, not this L1-L3 detector")

        if overall < 0.30:
            threat = "LOW"
        elif overall < 0.60:
            threat = "MEDIUM"
        elif overall < 0.80:
            threat = "HIGH"
        else:
            threat = "CRITICAL"

        return FusedAssessment(
            icao=icao, timestamp=datetime.now(timezone.utc), layers=layers,
            overall_score=overall, threat_level=threat,
            layers_available=sorted(available.keys()),
            corroborated=corroborated, notes=notes,
        )

    def forget(self, icao: str) -> None:
        """Drop per-aircraft history (call when a track ages out)."""
        self.previous_states.pop(icao, None)
        self.integrity_history.pop(icao, None)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    det = EnhancedAnomalyDetector()
    t0 = time.time()

    print("\n--- 1. Normal airliner, OpenSky-style feed (no NIC/NACp) ---")
    a = det.assess(
        icao="A1B2C3", lat=39.8717, lon=-75.2411, alt_baro=33000,
        alt_geo=33050, velocity=450, vertical_rate=0, heading=90,
        l1_result={"verdict": "LEGITIMATE", "confidence": 0.9,
                   "max_disagreement_m": 120.0, "sources_used": ["opensky", "adsb_lol"]},
        observed_at=t0,
    )
    print(f"  {a.threat_level} {a.overall_score:.3f}  layers={a.layers_available}")
    print(f"  L3 -> {a.layers['l3_integrity'].reason}")

    print("\n--- 2. Same aircraft 30s later, plausible movement ---")
    a = det.assess(
        icao="A1B2C3", lat=39.9000, lon=-75.1800, alt_baro=33000,
        alt_geo=33050, velocity=450, vertical_rate=0, heading=92,
        l1_result={"verdict": "LEGITIMATE", "confidence": 0.9,
                   "max_disagreement_m": 130.0, "sources_used": ["opensky", "adsb_lol"]},
        observed_at=t0 + 30,
    )
    k = a.layers["l2_kinematic"]
    print(f"  {a.threat_level} {a.overall_score:.3f}  implied={k.detail.get('implied_speed_kts', 0):.0f}kt")
    print(f"  (old code assumed dt=1s and would have called this ~13,000kt)")

    print("\n--- 3. Teleport: 400km jump in 30s ---")
    a = det.assess(
        icao="A1B2C3", lat=43.5000, lon=-75.1800, alt_baro=33000,
        alt_geo=33050, velocity=450, vertical_rate=0, heading=92,
        l1_result={"verdict": "SPOOFED", "confidence": 0.85,
                   "max_disagreement_m": 8200.0, "sources_used": ["opensky", "adsb_fi"]},
        observed_at=t0 + 60,
    )
    print(f"  {a.threat_level} {a.overall_score:.3f}  corroborated={a.corroborated}")
    print(f"  L2 -> {a.layers['l2_kinematic'].reason}")

    print("\n--- 4. Integrity anomaly, with metadata present ---")
    for i in range(6):
        det.assess(icao="DEADBE", lat=40.0 + i * 0.01, lon=-75.0,
                   velocity=300, heading=0, nic=8, nac_p=9,
                   observed_at=t0 + i * 10)
    a = det.assess(icao="DEADBE", lat=40.07, lon=-75.0, velocity=300,
                   heading=0, nic=0, nac_p=0, observed_at=t0 + 70)
    print(f"  {a.threat_level} {a.overall_score:.3f}")
    print(f"  L3 -> {a.layers['l3_integrity'].reason}")

    print("\n--- 5. No data at all ---")
    a = det.assess(icao="000000", lat=0.0, lon=0.0)
    print(f"  {a.threat_level} {a.overall_score:.3f}  notes={a.notes[0]}")