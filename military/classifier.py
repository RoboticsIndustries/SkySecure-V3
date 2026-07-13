"""
military/classifier.py
──────────────────────
Probabilistic military aircraft identification.

Combines multiple weak signals into P(military) ∈ [0, 1]:

  1. ICAO24 address block lookup (strongest signal)
  2. No callsign / anonymized callsign
  3. Mode S only (no ADS-B position broadcast)
  4. Formation flying pattern
  5. Proximity to known military bases
  6. Operation in restricted/military airspace
  7. Irregular / non-commercial flight pattern
  8. ACARS absence
  9. RF signal fingerprint (if available)

Classification thresholds:
  P(mil) ≥ 0.80 → CONFIRMED_MILITARY
  P(mil) ≥ 0.50 → LIKELY_MILITARY
  P(mil) < 0.50 → CIVILIAN / UNKNOWN
"""

from __future__ import annotations

import math
import logging
from dataclasses import dataclass
from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from models import StateVector, Classification, DataSource

log = logging.getLogger(__name__)


# ─── Known Military ICAO Ranges ───────────────────────────────────────────────
# Sourced from public aviation databases and research papers.
# Comprehensive list maintained in DB; this is a fast in-memory cache.

# CONFIRMED exclusively-military ICAO blocks.
# Ranges that contain significant civilian traffic are excluded.
# China (0x78xxxx), Russia (0x10xxxx-0x1Fxxxx), Australia (0x7Cxxxx)
# and Japan (0x72xxxx) are NOT included because their blocks are shared
# with large civilian fleets — including them causes massive false positives.
# Sources: ICAO Doc 9303, OpenSky military-mode-s DB, Junzi Sun (TU Delft).
MILITARY_ICAO_RANGES: List[Tuple[int, int, str, str]] = [
    # ── United States (exclusively DOD) ───────────────────────
    (0xADF000, 0xADFFFF, "US", "USAF"),           # USAF airlift/tanker
    (0xAE0000, 0xAFFFFF, "US", "US-DOD"),          # All US military services
    (0xA9F000, 0xA9FFFF, "US", "USCG"),            # US Coast Guard

    # ── United Kingdom ────────────────────────────────────────
    (0x43C000, 0x43CFFF, "GB", "RAF"),
    (0x43D000, 0x43D7FF, "GB", "RN"),              # Royal Navy Fleet Air Arm

    # ── France ────────────────────────────────────────────────
    (0x3C4000, 0x3C47FF, "FR", "AdlA"),            # Armée de l'Air
    (0x3C6000, 0x3C63FF, "FR", "MN"),              # Marine Nationale

    # ── Germany ───────────────────────────────────────────────
    (0x3C0000, 0x3C07FF, "DE", "Luftwaffe"),       # Narrow confirmed range only

    # ── Italy ─────────────────────────────────────────────────
    (0x47A000, 0x47AFFF, "IT", "AMI"),

    # ── Spain ─────────────────────────────────────────────────
    (0x340000, 0x3407FF, "ES", "EdA"),

    # ── Netherlands ───────────────────────────────────────────
    (0x480000, 0x4807FF, "NL", "KLu"),

    # ── NATO ──────────────────────────────────────────────────
    (0x4D2000, 0x4D2FFF, "NATO", "AWACS"),
    (0x4D0000, 0x4D0FFF, "NATO", "NAEW"),
]

# Compile to sorted list for O(log n) binary search
_SORTED_RANGES = sorted(MILITARY_ICAO_RANGES, key=lambda x: x[0])


def icao_to_int(icao24: str) -> Optional[int]:
    try:
        return int(icao24, 16)
    except ValueError:
        return None


def lookup_military_block(icao24: str) -> Optional[Tuple[str, str]]:
    """Returns (country, service) if ICAO falls in a known military block, else None."""
    val = icao_to_int(icao24)
    if val is None:
        return None

    for start, end, country, service in _SORTED_RANGES:
        if start <= val <= end:
            return (country, service)
        if start > val:
            break   # sorted, no need to continue

    return None


# ─── Known Military Bases ─────────────────────────────────────────────────────
# (lat, lon, name, country)
MILITARY_BASES: List[Tuple[float, float, str, str]] = [
    (38.811, -104.759, "Peterson AFB", "US"),
    (38.050, -84.606, "Bluegrass Airport / USAF", "US"),
    (51.551,  -1.783, "RAF Brize Norton", "GB"),
    (51.750,   0.497, "RAF Wattisham", "GB"),
    (48.778,   2.392, "Villacoublay AB", "FR"),
    (47.886,  11.530, "Neubiberg AB", "DE"),
    (55.610,  37.668, "Kubinka AB", "RU"),
    (39.900, 116.400, "PLAAF Beijing", "CN"),
    # ... in production: load full database of ~2000 military airfields
]


def nearest_military_base(lat: float, lon: float) -> Tuple[float, Optional[str]]:
    """Returns (distance_nm, base_name) for nearest known military base."""
    if not lat or not lon:
        return 9999.0, None

    min_dist = 9999.0
    nearest = None

    for blat, blon, name, _ in MILITARY_BASES:
        dist = haversine_nm(lat, lon, blat, blon)
        if dist < min_dist:
            min_dist = dist
            nearest = name

    return min_dist, nearest


def haversine_nm(lat1, lon1, lat2, lon2) -> float:
    R = 3440.065
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


# ─── Military Classifier ──────────────────────────────────────────────────────

@dataclass
class MilitarySignals:
    """Evidence accumulation structure."""
    icao_block_match: bool     = False
    icao_block_label: str      = ""
    no_callsign:      bool     = False
    mode_s_only:      bool     = False   # MLAT but no ADS-B
    anon_callsign:    bool     = False   # "BLOCKED", "MILITARY", "REDACTED", etc.
    near_mil_base:    bool     = False
    base_distance_nm: float    = 9999.0
    base_name:        str      = ""
    no_acars:         bool     = False
    irregular_path:   bool     = False   # from anomaly engine
    formation_flag:   bool     = False


class MilitaryClassifier:
    """
    Bayesian-inspired score that accumulates evidence of military identity.

    Each signal is assigned a weight based on its discriminative power.
    Signals are (roughly) treated as conditionally independent for simplicity;
    in production, a proper Naive Bayes or logistic regression classifier
    trained on labelled data would replace this.
    """

    # Signal weights (must sum to ≤ 1.0 to cap at CONFIRMED with all signals)
    WEIGHTS = {
        "icao_block_match": 0.55,   # strongest: direct ICAO registration
        "mode_s_only":       0.20,
        "near_mil_base":     0.15,
        "no_callsign":       0.10,
        "anon_callsign":     0.15,
        "no_acars":          0.05,
        "irregular_path":    0.10,
        "formation_flag":    0.25,
    }

    # Anonymized callsign patterns used by military
    ANON_PATTERNS = {
        "BLOCKED", "MILITARY", "SPAR", "RCH", "REACH",   # USAF mobility
        "JAKE", "PACK", "DOOM", "EVIL", "KNIFE",           # tactical callsigns
        "INTEL", "GREY", "BLACK", "SHADOW", "GHOST",
        "NCMF", "NIGHT",
    }

    def classify(self, sv: StateVector) -> Tuple[float, MilitarySignals]:
        """
        Returns (P_military, signals) where P_military ∈ [0, 1].
        """
        sig = MilitarySignals()

        # ── Signal 1: ICAO block ──────────────────────────────────
        block = lookup_military_block(sv.icao24)
        if block:
            sig.icao_block_match = True
            sig.icao_block_label = f"{block[1]} ({block[0]})"

        # ── Signal 2: Mode S only (no ADS-B) ─────────────────────
        if DataSource.MLAT in sv.sources and DataSource.ADSB not in sv.sources:
            sig.mode_s_only = True

        # ── Signal 3: No callsign ─────────────────────────────────
        if not sv.callsign:
            sig.no_callsign = True
        elif sv.callsign.upper().split()[0] in self.ANON_PATTERNS:
            sig.anon_callsign = True

        # ── Signal 4: Near military base ──────────────────────────
        if sv.lat and sv.lon:
            dist, name = nearest_military_base(sv.lat, sv.lon)
            sig.base_distance_nm = dist
            if name:
                sig.base_name = name
            if dist < 50.0:
                sig.near_mil_base = True

        # ── Signal 5: No ACARS ────────────────────────────────────
        if DataSource.ACARS not in sv.sources and sv.update_count > 10:
            sig.no_acars = True

        # ── Signal 6: Irregular path (from anomaly engine) ────────
        from models import AnomalyType
        if any(a.anomaly_type == AnomalyType.MILITARY_BEHAVIOR for a in sv.anomalies):
            sig.irregular_path = True

        # ── Signal 7: Formation flying ────────────────────────────
        if any(a.anomaly_type == AnomalyType.FORMATION_FLIGHT for a in sv.anomalies):
            sig.formation_flag = True

        # ── Compute P(military) ───────────────────────────────────
        score = 0.0
        for attr, weight in self.WEIGHTS.items():
            if getattr(sig, attr, False):
                score += weight

        p_military = min(1.0, score)

        return p_military, sig

    def apply_classification(self, sv: StateVector) -> StateVector:
        p_mil, signals = self.classify(sv)
        sv.military_score = p_mil

        if p_mil >= 0.80:
            sv.classification = Classification.CONFIRMED_MILITARY
        elif p_mil >= 0.50:
            sv.classification = Classification.LIKELY_MILITARY
        elif signals.mode_s_only and not sv.callsign:
            sv.classification = Classification.DARK_AIRCRAFT
        elif sv.classification not in (Classification.CONFIRMED_MILITARY,
                                        Classification.LIKELY_MILITARY,
                                        Classification.DARK_AIRCRAFT):
            sv.classification = Classification.CIVILIAN

        if p_mil >= 0.50:
            log.debug(
                "Military flag: %s  P=%.2f  signals=%s",
                sv.icao24, p_mil,
                [k for k, w in self.WEIGHTS.items() if getattr(signals, k, False)]
            )

        return sv


# ─── Formation Detector ───────────────────────────────────────────────────────

class FormationDetector:
    """
    Detects formation flight patterns across the active track database.
    Formation: ≥2 aircraft within DIST NM, same heading ±HDG deg, same alt ±ALT ft.
    Runs periodically (not per-message) against all live state vectors.
    """

    def __init__(self, redis_client) -> None:
        self.redis = redis_client
        self._formation_groups: Dict[str, List[str]] = {}  # leader_icao → member ICAOs 

    async def scan(self) -> List[List[str]]:
        """
        Scan all active state vectors for formation patterns.
        Returns list of formation groups (each a list of ICAO24 strings).
        """
        import redis.asyncio as aioredis

        # Fetch all active state vectors from Redis
        keys = await self.redis.keys("sv:*")
        if not keys: 
            return []
        pipe = self.redis.pipeline()
        for k in keys:
            pipe.get(k)
        raw_values = await pipe.execute()

        vectors: List[StateVector] = []
        for raw in raw_values:
            if raw:
                try:
                    vectors.append(StateVector.from_bytes(raw))
                except Exception:
                    pass

        # Filter to airborne aircraft with position + heading
        airborne = [
            v for v in vectors
            if v.lat and v.lon and v.heading is not None
            and not v.on_ground and v.altitude_baro and v.altitude_baro > 500
        ]

        formations = []
        used = set()

        for i, a in enumerate(airborne):
            if a.icao24 in used:
                continue
            group = [a.icao24]

            for j, b in enumerate(airborne):
                if i == j or b.icao24 in used:
                    continue

                dist = haversine_nm(a.lat, a.lon, b.lat, b.lon)
                hdg_diff = abs(a.heading - b.heading) % 360
                hdg_diff = min(hdg_diff, 360 - hdg_diff)
                alt_diff = abs((a.altitude_baro or 0) - (b.altitude_baro or 0))

                if (dist < 1.0 and hdg_diff < 10.0 and alt_diff < 500):
                    group.append(b.icao24)

            if len(group) >= 2:
                for icao in group:
                    used.add(icao)
                formations.append(group)

        return formations
