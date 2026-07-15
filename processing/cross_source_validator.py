"""
processing/cross_source_validator.py
=====================================
L1 Detection Layer — Multi-Source Position Cross-Validation.

HONEST SCOPE NOTE (read before wiring this up or citing it in the JSHS paper):
True TDOA multilateration requires physically distributed, GPS-disciplined
receivers you own, so you can compare raw arrival timestamps of the SAME
transmission at each site. Without that hardware, there is no legitimate way
to produce real TDOA — any "receive_times" dict is fabricated.

What this module does instead, using only data you can pull right now:
compares the SAME aircraft's position as independently computed by separate
live ADS-B aggregator networks (OpenSky, adsb.lol, adsb.fi). Each network
runs its own receivers and its own MLAT solver, so their position estimates
are independent measurements of the same physical aircraft. Real disagreement
between them — beyond what normal reporting latency/interpolation explains —
is a genuine anomaly signal computed from live data.

This is NOT TDOA. It has coarser resolution (positions are already fused by
each network, not raw timing) and fewer independent "baselines" (2-3 networks
vs 4+ receivers). Label it as such in any paper or pitch: "L1 — Multi-Source
Position Cross-Validation (TDOA-equivalent; upgrades to true TDOA pending
receiver hardware, see L4)".

When receivers exist: point processing/mlat_solver.py at real Beast-format
feeds from your own RTL-SDR sites and retire this module for genuine TDOA.
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import aiohttp

logger = logging.getLogger(__name__)

EARTH_RADIUS_M = 6371000.0

# Independent live networks. Each has its own receiver network + MLAT solver,
# so they are genuinely independent measurements — not the same data twice.
SOURCES = {
    "opensky": {
        "url": "https://opensky-network.org/api/states/all",
        "parser": "opensky",
    },
    "adsb_lol": {
        "url": "https://api.adsb.lol/v2/point/{lat}/{lon}/250",
        "parser": "adsb_lol",
        "needs_point": True,
    },
    "adsb_fi": {
        "url": "https://opendata.adsb.fi/api/v2/lat/{lat}/lon/{lon}/dist/250",
        "parser": "adsb_fi",
        "needs_point": True,
    },
}

# Disagreement thresholds. These are NOT arbitrary — they're set by what
# cross-network latency/interpolation error actually looks like (networks
# poll independently, typically 1-15s apart, and an airliner covers
# ~130-260m/s, so a few seconds of skew alone can produce hundreds of
# meters of "disagreement" with zero spoofing involved). Time-projection
# below removes most of that; residual thresholds stay conservative.
DISAGREEMENT_UNCERTAIN_M = 1500.0   # beyond normal projection error
DISAGREEMENT_SPOOFED_M = 5000.0     # implausible for real aircraft kinematics


def haversine_m(lat1, lon1, lat2, lon2) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * EARTH_RADIUS_M * math.asin(math.sqrt(a))


@dataclass
class SourceReport:
    source: str
    icao: str
    lat: float
    lon: float
    alt_ft: Optional[float]
    velocity_kts: Optional[float]
    heading_deg: Optional[float]
    observed_at: float  # unix time this position was valid at (source-reported, not fetch time)

    def project_to(self, t: float) -> "SourceReport":
        """Dead-reckon this report forward/backward to time t using its own
        velocity/heading, so two reports from different poll times can be
        compared at a common instant. Straight-line approximation — fine
        for the few seconds of typical cross-network skew, not for minutes."""
        dt = t - self.observed_at
        if self.velocity_kts is None or self.heading_deg is None or abs(dt) > 30:
            return self
        speed_mps = self.velocity_kts * 0.514444
        dist_m = speed_mps * dt
        brg = math.radians(self.heading_deg)
        lat_rad = math.radians(self.lat)
        lon_rad = math.radians(self.lon)
        ang = dist_m / EARTH_RADIUS_M
        new_lat = math.asin(
            math.sin(lat_rad) * math.cos(ang)
            + math.cos(lat_rad) * math.sin(ang) * math.cos(brg)
        )
        new_lon = lon_rad + math.atan2(
            math.sin(brg) * math.sin(ang) * math.cos(lat_rad),
            math.cos(ang) - math.sin(lat_rad) * math.sin(new_lat),
        )
        return SourceReport(
            source=self.source, icao=self.icao,
            lat=math.degrees(new_lat), lon=math.degrees(new_lon),
            alt_ft=self.alt_ft, velocity_kts=self.velocity_kts,
            heading_deg=self.heading_deg, observed_at=t,
        )


@dataclass
class CrossValidationResult:
    icao: str
    is_valid: bool
    max_disagreement_m: float
    confidence: float
    sources_used: List[str]
    verdict: str  # "LEGITIMATE", "UNCERTAIN", "SPOOFED", "INSUFFICIENT_SOURCES"
    per_pair_m: Dict[str, float] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return {
            "icao": self.icao, "is_valid": self.is_valid,
            "max_disagreement_m": round(self.max_disagreement_m, 1),
            "confidence": round(self.confidence, 3),
            "sources_used": self.sources_used,
            "verdict": self.verdict,
            "per_pair_m": {k: round(v, 1) for k, v in self.per_pair_m.items()},
            "timestamp": self.timestamp,
        }


class CrossSourceValidator:
    """
    Pulls the same aircraft from independent live ADS-B networks and
    cross-checks position agreement. Real data only — no fabricated
    receive_times, no synthetic receivers.
    """

    def __init__(self, session: Optional[aiohttp.ClientSession] = None):
        self._session = session
        self._owns_session = session is None

    async def __aenter__(self):
        if self._session is None:
            self._session = aiohttp.ClientSession(
                headers={"User-Agent": "SkySecure/2.0 (airspace-research; L1-cross-validation)"}
            )
        return self

    async def __aexit__(self, *exc):
        if self._owns_session and self._session:
            await self._session.close()

    # ── Fetching ──────────────────────────────────────────────────────

    async def _fetch_opensky(self, icao: str) -> Optional[SourceReport]:
        try:
            async with self._session.get(
                SOURCES["opensky"]["url"],
                params={"icao24": icao.lower()},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json(content_type=None)
                states = data.get("states") or []
                if not states:
                    return None
                s = states[0]
                if s[6] is None or s[5] is None:
                    return None
                return SourceReport(
                    source="opensky", icao=icao,
                    lat=float(s[6]), lon=float(s[5]),
                    alt_ft=float(s[7]) * 3.28084 if s[7] else None,
                    velocity_kts=float(s[9]) * 1.94384 if s[9] else None,
                    heading_deg=float(s[10]) if s[10] else None,
                    observed_at=float(s[3] or s[4] or data.get("time", time.time())),
                )
        except Exception as e:
            logger.warning(f"OpenSky fetch failed for {icao}: {e}")
            return None

    async def _fetch_point_source(self, name: str, icao: str, near_lat: float, near_lon: float) -> Optional[SourceReport]:
        """adsb.lol / adsb.fi are point+radius APIs, not lookup-by-icao,
        so we query around the aircraft's last known position and filter."""
        try:
            url = SOURCES[name]["url"].format(lat=near_lat, lon=near_lon)
            async with self._session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json(content_type=None)
                aircraft = data.get("ac") or data.get("aircraft") or []
                for a in aircraft:
                    a_icao = (a.get("hex") or "").upper().lstrip("~")
                    if a_icao != icao.upper():
                        continue
                    lat, lon = a.get("lat"), a.get("lon")
                    if lat is None or lon is None:
                        continue
                    return SourceReport(
                        source=name, icao=icao,
                        lat=float(lat), lon=float(lon),
                        alt_ft=a.get("alt_baro") if isinstance(a.get("alt_baro"), (int, float)) else None,
                        velocity_kts=a.get("gs"),
                        heading_deg=a.get("track"),
                        observed_at=time.time() - float(a.get("seen", 0) or 0),
                    )
                return None
        except Exception as e:
            logger.warning(f"{name} fetch failed for {icao}: {e}")
            return None

    async def gather_reports(self, icao: str, approx_lat: float, approx_lon: float) -> List[SourceReport]:
        """Query all independent networks concurrently for one aircraft."""
        tasks = [
            self._fetch_opensky(icao),
            self._fetch_point_source("adsb_lol", icao, approx_lat, approx_lon),
            self._fetch_point_source("adsb_fi", icao, approx_lat, approx_lon),
        ]
        results = await asyncio.gather(*tasks, return_exceptions=False)
        return [r for r in results if r is not None]

    # ── Validation ────────────────────────────────────────────────────

    def validate_reports(self, icao: str, reports: List[SourceReport]) -> CrossValidationResult:
        if len(reports) < 2:
            return CrossValidationResult(
                icao=icao, is_valid=True, max_disagreement_m=0.0, confidence=0.0,
                sources_used=[r.source for r in reports],
                verdict="INSUFFICIENT_SOURCES",
            )

        # Project every report to the most recent observed_at so we're
        # comparing positions at a common instant, not raw poll-time snapshots.
        t_common = max(r.observed_at for r in reports)
        projected = [r.project_to(t_common) for r in reports]

        per_pair: Dict[str, float] = {}
        max_disagreement = 0.0
        for i in range(len(projected)):
            for j in range(i + 1, len(projected)):
                a, b = projected[i], projected[j]
                dist = haversine_m(a.lat, a.lon, b.lat, b.lon)
                per_pair[f"{a.source}-{b.source}"] = dist
                max_disagreement = max(max_disagreement, dist)

        if max_disagreement < DISAGREEMENT_UNCERTAIN_M:
            verdict = "LEGITIMATE"
            confidence = max(0.7, 1.0 - (max_disagreement / DISAGREEMENT_UNCERTAIN_M) * 0.3)
            is_valid = True
        elif max_disagreement < DISAGREEMENT_SPOOFED_M:
            verdict = "UNCERTAIN"
            span = DISAGREEMENT_SPOOFED_M - DISAGREEMENT_UNCERTAIN_M
            confidence = max(0.3, 0.7 - ((max_disagreement - DISAGREEMENT_UNCERTAIN_M) / span) * 0.4)
            is_valid = False
        else:
            verdict = "SPOOFED"
            confidence = min(0.95, 0.3 + (max_disagreement - DISAGREEMENT_SPOOFED_M) / DISAGREEMENT_SPOOFED_M * 0.65)
            is_valid = False

        return CrossValidationResult(
            icao=icao, is_valid=is_valid, max_disagreement_m=max_disagreement,
            confidence=confidence, sources_used=[r.source for r in reports],
            verdict=verdict, per_pair_m=per_pair,
        )

    async def validate_aircraft(self, icao: str, approx_lat: float, approx_lon: float) -> CrossValidationResult:
        reports = await self.gather_reports(icao, approx_lat, approx_lon)
        return self.validate_reports(icao, reports)


# ── Manual smoke test against live networks ────────────────────────────
# Not a unit test with fixtures — genuinely hits the live APIs. Needs
# outbound network access to opensky-network.org / adsb.lol / adsb.fi.
async def _smoke_test():
    logging.basicConfig(level=logging.INFO)
    icao = "ada296"       # replace with a real live ICAO24 near you
    lat, lon =  39.9659, -75.605   # approx last-known position, for the point APIs
    async with CrossSourceValidator() as validator:
        result = await validator.validate_aircraft(icao, lat, lon)
        print(result.to_dict())


if __name__ == "__main__":
    asyncio.run(_smoke_test())
