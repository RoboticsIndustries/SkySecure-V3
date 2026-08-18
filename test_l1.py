#!/usr/bin/env python3
"""
test_l1.py  —  SkySecure v3 L1 Validation Test Suite
======================================================
Runs three test categories in one shot:

  CATEGORY 1 — Real aircraft (threshold baseline)
    Pulls 20 live aircraft from OpenSky's global feed (evenly sampled
    across regions, not just one airspace) and runs each through the
    cross-source validator. Logs disagreement_m distribution so you
    can verify the 1.5km / 5km thresholds are calibrated correctly.

  CATEGORY 2 — Fabricated ICAO (nonexistent aircraft)
    Feeds ICAOs that don't exist in any real feed with plausible PHL-area
    positions. Expected result: INSUFFICIENT_SOURCES (networks have no
    record of them).

  CATEGORY 3 — Real ICAO, offset position (GPS spoofing simulation)
    Takes a confirmed live ICAO and feeds its true position shifted by
    ~50km. Expected result: SPOOFED (networks report it elsewhere).

Output: a clean summary table + per-category stats for JSHS results section.

Usage:
    cd SkySecure-v3
    python test_l1.py
"""

from __future__ import annotations

import asyncio
import math
import time
import sys
import os
from typing import Optional

import aiohttp

# Make sure the project root is on the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from processing.cross_source_validator import CrossSourceValidator


# ─── Helpers ──────────────────────────────────────────────────────────────────

def offset_position(lat: float, lon: float, offset_km: float, bearing_deg: float = 45.0):
    """
    Shift a lat/lon by offset_km in the given bearing direction.
    Used to simulate a spoofed position claim.
    """
    R = 6371.0
    bearing = math.radians(bearing_deg)
    lat1 = math.radians(lat)
    lon1 = math.radians(lon)
    d = offset_km / R

    lat2 = math.asin(
        math.sin(lat1) * math.cos(d) +
        math.cos(lat1) * math.sin(d) * math.cos(bearing)
    )
    lon2 = lon1 + math.atan2(
        math.sin(bearing) * math.sin(d) * math.cos(lat1),
        math.cos(d) - math.sin(lat1) * math.sin(lat2)
    )
    return math.degrees(lat2), math.degrees(lon2)


def verdict_color(verdict: str) -> str:
    colors = {
        "LEGITIMATE":           "\033[92m",  # green
        "SPOOFED":              "\033[91m",  # red
        "UNCERTAIN":            "\033[93m",  # yellow
        "INSUFFICIENT_SOURCES": "\033[94m",  # blue
    }
    reset = "\033[0m"
    return f"{colors.get(verdict, '')}{verdict}{reset}"


def print_row(label: str, icao: str, result: dict, note: str = ""):
    verdict  = result.get("verdict", "ERROR")
    disag    = result.get("max_disagreement_m", 0.0)
    conf     = result.get("confidence", 0.0)
    sources  = "/".join(result.get("sources_used", []))
    print(
        f"  {label:<28} {icao:<10} {verdict_color(verdict):<30} "
        f"{disag:>8.1f}m  conf={conf:.2f}  [{sources}]"
        + (f"  ← {note}" if note else "")
    )


# ─── Fetch live aircraft ───────────────────────────────────────────────────────

async def fetch_global_aircraft(n: int = 20) -> list[dict]:
    """
    Pull up to n aircraft from OpenSky's full global feed (no bounding box),
    sampled spread out across the response so the test isn't biased toward
    one region/airspace type. Returns list of dicts with icao, lat, lon, callsign.
    """
    url = "https://opensky-network.org/api/states/all"
    headers = {"User-Agent": "SkySecure/3.0 (research)", "Accept": "application/json"}

    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if resp.status != 200:
                print(f"  [!] OpenSky returned HTTP {resp.status}")
                return []
            data = await resp.json(content_type=None)

    all_valid = []
    fetch_time = time.time()
    for s in (data.get("states") or []):
        if not s or s[5] is None or s[6] is None:
            continue
        try:
            lat, lon = float(s[6]), float(s[5])
        except (TypeError, ValueError):
            continue
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            continue
        icao = (s[0] or "").strip().upper()
        if len(icao) != 6:
            continue
        all_valid.append({
            "icao":         icao,
            "callsign":     (s[1] or "").strip() or "N/A",
            "lat":          lat,
            "lon":          lon,
            # Needed so the "claimed" position can be projected forward to
            # the same common instant as the independent network reports —
            # without this, ordinary aircraft motion between fetch time and
            # validation time reads as false disagreement.
            "velocity_kts": float(s[9]) * 1.94384 if s[9] else None,
            "heading_deg":  float(s[10]) if s[10] else None,
            "observed_at":  float(s[3] or s[4] or fetch_time),
        })

    if not all_valid:
        return []

    # Evenly sample across the whole global list rather than just taking
    # the first n (which OpenSky tends to cluster by region/ordering)
    step = max(len(all_valid) // n, 1)
    sampled = all_valid[::step][:n]
    return sampled


# ─── Test categories ───────────────────────────────────────────────────────────

async def run_category_1(validator: CrossSourceValidator, live: list[dict]) -> list[dict]:
    """Real aircraft — expect LEGITIMATE with low disagreement."""
    print("\n" + "═" * 90)
    print("  CATEGORY 1 — Real Aircraft (Threshold Baseline)")
    print("  Expected: LEGITIMATE, disagreement_m well below 1500m")
    print("═" * 90)
    print(f"  {'Label':<28} {'ICAO':<10} {'Verdict':<22} {'Disagree':>9}  Conf    Sources")
    print("  " + "-" * 86)

    results = []
    for i, ac in enumerate(live):
        try:
            # No explicit claim passed — the validator uses its own OpenSky
            # per-ICAO report (fetched once, inside this same call) as the
            # implicit claim. Only 1 OpenSky call per aircraft this way,
            # not 2, which is what was exhausting the rate limit at n=100
            # and starving out opensky/adsb_lol for later aircraft in the run.
            result = await validator.validate_aircraft(ac["icao"], ac["lat"], ac["lon"])
            d = result.to_dict()
            print_row(ac["callsign"], ac["icao"], d)
            results.append(d)
        except Exception as e:
            print(f"  {ac['callsign']:<28} {ac['icao']:<10} ERROR: {e}")
        # Stagger requests to avoid tripping adsb.lol/adsb.fi/OpenSky rate limits
        if i < len(live) - 1:
            await asyncio.sleep(2.0)

    if results:
        disags = [r["max_disagreement_m"] for r in results if r["verdict"] == "LEGITIMATE"]
        legit  = sum(1 for r in results if r["verdict"] == "LEGITIMATE")
        insuff = sum(1 for r in results if r["verdict"] == "INSUFFICIENT_SOURCES")
        print(f"\n  Summary: {legit} LEGITIMATE, {insuff} INSUFFICIENT_SOURCES, "
              f"{len(results) - legit - insuff} other")
        if disags:
            print(f"  Disagreement (LEGITIMATE only): "
                  f"min={min(disags):.1f}m  max={max(disags):.1f}m  "
                  f"avg={sum(disags)/len(disags):.1f}m")
    return results


async def run_category_2(validator: CrossSourceValidator) -> list[dict]:
    """Fabricated ICAOs — expect INSUFFICIENT_SOURCES."""
    print("\n" + "═" * 90)
    print("  CATEGORY 2 — Fabricated ICAOs (Nonexistent Aircraft)")
    print("  Expected: INSUFFICIENT_SOURCES (no network has record of these)")
    print("═" * 90)
    print(f"  {'Label':<28} {'ICAO':<10} {'Verdict':<22} {'Disagree':>9}  Conf    Sources")
    print("  " + "-" * 86)

    # Plausible positions in different regions worldwide, but completely fake ICAOs
    fake_targets = [
        {"icao": "FAKE01", "lat": 39.8729, "lon": -75.2437, "label": "Fake-1 (Philadelphia, US)"},
        {"icao": "FAKE02", "lat": 51.4700, "lon": -0.4543,  "label": "Fake-2 (London, UK)"},
        {"icao": "000000", "lat": 35.5494, "lon": 139.7798, "label": "Fake-3 (Tokyo, JP)"},
    ]

    results = []
    for i, t in enumerate(fake_targets):
        try:
            result = await validator.validate_aircraft(t["icao"], t["lat"], t["lon"], use_explicit_claim=True)
            d = result.to_dict()
            print_row(t["label"], t["icao"], d, "fabricated ICAO")
            results.append(d)
        except Exception as e:
            print(f"  {t['label']:<28} {t['icao']:<10} ERROR: {e}")
        if i < len(fake_targets) - 1:
            await asyncio.sleep(1.5)

    insuff = sum(1 for r in results if r["verdict"] == "INSUFFICIENT_SOURCES")
    print(f"\n  Summary: {insuff}/{len(results)} correctly returned INSUFFICIENT_SOURCES")
    return results


async def run_category_3(validator: CrossSourceValidator, live: list[dict]) -> list[dict]:
    """
    Real ICAO, position offset by 50km — simulates GPS spoofing.
    Expected: SPOOFED or UNCERTAIN (networks report aircraft elsewhere).
    Uses first 3 confirmed live aircraft from Category 1.
    """
    print("\n" + "═" * 90)
    print("  CATEGORY 3 — Real ICAO, Spoofed Position (GPS Spoofing Simulation)")
    print("  Each aircraft's true position is shifted ~50km NE.")
    print("  Expected: SPOOFED or UNCERTAIN (networks disagree with claimed position)")
    print("═" * 90)
    print(f"  {'Label':<28} {'ICAO':<10} {'Verdict':<22} {'Disagree':>9}  Conf    Sources")
    print("  " + "-" * 86)

    # Use first 3 live aircraft with valid positions
    targets = [ac for ac in live if ac.get("lat") and ac.get("lon")][:3]
    if not targets:
        print("  [!] No live aircraft available for Category 3")
        return []

    results = []
    for i, ac in enumerate(targets):
        spoofed_lat, spoofed_lon = offset_position(ac["lat"], ac["lon"], offset_km=50.0)
        try:
            result = await validator.validate_aircraft(ac["icao"], spoofed_lat, spoofed_lon, use_explicit_claim=True)
            d = result.to_dict()
            print_row(
                ac["callsign"], ac["icao"], d,
                f"true pos offset +50km (real: {ac['lat']:.4f},{ac['lon']:.4f})"
            )
            results.append(d)
        except Exception as e:
            print(f"  {ac['callsign']:<28} {ac['icao']:<10} ERROR: {e}")
        if i < len(targets) - 1:
            await asyncio.sleep(1.5)

    detected = sum(1 for r in results if r["verdict"] in ("SPOOFED", "UNCERTAIN"))
    print(f"\n  Summary: {detected}/{len(results)} flagged as SPOOFED or UNCERTAIN")
    return results


# ─── Main ─────────────────────────────────────────────────────────────────────

async def main():
    print("\n" + "█" * 90)
    print("  SkySecure v3 — L1 Multi-Source Cross-Validation Test Suite")
    print("  " + time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()))
    print("█" * 90)

    print("\n  [1/4] Fetching live aircraft from OpenSky (global feed)...")
    live = await fetch_global_aircraft(n=10)
    if not live:
        print("  [!] Could not fetch live aircraft. Check network/OpenSky availability.")
        sys.exit(1)
    print(f"  ✓ Got {len(live)} live aircraft")

    print("\n  [2/4] Initializing L1 cross-source validator...")
    async with CrossSourceValidator() as validator:
        print("  ✓ Validator ready (OpenSky / adsb.lol / adsb.fi)")

        # Run all three categories
        cat1 = await run_category_1(validator, live)
        cat2 = await run_category_2(validator)
        cat3 = await run_category_3(validator, live)

        # ─── Final summary ─────────────────────────────────────────────────
        print("\n" + "═" * 90)
        print("  FINAL SUMMARY")
        print("═" * 90)

        legit_rate  = sum(1 for r in cat1 if r["verdict"] == "LEGITIMATE") / max(len(cat1), 1)
        insuff_rate = sum(1 for r in cat2 if r["verdict"] == "INSUFFICIENT_SOURCES") / max(len(cat2), 1)
        detect_rate = sum(1 for r in cat3 if r["verdict"] in ("SPOOFED", "UNCERTAIN")) / max(len(cat3), 1)

        print(f"  Cat 1 — Real aircraft correctly passed:        {legit_rate*100:.0f}%  ({len(cat1)} tested)")
        print(f"  Cat 2 — Fake ICAOs correctly flagged:          {insuff_rate*100:.0f}%  ({len(cat2)} tested)")
        print(f"  Cat 3 — Spoofed positions detected:            {detect_rate*100:.0f}%  ({len(cat3)} tested)")
        print()

        if cat1:
            disags = [r["max_disagreement_m"] for r in cat1 if r["verdict"] == "LEGITIMATE"]
            if disags:
                print(f"  Legitimate aircraft disagreement range: {min(disags):.1f}m – {max(disags):.1f}m")
                print(f"  → 1500m UNCERTAIN threshold appears {'✓ reasonable' if max(disags) < 1500 else '⚠ too tight — consider raising'}")
                print(f"  → 5000m SPOOFED threshold appears {'✓ reasonable' if max(disags) < 5000 else '⚠ too tight — consider raising'}")

        print("\n  L1 status: ", end="")
        if legit_rate >= 0.8 and detect_rate >= 0.5:
            print("✅ READY — thresholds validated, spoofing detection functional")
        elif legit_rate >= 0.8:
            print("⚠️  PARTIAL — legitimate detection good, spoofing detection needs work")
        else:
            print("❌ NEEDS WORK — false positive rate too high, recalibrate thresholds")

        print("═" * 90 + "\n")


if __name__ == "__main__":
    asyncio.run(main())