"""
ingestion/adsb_receiver.py
──────────────────────────
Polls OpenSky every 15 seconds.
Writes aircraft directly to Redis (key: ac:ICAO) so the API
can serve them immediately — no Kafka/fusion bottleneck.
Also publishes to Kafka for the anomaly detection pipeline.
"""

from __future__ import annotations
import asyncio, time, logging, json
from typing import Any, Optional

import aiohttp
import redis.asyncio as aioredis
from aiokafka import AIOKafkaProducer

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from models import RawADSBMessage
from config import settings
from coverage_area import COVERAGE_LOCK_KEY, coverage_url, load_coverage_area, load_coverage_area_record, within_coverage_area

log = logging.getLogger(__name__)

POLL_INTERVAL = 15   # seconds — stay well within OpenSky rate limits
REDIS_TTL     = 60   # seconds — aircraft expire if not refreshed

def adsb_lol_fallback_url(
    lat: float | None = None,
    lon: float | None = None,
    radius_nm: int | None = None,
) -> str:
    """Build a configurable adsb.lol area query (API maximum: 250 NM)."""
    lat = settings.ADSB_FALLBACK_LAT if lat is None else max(-90.0, min(90.0, lat))
    lon = settings.ADSB_FALLBACK_LON if lon is None else max(-180.0, min(180.0, lon))
    radius_nm = settings.ADSB_FALLBACK_RADIUS_NM if radius_nm is None else radius_nm
    radius_nm = max(0, min(250, int(radius_nm)))
    return f"https://api.adsb.lol/v2/point/{lat}/{lon}/{radius_nm}"


async def current_coverage_url(redis_client: Any) -> str:
    return coverage_url(await load_coverage_area(redis_client))


def _parse_adsb_lol_states(data: dict) -> list[list[Any]]:
    """Convert adsb.lol records into the subset of OpenSky's state shape used here."""
    states = []
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

        def number(value):
            try:
                return float(value)
            except (TypeError, ValueError):
                return None

        alt_ft = number(raw.get("alt_baro"))
        speed_kts = number(raw.get("gs"))
        vertical_fpm = number(raw.get("baro_rate"))
        state: list[Any] = [None] * 17
        state[0] = icao
        state[1] = str(raw.get("flight") or "").strip() or None
        state[5], state[6] = lon, lat
        state[7] = alt_ft / 3.28084 if alt_ft is not None else None
        state[8] = str(raw.get("alt_baro") or "").lower() == "ground"
        state[9] = speed_kts / 1.944 if speed_kts is not None else None
        state[10] = number(raw.get("track"))
        state[11] = vertical_fpm / 196.85 if vertical_fpm is not None else None
        states.append(state)
    return states


async def run() -> None:
    logging.basicConfig(level=settings.LOG_LEVEL,
                        format="%(asctime)s %(levelname)s %(message)s")
    log.info("ADS-B ingestor starting — OpenSky global feed, writing direct to Redis")

    redis_client = aioredis.from_url(settings.REDIS_URL, decode_responses=False)

    producer = AIOKafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP,
        value_serializer=lambda v: v,
        compression_type="lz4",
        linger_ms=20, acks=1,
    )
    await producer.start()

    headers = {"User-Agent": "SkySecure/2.0 airspace-research"}
    auth = aiohttp.BasicAuth(settings.OPENSKY_USERNAME, settings.OPENSKY_PASSWORD) if settings.OPENSKY_USERNAME else None

    async with aiohttp.ClientSession(headers=headers, auth=auth) as session:
        while True:
            t0 = time.time()
            try:
                area, coverage_token = await load_coverage_area_record(redis_client)
                async with session.get(
                    "https://opensky-network.org/api/states/all",
                    timeout=aiohttp.ClientTimeout(total=25),
                ) as resp:
                    source = "opensky"
                    if resp.status == 429:
                        log.warning("Rate limited by OpenSky — using adsb.lol fallback")
                        async with session.get(
                            coverage_url(area),
                            timeout=aiohttp.ClientTimeout(total=25),
                        ) as fallback_resp:
                            if fallback_resp.status != 200:
                                log.warning("adsb.lol fallback HTTP %d", fallback_resp.status)
                                await asyncio.sleep(POLL_INTERVAL)
                                continue
                            fallback_data = await fallback_resp.json(content_type=None)
                            states = _parse_adsb_lol_states(fallback_data)
                            recv_t = time.time()
                            source = "adsb_lol"
                            log.info("adsb.lol fallback returned %d states", len(states))
                    elif resp.status != 200:
                        log.warning("OpenSky HTTP %d", resp.status)
                        await asyncio.sleep(POLL_INTERVAL)
                        continue
                    else:
                        data = await resp.json(content_type=None)
                        states = data.get("states") or []
                        recv_t = float(data.get("time", time.time()))
                        log.info("OpenSky returned %d states", len(states))

                    _, current_token = await load_coverage_area_record(redis_client)
                    if current_token != coverage_token:
                        log.info("Coverage changed during fetch; discarding stale batch")
                        continue

                    pipe = redis_client.pipeline()
                    kafka_messages = []
                    published = 0

                    for s in states:
                        if not s or s[0] is None or s[5] is None or s[6] is None:
                            continue
                        icao = s[0].upper().strip()
                        if len(icao) != 6:
                            continue
                        try:
                            lat, lon = float(s[6]), float(s[5])
                        except (TypeError, ValueError):
                            continue
                        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                            continue
                        if not within_coverage_area(lat, lon, area):
                            continue

                        def ft(m):
                            try: return int(float(m) * 3.28084) if m else None
                            except: return None
                        def kts(ms):
                            try: return int(float(ms) * 1.944) if ms else None
                            except: return None

                        # Compact aircraft dict written directly to Redis
                        ac = {
                            "icao": icao,
                            "cs":   (s[1] or "").strip() or None,
                            "lat":  lat, "lon": lon,
                            "alt":  ft(s[7]), "vel": kts(s[9]),
                            "hdg":  float(s[10]) if s[10] else None,
                            "vr":   int(float(s[11]) * 196.85) if s[11] else None,
                            "gnd":  bool(s[8]) if s[8] is not None else False,
                            "src":  source,
                            "risk": 0, "anoms": [], "cls": "CIVILIAN",
                            "conf": 0.85, "mil": 0.0, "band": "NORMAL", "trail": [],
                        }

                        # Write to Redis directly — instantly visible to API
                        pipe.setex(
                            f"ac:{icao}",
                            REDIS_TTL,
                            json.dumps(ac).encode(),
                        )

                        # Also publish to Kafka for anomaly detection pipeline
                        try:
                            msg = RawADSBMessage(
                                receiver_id=source, recv_time=recv_t,
                                icao24=icao, raw_message="", msg_type=17,
                                callsign=ac["cs"], lat=lat, lon=lon,
                                altitude_baro=ft(s[7]),
                                velocity=kts(s[9]),
                                heading=float(s[10]) if s[10] else None,
                                vertical_rate=int(float(s[11]) * 196.85) if s[11] else None,
                                on_ground=bool(s[8]) if s[8] is not None else False,
                            )
                            kafka_messages.append((icao.encode(), msg.to_bytes()))
                        except Exception:
                            pass  # Kafka failure doesn't block display

                        published += 1

                    lock = redis_client.lock(COVERAGE_LOCK_KEY, timeout=30, blocking_timeout=5)
                    async with lock:
                        _, final_token = await load_coverage_area_record(redis_client)
                        if final_token != coverage_token:
                            log.info("Coverage changed during processing; discarding stale batch")
                            continue
                        for key, value in kafka_messages:
                            await lock.extend(30, replace_ttl=True)
                            try:
                                await asyncio.wait_for(
                                    producer.send(
                                        topic=settings.TOPIC_RAW_ADSB,
                                        key=key,
                                        value=value,
                                    ),
                                    timeout=2.0,
                                )
                            except asyncio.TimeoutError:
                                log.warning("Kafka send timed out for %s", key.decode(errors="ignore"))
                            except Exception:
                                pass  # Kafka failure does not block direct Redis display
                        await lock.extend(30, replace_ttl=True)
                        await asyncio.wait_for(pipe.execute(), timeout=5.0)
                    log.info("Wrote %d aircraft to Redis (ac:*)", published)

            except Exception as e:
                log.error("Ingestor error: %s", e)

            elapsed = time.time() - t0
            await asyncio.sleep(max(2.0, POLL_INTERVAL - elapsed))


if __name__ == "__main__":
    asyncio.run(run())