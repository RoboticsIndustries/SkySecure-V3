"""
ingestion/adsb_receiver.py
──────────────────────────
Polls OpenSky every 15 seconds.
Writes aircraft directly to Redis (key: ac:ICAO) so the API
can serve them immediately — no Kafka/fusion bottleneck.
Also publishes to Kafka for the anomaly detection pipeline.
"""

from __future__ import annotations
import asyncio, time, logging, json, math
from typing import Any, Optional

import aiohttp
import redis.asyncio as aioredis
from aiokafka import AIOKafkaProducer

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from models import RawADSBMessage, normalize_icao24
from config import settings
from coverage_area import COVERAGE_LOCK_KEY, coverage_url, load_coverage_area, load_coverage_area_record, within_coverage_area
from world_scan import tick_world_scan

log = logging.getLogger(__name__)

POLL_INTERVAL = 15   # seconds — stay well within OpenSky rate limits
REDIS_TTL     = 60   # seconds — aircraft expire if not refreshed
MAX_PROVIDER_RECORDS = 10_000
REDIS_PIPELINE_CHUNK = 500
COVERAGE_PUBLISH_CHUNK = 25
COVERAGE_LOCK_HANDOFF_SECONDS = 0.15

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


async def prepare_scan_cycle(redis_client: Any, *, now: float):
    """Advance world scanning before freezing this ingestion cycle's coverage."""
    await tick_world_scan(redis_client, now=now)
    return await load_coverage_area_record(redis_client)


def _finite_number(value: Any) -> Optional[float]:
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if math.isfinite(parsed) else None


def _parse_adsb_lol_states(data: dict, received_at: Optional[float] = None) -> list[list[Any]]:
    """Convert adsb.lol records into the subset of OpenSky's state shape used here."""
    states = []
    if not isinstance(data, dict) or not isinstance(data.get("ac") or [], list):
        return states
    received_at = time.time() if received_at is None else received_at
    for raw in data.get("ac") or []:
        if not isinstance(raw, dict):
            continue
        try:
            icao = normalize_icao24(str(raw.get("hex") or "").lstrip("~"))
        except ValueError:
            continue
        lat, lon = raw.get("lat"), raw.get("lon")
        if len(icao) != 6 or lat is None or lon is None:
            continue
        try:
            lat, lon = float(lat), float(lon)
        except (TypeError, ValueError):
            continue
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            continue

        alt_ft = _finite_number(raw.get("alt_baro"))
        speed_kts = _finite_number(raw.get("gs"))
        vertical_fpm = _finite_number(raw.get("baro_rate"))
        # Preserve the 17-field OpenSky layout and append integrity metadata
        # supplied by adsb.lol. OpenSky leaves these extension slots absent.
        state: list[Any] = [None] * 20
        state[0] = icao
        state[1] = str(raw.get("flight") or "").strip() or None
        age = _finite_number(raw.get("seen_pos"))
        if age is not None:
            event_time = received_at - max(0.0, age)
            state[3] = event_time
            state[4] = event_time
        state[5], state[6] = lon, lat
        state[7] = alt_ft / 3.28084 if alt_ft is not None else None
        state[8] = str(raw.get("alt_baro") or "").lower() == "ground"
        state[9] = speed_kts / 1.944 if speed_kts is not None else None
        state[10] = _finite_number(raw.get("track"))
        state[11] = vertical_fpm / 196.85 if vertical_fpm is not None else None
        nic = _finite_number(raw.get("nic"))
        nac_p = _finite_number(raw.get("nac_p"))
        state[17] = int(nic) if nic is not None else None
        state[18] = int(nac_p) if nac_p is not None else None
        squawk = raw.get("squawk")
        state[19] = str(squawk).strip() if squawk is not None else None
        states.append(state)
    return states

def _squawk_from_state(state: list[Any] | tuple[Any, ...]) -> Optional[str]:
    """Read a transponder squawk code from OpenSky slot 14 or extension slot 19."""
    for index in (14, 19):
        if len(state) <= index or state[index] is None:
            continue
        candidate = str(state[index]).strip()
        if len(candidate) == 4 and all(char in "01234567" for char in candidate):
            return candidate
    return None


def _integrity_from_state(state: list[Any] | tuple[Any, ...]) -> tuple[Optional[int], Optional[int]]:
    """Read optional NIC/NACp extension fields without breaking OpenSky rows."""
    def optional_int(index: int) -> Optional[int]:
        if len(state) <= index or state[index] is None:
            return None
        try:
            return int(state[index])
        except (TypeError, ValueError):
            return None

    return optional_int(17), optional_int(18)


def _parse_feed_state(
    state: Any,
    source: str,
    fallback_time: float,
) -> Optional[tuple[dict[str, Any], RawADSBMessage]]:
    """Validate one OpenSky-shaped row before any Redis/Kafka side effect."""
    if not isinstance(state, (list, tuple)) or len(state) < 12:
        return None
    try:
        icao = normalize_icao24(state[0])
    except (TypeError, ValueError):
        return None
    lat, lon = _finite_number(state[6]), _finite_number(state[5])
    if lat is None or lon is None or not (-90 <= lat <= 90 and -180 <= lon <= 180):
        return None
    event_time = _finite_number(state[4])
    if event_time is None:
        event_time = _finite_number(state[3])
    if event_time is None or event_time < 0:
        return None
    receipt_time = _finite_number(fallback_time)
    if receipt_time is None or not (
        receipt_time - settings.SOURCE_EVENT_MAX_AGE_SEC
        <= event_time
        <= receipt_time + settings.SOURCE_EVENT_FUTURE_SKEW_SEC
    ):
        return None
    altitude_m = _finite_number(state[7])
    velocity_ms = _finite_number(state[9])
    heading = _finite_number(state[10])
    vertical_ms = _finite_number(state[11])
    altitude = int(altitude_m * 3.28084) if altitude_m is not None else None
    velocity = int(velocity_ms * 1.944) if velocity_ms is not None else None
    vertical_rate = int(vertical_ms * 196.85) if vertical_ms is not None else None
    callsign = state[1].strip().upper() if isinstance(state[1], str) else None
    nic, nac_p = _integrity_from_state(state)
    squawk = _squawk_from_state(state)
    message = RawADSBMessage(
        receiver_id=source, recv_time=event_time, icao24=icao,
        raw_message="", msg_type=17, callsign=callsign or None,
        lat=lat, lon=lon, altitude_baro=altitude, velocity=velocity,
        heading=heading, vertical_rate=vertical_rate,
        on_ground=state[8] is True, nic=nic, nac_p=nac_p, squawk=squawk,
    )
    aircraft = {
        "icao": icao, "cs": callsign or None, "lat": lat, "lon": lon,
        "alt": altitude, "vel": velocity, "hdg": heading,
        "vr": vertical_rate, "gnd": state[8] is True,
        "nic": nic, "nac_p": nac_p, "sqk": squawk, "ts": event_time, "src": source,
        "risk": 0, "anoms": [], "cls": "CIVILIAN", "conf": 0.85,
        "mil": 0.0, "band": "NORMAL", "trail": [],
    }
    return aircraft, message


async def publish_coverage_batch(
    redis_client: Any,
    producer: Any,
    kafka_messages: list[tuple[bytes, bytes, dict]],
    coverage_token: bytes,
) -> tuple[int, bool]:
    """Publish bounded chunks while allowing operator coverage changes between them."""
    published = 0
    for chunk_start in range(0, len(kafka_messages), COVERAGE_PUBLISH_CHUNK):
        chunk = kafka_messages[chunk_start:chunk_start + COVERAGE_PUBLISH_CHUNK]
        lock = redis_client.lock(COVERAGE_LOCK_KEY, timeout=30, blocking_timeout=5)
        async with lock:
            _, final_token = await load_coverage_area_record(redis_client)
            if final_token != coverage_token:
                log.info("Coverage changed during processing; discarding stale batch remainder")
                return published, False
            redis_batch = []
            for key, value, ac in chunk:
                await lock.extend(30, replace_ttl=True)
                try:
                    await asyncio.wait_for(
                        producer.send_and_wait(
                            topic=settings.TOPIC_RAW_ADSB,
                            key=key,
                            value=value,
                        ),
                        timeout=2.0,
                    )
                    redis_batch.append(ac)
                    published += 1
                except asyncio.TimeoutError:
                    log.warning("Kafka send timed out for %s", key.decode(errors="ignore"))
                except Exception as exc:
                    log.warning(
                        "Kafka send failed for %s: %s",
                        key.decode(errors="ignore"), exc,
                    )
            if redis_batch:
                await lock.extend(30, replace_ttl=True)
                pipe = redis_client.pipeline()
                for pending in redis_batch:
                    pipe.setex(
                        f"ac:{pending['icao']}", REDIS_TTL,
                        json.dumps(pending).encode(),
                    )
                await asyncio.wait_for(pipe.execute(), timeout=5.0)
        if chunk_start + len(chunk) < len(kafka_messages):
            # A short gap prevents this producer from immediately reacquiring
            # the distributed lock ahead of waiting operator/API requests.
            await asyncio.sleep(COVERAGE_LOCK_HANDOFF_SECONDS)
    return published, True


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
                area, coverage_token = await prepare_scan_cycle(redis_client, now=t0)
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
                        states = data.get("states") or [] if isinstance(data, dict) else []
                        if not isinstance(states, list):
                            states = []
                        recv_t = _finite_number(data.get("time")) if isinstance(data, dict) else None
                        recv_t = time.time() if recv_t is None else recv_t
                        log.info("OpenSky returned %d states", len(states))

                    _, current_token = await load_coverage_area_record(redis_client)
                    if current_token != coverage_token:
                        log.info("Coverage changed during fetch; discarding stale batch")
                        continue

                    kafka_messages = []
                    published = 0

                    if not isinstance(states, list):
                        states = []
                    if len(states) > MAX_PROVIDER_RECORDS:
                        log.warning(
                            "Provider batch capped from %d to %d records",
                            len(states), MAX_PROVIDER_RECORDS,
                        )
                    for state in states[:MAX_PROVIDER_RECORDS]:
                        try:
                            parsed = _parse_feed_state(state, source, recv_t)
                        except Exception as exc:
                            log.warning("Skipping malformed %s row: %s", source, exc)
                            continue
                        if parsed is None:
                            continue
                        ac, msg = parsed
                        if not within_coverage_area(ac["lat"], ac["lon"], area):
                            continue
                        icao = ac["icao"]
                        kafka_messages.append((icao.encode(), msg.to_bytes(), ac))

                    published, _completed = await publish_coverage_batch(
                        redis_client, producer, kafka_messages, coverage_token
                    )
                    log.info("Wrote %d aircraft to Redis (ac:*)", published)

            except Exception as e:
                log.error("Ingestor error: %s", e)

            elapsed = time.time() - t0
            await asyncio.sleep(max(2.0, POLL_INTERVAL - elapsed))


if __name__ == "__main__":
    asyncio.run(run())
