"""
processing/fusion_engine.py
────────────────────────────
The core data fusion loop.

Reads from Kafka topics: raw.adsb, raw.mlat, raw.acars
Maintains a live state vector per aircraft in Redis.
Writes fused state vectors to: fused.tracks

Fusion algorithm:
  1. Parse incoming message, determine source + confidence
  2. Load existing state vector from Redis (or create new)
  3. Apply weighted position fusion (Kalman-assisted)
  4. Update metadata (callsign, squawk, classification)
  5. Write back to Redis + publish to fused.tracks

Conflict detection:
  - If two sources disagree by >FUSION_CONFLICT_NM, raise GNSS_SPOOF flag
  - Duplicate ICAO24 at separated positions → DUPLICATE_ICAO anomaly
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional, List

import redis.asyncio as aioredis
import orjson
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from models import (
    RawADSBMessage, RawMLATReport, RawACARSMessage,
    StateVector, SourceReport, AnomalyFlag,
    DataSource, Classification, AnomalyType, RiskBand
)
from config import settings

log = logging.getLogger(__name__)


# ─── Kalman Filter (1D altitude / horizontal position) ────────────────────────

class KalmanFilter1D:
    """
    Simple constant-velocity Kalman filter for a single state dimension.
    Used to smooth individual position components.

    State: [position, velocity]
    """

    def __init__(self, process_noise: float = 0.1, measurement_noise: float = 10.0) -> None:
        self.q = process_noise
        self.r = measurement_noise
        self.x = None          # state [pos, vel]
        self.P = None          # covariance

    def initialize(self, position: float, velocity: float = 0.0) -> None:
        self.x = [position, velocity]
        self.P = [[100.0, 0.0], [0.0, 1.0]]

    def predict(self, dt: float) -> None:
        if self.x is None:
            return
        # x = F·x
        self.x[0] += self.x[1] * dt
        # P = F·P·Fᵀ + Q
        self.P[0][0] += dt * (self.P[1][0] + self.P[0][1]) + dt * dt * self.P[1][1] + self.q
        self.P[0][1] += dt * self.P[1][1]
        self.P[1][0] += dt * self.P[1][1]

    def update(self, measurement: float, measurement_noise: Optional[float] = None) -> float:
        if self.x is None:
            self.initialize(measurement)
            return measurement

        r = measurement_noise if measurement_noise is not None else self.r
        # Innovation
        y = measurement - self.x[0]
        # Innovation covariance
        S = self.P[0][0] + r
        # Kalman gain
        K0 = self.P[0][0] / S
        K1 = self.P[1][0] / S
        # Update state
        self.x[0] += K0 * y
        self.x[1] += K1 * y
        # Update covariance
        self.P[0][0] *= (1 - K0)
        self.P[0][1] *= (1 - K0)
        self.P[1][0] -= K1 * self.P[0][0]
        self.P[1][1] -= K1 * self.P[0][1]

        return self.x[0]


# ─── Source Confidence Weights ─────────────────────────────────────────────────

SOURCE_WEIGHTS = {
    DataSource.ADSB:      0.85,
    DataSource.MLAT:      0.80,
    DataSource.ACARS:     0.50,
    DataSource.SATELLITE: 0.70,
}


def confidence_for_source(source: DataSource, extra: float = 1.0) -> float:
    base = SOURCE_WEIGHTS.get(source, 0.5)
    return min(1.0, base * extra)


# ─── Distance Utility ──────────────────────────────────────────────────────────

def haversine_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    import math
    R = 3440.065
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


# ─── Fusion Engine ────────────────────────────────────────────────────────────

class FusionEngine:

    def __init__(self, redis_client: aioredis.Redis) -> None:
        self.redis = redis_client
        # In-memory Kalman state per aircraft (lat, lon, alt)
        # Cleared on restart (Redis carries position state)
        self._kalman: dict = {}   # icao24 → {lat: KF, lon: KF, alt: KF}

    async def process_adsb(self, msg: RawADSBMessage) -> Optional[StateVector]:
        sv = await self._load_or_create(msg.icao24)

        source_report = SourceReport(
            source=DataSource.ADSB,
            lat=msg.lat,
            lon=msg.lon,
            altitude=msg.altitude_baro,
            velocity=msg.velocity,
            heading=msg.heading,
            weight=confidence_for_source(DataSource.ADSB),
            confidence=confidence_for_source(DataSource.ADSB),
            timestamp=msg.recv_time,
        )

        # Conflict detection with existing position
        if sv.lat and sv.lon and msg.lat and msg.lon:
            dist = haversine_nm(sv.lat, sv.lon, msg.lat, msg.lon)
            dt = msg.recv_time - sv.last_seen
            # Allow for aircraft movement: max ~1200 kts = 20 NM/min
            max_dist = max(settings.FUSION_CONFLICT_NM, (dt / 60.0) * 25.0)
            if dist > max_dist:
                sv.anomalies.append(AnomalyFlag(
                    anomaly_type=AnomalyType.GNSS_SPOOF,
                    score_delta=35,
                    description=f"ADS-B position conflicts with last known by {dist:.1f} NM",
                    meta={"dist_nm": dist, "dt_sec": dt},
                ))

        # Update state
        if msg.lat is not None:
            sv.lat = self._smooth_position(msg.icao24, "lat", msg.lat, msg.recv_time)
        if msg.lon is not None:
            sv.lon = self._smooth_position(msg.icao24, "lon", msg.lon, msg.recv_time)
        if msg.altitude_baro is not None:
            sv.altitude_baro = int(self._smooth_position(
                msg.icao24, "alt", float(msg.altitude_baro), msg.recv_time))
        if msg.altitude_geo is not None:
            sv.altitude_geo = msg.altitude_geo
        if msg.velocity is not None:
            sv.velocity = msg.velocity
        if msg.heading is not None:
            sv.heading = msg.heading
        if msg.vertical_rate is not None:
            sv.vertical_rate = msg.vertical_rate
        if msg.callsign:
            sv.callsign = msg.callsign
        if msg.on_ground is not None:
            sv.on_ground = msg.on_ground

        sv.primary_source = DataSource.ADSB
        if DataSource.ADSB not in sv.sources:
            sv.sources.append(DataSource.ADSB)

        sv.source_reports.append(source_report)
        sv.source_reports = sv.source_reports[-10:]   # keep last 10

        sv.confidence = confidence_for_source(DataSource.ADSB)
        sv.last_seen = msg.recv_time
        sv.update_count += 1
        sv.add_position_history()

        return sv

    async def process_mlat(self, report: RawMLATReport) -> Optional[StateVector]:
        sv = await self._load_or_create(report.icao24)

        # MLAT confidence scales with number of receivers and residual
        receiver_bonus = min(1.0, report.num_receivers / 6.0)
        residual_penalty = max(0.0, 1.0 - (report.tdoa_residual / settings.MLAT_MAX_TDOA_RESIDUAL))
        mlat_conf = confidence_for_source(DataSource.MLAT) * receiver_bonus * residual_penalty

        source_report = SourceReport(
            source=DataSource.MLAT,
            lat=report.lat,
            lon=report.lon,
            altitude=report.altitude_baro,
            weight=mlat_conf,
            confidence=mlat_conf,
            timestamp=report.solve_time,
        )

        # If ADS-B and MLAT disagree significantly → spoofing candidate
        if sv.lat and sv.lon and DataSource.ADSB in sv.sources:
            dist = haversine_nm(sv.lat, sv.lon, report.lat, report.lon)
            if dist > settings.GHOST_MLAT_CONFIRM_NM:
                sv.anomalies.append(AnomalyFlag(
                    anomaly_type=AnomalyType.GHOST_AIRCRAFT,
                    score_delta=25,
                    description=f"ADS-B and MLAT positions differ by {dist:.1f} NM",
                    meta={"dist_nm": dist, "mlat_receivers": report.num_receivers},
                ))

        # MLAT-only aircraft (no ADS-B) → dark/unknown classification
        if DataSource.ADSB not in sv.sources and sv.classification == Classification.UNKNOWN:
            sv.classification = Classification.DARK_AIRCRAFT

        # Weighted position merge if we have both ADS-B and MLAT
        if DataSource.ADSB in sv.sources and sv.lat and sv.lon:
            adsb_w = SOURCE_WEIGHTS[DataSource.ADSB]
            mlat_w = mlat_conf
            total_w = adsb_w + mlat_w
            sv.lat = (sv.lat * adsb_w + report.lat * mlat_w) / total_w
            sv.lon = (sv.lon * adsb_w + report.lon * mlat_w) / total_w
        else:
            sv.lat = report.lat
            sv.lon = report.lon

        if report.altitude_baro:
            sv.altitude_baro = report.altitude_baro

        if DataSource.MLAT not in sv.sources:
            sv.sources.append(DataSource.MLAT)
        sv.source_reports.append(source_report)
        sv.source_reports = sv.source_reports[-10:]
        sv.confidence = max(sv.confidence, mlat_conf)
        sv.last_seen = report.solve_time
        sv.update_count += 1
        sv.add_position_history()

        return sv

    async def process_acars(self, msg: RawACARSMessage) -> Optional[StateVector]:
        if not msg.flight:
            return None

        # ACARS doesn't have position directly; enriches callsign / operator
        icao = await self._lookup_icao_by_registration(msg.registration)
        if not icao:
            return None

        sv = await self._load_or_create(icao)
        if msg.flight:
            sv.callsign = msg.flight.strip()
        if DataSource.ACARS not in sv.sources:
            sv.sources.append(DataSource.ACARS)
        sv.last_seen = msg.recv_time
        return sv

    async def _load_or_create(self, icao24: str) -> StateVector:
        key = f"sv:{icao24.upper()}"
        raw = await self.redis.get(key)

        if raw:
            try:
                sv = StateVector.from_bytes(raw)
                # Reset ephemeral anomaly list on each cycle (re-computed by anomaly detector)
                sv.anomalies = []
                return sv
            except Exception:
                pass

        return StateVector(icao24=icao24.upper(), first_seen=time.time())

    async def save(self, sv: StateVector, producer: AIOKafkaProducer) -> None:
        key = f"sv:{sv.icao24}"
        await self.redis.setex(key, settings.REDIS_TTL_STATE_VECTOR, sv.to_bytes())
        await producer.send(
            topic=settings.TOPIC_FUSED_TRACKS,
            key=sv.icao24.encode(),
            value=sv.to_bytes(),
        )

    async def _lookup_icao_by_registration(self, registration: Optional[str]) -> Optional[str]:
        if not registration:
            return None
        key = f"reg:{registration.upper()}"
        return await self.redis.get(key)

    def _smooth_position(self, icao24: str, axis: str, value: float, timestamp: float) -> float:
        """Apply Kalman smoothing to a position component."""
        if icao24 not in self._kalman:
            self._kalman[icao24] = {}
        kf_map = self._kalman[icao24]

        if axis not in kf_map:
            kf = KalmanFilter1D(process_noise=0.001, measurement_noise=5.0)
            kf.initialize(value)
            kf_map[axis] = {"kf": kf, "last_t": timestamp}
            return value

        entry = kf_map[axis]
        dt = max(0.001, timestamp - entry["last_t"])
        entry["kf"].predict(dt)
        smoothed = entry["kf"].update(value)
        entry["last_t"] = timestamp
        return smoothed


# ─── Duplicate ICAO Detector ───────────────────────────────────────────────────

class DuplicateICAODetector:
    """
    Detects two aircraft reporting the same ICAO24 address from different locations.
    This is a strong indicator of identity spoofing.
    """

    def __init__(self, redis_client: aioredis.Redis) -> None:
        self.redis = redis_client

    async def check(self, icao24: str, lat: float, lon: float) -> Optional[AnomalyFlag]:
        key = f"pos_check:{icao24}"
        raw = await self.redis.get(key)

        if raw:
            prev = orjson.loads(raw)
            from processing.mlat_solver import haversine_nm as hnm
            dist = haversine_nm(prev["lat"], prev["lon"], lat, lon)

            if dist > settings.DUPLICATE_WINDOW_NM:
                return AnomalyFlag(
                    anomaly_type=AnomalyType.DUPLICATE_ICAO,
                    score_delta=60,
                    description=f"ICAO {icao24} reported at two locations {dist:.0f} NM apart",
                    meta={"dist_nm": dist, "prev_lat": prev["lat"], "prev_lon": prev["lon"]},
                )

        await self.redis.setex(key, 10, orjson.dumps({"lat": lat, "lon": lon}))
        return None


# ─── Main Loop ────────────────────────────────────────────────────────────────

async def run() -> None:
    logging.basicConfig(level=settings.LOG_LEVEL)
    log.info("Starting fusion engine")

    redis_client = aioredis.from_url(settings.REDIS_URL, decode_responses=False)
    engine = FusionEngine(redis_client)
    dup_detector = DuplicateICAODetector(redis_client)

    consumer_adsb = AIOKafkaConsumer(
        settings.TOPIC_RAW_ADSB,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP,
        group_id=f"{settings.KAFKA_GROUP_PREFIX}.fusion-adsb",
        value_deserializer=lambda v: v,
        auto_offset_reset="latest",
        fetch_max_bytes=10_485_760,
    )

    consumer_mlat = AIOKafkaConsumer(
        settings.TOPIC_RAW_MLAT,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP,
        group_id=f"{settings.KAFKA_GROUP_PREFIX}.fusion-mlat",
        value_deserializer=lambda v: v,
        auto_offset_reset="latest",
    )

    producer = AIOKafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP,
        compression_type="lz4",
        linger_ms=10,
    )

    await consumer_adsb.start()
    await consumer_mlat.start()
    await producer.start()

    async def process_adsb_stream():
        count = 0
        async for msg in consumer_adsb:
            try:
                adsb = RawADSBMessage.from_bytes(msg.value)

                # Check for duplicate ICAO at different position
                if adsb.lat and adsb.lon:
                    dup_flag = await dup_detector.check(adsb.icao24, adsb.lat, adsb.lon)

                sv = await engine.process_adsb(adsb)
                if sv:
                    if dup_flag:
                        sv.anomalies.append(dup_flag)
                    await engine.save(sv, producer)
                    count += 1
                    if count % 5000 == 0:
                        log.info("Fusion: processed %d ADS-B messages", count)
            except Exception as e:
                log.error("ADS-B fusion error: %s", e)

    async def process_mlat_stream():
        async for msg in consumer_mlat:
            try:
                report = RawMLATReport.from_bytes(msg.value)
                sv = await engine.process_mlat(report)
                if sv:
                    await engine.save(sv, producer)
            except Exception as e:
                log.error("MLAT fusion error: %s", e)

    try:
        await asyncio.gather(process_adsb_stream(), process_mlat_stream())
    finally:
        await consumer_adsb.stop()
        await consumer_mlat.stop()
        await producer.stop()
        await redis_client.close()


if __name__ == "__main__":
    asyncio.run(run())
