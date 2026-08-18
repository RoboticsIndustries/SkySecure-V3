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
import math
import time
import weakref
from typing import List, Optional

import redis.asyncio as aioredis
import asyncpg
import orjson
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from models import (
    RawADSBMessage, RawMLATReport, RawACARSMessage,
    StateVector, SourceReport, AnomalyFlag,
    DataSource, Classification, AnomalyType, DetectionLayer,
    LayerEvaluation, LayerStatus, RiskBand
)
from config import settings, KAFKA_CONSUMER_STABILITY

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

    def __init__(self, redis_client: aioredis.Redis, postgres_pool=None) -> None:
        self.redis = redis_client
        self.postgres = postgres_pool
        # In-memory Kalman state per aircraft (lat, lon, alt)
        # Cleared on restart (Redis carries position state)
        self._kalman: dict = {}   # icao24 → {lat: KF, lon: KF, alt: KF}
        self._aircraft_locks: weakref.WeakValueDictionary[str, asyncio.Lock] = (
            weakref.WeakValueDictionary()
        )

    def _aircraft_lock(self, icao24: str) -> asyncio.Lock:
        return self._aircraft_locks.setdefault(icao24.upper(), asyncio.Lock())

    async def process_adsb(self, msg: RawADSBMessage) -> Optional[StateVector]:
        sv = await self._load_or_create(msg.icao24)
        latest_adsb = max(
            (report for report in sv.source_reports
             if report.source == DataSource.ADSB),
            key=lambda report: report.timestamp,
            default=None,
        )
        if latest_adsb is not None and msg.recv_time <= latest_adsb.timestamp:
            return None
        prior_last_seen = sv.last_seen
        is_current_event = msg.recv_time >= prior_last_seen
        # ADS-B-owned metadata must advance against the latest ADS-B report,
        # not against a newer unrelated MLAT event that moved global last_seen.
        is_current_adsb_event = (
            latest_adsb is None and is_current_event
        ) or (
            latest_adsb is not None and msg.recv_time > latest_adsb.timestamp
        )

        # ADS-B processing owns these current-cycle checks; replace their prior
        # evidence rather than accumulating stale duplicate/conflict flags.
        expired_l4 = any(
            flag.layer == DetectionLayer.L4
            and msg.recv_time - flag.timestamp > settings.FUSION_TRIGGER_TTL_SEC
            for flag in sv.anomalies
        )
        sv.anomalies = [
            flag for flag in sv.anomalies
            if not (
                flag.layer == DetectionLayer.L5
                and flag.detector == "duplicate_icao"
            )
            and not (
                flag.layer == DetectionLayer.L2
                and flag.detector == "adsb_position_conflict"
            )
            and not (
                flag.layer == DetectionLayer.L4
                and msg.recv_time - flag.timestamp > settings.FUSION_TRIGGER_TTL_SEC
            )
        ]
        active_l4 = any(flag.layer == DetectionLayer.L4 for flag in sv.anomalies)
        current_l4_evaluation = sv.layer_evaluations.get(DetectionLayer.L4.value)
        evaluation_is_current = (
            current_l4_evaluation is None
            or msg.recv_time >= current_l4_evaluation.timestamp
        )
        if (expired_l4 or not active_l4) and evaluation_is_current:
            sv.layer_evaluations[DetectionLayer.L4.value] = LayerEvaluation(
                layer=DetectionLayer.L4,
                status=LayerStatus.SKIPPED,
                skipped_reason="No MLAT report available for multi-sensor comparison",
                timestamp=msg.recv_time,
            )
        elif DetectionLayer.L4.value not in sv.layer_evaluations:
            l4_flags = [flag for flag in sv.anomalies if flag.layer == DetectionLayer.L4]
            sv.layer_evaluations[DetectionLayer.L4.value] = LayerEvaluation(
                layer=DetectionLayer.L4,
                status=LayerStatus.TRIGGERED,
                triggered_detectors=sorted({flag.detector for flag in l4_flags}),
                score_delta=sum(flag.score_delta for flag in l4_flags),
                timestamp=max(flag.timestamp for flag in l4_flags),
            )

        source_report = SourceReport(
            source=DataSource.ADSB,
            receiver_id=msg.receiver_id,
            lat=msg.lat,
            lon=msg.lon,
            altitude=msg.altitude_baro,
            velocity=msg.velocity,
            heading=msg.heading,
            vertical_rate=msg.vertical_rate,
            weight=confidence_for_source(DataSource.ADSB),
            confidence=confidence_for_source(DataSource.ADSB),
            timestamp=msg.recv_time,
        )

        # Conflict detection with existing position
        if (
            latest_adsb is not None
            and latest_adsb.lat is not None and latest_adsb.lon is not None
            and msg.lat is not None and msg.lon is not None
        ):
            dist = haversine_nm(latest_adsb.lat, latest_adsb.lon, msg.lat, msg.lon)
            dt = msg.recv_time - latest_adsb.timestamp
            # Allow for aircraft movement: max ~1200 kts = 20 NM/min
            max_dist = max(settings.FUSION_CONFLICT_NM, (dt / 60.0) * 25.0)
            if dist > max_dist:
                sv.anomalies.append(AnomalyFlag(
                    anomaly_type=AnomalyType.GNSS_SPOOF,
                    layer=DetectionLayer.L2,
                    detector="adsb_position_conflict",
                    score_delta=35,
                    description=f"ADS-B position conflicts with last known by {dist:.1f} NM",
                    meta={"dist_nm": dist, "dt_sec": dt},
                    timestamp=msg.recv_time,
                ))

        # Update state
        if is_current_event and msg.lat is not None:
            sv.lat = self._smooth_position(msg.icao24, "lat", msg.lat, msg.recv_time)
        if is_current_event and msg.lon is not None:
            sv.lon = self._smooth_position(msg.icao24, "lon", msg.lon, msg.recv_time)
        if is_current_event and msg.altitude_baro is not None:
            sv.altitude_baro = int(self._smooth_position(
                msg.icao24, "alt", float(msg.altitude_baro), msg.recv_time))
        if is_current_event and msg.altitude_geo is not None:
            sv.altitude_geo = msg.altitude_geo
        if is_current_event and msg.velocity is not None:
            sv.velocity = msg.velocity
        if is_current_event and msg.heading is not None:
            sv.heading = msg.heading
        if is_current_event and msg.vertical_rate is not None:
            sv.vertical_rate = msg.vertical_rate
        # Integrity metadata belongs to the current ADS-B report.  A current
        # source that omits NIC/NACp means "unavailable now", not "reuse an old
        # value".  Delayed reports must not clear newer metadata.
        if is_current_adsb_event:
            sv.nic = msg.nic
            sv.nac_p = msg.nac_p
        if is_current_event and msg.callsign:
            sv.callsign = msg.callsign
        if is_current_event and msg.on_ground is not None:
            sv.on_ground = msg.on_ground

        if is_current_event:
            sv.primary_source = DataSource.ADSB
        if DataSource.ADSB not in sv.sources:
            sv.sources.append(DataSource.ADSB)

        sv.source_reports.append(source_report)
        sv.source_reports = sorted(
            sv.source_reports, key=lambda report: report.timestamp
        )[-10:]

        if is_current_event:
            sv.confidence = confidence_for_source(DataSource.ADSB)
        sv.last_seen = max(prior_last_seen, msg.recv_time)
        sv.last_update_source = DataSource.ADSB
        sv.update_count += 1
        if is_current_event:
            sv.add_position_history()

        return sv

    async def process_mlat(self, report: RawMLATReport) -> Optional[StateVector]:
        sv = await self._load_or_create(report.icao24)
        latest_mlat = max(
            (source for source in sv.source_reports
             if source.source == DataSource.MLAT),
            key=lambda source: source.timestamp,
            default=None,
        )
        if latest_mlat is not None and report.solve_time <= latest_mlat.timestamp:
            return None
        current_l4_evaluation = sv.layer_evaluations.get(DetectionLayer.L4.value)
        latest_l4_event = max(
            [flag.timestamp for flag in sv.anomalies if flag.layer == DetectionLayer.L4]
            + ([current_l4_evaluation.timestamp] if current_l4_evaluation else []),
            default=float("-inf"),
        )
        if (
            report.solve_time < latest_l4_event
            and latest_l4_event - report.solve_time
                > settings.FUSION_COMPARISON_WINDOW_SEC
        ):
            return None
        prior_last_seen = sv.last_seen
        # A new MLAT report supersedes the previous L4 comparison evidence.
        sv.anomalies = [
            flag for flag in sv.anomalies if flag.layer != DetectionLayer.L4
        ]
        l4_flags: List[AnomalyFlag] = []

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

        latest_adsb = max(
            (source for source in sv.source_reports
             if source.source == DataSource.ADSB
             and source.lat is not None and source.lon is not None
             and abs(report.solve_time - source.timestamp)
                 <= settings.FUSION_COMPARISON_WINDOW_SEC),
            key=lambda source: source.timestamp,
            default=None,
        )
        aligned_adsb = latest_adsb is not None

        # Compare only measurements aligned in event time; the fused current
        # position may represent a newer event and is not valid evidence here.
        if aligned_adsb and latest_adsb is not None:
            assert latest_adsb.lat is not None and latest_adsb.lon is not None
            dist = haversine_nm(
                latest_adsb.lat, latest_adsb.lon, report.lat, report.lon
            )
            if dist > settings.GHOST_MLAT_CONFIRM_NM:
                flag = AnomalyFlag(
                    anomaly_type=AnomalyType.GHOST_AIRCRAFT,
                    layer=DetectionLayer.L4,
                    detector="adsb_mlat_disagreement",
                    score_delta=25,
                    description=f"ADS-B and MLAT positions differ by {dist:.1f} NM",
                    meta={"dist_nm": dist, "mlat_receivers": report.num_receivers},
                    timestamp=report.solve_time,
                )
                sv.anomalies.append(flag)
                l4_flags.append(flag)

        has_adsb = aligned_adsb
        sv.layer_evaluations[DetectionLayer.L4.value] = LayerEvaluation(
            layer=DetectionLayer.L4,
            status=(LayerStatus.TRIGGERED if l4_flags else
                    LayerStatus.EVALUATED if has_adsb else LayerStatus.SKIPPED),
            detectors_evaluated=["adsb_mlat_disagreement"] if has_adsb else [],
            triggered_detectors=[flag.detector for flag in l4_flags],
            score_delta=sum(flag.score_delta for flag in l4_flags),
            skipped_reason=(None if has_adsb else
                            "No temporally aligned ADS-B position available for comparison"),
            timestamp=report.solve_time,
        )

        # MLAT-only aircraft (no ADS-B) → dark/unknown classification
        if DataSource.ADSB not in sv.sources and sv.classification == Classification.UNKNOWN:
            sv.classification = Classification.DARK_AIRCRAFT

        # Weighted position merge if we have both ADS-B and MLAT
        if aligned_adsb and latest_adsb is not None and report.solve_time >= prior_last_seen:
            assert latest_adsb.lat is not None and latest_adsb.lon is not None
            adsb_w = SOURCE_WEIGHTS[DataSource.ADSB]
            mlat_w = mlat_conf
            total_w = adsb_w + mlat_w
            sv.lat = (latest_adsb.lat * adsb_w + report.lat * mlat_w) / total_w
            sv.lon = (latest_adsb.lon * adsb_w + report.lon * mlat_w) / total_w
        elif report.solve_time >= prior_last_seen:
            sv.lat = report.lat
            sv.lon = report.lon

        if report.altitude_baro and report.solve_time >= prior_last_seen:
            sv.altitude_baro = report.altitude_baro

        if DataSource.MLAT not in sv.sources:
            sv.sources.append(DataSource.MLAT)
        sv.source_reports.append(source_report)
        sv.source_reports = sorted(
            sv.source_reports, key=lambda source: source.timestamp
        )[-10:]
        sv.confidence = max(sv.confidence, mlat_conf)
        sv.last_seen = max(prior_last_seen, report.solve_time)
        sv.last_update_source = DataSource.MLAT
        sv.update_count += 1
        if report.solve_time >= prior_last_seen:
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

    async def handle_adsb(self, msg, producer, duplicate_detector) -> bool:
        """Serialize the complete ADS-B read/modify/write transaction per aircraft."""
        async with self._aircraft_lock(msg.icao24):
            sv = await self.process_adsb(msg)
            if sv is None:
                return False
            dup_flag = None
            if msg.lat is not None and msg.lon is not None:
                dup_flag = await duplicate_detector.check(
                    msg.icao24, msg.lat, msg.lon, msg.recv_time
                )
            if dup_flag:
                sv.anomalies.append(dup_flag)
            sv.layer_evaluations[DetectionLayer.L5.value] = LayerEvaluation(
                layer=DetectionLayer.L5,
                status=LayerStatus.TRIGGERED if dup_flag else LayerStatus.EVALUATED,
                detectors_evaluated=["duplicate_icao"],
                triggered_detectors=["duplicate_icao"] if dup_flag else [],
                score_delta=dup_flag.score_delta if dup_flag else 0,
                timestamp=msg.recv_time,
            )
            await self.save(sv, producer)
            return True

    async def handle_mlat(self, report, producer) -> bool:
        """Serialize the complete MLAT read/modify/write transaction per aircraft."""
        async with self._aircraft_lock(report.icao24):
            sv = await self.process_mlat(report)
            if sv is None:
                return False
            await self.save(sv, producer)
            return True

    async def _load_or_create(self, icao24: str) -> StateVector:
        key = f"fusion:sv:{icao24.upper()}"
        raw = await self.redis.get(key)

        if raw:
            try:
                return StateVector.from_bytes(raw)
            except Exception:
                pass

        return StateVector(icao24=icao24.upper(), first_seen=time.time())

    async def save(self, sv: StateVector, producer: AIOKafkaProducer) -> None:
        key = f"fusion:sv:{sv.icao24}"
        payload = sv.to_bytes()
        await producer.send_and_wait(
            topic=settings.TOPIC_FUSED_TRACKS,
            key=sv.icao24.encode(),
            value=payload,
        )
        await self.redis.setex(key, settings.REDIS_TTL_STATE_VECTOR, payload)
        if self.postgres is not None:
            try:
                await self.postgres.execute(
                    """
                    INSERT INTO track_points (
                        time, icao24, callsign, lat, lon, altitude_baro,
                        altitude_geo, velocity, heading, vertical_rate,
                        source, confidence, risk_score, classification,
                        raw_icao, on_ground
                    ) VALUES (
                        to_timestamp($1), $2, $3, $4, $5, $6, $7, $8,
                        $9, $10, $11, $12, $13, $14, $15, $16
                    )
                    """,
                    sv.last_seen,
                    sv.icao24,
                    sv.callsign,
                    sv.lat,
                    sv.lon,
                    sv.altitude_baro,
                    sv.altitude_geo,
                    sv.velocity,
                    sv.heading,
                    sv.vertical_rate,
                    sv.primary_source.value,
                    sv.confidence,
                    sv.risk_score,
                    sv.classification.value,
                    int(sv.icao24, 16),
                    sv.on_ground,
                )
            except Exception as exc:
                log.error("PostgreSQL track persistence failed for %s: %s", sv.icao24, exc)

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

    async def check(
        self, icao24: str, lat: float, lon: float, event_time: float
    ) -> Optional[AnomalyFlag]:
        key = f"pos_check:{icao24}"
        raw = await self.redis.get(key)

        if raw:
            prev = orjson.loads(raw)
            dist = haversine_nm(prev["lat"], prev["lon"], lat, lon)
            event_delta = event_time - float(prev.get("timestamp", event_time))

            # Never let delayed delivery roll the detector's comparison point
            # backward; doing so can make the next current report look like a
            # geographically impossible duplicate.
            if event_delta < 0:
                return None

            if event_delta <= 10 and dist > settings.DUPLICATE_WINDOW_NM:
                return AnomalyFlag(
                    anomaly_type=AnomalyType.DUPLICATE_ICAO,
                    layer=DetectionLayer.L5,
                    detector="duplicate_icao",
                    score_delta=60,
                    description=f"ICAO {icao24} reported at two locations {dist:.0f} NM apart",
                    meta={"dist_nm": dist, "prev_lat": prev["lat"], "prev_lon": prev["lon"]},
                    timestamp=event_time,
                )

        await self.redis.setex(
            key, 10,
            orjson.dumps({"lat": lat, "lon": lon, "timestamp": event_time}),
        )
        return None


# ─── Main Loop ────────────────────────────────────────────────────────────────

async def run() -> None:
    logging.basicConfig(level=settings.LOG_LEVEL)
    log.info("Starting fusion engine")

    redis_client = aioredis.from_url(settings.REDIS_URL, decode_responses=False)
    postgres_pool = await asyncpg.create_pool(
        settings.POSTGRES_DSN,
        min_size=1,
        max_size=4,
        command_timeout=10,
    )
    engine = FusionEngine(redis_client, postgres_pool=postgres_pool)
    dup_detector = DuplicateICAODetector(redis_client)

    consumer_adsb = AIOKafkaConsumer(
        settings.TOPIC_RAW_ADSB,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP,
        group_id=f"{settings.KAFKA_GROUP_PREFIX}.fusion-adsb",
        value_deserializer=lambda v: v,
        auto_offset_reset="latest",
        fetch_max_bytes=10_485_760,
        **KAFKA_CONSUMER_STABILITY,
    )

    consumer_mlat = AIOKafkaConsumer(
        settings.TOPIC_RAW_MLAT,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP,
        group_id=f"{settings.KAFKA_GROUP_PREFIX}.fusion-mlat",
        value_deserializer=lambda v: v,
        auto_offset_reset="latest",
        **KAFKA_CONSUMER_STABILITY,
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
                accepted = await engine.handle_adsb(adsb, producer, dup_detector)
                if accepted:
                    count += 1
                    if count % 5000 == 0:
                        log.info("Fusion: processed %d ADS-B messages", count)
                await consumer_adsb.commit()
            except Exception as e:
                log.error("ADS-B fusion error: %s", e)
                raise

    async def process_mlat_stream():
        async for msg in consumer_mlat:
            try:
                report = RawMLATReport.from_bytes(msg.value)
                await engine.handle_mlat(report, producer)
                await consumer_mlat.commit()
            except Exception as e:
                log.error("MLAT fusion error: %s", e)
                raise

    try:
        await asyncio.gather(process_adsb_stream(), process_mlat_stream())
    finally:
        await consumer_adsb.stop()
        await consumer_mlat.stop()
        await producer.stop()
        if postgres_pool is not None:
            await postgres_pool.close()
        await redis_client.close()


if __name__ == "__main__":
    asyncio.run(run())
