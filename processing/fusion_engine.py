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
import hashlib
import logging
import math
import secrets
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
from kafka_offsets import commit_record
from processing.mlat_solver import (
    geodetic_to_ecef, receiver_geometry_is_safe, validate_receiver_configuration,
    verify_mlat_report,
)

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


def mlat_disagreement_threshold_nm(cep90_m: float) -> float:
    """Return a floor-bounded comparison radius from solver uncertainty."""
    return max(settings.GHOST_MLAT_CONFIRM_NM, cep90_m / 1852.0)


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

    @staticmethod
    def _source_event_time_is_acceptable(timestamp: float) -> bool:
        now = time.time()
        return (
            now - settings.SOURCE_EVENT_MAX_AGE_SEC
            <= timestamp
            <= now + settings.SOURCE_EVENT_FUTURE_SKEW_SEC
        )

    async def process_adsb(self, msg: RawADSBMessage) -> Optional[StateVector]:
        if not self._source_event_time_is_acceptable(msg.recv_time):
            log.warning("Rejecting stale/future ADS-B event time %s", msg.recv_time)
            return None
        sv = await self._load_or_create(msg.icao24, msg.recv_time)
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

        adsb_confidence = confidence_for_source(DataSource.ADSB)
        source_report = SourceReport(
            source=DataSource.ADSB,
            receiver_id=msg.receiver_id,
            lat=msg.lat,
            lon=msg.lon,
            altitude=msg.altitude_baro,
            velocity=msg.velocity,
            heading=msg.heading,
            vertical_rate=msg.vertical_rate,
            on_ground=msg.on_ground,
            weight=adsb_confidence,
            confidence=adsb_confidence,
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
            sv.squawk = msg.squawk
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
            sv.confidence = adsb_confidence
        sv.last_seen = max(prior_last_seen, msg.recv_time)
        sv.last_update_source = DataSource.ADSB
        sv.last_update_timestamp = msg.recv_time
        sv.update_count += 1
        if is_current_event:
            sv.add_position_history()

        return sv

    async def process_mlat(self, report: RawMLATReport) -> Optional[StateVector]:
        if not self._source_event_time_is_acceptable(report.solve_time):
            log.warning("Rejecting stale/future MLAT event time %s", report.solve_time)
            return None
        if (
            report.tdoa_residual > settings.MLAT_MAX_TDOA_RESIDUAL
            or report.cep90 > settings.MLAT_MAX_CEP90_M
        ):
            log.warning(
                "Rejecting operationally invalid MLAT report %s: residual=%s cep90=%s",
                report.session_id, report.tdoa_residual, report.cep90,
            )
            return None
        try:
            receiver_positions = [
                geodetic_to_ecef(*settings.MLAT_RECEIVER_LOCATIONS[receiver_id])
                for receiver_id in report.receiver_ids
            ]
        except (KeyError, TypeError, ValueError):
            log.warning("Rejecting MLAT report with unknown receiver identity")
            return None
        if not receiver_geometry_is_safe(receiver_positions):
            return None
        sv = await self._load_or_create(report.icao24, report.solve_time)
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
        l4_detectors: List[str] = []

        # Compare only measurements aligned in event time; the fused current
        # position may represent a newer event and is not valid evidence here.
        if aligned_adsb and latest_adsb is not None:
            assert latest_adsb.lat is not None and latest_adsb.lon is not None
            l4_detectors.append("adsb_mlat_disagreement")
            dist = haversine_nm(
                latest_adsb.lat, latest_adsb.lon, report.lat, report.lon
            )
            disagreement_threshold_nm = mlat_disagreement_threshold_nm(report.cep90)
            if dist > disagreement_threshold_nm:
                flag = AnomalyFlag(
                    anomaly_type=AnomalyType.GHOST_AIRCRAFT,
                    layer=DetectionLayer.L4,
                    detector="adsb_mlat_disagreement",
                    score_delta=25,
                    description=(
                        f"ADS-B and MLAT positions differ by {dist:.1f} NM "
                        f"(threshold {disagreement_threshold_nm:.1f} NM)"
                    ),
                    meta={
                        "dist_nm": dist,
                        "threshold_nm": disagreement_threshold_nm,
                        "mlat_cep90_m": report.cep90,
                        "mlat_receivers": report.num_receivers,
                    },
                    timestamp=report.solve_time,
                )
                sv.anomalies.append(flag)
                l4_flags.append(flag)

        has_adsb = aligned_adsb
        sv.layer_evaluations[DetectionLayer.L4.value] = LayerEvaluation(
            layer=DetectionLayer.L4,
            status=(LayerStatus.TRIGGERED if l4_flags else
                    LayerStatus.EVALUATED if has_adsb else LayerStatus.SKIPPED),
            detectors_evaluated=l4_detectors,
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

        if report.solve_time >= prior_last_seen:
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
        sv.last_update_timestamp = report.solve_time
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

        sv = await self._load_or_create(icao, msg.recv_time)
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
            await self.save(
                sv, producer, event=msg,
                duplicate_position=(msg.lat, msg.lon, msg.recv_time)
                if msg.lat is not None and msg.lon is not None else None,
            )
            return True

    async def handle_mlat(self, report, producer) -> bool:
        """Serialize the complete MLAT read/modify/write transaction per aircraft."""
        async with self._aircraft_lock(report.icao24):
            sv = await self.process_mlat(report)
            if sv is None:
                return False
            await self.save(
                sv, producer, event=report,
            )
            return True

    async def _load_or_create(
        self, icao24: str, observed_at: Optional[float] = None
    ) -> StateVector:
        key = f"fusion:sv:{icao24.upper()}"
        raw = await self.redis.get(key)

        if raw:
            try:
                sv = StateVector.from_bytes(raw)
                if sv.icao24 == icao24.upper():
                    return sv
                log.error("Ignoring identity-mismatched state at %s", key)
            except Exception:
                pass

        event_time = observed_at if observed_at is not None else time.time()
        return StateVector(
            icao24=icao24.upper(), first_seen=event_time, last_seen=event_time
        )

    async def deliver_outbox_event(self, event_id: str, producer) -> bytes:
        """Lease, acknowledge, and conditionally mark one durable outbox row."""
        if self.postgres is None:
            raise RuntimeError("PostgreSQL is required for fusion outbox delivery")
        token = secrets.token_hex(16)
        row = await self.postgres.fetchrow(
            """
            UPDATE fusion_outbox
            SET lease_owner=$2, lease_until=NOW() + INTERVAL '30 seconds',
                attempts=attempts + 1
            WHERE event_id=$1 AND delivered_at IS NULL
              AND (lease_until IS NULL OR lease_until < NOW())
            RETURNING topic, message_key, payload
            """,
            event_id, token,
        )
        if row is None:
            existing = await self.postgres.fetchrow(
                "SELECT payload, delivered_at FROM fusion_outbox WHERE event_id=$1",
                event_id,
            )
            if existing is None:
                raise RuntimeError("Fusion outbox row is missing")
            if existing["delivered_at"] is None:
                raise RuntimeError("Fusion outbox row is leased by another worker")
            return bytes(existing["payload"])

        payload = bytes(row["payload"])
        try:
            await producer.send_and_wait(
                topic=row["topic"], key=bytes(row["message_key"]), value=payload,
            )
            result = await self.postgres.execute(
                """
                UPDATE fusion_outbox
                SET delivered_at=NOW(), lease_owner=NULL, lease_until=NULL
                WHERE event_id=$1 AND lease_owner=$2 AND delivered_at IS NULL
                """,
                event_id, token,
            )
            if result != "UPDATE 1":
                raise RuntimeError("Lost fusion outbox lease before delivery mark")
        except BaseException:
            await self.postgres.execute(
                """
                UPDATE fusion_outbox SET lease_owner=NULL, lease_until=NULL
                WHERE event_id=$1 AND lease_owner=$2 AND delivered_at IS NULL
                """,
                event_id, token,
            )
            raise
        return payload

    async def save(
        self,
        sv: StateVector,
        producer: AIOKafkaProducer,
        *,
        event: Optional[RawADSBMessage | RawMLATReport] = None,
        duplicate_position: Optional[tuple[float, float, float]] = None,
    ) -> None:
        if self.postgres is not None and event is None:
            raise ValueError("PostgreSQL persistence requires an immutable source event")
        if event is not None and sv.icao24 != event.icao24:
            raise ValueError("Fused state and source event identities do not match")
        persisted_icao24 = event.icao24 if event is not None else sv.icao24
        key = f"fusion:sv:{persisted_icao24}"
        payload = sv.to_bytes()
        raw_event = b""
        raw_event_sha256 = ""
        if isinstance(event, RawADSBMessage):
            source_value = DataSource.ADSB.value
            persisted_event_time = event.recv_time
            raw_event = event.to_bytes()
            raw_event_sha256 = hashlib.sha256(raw_event).hexdigest()
            persisted_event_id = hashlib.sha256(
                b"ADSB:" + raw_event
            ).hexdigest()
            record = (
                event.callsign, event.lat, event.lon, event.altitude_baro,
                event.altitude_geo, event.velocity, event.heading,
                event.vertical_rate, confidence_for_source(DataSource.ADSB),
                0, Classification.UNKNOWN.value, event.on_ground,
            )
        elif isinstance(event, RawMLATReport):
            source_value = DataSource.MLAT.value
            persisted_event_time = event.solve_time
            raw_event = event.to_bytes()
            raw_event_sha256 = hashlib.sha256(raw_event).hexdigest()
            persisted_event_id = hashlib.sha256(
                b"MLAT:" + raw_event
            ).hexdigest()
            receiver_bonus = min(1.0, event.num_receivers / 6.0)
            residual_penalty = max(
                0.0,
                1.0 - event.tdoa_residual / settings.MLAT_MAX_TDOA_RESIDUAL,
            )
            event_confidence = (
                confidence_for_source(DataSource.MLAT)
                * receiver_bonus
                * residual_penalty
            )
            record = (
                None, event.lat, event.lon, event.altitude_baro, None,
                event.velocity, event.heading, None, event_confidence,
                0, Classification.UNKNOWN.value, None,
            )
        else:
            source = sv.last_update_source or sv.primary_source
            source_value = source.value
            persisted_event_time = sv.last_seen
            persisted_event_id = hashlib.sha256(
                source_value.encode() + b":" + payload
            ).hexdigest()
            record = (
                sv.callsign, sv.lat, sv.lon, sv.altitude_baro,
                sv.altitude_geo, sv.velocity, sv.heading, sv.vertical_rate,
                sv.confidence, sv.risk_score, sv.classification.value,
                sv.on_ground,
            )
        (
            event_callsign, event_lat, event_lon, event_altitude_baro,
            event_altitude_geo, event_velocity, event_heading,
            event_vertical_rate, event_confidence, event_risk_score,
            event_classification, event_on_ground,
        ) = record
        sv.source_event_id = persisted_event_id
        payload = sv.to_bytes()
        # Persist first. The event claim makes replay after a later Kafka/Redis
        # failure idempotent while distinct delayed source events retain rows.
        if self.postgres is not None:
            await self.postgres.execute(
                """
                WITH claimed AS (
                    INSERT INTO fusion_event_commits (
                        event_id, event_time, icao24, source,
                        raw_event, raw_event_sha256
                    ) VALUES ($1, to_timestamp($2), $3, $4, $18, $19)
                    ON CONFLICT (event_id) DO NOTHING
                    RETURNING 1
                )
                , inserted_track AS (
                INSERT INTO track_points (
                    time, icao24, callsign, lat, lon, altitude_baro,
                    altitude_geo, velocity, heading, vertical_rate,
                    source, confidence, risk_score, classification,
                    raw_icao, on_ground
                )
                SELECT
                    to_timestamp($2), $3, $5, $6, $7, $8, $9, $10,
                    $11, $12, $4, $13, $14, $15, $16, $17
                FROM claimed
                RETURNING 1
                )
                INSERT INTO fusion_outbox (
                    event_id, topic, message_key, payload
                )
                SELECT $1, $20, $21, $22 FROM claimed
                ON CONFLICT (event_id) DO NOTHING
                """,
                persisted_event_id,
                persisted_event_time,
                persisted_icao24,
                source_value,
                event_callsign,
                event_lat,
                event_lon,
                event_altitude_baro,
                event_altitude_geo,
                event_velocity,
                event_heading,
                event_vertical_rate,
                event_confidence,
                event_risk_score,
                event_classification,
                int(persisted_icao24, 16),
                event_on_ground,
                raw_event,
                raw_event_sha256,
                settings.TOPIC_FUSED_TRACKS,
                persisted_icao24.encode(),
                payload,
            )
            payload = await self.deliver_outbox_event(persisted_event_id, producer)
        else:
            await producer.send_and_wait(
                topic=settings.TOPIC_FUSED_TRACKS,
                key=persisted_icao24.encode(),
                value=payload,
            )
        if duplicate_position is None:
            await self.redis.setex(key, settings.REDIS_TTL_STATE_VECTOR, payload)
        else:
            lat, lon, event_time = duplicate_position
            pipeline = self.redis.pipeline(transaction=True)
            pipeline.setex(key, settings.REDIS_TTL_STATE_VECTOR, payload)
            pipeline.setex(
                f"pos_check:{persisted_icao24}", 10,
                orjson.dumps({"lat": lat, "lon": lon, "timestamp": event_time}),
            )
            await pipeline.execute()

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

        return None

    async def record(
        self, icao24: str, lat: float, lon: float, event_time: float
    ) -> None:
        key = f"pos_check:{icao24}"
        await self.redis.setex(
            key, 10,
            orjson.dumps({"lat": lat, "lon": lon, "timestamp": event_time}),
        )


# ─── Main Loop ────────────────────────────────────────────────────────────────

async def drain_fusion_outbox_periodically(
    engine: FusionEngine, producer, *, batch_size: int = 100, interval: float = 2.0,
) -> None:
    """Boundedly recover pending PostgreSQL outbox rows independent of source replay."""
    if engine.postgres is None:
        raise RuntimeError("PostgreSQL is required for fusion outbox recovery")
    while True:
        rows = await engine.postgres.fetch(
            """
            SELECT o.event_id FROM fusion_outbox AS o
            JOIN fusion_event_commits AS c USING (event_id)
            WHERE o.delivered_at IS NULL
              AND c.committed_at < NOW() - INTERVAL '5 seconds'
              AND (o.lease_until IS NULL OR o.lease_until < NOW())
            ORDER BY o.event_id LIMIT $1
            """,
            batch_size,
        )
        failed = False
        for row in rows:
            try:
                await engine.deliver_outbox_event(row["event_id"], producer)
            except Exception as exc:
                failed = True
                log.warning("Fusion outbox recovery failed for %s: %s", row["event_id"], exc)
        await asyncio.sleep(interval if failed or len(rows) < batch_size else 0)


async def supervise_fusion_tasks(*coroutines) -> None:
    """Fail the service if any consumer or outbox recovery task stops."""
    tasks = [asyncio.create_task(coro) for coro in coroutines]
    try:
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            task.result()
        raise RuntimeError("A supervised fusion task stopped unexpectedly")
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

async def run() -> None:
    logging.basicConfig(level=settings.LOG_LEVEL)
    log.info("Starting fusion engine")

    validate_receiver_configuration()
    if not settings.MLAT_SOLVER_SIGNING_KEY:
        raise ValueError("MLAT solver signing key is required")
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
            except Exception as exc:
                log.error("Discarding malformed ADS-B record: %s", exc)
                await commit_record(consumer_adsb, msg)
                continue
            try:
                accepted = await engine.handle_adsb(adsb, producer, dup_detector)
                if accepted:
                    count += 1
                    if count % 5000 == 0:
                        log.info("Fusion: processed %d ADS-B messages", count)
                await commit_record(consumer_adsb, msg)
            except Exception as e:
                log.error("ADS-B fusion error: %s", e)
                raise

    async def process_mlat_stream():
        async for msg in consumer_mlat:
            try:
                report = RawMLATReport.from_bytes(msg.value)
            except Exception as exc:
                log.error("Discarding malformed MLAT record: %s", exc)
                await commit_record(consumer_mlat, msg)
                continue
            if not verify_mlat_report(report, settings.MLAT_SOLVER_SIGNING_KEY):
                log.error("Discarding unauthenticated solver MLAT report")
                await commit_record(consumer_mlat, msg)
                continue
            try:
                await engine.handle_mlat(report, producer)
                await commit_record(consumer_mlat, msg)
            except Exception as e:
                log.error("MLAT fusion error: %s", e)
                raise

    try:
        await supervise_fusion_tasks(
            process_adsb_stream(),
            process_mlat_stream(),
            drain_fusion_outbox_periodically(engine, producer),
        )
    finally:
        await consumer_adsb.stop()
        await consumer_mlat.stop()
        await producer.stop()
        if postgres_pool is not None:
            await postgres_pool.close()
        await redis_client.close()


if __name__ == "__main__":
    asyncio.run(run())
