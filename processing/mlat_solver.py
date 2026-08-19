"""
processing/mlat_solver.py
─────────────────────────
Multilateration (MLAT) solver.

Principle: A Mode S transponder response (DF11, DF17, etc.) is received by
multiple ground receivers at slightly different times due to distance.
These Time Differences of Arrival (TDOA) form hyperbolic surfaces in 3D space.
The intersection of ≥3 such surfaces gives the aircraft position.

Architecture:
  - Consumes raw Beast-format messages from Kafka (keyed by receiver_id)
  - Groups messages by ICAO24 + time window
  - When ≥4 receivers see the same message, runs the unconstrained 3D TDOA solver
  - Publishes RawMLATReport to Kafka topic: raw.mlat

Math:
  For N receivers at known positions rᵢ = (xᵢ, yᵢ, zᵢ):
    tᵢ = |p - rᵢ| / c + t₀
  where p = aircraft position, c = speed of light, t₀ = emission time.
  TDOA: Δtᵢⱼ = tᵢ - tⱼ → eliminates t₀
  This gives hyperbolic equations solved iteratively (Gauss-Newton / Levenberg-Marquardt).
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import math
import re
import time
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

import numpy as np
import orjson
import redis.asyncio as aioredis
from scipy.optimize import least_squares
from pyproj import Transformer
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from models import RawADSBMessage, RawMLATReport
from config import settings, KAFKA_CONSUMER_STABILITY
from kafka_offsets import commit_record
from receiver_auth import validate_receiver_credentials

log = logging.getLogger(__name__)

# Speed of light in m/s
C = 299_792_458.0


def physical_reception_key(reception: RawADSBMessage, receiver_secret: str) -> bytes:
    signature = hmac.new(
        receiver_secret.encode(), reception.to_bytes(), hashlib.sha256
    ).hexdigest()
    return f"{reception.receiver_id}:{signature}".encode()


def covariance_variance(
    cost: float, residual_count: int, parameter_count: int, noise_ns: float
) -> float:
    dof = residual_count - parameter_count
    residual_variance = (2.0 * cost / dof) if dof > 0 else 0.0
    range_noise_m = C * noise_ns * 1e-9
    return max(residual_variance, range_noise_m * range_noise_m)


def physical_reception_is_valid(reception: RawADSBMessage) -> bool:
    return (
        reception.receiver_id in settings.MLAT_RECEIVER_LOCATIONS
        and reception.msg_type in (11, 17, 18, 20, 21)
        and re.fullmatch(r"(?:[0-9A-F]{14}|[0-9A-F]{28})", reception.raw_message)
        is not None
    )


def sign_mlat_report(report: RawMLATReport, secret: str) -> str:
    payload = orjson.dumps(
        report.model_dump(exclude={"auth_tag"}), option=orjson.OPT_SORT_KEYS
    )
    return hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()


def verify_mlat_report(report: RawMLATReport, secret: str) -> bool:
    return bool(
        secret and report.auth_tag
        and hmac.compare_digest(report.auth_tag, sign_mlat_report(report, secret))
    )

# ECEF ↔ geodetic transformer
_ecef_to_wgs84 = Transformer.from_crs("EPSG:4978", "EPSG:4326", always_xy=True)
_wgs84_to_ecef = Transformer.from_crs("EPSG:4326", "EPSG:4978", always_xy=True)


# ─── Coordinate Utilities ──────────────────────────────────────────────────────

def geodetic_to_ecef(lat_deg: float, lon_deg: float, alt_m: float) -> np.ndarray:
    """Convert geodetic (lat, lon, alt) to ECEF (x, y, z) in metres."""
    x, y, z = _wgs84_to_ecef.transform(lon_deg, lat_deg, alt_m)
    return np.array([x, y, z], dtype=np.float64)


def ecef_to_geodetic(xyz: np.ndarray) -> Tuple[float, float, float]:
    """Convert ECEF to (lat_deg, lon_deg, alt_m)."""
    lon, lat, alt = _ecef_to_wgs84.transform(xyz[0], xyz[1], xyz[2])
    return lat, lon, alt


def horizontal_cep90(position_ecef: np.ndarray, covariance_ecef: np.ndarray) -> float:
    """Conservative CEP90 from covariance projected into local East/North."""
    lat_deg, lon_deg, _ = ecef_to_geodetic(position_ecef)
    lat = math.radians(lat_deg)
    lon = math.radians(lon_deg)
    east = np.array([-math.sin(lon), math.cos(lon), 0.0])
    north = np.array([
        -math.sin(lat) * math.cos(lon),
        -math.sin(lat) * math.sin(lon),
        math.cos(lat),
    ])
    projection = np.vstack((east, north))
    horizontal_covariance = projection @ covariance_ecef @ projection.T
    eigenvalues = np.linalg.eigvalsh(horizontal_covariance)
    if not np.all(np.isfinite(eigenvalues)) or float(eigenvalues[-1]) < 0:
        raise ValueError("Invalid horizontal covariance")
    return 2.146 * math.sqrt(max(0.0, float(eigenvalues[-1])))


def haversine_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in nautical miles."""
    R = 3440.065  # Earth radius in NM
    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlambda = np.radians(lon2 - lon1)
    a = np.sin(dphi / 2) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlambda / 2) ** 2
    return R * 2 * np.arcsin(np.sqrt(a))


def receiver_geometry_is_safe(receiver_positions: List[np.ndarray]) -> bool:
    """Require useful horizontal baseline and cross-track spread in local ENU."""
    if len(receiver_positions) < 4:
        return False
    points = np.vstack(receiver_positions)
    origin = np.mean(points, axis=0)
    origin_norm = float(np.linalg.norm(origin))
    if origin_norm <= 0:
        return False
    up = origin / origin_norm
    east = np.cross(np.array([0.0, 0.0, 1.0]), up)
    east_norm = float(np.linalg.norm(east))
    if east_norm <= 1e-12:
        east = np.cross(np.array([1.0, 0.0, 0.0]), up)
        east_norm = float(np.linalg.norm(east))
    if east_norm <= 1e-12:
        return False
    east /= east_norm
    north = np.cross(up, east)
    offsets = points - origin
    horizontal = np.column_stack((offsets @ east, offsets @ north))
    max_baseline = max(
        np.linalg.norm(horizontal[i] - horizontal[j])
        for i in range(len(horizontal))
        for j in range(i + 1, len(horizontal))
    )
    centered = horizontal - np.mean(horizontal, axis=0)
    singular_values = np.linalg.svd(centered, compute_uv=False)
    geometry_ratio = (
        float(singular_values[1] / singular_values[0])
        if len(singular_values) > 1 and singular_values[0] > 0
        else 0.0
    )
    safe = (
        max_baseline >= settings.MLAT_MIN_BASELINE_M
        and geometry_ratio >= settings.MLAT_MIN_GEOMETRY_RATIO
    )
    if not safe:
        log.warning(
            "MLAT rejected: unsafe horizontal receiver geometry baseline=%.1fm ratio=%.4f",
            max_baseline, geometry_ratio,
        )
    return safe


def validate_receiver_configuration(
    locations=None,
    min_receivers: Optional[int] = None,
) -> dict[str, np.ndarray]:
    """Validate the complete receiver trust configuration before I/O starts."""
    locations = settings.MLAT_RECEIVER_LOCATIONS if locations is None else locations
    minimum = settings.MLAT_MIN_RECEIVERS if min_receivers is None else min_receivers
    if not isinstance(locations, dict) or len(locations) < max(4, minimum):
        raise ValueError("At least four configured MLAT receivers are required")
    positions: dict[str, np.ndarray] = {}
    for receiver_id, location in locations.items():
        if not isinstance(receiver_id, str) or not receiver_id.strip():
            raise ValueError("MLAT receiver identities must be nonempty strings")
        if not isinstance(location, (list, tuple)) or len(location) != 3:
            raise ValueError(f"Invalid location for MLAT receiver {receiver_id}")
        lat, lon, altitude = (float(value) for value in location)
        if not all(math.isfinite(value) for value in (lat, lon, altitude)):
            raise ValueError(f"Nonfinite location for MLAT receiver {receiver_id}")
        if not (-90 <= lat <= 90 and -180 <= lon <= 180 and -500 <= altitude <= 20_000):
            raise ValueError(f"Out-of-range location for MLAT receiver {receiver_id}")
        positions[receiver_id] = geodetic_to_ecef(lat, lon, altitude)
    if not receiver_geometry_is_safe(list(positions.values())):
        raise ValueError("Unsafe configured MLAT receiver geometry")
    return positions


# ─── Receiver Registry ─────────────────────────────────────────────────────────

class ReceiverRegistry:
    """
    Tracks known receivers and their ECEF positions.
    In production this would load from a database; here we allow dynamic registration.
    """

    def __init__(self) -> None:
        # receiver_id → (lat, lon, alt_m, ecef np.array)
        self._receivers: Dict[str, dict] = {}

    def register(self, receiver_id: str, lat: float, lon: float, alt_m: float = 10.0) -> None:
        ecef = geodetic_to_ecef(lat, lon, alt_m)
        self._receivers[receiver_id] = {
            "lat": lat, "lon": lon, "alt": alt_m, "ecef": ecef
        }
        log.info("Receiver registered: %s @ (%.4f, %.4f, %.1fm)", receiver_id, lat, lon, alt_m)

    def get(self, receiver_id: str) -> Optional[np.ndarray]:
        r = self._receivers.get(receiver_id)
        return r["ecef"] if r else None

    def get_all(self) -> Dict[str, np.ndarray]:
        return {rid: r["ecef"] for rid, r in self._receivers.items()}


# ─── TDOA Frame Grouping ───────────────────────────────────────────────────────

class TDOAFrame:
    """
    Groups raw reception reports for the same Mode S message across multiple receivers.
    Key: (icao24, message_hash) — same physical transmission
    """

    def __init__(self, icao24: str, raw_message: str) -> None:
        self.icao24 = icao24
        self.raw_message = raw_message
        self.receptions: List[Tuple[str, float]] = []   # (receiver_id, timestamp)
        self.created_at = time.time()

    def add_reception(self, receiver_id: str, timestamp: float) -> None:
        self.receptions.append((receiver_id, timestamp))

    def is_solvable(self, min_receivers: int = 4) -> bool:
        return len(self.receptions) >= min_receivers

    def age(self) -> float:
        return time.time() - self.created_at


# ─── MLAT Solver Core ─────────────────────────────────────────────────────────

class MLATSolver:
    """
    Solves aircraft position from TDOA measurements using
    Levenberg-Marquardt nonlinear least-squares optimization.
    """

    def solve(
        self,
        receiver_positions: List[np.ndarray],
        timestamps: List[float],
        initial_guess: Optional[np.ndarray] = None,
    ) -> Optional[dict]:
        """
        Parameters
        ----------
        receiver_positions : list of ECEF position arrays [N × 3]
        timestamps         : list of arrival times in seconds [N]
        initial_guess      : ECEF position [3] or None

        Returns
        -------
        dict with keys: lat, lon, alt_m, cep90, tdoa_residual, num_receivers
        or None if solve failed
        """
        n = len(receiver_positions)
        assert n == len(timestamps), "Position/timestamp count mismatch"

        if n < 4:
            return None

        # Use first receiver as reference; compute TDOAs relative to it
        t0 = timestamps[0]
        r0 = receiver_positions[0]
        tdoas = np.array([(t - t0) * C for t in timestamps[1:]])   # in metres
        receivers = receiver_positions

        # Initial guess: centroid of receivers at 10,000m altitude
        if initial_guess is None:
            centroid = np.mean(receivers, axis=0)
            centroid = centroid / np.linalg.norm(centroid) * (np.linalg.norm(centroid) + 10_000)
            x0 = centroid
        else:
            x0 = initial_guess.copy()

        def residuals(p: np.ndarray) -> np.ndarray:
            """
            For each receiver i (i>0):
              predicted_tdoa[i] = (|p - rᵢ| - |p - r₀|)
              residual[i] = predicted_tdoa[i] - measured_tdoa[i]
            """
            d0 = np.linalg.norm(p - r0)
            res = []
            for i in range(1, n):
                di = np.linalg.norm(p - receivers[i])
                predicted = di - d0
                res.append(predicted - tdoas[i - 1])
            return np.array(res)

        try:
            result = least_squares(
                residuals,
                x0,
                method="lm",
                ftol=1e-9,
                xtol=1e-9,
                gtol=1e-9,
                max_nfev=200,
            )
        except Exception as e:
            log.debug("MLAT solve failed: %s", e)
            return None

        if not result.success and result.cost > 1e6:
            return None

        pos = result.x
        lat, lon, alt_m = ecef_to_geodetic(pos)

        # Sanity checks
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            return None
        if alt_m < -500 or alt_m > 20_000:   # metres: surface to ~65,000 ft
            return None

        # RMS TDOA residual in nanoseconds
        rms_ns = float(np.sqrt(np.mean(result.fun ** 2)) / C * 1e9)

        # CEP90 approximation from Jacobian covariance
        try:
            J = result.jac
            if np.linalg.matrix_rank(J) < 3:
                raise ValueError("Rank-deficient MLAT geometry")
            variance = covariance_variance(
                result.cost, len(result.fun), 3, settings.MLAT_TIMESTAMP_NOISE_NS
            )
            cov = np.linalg.inv(J.T @ J) * variance
            cep90 = horizontal_cep90(pos, cov)
        except Exception:
            cep90 = 9999.0

        return {
            "lat": lat,
            "lon": lon,
            "alt_m": alt_m,
            "alt_ft": int(alt_m * 3.28084),
            "cep90": cep90,
            "tdoa_residual": rms_ns,
            "num_receivers": n,
        }


# ─── Frame Accumulator ────────────────────────────────────────────────────────

class FrameAccumulator:
    """
    Accumulates TDOA frames and solves when enough receivers have reported.
    Frames expire after WINDOW_SEC if unsolvable.
    """
    TRANSMISSION_WINDOW_SEC = 0.01
    FRAME_TTL_SEC = 2.0
    WINDOW_SEC = TRANSMISSION_WINDOW_SEC

    def __init__(self, registry: ReceiverRegistry, solver: MLATSolver) -> None:
        self.registry = registry
        self.solver = solver
        # key: (icao24, msg_hash) → TDOAFrame
        self._frames: Dict[str, TDOAFrame] = {}
        self._solved: Dict[str, float] = {}   # key → solve time

    def snapshot(self) -> dict:
        return {
            "frames": {
                key: {
                    "icao24": frame.icao24,
                    "raw_message": frame.raw_message,
                    "receptions": frame.receptions,
                    "created_at": frame.created_at,
                }
                for key, frame in self._frames.items()
            },
            "solved": self._solved,
        }

    def restore(self, data: dict) -> None:
        self._frames = {}
        for key, raw in data.get("frames", {}).items():
            frame = TDOAFrame(raw["icao24"], raw["raw_message"])
            frame.receptions = [tuple(item) for item in raw.get("receptions", [])]
            frame.created_at = float(raw.get("created_at", time.time()))
            self._frames[key] = frame
        self._solved = {
            key: float(solved_at) for key, solved_at in data.get("solved", {}).items()
        }

    def add_message(self, msg: RawADSBMessage) -> Optional[RawMLATReport]:
        """
        Add a received message. Returns a solved position report if ready.
        """
        # Prune expired frames
        self._prune()

        # Only process DF11 / DF17 / DF18 / DF20 messages
        if msg.msg_type not in (11, 17, 18, 20, 21):
            return None

        recv_pos = self.registry.get(msg.receiver_id)
        if recv_pos is None:
            return None  # Unknown receiver

        # Repeated Mode-S payloads are distinct transmissions. Associate only
        # receptions whose physical event times fit the same narrow window.
        base_key = f"{msg.icao24}:{msg.raw_message}"
        compatible: list[tuple[float, str]] = []
        for candidate_key, candidate in self._frames.items():
            if not (candidate_key == base_key or candidate_key.startswith(base_key + ":")) \
                    or not candidate.receptions:
                continue
            candidate_times = [ts for _, ts in candidate.receptions]
            times = candidate_times + [msg.recv_time]
            if max(times) - min(times) > self.TRANSMISSION_WINDOW_SEC:
                continue
            if msg.receiver_id in {rid for rid, _ in candidate.receptions}:
                log.warning(
                    "Rejecting duplicate MLAT receiver %s across compatible frames for %s",
                    msg.receiver_id, msg.icao24,
                )
                return None
            distance = abs(msg.recv_time - float(np.median(candidate_times)))
            compatible.append((distance, candidate_key))

        key = ""
        if compatible:
            compatible.sort(key=lambda item: (item[0], item[1]))
            if (
                len(compatible) > 1
                and math.isclose(
                    compatible[0][0], compatible[1][0],
                    rel_tol=0.0, abs_tol=1e-9,
                )
            ):
                log.warning(
                    "Rejecting ambiguous MLAT reception for %s across frames %s and %s",
                    msg.icao24, compatible[0][1], compatible[1][1],
                )
                return None
            key = compatible[0][1]
        if not key:
            bucket = int(msg.recv_time / self.TRANSMISSION_WINDOW_SEC)
            key = f"{base_key}:{bucket}"
        if key in self._solved:
            return None

        if key not in self._frames:
            self._frames[key] = TDOAFrame(msg.icao24, msg.raw_message)

        frame = self._frames[key]
        # Deduplicate receiver reports
        existing_receivers = {r for r, _ in frame.receptions}
        if msg.receiver_id not in existing_receivers:
            frame.add_reception(msg.receiver_id, msg.recv_time)

        # Try to solve
        if frame.is_solvable(settings.MLAT_MIN_RECEIVERS):
            result = self._try_solve(frame)
            if result:
                self._solved[key] = time.time()
                del self._frames[key]
                return result

        return None

    def _try_solve(self, frame: TDOAFrame) -> Optional[RawMLATReport]:
        positions = []
        timestamps = []
        receiver_ids = []

        for rid, ts in frame.receptions:
            ecef = self.registry.get(rid)
            if ecef is not None:
                positions.append(ecef)
                timestamps.append(ts)
                receiver_ids.append(rid)

        if len(positions) < settings.MLAT_MIN_RECEIVERS:
            return None

        if not receiver_geometry_is_safe(positions):
            return None

        result = self.solver.solve(positions, timestamps)
        if not result:
            return None

        if result["tdoa_residual"] > settings.MLAT_MAX_TDOA_RESIDUAL:
            log.debug("MLAT rejected: residual %.0f ns > threshold", result["tdoa_residual"])
            return None

        event_time = float(np.median(timestamps))
        report = RawMLATReport(
            session_id=f"mlat-{frame.icao24}-{int(event_time * 1_000_000)}",
            solve_time=event_time,
            icao24=frame.icao24.upper(),
            lat=result["lat"],
            lon=result["lon"],
            altitude_baro=result["alt_ft"],
            num_receivers=result["num_receivers"],
            tdoa_residual=result["tdoa_residual"],
            cep90=result["cep90"],
            receiver_ids=receiver_ids,
            source_event_ids=[
                hashlib.sha256(
                    f"{rid}:{ts:.9f}:{frame.raw_message}".encode()
                ).hexdigest()
                for rid, ts in frame.receptions if rid in receiver_ids
            ],
        )
        report.auth_tag = sign_mlat_report(report, settings.MLAT_SOLVER_SIGNING_KEY)
        return report

    def _prune(self) -> None:
        expired = [k for k, f in self._frames.items() if f.age() > self.FRAME_TTL_SEC]
        for k in expired:
            del self._frames[k]
        solved_cutoff = time.time() - self.FRAME_TTL_SEC
        self._solved = {
            key: solved_at for key, solved_at in self._solved.items()
            if solved_at >= solved_cutoff
        }


# ─── Main Processing Loop ─────────────────────────────────────────────────────

async def process_adsb_for_mlat(
    accumulator, producer, raw_value: bytes, state_store=None, adsb_msg=None
) -> bool:
    """Process one input and wait for any MLAT output to reach Kafka."""
    before = accumulator.snapshot()
    try:
        adsb_msg = adsb_msg or RawADSBMessage.from_bytes(raw_value)
        now = time.time()
        if (
            adsb_msg.recv_time < now - settings.SOURCE_EVENT_MAX_AGE_SEC
            or adsb_msg.recv_time > now + settings.SOURCE_EVENT_FUTURE_SKEW_SEC
        ):
            log.warning("Rejecting stale/future physical reception %s", adsb_msg.recv_time)
            return False
        report = accumulator.add_message(adsb_msg)
        if report is not None:
            await producer.send_and_wait(
                topic=settings.TOPIC_RAW_MLAT,
                key=report.icao24.encode(),
                value=report.to_bytes(),
            )
        if state_store is not None:
            await state_store.setex(
                "mlat:accumulator", 60, orjson.dumps(accumulator.snapshot())
            )
        return report is not None
    except Exception:
        accumulator.restore(before)
        raise


async def run() -> None:
    logging.basicConfig(level=settings.LOG_LEVEL)
    log.info("Starting MLAT solver")

    validate_receiver_configuration()
    receiver_keys = validate_receiver_credentials(
        settings.MLAT_RECEIVER_LOCATIONS, settings.MLAT_RECEIVER_API_KEYS
    )
    if not settings.MLAT_SOLVER_SIGNING_KEY:
        raise ValueError("MLAT solver signing key is required")
    registry = ReceiverRegistry()
    solver = MLATSolver()
    accumulator = FrameAccumulator(registry, solver)
    state_store = aioredis.from_url(settings.REDIS_URL, decode_responses=False)
    saved_state = await state_store.get("mlat:accumulator")
    if saved_state:
        try:
            accumulator.restore(orjson.loads(saved_state))
        except Exception as exc:
            log.warning("Discarding invalid MLAT accumulator state: %s", exc)

    for receiver_id, location in settings.MLAT_RECEIVER_LOCATIONS.items():
        registry.register(receiver_id, *location)

    consumer = AIOKafkaConsumer(
        settings.TOPIC_MLAT_RECEPTIONS,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP,
        group_id=f"{settings.KAFKA_GROUP_PREFIX}.mlat-solver",
        value_deserializer=lambda v: v,
        auto_offset_reset="latest",
        **KAFKA_CONSUMER_STABILITY,
    )

    producer = AIOKafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP,
        compression_type="lz4",
    )

    await consumer.start()
    await producer.start()
    solved_count = 0

    try:
        async for kafka_msg in consumer:
            try:
                try:
                    adsb_msg = RawADSBMessage.from_bytes(kafka_msg.value)
                except Exception as exc:
                    log.error("Discarding malformed MLAT input record: %s", exc)
                    await commit_record(consumer, kafka_msg)
                    continue
                if (
                    not physical_reception_is_valid(adsb_msg)
                    or kafka_msg.key != physical_reception_key(
                        adsb_msg, receiver_keys[adsb_msg.receiver_id]
                    )
                ):
                    log.error("Discarding unauthenticated or identity-mismatched MLAT input")
                    await commit_record(consumer, kafka_msg)
                    continue
                if await process_adsb_for_mlat(
                    accumulator, producer, kafka_msg.value, state_store, adsb_msg
                ):
                    solved_count += 1
                    if solved_count % 100 == 0:
                        log.info("MLAT: %d positions solved", solved_count)
                await commit_record(consumer, kafka_msg)

            except Exception as e:
                log.error("MLAT processing error: %s", e)
                raise

    finally:
        await consumer.stop()
        await producer.stop()
        await state_store.close()


if __name__ == "__main__":
    asyncio.run(run())
