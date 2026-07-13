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
  - When ≥3 receivers see the same message, runs the TDOA solver
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
import logging
import time
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

import numpy as np
from scipy.optimize import least_squares
from pyproj import Transformer
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from models import RawADSBMessage, RawMLATReport
from config import settings

log = logging.getLogger(__name__)

# Speed of light in m/s
C = 299_792_458.0

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


def haversine_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in nautical miles."""
    R = 3440.065  # Earth radius in NM
    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlambda = np.radians(lon2 - lon1)
    a = np.sin(dphi / 2) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlambda / 2) ** 2
    return R * 2 * np.arcsin(np.sqrt(a))


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

    def is_solvable(self, min_receivers: int = 3) -> bool:
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

        if n < 3:
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
            cov = np.linalg.inv(J.T @ J) * (result.cost / max(len(result.fun) - 3, 1))
            sigma_h = float(np.sqrt(cov[0, 0] + cov[1, 1]))
            cep90 = sigma_h * 2.146   # approx 90th percentile factor for 2D Gaussian
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
    WINDOW_SEC = 0.5     # messages within 500ms are considered same transmission

    def __init__(self, registry: ReceiverRegistry, solver: MLATSolver) -> None:
        self.registry = registry
        self.solver = solver
        # key: (icao24, msg_hash) → TDOAFrame
        self._frames: Dict[str, TDOAFrame] = {}
        self._solved: set = set()   # avoid re-solving

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

        # Key: ICAO + raw message hash (same transmission across receivers)
        key = f"{msg.icao24}:{msg.raw_message}"
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
                self._solved.add(key)
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

        result = self.solver.solve(positions, timestamps)
        if not result:
            return None

        if result["tdoa_residual"] > settings.MLAT_MAX_TDOA_RESIDUAL:
            log.debug("MLAT rejected: residual %.0f ns > threshold", result["tdoa_residual"])
            return None

        return RawMLATReport(
            session_id=f"mlat-{int(time.time()*1000)}",
            solve_time=time.time(),
            icao24=frame.icao24.upper(),
            lat=result["lat"],
            lon=result["lon"],
            altitude_baro=result["alt_ft"],
            num_receivers=result["num_receivers"],
            tdoa_residual=result["tdoa_residual"],
            cep90=result["cep90"],
            receiver_ids=receiver_ids,
        )

    def _prune(self) -> None:
        expired = [k for k, f in self._frames.items() if f.age() > self.WINDOW_SEC * 4]
        for k in expired:
            del self._frames[k]


# ─── Main Processing Loop ─────────────────────────────────────────────────────

async def run() -> None:
    logging.basicConfig(level=settings.LOG_LEVEL)
    log.info("Starting MLAT solver")

    registry = ReceiverRegistry()
    solver = MLATSolver()
    accumulator = FrameAccumulator(registry, solver)

    # In production: load receiver locations from DB
    # For demo: register a few example receivers
    registry.register("receiver-london",   51.5074,  -0.1278,  15.0)
    registry.register("receiver-paris",    48.8566,   2.3522,  35.0)
    registry.register("receiver-brussels", 50.8503,   4.3517,  20.0)
    registry.register("receiver-amsterdam", 52.3676,  4.9041,  5.0)

    consumer = AIOKafkaConsumer(
        settings.TOPIC_RAW_ADSB,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP,
        group_id=f"{settings.KAFKA_GROUP_PREFIX}.mlat-solver",
        value_deserializer=lambda v: v,
        auto_offset_reset="latest",
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
                adsb_msg = RawADSBMessage.from_bytes(kafka_msg.value)
                report = accumulator.add_message(adsb_msg)

                if report:
                    await producer.send(
                        topic=settings.TOPIC_RAW_MLAT,
                        key=report.icao24.encode(),
                        value=report.to_bytes(),
                    )
                    solved_count += 1
                    if solved_count % 100 == 0:
                        log.info("MLAT: %d positions solved", solved_count)

            except Exception as e:
                log.error("MLAT processing error: %s", e)

    finally:
        await consumer.stop()
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(run())
