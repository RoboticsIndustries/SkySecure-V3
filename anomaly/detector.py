"""
anomaly/detector.py
───────────────────
Three-layer anomaly detection pipeline:

  Layer 1 — Rule-based:   Fast deterministic checks (physics violations,
                           spoofing patterns, airspace rules)
  Layer 2 — Statistical:  Per-aircraft baseline deviation (z-score)
  Layer 3 — ML:           LSTM sequence predictor — large prediction error
                           indicates anomalous trajectory

Consumes: fused.tracks
Produces: alerts.anomaly  (high-score events only)
Also writes enriched StateVectors back to Redis with updated risk scores.
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from collections import defaultdict, deque
from typing import Dict, List, Optional, Deque

import numpy as np
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import redis.asyncio as aioredis

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from models import StateVector, AnomalyFlag, AnomalyType, RiskBand
from config import settings

log = logging.getLogger(__name__)


# ─── Utility ──────────────────────────────────────────────────────────────────

def haversine_nm(lat1, lon1, lat2, lon2) -> float:
    R = 3440.065
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


# ─── Layer 1: Rule Engine ─────────────────────────────────────────────────────

class RuleEngine:
    """
    Fast deterministic checks applied to every state vector update.
    Each rule returns an AnomalyFlag or None.
    """

    def check_all(self, sv: StateVector) -> List[AnomalyFlag]:
        flags = []
        for rule in [
            self._check_impossible_speed,
            self._check_altitude_jump,
            self._check_baro_geo_delta,
            self._check_teleportation,
            self._check_transponder_loss,
            self._check_squawk_emergency,
        ]:
            flag = rule(sv)
            if flag:
                flags.append(flag)
        return flags

    def _check_impossible_speed(self, sv: StateVector) -> Optional[AnomalyFlag]:
        if sv.velocity is None:
            return None
        if sv.velocity > settings.MAX_GROUNDSPEED_KNOTS:
            return AnomalyFlag(
                anomaly_type=AnomalyType.IMPOSSIBLE_SPEED,
                score_delta=40,
                description=f"Speed {sv.velocity:.0f} kts exceeds physical maximum",
                meta={"velocity": sv.velocity},
            )
        return None

    def _check_altitude_jump(self, sv: StateVector) -> Optional[AnomalyFlag]:
        """Detect sudden altitude jumps that exceed aircraft performance limits."""
        history = sv.position_history
        if len(history) < 2:
            return None

        prev = history[-2]
        curr = history[-1]

        if prev.get("alt") is None or curr.get("alt") is None:
            return None

        dt = curr["t"] - prev["t"]
        if dt < 0.1:
            return None

        delta_alt = abs(curr["alt"] - prev["alt"])
        max_vr = 10_000   # ft/min for fastest jets
        max_possible = (dt / 60.0) * max_vr

        if delta_alt > settings.MAX_ALTITUDE_JUMP_FT and delta_alt > max_possible:
            return AnomalyFlag(
                anomaly_type=AnomalyType.IMPOSSIBLE_ALTITUDE,
                score_delta=35,
                description=f"Altitude jumped {delta_alt:.0f} ft in {dt:.1f}s",
                meta={"delta_alt": delta_alt, "dt": dt},
            )
        return None

    def _check_baro_geo_delta(self, sv: StateVector) -> Optional[AnomalyFlag]:
        """
        Large delta between barometric and GNSS altitude is a GNSS spoofing indicator.
        Normal delta: <500 ft. Suspicious: >2000 ft.
        """
        if sv.altitude_baro is None or sv.altitude_geo is None:
            return None
        delta = abs(sv.altitude_baro - sv.altitude_geo)
        if delta > settings.BARO_GEO_DELTA_FT:
            return AnomalyFlag(
                anomaly_type=AnomalyType.ALTITUDE_BARO_GEO_DELTA,
                score_delta=25,
                description=f"Baro/GNSS altitude delta {delta} ft — possible GNSS manipulation",
                meta={"baro": sv.altitude_baro, "geo": sv.altitude_geo, "delta": delta},
            )
        return None

    def _check_teleportation(self, sv: StateVector) -> Optional[AnomalyFlag]:
        """
        Detect impossibly fast position change (teleportation).
        Considers aircraft speed to allow for actual fast aircraft.
        """
        history = sv.position_history
        if len(history) < 2:
            return None

        prev = history[-2]
        curr = history[-1]

        dt = curr["t"] - prev["t"]
        if dt < 1.0:
            return None

        try:
            dist = haversine_nm(prev["lat"], prev["lon"], curr["lat"], curr["lon"])
        except Exception:
            return None

        # Max possible distance in dt at 1200 kts
        max_dist = (dt / 3600.0) * settings.MAX_GROUNDSPEED_KNOTS

        if dist > settings.MAX_TELEPORT_NM and dist > max_dist * 2:
            return AnomalyFlag(
                anomaly_type=AnomalyType.TELEPORTATION,
                score_delta=45,
                description=f"Position jumped {dist:.1f} NM in {dt:.0f}s",
                meta={"dist_nm": dist, "dt": dt},
            )
        return None

    def _check_transponder_loss(self, sv: StateVector) -> Optional[AnomalyFlag]:
        """
        Transponder switched off mid-flight (not landed).
        """
        if sv.on_ground:
            return None
        stale_threshold = settings.FUSION_STALE_THRESHOLD * 2   # 60s
        age = time.time() - sv.last_seen
        if age > stale_threshold and sv.altitude_baro and sv.altitude_baro > 2000:
            return AnomalyFlag(
                anomaly_type=AnomalyType.TRANSPONDER_OFF,
                score_delta=30,
                description=f"No signal for {age:.0f}s while airborne at {sv.altitude_baro} ft",
                meta={"age": age, "altitude": sv.altitude_baro},
            )
        return None

    def _check_squawk_emergency(self, sv: StateVector) -> Optional[AnomalyFlag]:
        # Emergency squawks are handled separately — no anomaly needed, just note
        return None


# ─── Layer 2: Statistical Detector ────────────────────────────────────────────

class AircraftBaseline:
    """Per-aircraft rolling statistics for velocity and altitude."""
    WINDOW = 300   # samples

    def __init__(self) -> None:
        self.velocities: Deque[float] = deque(maxlen=self.WINDOW)
        self.altitudes:  Deque[float] = deque(maxlen=self.WINDOW)
        self.vrates:     Deque[float] = deque(maxlen=self.WINDOW)

    def update(self, sv: StateVector) -> None:
        if sv.velocity is not None:
            self.velocities.append(sv.velocity)
        if sv.altitude_baro is not None:
            self.altitudes.append(float(sv.altitude_baro))
        if sv.vertical_rate is not None:
            self.vrates.append(float(sv.vertical_rate))

    def z_score(self, value: float, data: Deque[float]) -> float:
        if len(data) < 30:
            return 0.0
        arr = np.array(data)
        mu = arr.mean()
        sigma = arr.std()
        if sigma < 1e-6:
            return 0.0
        return abs((value - mu) / sigma)


class StatisticalDetector:
    """
    Maintain per-aircraft baselines and flag deviations > 3σ.
    """

    def __init__(self) -> None:
        self._baselines: Dict[str, AircraftBaseline] = {}

    def check(self, sv: StateVector) -> List[AnomalyFlag]:
        icao = sv.icao24
        if icao not in self._baselines:
            self._baselines[icao] = AircraftBaseline()

        baseline = self._baselines[icao]
        flags = []

        if sv.velocity is not None and len(baseline.velocities) >= 30:
            z = baseline.z_score(sv.velocity, baseline.velocities)
            if z > 4.0:
                flags.append(AnomalyFlag(
                    anomaly_type=AnomalyType.IMPOSSIBLE_SPEED,
                    score_delta=min(30, int(z * 5)),
                    description=f"Velocity {sv.velocity:.0f} kts is {z:.1f}σ from aircraft baseline",
                    meta={"z_score": z, "velocity": sv.velocity},
                ))

        if sv.vertical_rate is not None and len(baseline.vrates) >= 30:
            z = baseline.z_score(float(sv.vertical_rate), baseline.vrates)
            if z > 5.0:
                flags.append(AnomalyFlag(
                    anomaly_type=AnomalyType.IMPOSSIBLE_ALTITUDE,
                    score_delta=min(20, int(z * 3)),
                    description=f"Vertical rate {sv.vertical_rate} fpm is {z:.1f}σ from baseline",
                    meta={"z_score": z, "vrate": sv.vertical_rate},
                ))

        baseline.update(sv)
        return flags


# ─── Layer 3: LSTM Trajectory Predictor ───────────────────────────────────────

class LSTMTrajectoryPredictor:
    """
    Lightweight LSTM that predicts the next position/velocity from a window
    of past observations. Large prediction error → anomaly.

    In production: load a pre-trained model from disk.
    Here we implement the full architecture + a simple heuristic fallback.
    """

    SEQ_LEN = 20       # input sequence length
    FEATURES = 5       # lat, lon, alt, vel, heading
    HIDDEN = 64
    ANOMALY_THRESHOLD = 0.15   # normalized prediction error

    def __init__(self) -> None:
        self._model = None
        self._sequences: Dict[str, Deque] = {}
        self._model_loaded = False
        self._load_model()

    def _load_model(self) -> None:
        """Attempt to load a pre-trained LSTM model."""
        try:
            import torch
            import torch.nn as nn

            class TrajectoryLSTM(nn.Module):
                def __init__(self, input_size=5, hidden_size=64, num_layers=2):
                    super().__init__()
                    self.lstm = nn.LSTM(input_size, hidden_size, num_layers,
                                        batch_first=True, dropout=0.1)
                    self.fc = nn.Linear(hidden_size, input_size)

                def forward(self, x):
                    out, _ = self.lstm(x)
                    return self.fc(out[:, -1, :])   # predict next step from last hidden state

            model = TrajectoryLSTM()
            model_path = "data/trajectory_lstm.pt"
            if os.path.exists(model_path):
                model.load_state_dict(torch.load(model_path, map_location="cpu"))
                model.eval()
                self._model = model
                self._model_loaded = True
                log.info("LSTM trajectory model loaded")
            else:
                log.info("No pre-trained LSTM found; using heuristic predictor")
        except ImportError:
            log.warning("PyTorch not available; LSTM predictor disabled")

    def update_and_check(self, sv: StateVector) -> Optional[AnomalyFlag]:
        if sv.lat is None or sv.lon is None:
            return None

        icao = sv.icao24
        if icao not in self._sequences:
            self._sequences[icao] = deque(maxlen=self.SEQ_LEN + 1)

        seq = self._sequences[icao]
        seq.append([
            sv.lat or 0.0,
            sv.lon or 0.0,
            (sv.altitude_baro or 0) / 45_000.0,   # normalize to ~0-1
            (sv.velocity or 0) / 1200.0,
            (sv.heading or 0) / 360.0,
        ])

        if len(seq) < self.SEQ_LEN + 1:
            return None

        if self._model_loaded:
            return self._lstm_check(sv, seq)
        else:
            return self._heuristic_check(sv, seq)

    def _lstm_check(self, sv: StateVector, seq: Deque) -> Optional[AnomalyFlag]:
        import torch
        arr = np.array(list(seq), dtype=np.float32)
        x = torch.tensor(arr[:-1]).unsqueeze(0)   # [1, SEQ_LEN, FEATURES]
        actual = arr[-1]

        with torch.no_grad():
            predicted = self._model(x).numpy()[0]

        error = float(np.mean(np.abs(predicted - actual)))
        if error > self.ANOMALY_THRESHOLD:
            score_delta = min(25, int(error * 100))
            return AnomalyFlag(
                anomaly_type=AnomalyType.MILITARY_BEHAVIOR,
                score_delta=score_delta,
                description=f"LSTM trajectory prediction error {error:.3f} — unusual flight pattern",
                meta={"prediction_error": error},
            )
        return None

    def _heuristic_check(self, sv: StateVector, seq: Deque) -> Optional[AnomalyFlag]:
        """
        Simple heuristic: predict next position via linear extrapolation,
        compare to actual.
        """
        arr = list(seq)
        if len(arr) < 3:
            return None

        # Linear extrapolation from last 2 points
        prev2 = arr[-3]
        prev1 = arr[-2]
        actual = arr[-1]

        predicted_lat = prev1[0] + (prev1[0] - prev2[0])
        predicted_lon = prev1[1] + (prev1[1] - prev2[1])

        error_lat = abs(actual[0] - predicted_lat)
        error_lon = abs(actual[1] - predicted_lon)
        total_error = error_lat + error_lon

        if total_error > 0.5:   # ~30 NM sudden deviation
            return AnomalyFlag(
                anomaly_type=AnomalyType.MILITARY_BEHAVIOR,
                score_delta=15,
                description=f"Trajectory deviation {total_error:.3f} deg from predicted path",
                meta={"error": total_error},
            )
        return None


# ─── Threat Scorer ────────────────────────────────────────────────────────────

class ThreatScorer:
    """
    Combines anomaly flags from all three layers into a single 0–100 risk score.

    Score is additive but capped, with decay over time (score fades if no new anomalies).
    """

    DECAY_RATE = 2.0   # points/minute
    _last_scores: Dict[str, dict] = {}

    def compute(self, sv: StateVector, new_flags: List[AnomalyFlag]) -> int:
        """
        Returns updated risk score (0–100).
        """
        icao = sv.icao24
        now = time.time()

        # Load prior score
        prior = self._last_scores.get(icao, {"score": sv.risk_score, "t": now})
        elapsed_min = (now - prior["t"]) / 60.0

        # Decay prior score
        decayed = max(0, prior["score"] - elapsed_min * self.DECAY_RATE)

        # Add new flag deltas
        new_delta = sum(f.score_delta for f in new_flags)

        # Military classification bonus
        mil_bonus = int(sv.military_score * 20)

        final = min(100, int(decayed + new_delta + mil_bonus))

        self._last_scores[icao] = {"score": final, "t": now}
        return final


# ─── Full Detector Pipeline ───────────────────────────────────────────────────

class AnomalyDetector:

    def __init__(self) -> None:
        self.rule_engine  = RuleEngine()
        self.statistical  = StatisticalDetector()
        self.lstm         = LSTMTrajectoryPredictor()
        self.scorer       = ThreatScorer()

    def process(self, sv: StateVector) -> StateVector:
        """Run all detection layers on a state vector, return enriched SV."""
        all_flags: List[AnomalyFlag] = []

        # Layer 1: Rules
        all_flags.extend(self.rule_engine.check_all(sv))

        # Layer 2: Statistics
        all_flags.extend(self.statistical.check(sv))

        # Layer 3: LSTM
        lstm_flag = self.lstm.update_and_check(sv)
        if lstm_flag:
            all_flags.append(lstm_flag)

        # Merge into state vector
        sv.anomalies.extend(all_flags)

        # Compute risk score
        sv.risk_score = self.scorer.compute(sv, all_flags)
        sv.update_risk_band()

        return sv


# ─── Main Loop ────────────────────────────────────────────────────────────────

async def run() -> None:
    logging.basicConfig(level=settings.LOG_LEVEL)
    log.info("Starting anomaly detector")

    redis_client = aioredis.from_url(settings.REDIS_URL, decode_responses=False)
    detector = AnomalyDetector()

    consumer = AIOKafkaConsumer(
        settings.TOPIC_FUSED_TRACKS,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP,
        group_id=f"{settings.KAFKA_GROUP_PREFIX}.anomaly-detector",
        value_deserializer=lambda v: v,
        auto_offset_reset="latest",
        fetch_max_bytes=10_485_760,
    )

    alert_producer = AIOKafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP,
        compression_type="lz4",
    )

    await consumer.start()
    await alert_producer.start()

    count = 0
    try:
        async for msg in consumer:
            try:
                sv = StateVector.from_bytes(msg.value)
                sv = detector.process(sv)

                # Write enriched SV back to Redis
                key = f"sv:{sv.icao24}"
                await redis_client.setex(key, settings.REDIS_TTL_STATE_VECTOR, sv.to_bytes())

                # Publish alerts for ALERT/CRITICAL band
                if sv.risk_band in (RiskBand.ALERT, RiskBand.CRITICAL):
                    await alert_producer.send(
                        topic=settings.TOPIC_ALERTS_ANOMALY,
                        key=sv.icao24.encode(),
                        value=sv.to_bytes(),
                    )

                count += 1
                if count % 10_000 == 0:
                    log.info("Anomaly detector: processed %d state vectors", count)

            except Exception as e:
                log.error("Anomaly detection error: %s", e)

    finally:
        await consumer.stop()
        await alert_producer.stop()
        await redis_client.close()


if __name__ == "__main__":
    asyncio.run(run())
