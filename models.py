"""
skysecure/models.py
───────────────────
Core data models shared across ALL modules.
Uses Pydantic v2 for validation + fast serialization via orjson.
"""

from __future__ import annotations

import math
import time
from enum import Enum
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
import orjson


def normalize_icao24(value: str) -> str:
    """Return canonical six-hex ICAO identity or reject the record."""
    if not isinstance(value, str):
        raise ValueError("ICAO24 must be a string")
    normalized = value.strip().upper()
    if len(normalized) != 6 or any(char not in "0123456789ABCDEF" for char in normalized):
        raise ValueError("ICAO24 must contain exactly six hexadecimal characters")
    return normalized


class FiniteBaseModel(BaseModel):
    """Canonical telemetry models reject NaN/Infinity at deserialization."""
    model_config = ConfigDict(allow_inf_nan=False)

    @model_validator(mode="before")
    @classmethod
    def reject_nested_nonfinite(cls, value):
        def walk(item):
            if isinstance(item, float) and not math.isfinite(item):
                raise ValueError("non-finite numeric value")
            if isinstance(item, dict):
                for nested in item.values():
                    walk(nested)
            elif isinstance(item, (list, tuple)):
                for nested in item:
                    walk(nested)
        walk(value)
        return value


# ─── Enumerations ──────────────────────────────────────────────────────────────

class DataSource(str, Enum):
    ADSB      = "ADSB"
    MLAT      = "MLAT"
    ACARS     = "ACARS"
    SATELLITE = "SATELLITE"
    FUSED     = "FUSED"
    UNKNOWN   = "UNKNOWN"


class Classification(str, Enum):
    CIVILIAN          = "CIVILIAN"
    LIKELY_MILITARY   = "LIKELY_MILITARY"
    CONFIRMED_MILITARY = "CONFIRMED_MILITARY"
    UNKNOWN           = "UNKNOWN"
    DARK_AIRCRAFT     = "DARK_AIRCRAFT"   # MLAT-only, no ID
    SPOOFED           = "SPOOFED"


class AnomalyType(str, Enum):
    IMPOSSIBLE_SPEED        = "IMPOSSIBLE_SPEED"
    IMPOSSIBLE_ALTITUDE     = "IMPOSSIBLE_ALTITUDE"
    ABNORMAL_ACCELERATION   = "ABNORMAL_ACCELERATION"
    ABNORMAL_TURN_RATE      = "ABNORMAL_TURN_RATE"
    ABNORMAL_CLIMB_RATE     = "ABNORMAL_CLIMB_RATE"
    TRAJECTORY_DEVIATION    = "TRAJECTORY_DEVIATION"
    GNSS_SPOOF              = "GNSS_SPOOF"
    IDENTITY_SPOOF          = "IDENTITY_SPOOF"
    GHOST_AIRCRAFT          = "GHOST_AIRCRAFT"        # ADS-B but no MLAT confirm
    SILENT_AIRCRAFT         = "SILENT_AIRCRAFT"       # MLAT only, no ADS-B
    TRANSPONDER_OFF         = "TRANSPONDER_OFF"
    TELEPORTATION           = "TELEPORTATION"
    FORMATION_FLIGHT        = "FORMATION_FLIGHT"
    AIRSPACE_VIOLATION      = "AIRSPACE_VIOLATION"
    ALTITUDE_BARO_GEO_DELTA = "ALTITUDE_BARO_GEO_DELTA"
    MILITARY_BEHAVIOR       = "MILITARY_BEHAVIOR"
    DUPLICATE_ICAO          = "DUPLICATE_ICAO"
    INTEGRITY_DEGRADATION   = "INTEGRITY_DEGRADATION"


class DetectionLayer(str, Enum):
    """Canonical SkySecure detection layers used across services and UI."""
    L1 = "L1"  # position/source validation
    L2 = "L2"  # kinematic and behavioral detection
    L3 = "L3"  # learned trajectory models
    L4 = "L4"  # multi-sensor fusion
    L5 = "L5"  # identity and threat intelligence


class LayerStatus(str, Enum):
    EVALUATED = "EVALUATED"
    TRIGGERED = "TRIGGERED"
    SKIPPED = "SKIPPED"


class RiskBand(str, Enum):
    NORMAL   = "NORMAL"    # 0–20
    MONITOR  = "MONITOR"   # 21–50
    ALERT    = "ALERT"     # 51–75
    CRITICAL = "CRITICAL"  # 76–100


# ─── Raw Message Models ────────────────────────────────────────────────────────

class RawADSBMessage(FiniteBaseModel):
    """
    Decoded ADS-B / Mode S message as received from an edge receiver.
    Timestamps are Unix epoch with microsecond precision.
    """
    receiver_id:   str
    recv_time:     float           = Field(description="Unix timestamp (us precision) from GPS-disciplined clock")
    icao24:        str             = Field(min_length=6, max_length=6)
    raw_message:   str             = Field(description="Raw hex Mode S message (14 or 28 hex chars)")
    msg_type:      int             = Field(ge=0, le=31, description="DF (Downlink Format)")

    # Decoded fields (may be None depending on message type)
    callsign:      Optional[str]   = Field(None, max_length=8)
    lat:           Optional[float] = Field(None, ge=-90, le=90)
    lon:           Optional[float] = Field(None, ge=-180, le=180)
    altitude_baro: Optional[int]   = Field(None, ge=-(2**31), le=2**31 - 1, description="Barometric altitude, feet")
    altitude_geo:  Optional[int]   = Field(None, ge=-(2**31), le=2**31 - 1, description="GNSS altitude, feet")
    velocity:      Optional[float] = Field(None, ge=0, le=2000, description="Ground speed, knots")
    heading:       Optional[float] = Field(None, ge=0, lt=360)
    vertical_rate: Optional[int]   = Field(None, ge=-(2**31), le=2**31 - 1, description="ft/min, positive=climb")
    on_ground:     Optional[bool]  = None
    squawk:        Optional[str]   = Field(None, max_length=4)
    nic:           Optional[int]   = Field(None, ge=0, le=15, description="Navigation Integrity Category")
    nac_p:         Optional[int]   = Field(None, ge=0, le=15, description="Navigation Accuracy Category - Position")
    raim:          Optional[bool]   = Field(None, description="RAIM flag from GNSS")

    @field_validator("icao24")
    @classmethod
    def icao_uppercase(cls, v: str) -> str:
        return normalize_icao24(v)

    def to_bytes(self) -> bytes:
        return orjson.dumps(self.model_dump())

    @classmethod
    def from_bytes(cls, data: bytes) -> "RawADSBMessage":
        return cls(**orjson.loads(data))


class RawMLATReport(FiniteBaseModel):
    """
    Position estimate from the MLAT solver.
    """
    session_id:    str
    solve_time:    float
    icao24:        str
    lat:           float  = Field(ge=-90, le=90)
    lon:           float  = Field(ge=-180, le=180)
    altitude_baro: int    = Field(ge=-(2**31), le=2**31 - 1)
    velocity:      Optional[float] = Field(None, ge=0, le=2000)
    heading:       Optional[float] = Field(None, ge=0, lt=360)
    num_receivers: int             = Field(ge=4, le=64, description="Receivers used in solve")
    tdoa_residual: float           = Field(ge=0, le=500, description="RMS TDOA residual (ns)")
    cep90:         float           = Field(ge=0, le=10_000, description="90% circular error probable, meters")
    receiver_ids:  List[str]       = Field(default_factory=list, min_length=4, max_length=64)
    source_event_ids: List[str]    = Field(min_length=4, max_length=64)
    auth_tag:       Optional[str]   = Field(None, min_length=64, max_length=64)

    @model_validator(mode="after")
    def validate_receiver_identity(self):
        if len(self.receiver_ids) != self.num_receivers:
            raise ValueError("receiver_ids count must equal num_receivers")
        if len(set(self.receiver_ids)) != len(self.receiver_ids):
            raise ValueError("receiver_ids must be unique")
        if len(self.source_event_ids) != self.num_receivers:
            raise ValueError("source_event_ids count must equal num_receivers")
        if len(set(self.source_event_ids)) != len(self.source_event_ids):
            raise ValueError("source_event_ids must be unique")
        if any(
            len(event_id) != 64
            or event_id != event_id.lower()
            or any(char not in "0123456789abcdef" for char in event_id)
            for event_id in self.source_event_ids
        ):
            raise ValueError("source_event_ids must be lowercase SHA-256 hex")
        return self

    @field_validator("icao24")
    @classmethod
    def validate_icao24(cls, value: str) -> str:
        return normalize_icao24(value)

    def to_bytes(self) -> bytes:
        return orjson.dumps(self.model_dump())

    @classmethod
    def from_bytes(cls, data: bytes) -> "RawMLATReport":
        return cls(**orjson.loads(data))


class RawACARSMessage(BaseModel):
    """
    Decoded ACARS message.
    """
    recv_time:      float
    registration:   Optional[str]  = None
    flight:         Optional[str]  = None
    label:          Optional[str]  = None
    sublabel:       Optional[str]  = None
    message_number: Optional[str]  = None
    content:        Optional[str]  = None
    frequency:      Optional[float] = None
    raw:            str

    def to_bytes(self) -> bytes:
        return orjson.dumps(self.model_dump())


# ─── Fused State Vector ────────────────────────────────────────────────────────

class SourceReport(FiniteBaseModel):
    """One source's contribution to the fused state."""
    source:     DataSource
    receiver_id: Optional[str] = None
    lat:        Optional[float] = None
    lon:        Optional[float] = None
    altitude:   Optional[int]   = None
    velocity:   Optional[float] = None
    heading:    Optional[float] = None
    vertical_rate: Optional[int] = None
    weight:     float           = 1.0
    confidence: float           = 1.0
    timestamp:  float           = Field(default_factory=time.time)


class AnomalyFlag(FiniteBaseModel):
    anomaly_type: AnomalyType
    # Defaults preserve compatibility with state vectors written before layer
    # telemetry existed. New detectors always set these fields explicitly.
    layer:        DetectionLayer = DetectionLayer.L2
    detector:     str = "legacy"
    score_delta:  int
    description:  str
    timestamp:    float = Field(default_factory=time.time)
    meta:         Dict[str, Any] = {}

    def to_api_dict(self) -> Dict[str, Any]:
        return {
            "type": self.anomaly_type.value,
            "layer": self.layer.value,
            "detector": self.detector,
            "score_delta": self.score_delta,
            "description": self.description,
            "evidence": self.meta,
            "timestamp": self.timestamp,
        }


class LayerEvaluation(FiniteBaseModel):
    layer:                 DetectionLayer
    status:                LayerStatus = LayerStatus.EVALUATED
    detectors_evaluated:   List[str] = []
    triggered_detectors:   List[str] = []
    score_delta:           int = 0
    skipped_reason:        Optional[str] = None
    timestamp:             float = Field(default_factory=time.time)


class StateVector(FiniteBaseModel):
    """
    The canonical, unified representation of a single aircraft.
    This is the primary output of the fusion engine and the input
    to the anomaly detector, visualization layer, and API.
    """
    # Identity
    icao24:         str
    callsign:       Optional[str]  = None
    registration:   Optional[str]  = None
    operator:       Optional[str]  = None

    # Position (fused best-estimate)
    lat:            Optional[float] = None
    lon:            Optional[float] = None
    altitude_baro:  Optional[int]   = None
    altitude_geo:   Optional[int]   = None
    velocity:       Optional[float] = None
    heading:        Optional[float] = None
    vertical_rate:  Optional[int]   = None
    nic:            Optional[int]   = None
    nac_p:          Optional[int]   = None
    on_ground:      bool            = False

    # Data provenance
    primary_source: DataSource      = DataSource.UNKNOWN
    last_update_source: DataSource  = DataSource.UNKNOWN
    last_update_timestamp: Optional[float] = None
    source_event_id: Optional[str] = None
    sources:        List[DataSource] = []
    source_reports: List[SourceReport] = []
    confidence:     float           = 0.0   # 0.0–1.0

    # Classification
    classification: Classification  = Classification.UNKNOWN
    military_score: float           = 0.0   # P(military), 0–1

    # Threat
    risk_score:     int             = 0     # 0–100
    risk_band:      RiskBand        = RiskBand.NORMAL
    anomalies:      List[AnomalyFlag] = []
    layer_evaluations: Dict[str, LayerEvaluation] = {}

    # Temporal
    first_seen:     float           = Field(default_factory=time.time)
    last_seen:      float           = Field(default_factory=time.time)
    update_count:   int             = 0

    # History (last N positions for trajectory display)
    position_history: List[Dict[str, Any]] = []
    MAX_HISTORY:    int             = 120   # ~2 min at 1Hz

    @field_validator("icao24")
    @classmethod
    def validate_icao24(cls, value: str) -> str:
        return normalize_icao24(value)

    def update_risk_band(self) -> None:
        if self.risk_score <= 20:
            self.risk_band = RiskBand.NORMAL
        elif self.risk_score <= 50:
            self.risk_band = RiskBand.MONITOR
        elif self.risk_score <= 75:
            self.risk_band = RiskBand.ALERT
        else:
            self.risk_band = RiskBand.CRITICAL

    def add_position_history(self) -> None:
        if self.lat is not None and self.lon is not None:
            entry = {
                "t":   self.last_seen,
                "lat": self.lat,
                "lon": self.lon,
                "alt": self.altitude_baro,
            }
            self.position_history.append(entry)
            if len(self.position_history) > self.MAX_HISTORY:
                self.position_history = self.position_history[-self.MAX_HISTORY:]

    def to_bytes(self) -> bytes:
        return orjson.dumps(self.model_dump())

    @classmethod
    def from_bytes(cls, data: bytes) -> "StateVector":
        return cls(**orjson.loads(data))

    def to_api_dict(self) -> Dict[str, Any]:
        """Compact representation for WebSocket broadcast."""
        return {
            "icao":     self.icao24,
            "cs":       self.callsign,
            "lat":      self.lat,
            "lon":      self.lon,
            "alt":      self.altitude_baro,
            "vel":      self.velocity,
            "hdg":      self.heading,
            "vr":       self.vertical_rate,
            "nic":      self.nic,
            "nac_p":    self.nac_p,
            "gnd":      self.on_ground,
            "src":      self.primary_source.value,
            "conf":     round(self.confidence, 3),
            "cls":      self.classification.value,
            "mil":      round(self.military_score, 3),
            "risk":     self.risk_score,
            "band":     self.risk_band.value,
            "anoms":    [a.anomaly_type.value for a in self.anomalies],
            "layer_triggers": [a.to_api_dict() for a in self.anomalies],
            "layer_evaluations": {
                key: {
                    "layer": value.layer.value,
                    "status": value.status.value,
                    "detectors_evaluated": value.detectors_evaluated,
                    "triggered_detectors": value.triggered_detectors,
                    "score_delta": value.score_delta,
                    "skipped_reason": value.skipped_reason,
                    "timestamp": value.timestamp,
                }
                for key, value in self.layer_evaluations.items()
            },
            "ts":       self.last_seen,
            "update_count": self.update_count,
            "trail":    self.position_history[-20:],  # last 20 for trail
        }
