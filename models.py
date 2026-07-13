"""
skysecure/models.py
───────────────────
Core data models shared across ALL modules.
Uses Pydantic v2 for validation + fast serialization via orjson.
"""

from __future__ import annotations

import time
from enum import Enum
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, field_validator
import orjson


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


class RiskBand(str, Enum):
    NORMAL   = "NORMAL"    # 0–20
    MONITOR  = "MONITOR"   # 21–50
    ALERT    = "ALERT"     # 51–75
    CRITICAL = "CRITICAL"  # 76–100


# ─── Raw Message Models ────────────────────────────────────────────────────────

class RawADSBMessage(BaseModel):
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
    callsign:      Optional[str]   = None
    lat:           Optional[float] = Field(None, ge=-90, le=90)
    lon:           Optional[float] = Field(None, ge=-180, le=180)
    altitude_baro: Optional[int]   = Field(None, description="Barometric altitude, feet")
    altitude_geo:  Optional[int]   = Field(None, description="GNSS altitude, feet")
    velocity:      Optional[float] = Field(None, ge=0, description="Ground speed, knots")
    heading:       Optional[float] = Field(None, ge=0, lt=360)
    vertical_rate: Optional[int]   = Field(None, description="ft/min, positive=climb")
    on_ground:     Optional[bool]  = None
    squawk:        Optional[str]   = None
    nic:           Optional[int]   = Field(None, description="Navigation Integrity Category")
    nac_p:         Optional[int]   = Field(None, description="Navigation Accuracy Category - Position")
    raim:          Optional[bool]  = Field(None, description="RAIM flag from GNSS")

    @field_validator("icao24")
    @classmethod
    def icao_uppercase(cls, v: str) -> str:
        return v.upper()

    def to_bytes(self) -> bytes:
        return orjson.dumps(self.model_dump())

    @classmethod
    def from_bytes(cls, data: bytes) -> "RawADSBMessage":
        return cls(**orjson.loads(data))


class RawMLATReport(BaseModel):
    """
    Position estimate from the MLAT solver.
    """
    session_id:    str
    solve_time:    float
    icao24:        str
    lat:           float  = Field(ge=-90, le=90)
    lon:           float  = Field(ge=-180, le=180)
    altitude_baro: int
    velocity:      Optional[float] = None
    heading:       Optional[float] = None
    num_receivers: int             = Field(ge=2, description="Receivers used in solve")
    tdoa_residual: float           = Field(description="RMS TDOA residual (ns)")
    cep90:         float           = Field(description="90% circular error probable, meters")
    receiver_ids:  List[str]       = []

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

class SourceReport(BaseModel):
    """One source's contribution to the fused state."""
    source:     DataSource
    lat:        Optional[float] = None
    lon:        Optional[float] = None
    altitude:   Optional[int]   = None
    velocity:   Optional[float] = None
    heading:    Optional[float] = None
    weight:     float           = 1.0
    confidence: float           = 1.0
    timestamp:  float           = Field(default_factory=time.time)


class AnomalyFlag(BaseModel):
    anomaly_type: AnomalyType
    score_delta:  int
    description:  str
    timestamp:    float = Field(default_factory=time.time)
    meta:         Dict[str, Any] = {}


class StateVector(BaseModel):
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
    on_ground:      bool            = False

    # Data provenance
    primary_source: DataSource      = DataSource.UNKNOWN
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

    # Temporal
    first_seen:     float           = Field(default_factory=time.time)
    last_seen:      float           = Field(default_factory=time.time)
    update_count:   int             = 0

    # History (last N positions for trajectory display)
    position_history: List[Dict[str, Any]] = []
    MAX_HISTORY:    int             = 120   # ~2 min at 1Hz

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
            "gnd":      self.on_ground,
            "src":      self.primary_source.value,
            "conf":     round(self.confidence, 3),
            "cls":      self.classification.value,
            "mil":      round(self.military_score, 3),
            "risk":     self.risk_score,
            "band":     self.risk_band.value,
            "anoms":    [a.anomaly_type.value for a in self.anomalies],
            "ts":       self.last_seen,
            "trail":    self.position_history[-20:],  # last 20 for trail
        }
