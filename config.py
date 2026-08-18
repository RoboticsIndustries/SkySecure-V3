"""
config.py — Centralised configuration via environment variables.
"""
from __future__ import annotations
from functools import lru_cache
from typing import List, Dict, Any
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # ─── Kafka ─────────────────────────────────────────────────
    KAFKA_BOOTSTRAP:         str   = "localhost:9092"
    KAFKA_GROUP_PREFIX:      str   = "skysecure"

    TOPIC_RAW_ADSB:          str   = "raw.adsb"
    TOPIC_RAW_MLAT:          str   = "raw.mlat"
    TOPIC_RAW_ACARS:         str   = "raw.acars"
    TOPIC_FUSED_TRACKS:      str   = "fused.tracks"
    TOPIC_ALERTS_ANOMALY:    str   = "alerts.anomaly"

    # ─── Redis ─────────────────────────────────────────────────
    REDIS_URL:               str   = "redis://localhost:6379/0"
    REDIS_TTL_STATE_VECTOR:  int   = 120     # seconds — drop stale tracks
    REDIS_TTL_HISTORY:       int   = 3600

    # ─── Database ──────────────────────────────────────────────
    POSTGRES_DSN:            str   = "postgresql://skysecure@localhost:5432/skysecure"

    # ─── ADS-B Sources ─────────────────────────────────────────
    ADSB_SOURCE:             str   = "opensky"   # opensky | beast | dump1090
    OPENSKY_USERNAME:        str   = ""
    OPENSKY_PASSWORD:        str   = ""
    OPENSKY_POLL_INTERVAL:   int   = 10          # seconds
    OPENSKY_BASE_URL:        str   = "https://opensky-network.org/api"
    ADSB_FALLBACK_LAT:       float = 39.9526
    ADSB_FALLBACK_LON:       float = -75.1652
    ADSB_FALLBACK_RADIUS_NM: int   = 250

    DUMP1090_HOST:           str   = "localhost"
    DUMP1090_PORT:           int   = 30003        # BaseStation / SBS format
    DUMP1090_BEAST_PORT:     int   = 30005        # Beast binary

    # ─── Receiver Network (for MLAT) ───────────────────────────
    RECEIVER_TIMEOUT_SEC:    int   = 60
    MLAT_MIN_RECEIVERS:      int   = 3
    MLAT_MAX_TDOA_RESIDUAL:  float = 500.0        # nanoseconds

    # ─── Fusion ────────────────────────────────────────────────
    FUSION_CONFLICT_NM:      float = 5.0          # NM discrepancy → flag
    FUSION_STALE_THRESHOLD:  int   = 30           # seconds
    FUSION_COMPARISON_WINDOW_SEC: int = 5         # max ADS-B/MLAT event-time skew
    FUSION_TRIGGER_TTL_SEC:  int   = 60           # active L4 evidence lifetime

    # ─── Anomaly Detection ─────────────────────────────────────
    MAX_GROUNDSPEED_KNOTS:   float = 1200.0
    MAX_ALTITUDE_JUMP_FT:    int   = 5000
    MAX_TELEPORT_NM:         float = 50.0
    BARO_GEO_DELTA_FT:       int   = 2000         # suspicious if exceeded
    GHOST_MLAT_CONFIRM_NM:   float = 2.0          # radius to look for MLAT confirm
    DUPLICATE_WINDOW_NM:     float = 200.0        # same ICAO, different location

    # ─── Military Detection ────────────────────────────────────
    FORMATION_DIST_NM:       float = 1.0
    FORMATION_HDG_DEG:       float = 10.0
    FORMATION_ALT_FT:        float = 500.0
    MIL_BASE_RADIUS_NM:      float = 50.0
    MIL_SCORE_THRESHOLD:     float = 0.5

    # ─── API ───────────────────────────────────────────────────
    API_HOST:                str   = "0.0.0.0"
    API_PORT:                int   = 8000
    API_CORS_ORIGINS:        List[str] = [
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ]
    WS_BROADCAST_INTERVAL:   float = 1.0   # seconds

    # ─── Logging ───────────────────────────────────────────────
    LOG_LEVEL:               str   = "INFO"
    LOG_JSON:                bool  = True


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()

# Conservative client timeouts keep consumer groups stable when Kafka runs via
# amd64 emulation on Apple Silicon. Native Linux remains comfortably within them.
KAFKA_CONSUMER_STABILITY: Dict[str, Any] = {
    "enable_auto_commit": False,
    "request_timeout_ms": 90_000,
    "session_timeout_ms": 45_000,
    "heartbeat_interval_ms": 15_000,
    "max_poll_interval_ms": 600_000,
    "retry_backoff_ms": 1_000,
}
