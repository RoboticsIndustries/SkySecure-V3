"""Shared, runtime-configurable ADS-B coverage area."""
from __future__ import annotations

import logging
import math
from typing import Any

from pydantic import BaseModel, Field, ValidationError, field_validator

from config import settings

COVERAGE_REDIS_KEY = "config:coverage-area"
COVERAGE_LOCK_KEY = "config:coverage-area-lock"
COVERAGE_RATE_KEY = "config:coverage-area-rate-limit"
log = logging.getLogger(__name__)


class CoverageArea(BaseModel):
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)
    radius_nm: int = Field(ge=1, le=250)
    label: str = Field(default="Custom area", min_length=1, max_length=80)

    @field_validator("latitude", "longitude", "radius_nm", mode="before")
    @classmethod
    def reject_boolean_numbers(cls, value: Any) -> Any:
        if isinstance(value, bool):
            raise ValueError("boolean values are not valid coordinates or radii")
        return value

    @field_validator("label")
    @classmethod
    def normalize_label(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("label cannot be blank")
        return value


def default_coverage_area() -> CoverageArea:
    return CoverageArea(
        latitude=settings.ADSB_FALLBACK_LAT,
        longitude=settings.ADSB_FALLBACK_LON,
        radius_nm=settings.ADSB_FALLBACK_RADIUS_NM,
        label="Configured default",
    )


def coverage_url(area: CoverageArea) -> str:
    return (
        "https://api.adsb.lol/v2/point/"
        f"{area.latitude}/{area.longitude}/{area.radius_nm}"
    )


def within_coverage_area(latitude: float, longitude: float, area: CoverageArea) -> bool:
    """Return whether a point is within the area's great-circle radius."""
    lat1, lat2 = math.radians(area.latitude), math.radians(latitude)
    dlat = lat2 - lat1
    dlon = math.radians(longitude - area.longitude)
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    distance_nm = 3440.065 * 2 * math.asin(min(1.0, math.sqrt(a)))
    return distance_nm <= area.radius_nm


async def load_coverage_area_record(redis_client: Any) -> tuple[CoverageArea, bytes]:
    fallback = default_coverage_area()
    fallback_raw = fallback.model_dump_json().encode()
    if redis_client is None:
        return fallback, fallback_raw
    raw = await redis_client.get(COVERAGE_REDIS_KEY)
    if not raw:
        return fallback, fallback_raw
    try:
        raw_bytes = raw if isinstance(raw, bytes) else str(raw).encode()
        return CoverageArea.model_validate_json(raw_bytes), raw_bytes
    except (ValidationError, ValueError, TypeError, UnicodeDecodeError) as exc:
        log.warning("Invalid stored coverage area; using defaults: %s", exc)
        return fallback, fallback_raw


async def load_coverage_area(redis_client: Any) -> CoverageArea:
    area, _ = await load_coverage_area_record(redis_client)
    return area


async def save_coverage_area(redis_client: Any, area: CoverageArea) -> None:
    await redis_client.set(COVERAGE_REDIS_KEY, area.model_dump_json())
