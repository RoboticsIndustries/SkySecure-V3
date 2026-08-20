"""Slow, trajectory-aware rotation across representative worldwide airspace tiles."""
from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field, ValidationError

from coverage_area import COVERAGE_LOCK_KEY, COVERAGE_REDIS_KEY, CoverageArea

WORLD_SCAN_REDIS_KEY = "config:world-scan"
WORLD_SCAN_LOCK_KEY = "config:world-scan-lock"
MIN_WORLD_SCAN_DWELL_SECONDS = 315

# Representative high-traffic regions on every inhabited continent. Each point
# uses the provider's maximum 250 NM radius; operators can extend this list
# without changing scheduler behavior.
WORLD_SCAN_TILES: tuple[tuple[str, float, float], ...] = (
    ("North Atlantic / New York", 40.64, -73.78),
    ("US Southeast / Atlanta", 33.64, -84.43),
    ("US Gulf / Dallas", 32.90, -97.04),
    ("US West / Los Angeles", 33.94, -118.41),
    ("Pacific Northwest / Seattle", 47.45, -122.31),
    ("Canada / Toronto", 43.68, -79.63),
    ("Mexico", 19.44, -99.07),
    ("Brazil / Sao Paulo", -23.44, -46.47),
    ("Southern Cone / Buenos Aires", -34.82, -58.54),
    ("UK / London", 51.48, -0.46),
    ("Western Europe / Paris", 49.01, 2.55),
    ("Central Europe / Frankfurt", 50.04, 8.56),
    ("Nordic / Stockholm", 59.65, 17.92),
    ("Eastern Mediterranean", 37.94, 23.94),
    ("Middle East / Dubai", 25.25, 55.37),
    ("East Africa / Nairobi", -1.32, 36.93),
    ("Southern Africa / Johannesburg", -26.14, 28.25),
    ("India / Delhi", 28.56, 77.10),
    ("Southeast Asia / Singapore", 1.36, 103.99),
    ("China coast / Hong Kong", 22.31, 113.91),
    ("Korea / Seoul", 37.46, 126.44),
    ("Japan / Tokyo", 35.77, 140.39),
    ("Australia / Sydney", -33.95, 151.18),
    ("New Zealand / Auckland", -37.01, 174.79),
)


class WorldScanState(BaseModel):
    enabled: bool = False
    dwell_seconds: int = Field(default=360, ge=MIN_WORLD_SCAN_DWELL_SECONDS, le=86400)
    tile_index: int = Field(default=0, ge=0)
    switched_at: float = Field(default=0.0, ge=0)


def tile_area(index: int) -> CoverageArea:
    normalized = index % len(WORLD_SCAN_TILES)
    name, latitude, longitude = WORLD_SCAN_TILES[normalized]
    return CoverageArea(
        latitude=latitude,
        longitude=longitude,
        radius_nm=250,
        label=f"World scan {normalized + 1}/{len(WORLD_SCAN_TILES)} — {name}",
    )


def advance_world_scan_state(
    state: WorldScanState,
    now: float,
) -> tuple[WorldScanState, Optional[CoverageArea]]:
    """Advance at most one tile, guaranteeing full detector warm-up per tile."""
    if not state.enabled or now - state.switched_at < state.dwell_seconds:
        return state, None
    next_index = (state.tile_index + 1) % len(WORLD_SCAN_TILES)
    advanced = state.model_copy(update={"tile_index": next_index, "switched_at": now})
    return advanced, tile_area(next_index)


async def load_world_scan_state(redis_client) -> WorldScanState:
    raw = await redis_client.get(WORLD_SCAN_REDIS_KEY)
    if not raw:
        return WorldScanState()
    try:
        return WorldScanState.model_validate_json(raw)
    except (ValidationError, ValueError, TypeError):
        return WorldScanState()


async def save_world_scan_state(redis_client, state: WorldScanState) -> None:
    await redis_client.set(WORLD_SCAN_REDIS_KEY, state.model_dump_json())


async def save_world_scan_and_coverage(
    redis_client, state: WorldScanState, area: CoverageArea,
) -> None:
    """Atomically publish scheduler state and its matching active coverage."""
    script = """
    redis.call('SET', KEYS[1], ARGV[1])
    redis.call('SET', KEYS[2], ARGV[2])
    return 1
    """
    await redis_client.eval(
        script,
        2,
        WORLD_SCAN_REDIS_KEY,
        COVERAGE_REDIS_KEY,
        state.model_dump_json(),
        area.model_dump_json(),
    )


async def configure_world_scan(
    redis_client,
    *,
    enabled: bool,
    dwell_seconds: int,
    now: float,
) -> WorldScanState:
    """Atomically enable/disable scanning and select the first tile on enable."""
    state = WorldScanState(
        enabled=enabled,
        dwell_seconds=dwell_seconds,
        tile_index=0,
        switched_at=now,
    )
    lock = redis_client.lock(COVERAGE_LOCK_KEY, timeout=30, blocking_timeout=5)
    async with lock:
        if enabled:
            await save_world_scan_and_coverage(redis_client, state, tile_area(0))
        else:
            await save_world_scan_state(redis_client, state)
    return state


async def tick_world_scan(redis_client, *, now: float) -> WorldScanState:
    """Advance the shared coverage area when the configured dwell has elapsed."""
    lock = redis_client.lock(COVERAGE_LOCK_KEY, timeout=30, blocking_timeout=5)
    async with lock:
        state = await load_world_scan_state(redis_client)
        advanced, area = advance_world_scan_state(state, now)
        if area is not None:
            await save_world_scan_and_coverage(redis_client, advanced, area)
        return advanced
