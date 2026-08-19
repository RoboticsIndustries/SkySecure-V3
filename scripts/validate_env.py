#!/usr/bin/env python3
"""Fail closed when a SkySecure environment still contains example secrets."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Mapping


PLACEHOLDER_MARKERS = (
    "replace-with",
    "change-me",
    "changeme",
    "placeholder",
    "example-secret",
)


def parse_env(path: Path) -> dict[str, str]:
    """Parse the simple KEY=VALUE form used by SkySecure's environment file."""
    values: dict[str, str] = {}
    for line_number, raw_line in enumerate(path.read_text().splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            raise ValueError(f"invalid environment syntax on line {line_number}")
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            raise ValueError(f"empty environment key on line {line_number}")
        values[key] = value.strip().strip('"').strip("'")
    return values


def _validate_secret(name: str, value: object, minimum_length: int) -> str:
    if not isinstance(value, str) or len(value) < minimum_length:
        raise ValueError(f"{name} must contain at least {minimum_length} characters")
    lowered = value.lower()
    if any(marker in lowered for marker in PLACEHOLDER_MARKERS):
        raise ValueError(f"{name} still contains an example placeholder")
    return value


def validate_environment(values: Mapping[str, str]) -> None:
    """Validate required secrets without printing or returning their values."""
    _validate_secret("POSTGRES_PASSWORD", values.get("POSTGRES_PASSWORD"), 16)
    _validate_secret(
        "MLAT_SOLVER_SIGNING_KEY", values.get("MLAT_SOLVER_SIGNING_KEY"), 32
    )

    raw_receiver_keys = values.get("MLAT_RECEIVER_API_KEYS")
    try:
        receiver_keys = json.loads(raw_receiver_keys or "")
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError("MLAT_RECEIVER_API_KEYS must be a JSON object") from exc
    if not isinstance(receiver_keys, dict) or len(receiver_keys) < 4:
        raise ValueError("MLAT_RECEIVER_API_KEYS must configure at least four receivers")
    secrets = [
        _validate_secret(f"MLAT_RECEIVER_API_KEYS[{receiver_id}]", secret, 16)
        for receiver_id, secret in receiver_keys.items()
    ]
    if len(set(secrets)) != len(secrets):
        raise ValueError("MLAT receiver credentials must be distinct")

    operator_key = values.get("OPERATOR_API_KEY", "")
    if operator_key:
        _validate_secret("OPERATOR_API_KEY", operator_key, 16)


def main(argv: list[str]) -> int:
    path = Path(argv[1]) if len(argv) > 1 else Path(".env")
    try:
        validate_environment(parse_env(path))
    except (OSError, ValueError) as exc:
        print(f"Environment validation failed: {exc}", file=sys.stderr)
        return 2
    print("Environment validation passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
