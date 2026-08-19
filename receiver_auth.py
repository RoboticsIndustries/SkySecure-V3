"""Shared fail-closed receiver credential validation."""

from typing import Mapping, Sequence


def validate_receiver_credentials(
    locations: Mapping[str, Sequence[float]], keys: Mapping[str, str]
) -> dict[str, str]:
    configured = dict(keys)
    expected = set(locations)
    if (
        set(configured) != expected
        or any(not isinstance(value, str) or not value for value in configured.values())
        or len(set(configured.values())) != len(configured)
    ):
        raise ValueError("MLAT receiver credentials must be complete and distinct")
    return configured
