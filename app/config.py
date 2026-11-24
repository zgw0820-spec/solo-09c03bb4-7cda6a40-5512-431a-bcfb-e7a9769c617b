from __future__ import annotations

import os
from dataclasses import dataclass


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc
    if value < 0:
        raise ValueError(f"{name} must be >= 0, got {value}")
    return value


@dataclass(frozen=True)
class LinkConfig:
    link_id: str
    delivery_delay_ms: int
    ack_delay_ms: int


@dataclass(frozen=True)
class EngineConfig:
    """Initial (and reset) parameters for the two simulated member links."""

    link_a: LinkConfig = LinkConfig("A", 300, 200)
    link_b: LinkConfig = LinkConfig("B", 150, 80)

    def links(self) -> dict[str, LinkConfig]:
        return {self.link_a.link_id: self.link_a, self.link_b.link_id: self.link_b}


def config_from_env() -> EngineConfig:
    return EngineConfig(
        link_a=LinkConfig(
            "A",
            _env_int("DRILL_A_DELIVERY_MS", 300),
            _env_int("DRILL_A_ACK_MS", 200),
        ),
        link_b=LinkConfig(
            "B",
            _env_int("DRILL_B_DELIVERY_MS", 150),
            _env_int("DRILL_B_ACK_MS", 80),
        ),
    )
