from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = BASE_DIR.parent
STATIC_DIR = BASE_DIR / "static"


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default

    try:
        return int(value)
    except ValueError:
        return default


@dataclass(frozen=True, slots=True)
class Settings:
    app_name: str = "USD Pulse Wire"
    poll_interval_seconds: int = env_int("POLL_INTERVAL_SECONDS", 60)
    history_limit: int = env_int("HISTORY_LIMIT", 220)
    http_timeout_seconds: int = env_int("HTTP_TIMEOUT_SECONDS", 15)
    default_feed_limit: int = env_int("DEFAULT_FEED_LIMIT", 90)
    market_cache_seconds: int = env_int("MARKET_CACHE_SECONDS", 1)
    request_user_agent: str = os.getenv(
        "REQUEST_USER_AGENT",
        "USDPulseWire/1.0 (+https://localhost)",
    )
    project_dir: Path = field(default=PROJECT_DIR)
    static_dir: Path = field(default=STATIC_DIR)


settings = Settings()
