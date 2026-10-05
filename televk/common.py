from __future__ import annotations

import asyncio
import dataclasses
import json
import math
import os
import re
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit


class Permanent(Exception):
    """An explicit rejection; automatic retry cannot fix this operation."""


class Retry(Exception):
    def __init__(self, message: str, delay: float = 15):
        super().__init__(message)
        self.delay = delay


class Uncertain(Exception):
    """The remote operation might have succeeded; do not blindly retry."""


class AccessDenied(Permanent):
    pass


def subprocess_env() -> dict[str, str]:
    """Environment for helper processes that must not impersonate this systemd service."""
    env = os.environ.copy()
    env.pop("NOTIFY_SOCKET", None)
    for key in tuple(env):
        if key.startswith("WATCHDOG_"):
            env.pop(key, None)
    return env


class VKError(Permanent):
    def __init__(self, code: int, method: str):
        super().__init__(f"VK {method}: error {code}")
        self.code, self.method = code, method


def dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def split_text(text: str, limit: int = 3500) -> list[str]:
    """Split by UTF-16 units without splitting a Unicode code point."""
    if limit < 2:
        raise ValueError("limit must be >= 2")
    out, part, size = [], [], 0
    for ch in text:
        n = 2 if ord(ch) > 0xFFFF else 1
        if size + n > limit:
            out.append("".join(part))
            part, size = [], 0
        part.append(ch)
        size += n
    if part:
        out.append("".join(part))
    return out


def safe_name(name: str) -> str:
    name = str(name).replace("\\", "/").split("/")[-1]
    name = re.sub(r"[\x00-\x1f\x7f]", "_", name).strip(". ")
    return (name or "file.bin")[:180]


def authorized(message: dict, owner: int, group: int) -> bool:
    # A message sent as a channel/anonymous administrator is not the owner.
    return (message.get("chat", {}).get("id") == group
            and message.get("from", {}).get("id") == owner
            and not message.get("from", {}).get("is_bot", False)
            and not message.get("sender_chat"))


class RateGate:
    """Shared, non-bursting limiter. A retry_after also postpones other workers."""
    def __init__(self, interval: float):
        self.interval, self.next_at = interval, 0.0
        self.lock = asyncio.Lock()

    async def wait(self) -> None:
        async with self.lock:
            while self.next_at > time.monotonic():
                await asyncio.sleep(self.next_at - time.monotonic())
            self.next_at = time.monotonic() + self.interval

    def defer(self, delay: float) -> None:
        self.next_at = max(self.next_at, time.monotonic() + delay)


@dataclasses.dataclass(frozen=True)
class Config:
    TG_OWNER_ID: int
    TG_GROUP_ID: int
    TG_BOT_TOKEN_FILE: str
    VK_ACCESS_TOKEN_FILE: str
    STATE_DIR: str = "state"
    TG_PROXY: str = ""
    VK_PROXY: str = ""
    VK_API_VERSION: str = "5.199"
    VK_POLL_SECONDS: float = 2.0
    TG_SEND_INTERVAL: float = 3.1
    VK_API_INTERVAL: float = 0.36
    WORKERS: int = 4
    MAX_PENDING: int = 20000
    MAX_FILE_MB: int = 50
    MIN_FREE_MB: int = 256
    MAX_DB_MB: int = 2048
    RETENTION_DAYS: int = 30
    UPDATE_CHECK_HOURS: int = 168
    TIMEZONE: str = "Europe/Amsterdam"
    _base: Path = dataclasses.field(default=Path("."), repr=False, compare=False)

    @classmethod
    def load(cls, path: str | Path) -> "Config":
        p = Path(path).resolve()
        raw = json.loads(p.read_text("utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("config must be a JSON object")
        fields = {x.name for x in dataclasses.fields(cls) if not x.name.startswith("_")}
        if set(raw) - fields:
            raise ValueError("Unknown settings: " + ", ".join(sorted(set(raw) - fields)))
        c = cls(**raw, _base=p.parent)
        c.validate()
        return c

    def validate(self) -> None:
        if type(self.TG_OWNER_ID) is not int or self.TG_OWNER_ID <= 0:
            raise ValueError("TG_OWNER_ID must be your positive numeric Telegram user ID")
        if type(self.TG_GROUP_ID) is not int or self.TG_GROUP_ID >= 0:
            raise ValueError("TG_GROUP_ID must be a negative Telegram supergroup ID")
        for name in ("VK_POLL_SECONDS", "TG_SEND_INTERVAL", "VK_API_INTERVAL"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
                raise ValueError(f"Invalid {name}")
        for name in ("WORKERS", "MAX_PENDING", "MAX_FILE_MB", "MIN_FREE_MB", "MAX_DB_MB", "RETENTION_DAYS"):
            if type(getattr(self, name)) is not int or getattr(self, name) <= 0:
                raise ValueError(f"Invalid {name}")
        if type(self.UPDATE_CHECK_HOURS) is not int or self.UPDATE_CHECK_HOURS < 0:
            raise ValueError("UPDATE_CHECK_HOURS must be a nonnegative integer")
        if self.WORKERS > 16 or self.MAX_FILE_MB > 50:
            raise ValueError("v0.1 supports WORKERS <= 16 and MAX_FILE_MB <= 50 (cloud Bot API)")
        if self.TG_SEND_INTERVAL < 3.1:
            raise ValueError("TG_SEND_INTERVAL must be >= 3.1 for the shared Telegram group")
        from zoneinfo import ZoneInfo
        ZoneInfo(self.TIMEZONE)
        for field in ("TG_PROXY", "VK_PROXY"):
            value = getattr(self, field)
            if value and urlsplit(value).scheme not in {"socks5", "socks5h", "http", "https"}:
                raise ValueError(f"Invalid {field} scheme")
        if not re.fullmatch(r"\d+\.\d+", self.VK_API_VERSION):
            raise ValueError("Invalid VK_API_VERSION")

    def path(self, value: str) -> Path:
        p = Path(value).expanduser()
        return p if p.is_absolute() else self._base / p

    @property
    def state(self) -> Path:
        return self.path(self.STATE_DIR)

    def secret(self, field: str) -> str:
        p = self.path(getattr(self, field))
        if os.name == "posix" and p.stat().st_mode & 0o077:
            raise ValueError(f"Secret permissions must be 600: {p.name}")
        value = p.read_text("utf-8").strip()
        if not value or any(ch.isspace() for ch in value) or value.startswith("REPLACE"):
            raise ValueError(f"Fill {field} with a token on one line")
        return value
