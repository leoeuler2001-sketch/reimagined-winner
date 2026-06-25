"""Rate-limited Redis JSON writes (max 1/sec per key)."""

from __future__ import annotations

import asyncio
import time
from typing import Any, Callable, Dict, Optional


class RateLimitedRedisWriter:
    def __init__(
        self,
        set_json_fn: Callable[[str, Any], bool],
        min_interval_sec: float = 1.0,
    ) -> None:
        self._set_json = set_json_fn
        self._min_interval = min_interval_sec
        self._last_write: Dict[str, float] = {}
        self._lock = asyncio.Lock()
        self._pending: Dict[str, Any] = {}

    async def set_json(self, key: str, obj: Any, *, force: bool = False) -> bool:
        async with self._lock:
            now = time.time()
            last = self._last_write.get(key, 0.0)
            if not force and (now - last) < self._min_interval:
                self._pending[key] = obj
                return False
            ok = self._set_json(key, obj)
            if ok:
                self._last_write[key] = now
                self._pending.pop(key, None)
            return ok

    async def flush_pending(self) -> None:
        async with self._lock:
            now = time.time()
            for key, obj in list(self._pending.items()):
                last = self._last_write.get(key, 0.0)
                if (now - last) >= self._min_interval:
                    if self._set_json(key, obj):
                        self._last_write[key] = now
                        self._pending.pop(key, None)
