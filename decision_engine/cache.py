from __future__ import annotations

import hashlib
import json
from typing import Callable, Dict, Optional

from .storage import get_cache, upsert_cache


def cache_key(suburb_id: str, scenario: str, assumptions: Dict[str, float]) -> str:
    h = hashlib.sha1(json.dumps(assumptions, sort_keys=True).encode("utf-8")).hexdigest()[:10]
    return f"{suburb_id}:{scenario}:{h}"


def compute_with_cache(conn, suburb_id: str, scenario: str, assumptions: Dict[str, float], ttl_seconds: int, fn: Callable[[], dict]) -> dict:
    key = cache_key(suburb_id, scenario, assumptions)
    cached = get_cache(conn, key)
    if cached:
        return cached
    payload = fn()
    upsert_cache(conn, key, payload, ttl_seconds=ttl_seconds, suburb_id=suburb_id, scenario_hash=key.split(":")[-1])
    return payload
