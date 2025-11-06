
from __future__ import annotations
import json, logging, time, uuid
from functools import wraps
from typing import Any, Callable
import orjson

logger = logging.getLogger(__name__)

def load_json_bytes(b: bytes) -> dict:
    try:
        return orjson.loads(b)
    except Exception:
        return json.loads(b.decode("utf-8"))

def dict_pick(d: dict, keys: list[str]) -> dict:
    return {k: d[k] for k in keys if k in d and d[k] is not None}

def ensure_uuid(s: str | None) -> str | None:
    if not s: return None
    try:
        return str(uuid.UUID(s))
    except Exception:
        # allow non-uuid ids; callers may pass None to auto-generate at DB side
        return s

def extract_vector(payload: dict) -> list[float]:
    vec = payload["embedding"].get("embedding_vector")[0]
    if not isinstance(vec, (list, tuple)):
        raise ValueError("payload missing 'embedding_vector' (or 'embedding') as list of floats")
    return vec

def retry(exceptions: tuple[type[Exception], ...], tries=3, delay=0.5, backoff=2.0):
    def decorator(fn: Callable[..., Any]):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            _tries, _delay = tries, delay
            while _tries > 1:
                try:
                    return fn(*args, **kwargs)
                except exceptions as e:
                    logger.warning("%s failed (%s). Retrying in %.2fs...", fn.__name__, e, _delay)
                    time.sleep(_delay)
                    _tries -= 1
                    _delay *= backoff
            return fn(*args, **kwargs)
        return wrapper
    return decorator
