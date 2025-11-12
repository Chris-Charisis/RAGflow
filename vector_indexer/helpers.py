
from __future__ import annotations
import json, logging, time
from functools import wraps
from typing import Any, Callable
import orjson

logger = logging.getLogger(__name__)

def load_json_bytes(b: bytes) -> dict:
    try:
        return orjson.loads(b)
    except Exception:
        return json.loads(b.decode("utf-8"))

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

def _clean_text(x):
    if not isinstance(x, str): return None
    x = x.strip()
    return x or None

def _to_text_array(x):
    if x is None: return None
    if isinstance(x, str): x = [x]
    if not isinstance(x, (list, tuple)): return None
    out = []
    for it in x:
        if isinstance(it, str):
            it = it.strip()
            if it: out.append(it)
    return out or None

def _to_int(x):
    try: return int(x) if x is not None else None
    except (TypeError, ValueError): return None

def _drop_nones(d: dict):
    out = {}
    for k, v in d.items():
        if v is None: 
            continue
        if isinstance(v, dict):
            vv = _drop_nones(v)
            if vv: out[k] = vv
        elif isinstance(v, list):
            vv = []
            for e in v:
                if isinstance(e, dict):
                    ed = _drop_nones(e)
                    if ed: vv.append(ed)
                elif e is not None:
                    vv.append(e)
            if vv: out[k] = vv
        else:
            out[k] = v
    return out