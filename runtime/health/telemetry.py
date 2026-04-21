import json
from datetime import datetime, timezone

import redis


REDIS_HOST = "localhost"
REDIS_PORT = 6379
KEY_PREFIX = "agent_flux:health"


def _redis():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def _component_key(component):
    return f"{KEY_PREFIX}:{component}"


def record_component_state(component, ttl=600, **fields):
    payload = {k: _stringify(v) for k, v in fields.items() if v is not None}
    if not payload:
        return
    r = _redis()
    key = _component_key(component)
    r.hset(key, mapping=payload)
    if ttl:
        r.expire(key, ttl)


def heartbeat(component, ttl=600, **fields):
    payload = {"last_heartbeat_at": utc_now_iso(), **fields}
    record_component_state(component, ttl=ttl, **payload)


def clear_component_fields(component, *fields):
    if not fields:
        return
    _redis().hdel(_component_key(component), *fields)


def get_component_state(component):
    return _redis().hgetall(_component_key(component))


def record_latency(name, milliseconds, ttl=600):
    record_component_state(
        "latency",
        ttl=ttl,
        **{
            f"{name}_ms": round(float(milliseconds), 2),
            f"{name}_at": utc_now_iso(),
        },
    )


def get_latency_state():
    return get_component_state("latency")


def is_stale(iso_timestamp, max_age_seconds):
    if not iso_timestamp:
        return True
    try:
        seen_at = datetime.fromisoformat(iso_timestamp.replace("Z", "+00:00"))
    except Exception:
        return True
    age = (datetime.now(timezone.utc) - seen_at).total_seconds()
    return age > max_age_seconds


def _stringify(value):
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)
