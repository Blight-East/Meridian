import time
from collections import defaultdict


class RateLimiter:
    def __init__(self):
        self.limits = {
            "tool_execution": 120,
            "external_request": 60,
            "llm_call_sonnet": 60,
            "llm_call_opus": 10,
        }
        self._calls = defaultdict(list)

    def check(self, key, window=3600):
        limit = self.limits.get(key, 60)
        now = time.time()
        self._calls[key] = [t for t in self._calls[key] if now - t < window]
        if len(self._calls[key]) >= limit:
            raise RuntimeError(f"Rate limit hit for '{key}': {limit} calls per {window}s")
        self._calls[key].append(now)


rate_limiter = RateLimiter()
