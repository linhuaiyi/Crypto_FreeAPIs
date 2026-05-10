import time
import threading


class RateLimiter:
    def __init__(self, requests_per_minute: int, name: str = "RateLimiter"):
        self.requests_per_minute = requests_per_minute
        self.min_interval = 60.0 / requests_per_minute if requests_per_minute > 0 else 0
        self.name = name
        self._lock = threading.Lock()
        self._last_request_time = 0.0

    def wait(self):
        with self._lock:
            now = time.time()
            elapsed = now - self._last_request_time
            if elapsed < self.min_interval:
                sleep_time = self.min_interval - elapsed
                time.sleep(sleep_time)
            self._last_request_time = time.time()

    def try_acquire(self) -> bool:
        with self._lock:
            now = time.time()
            elapsed = now - self._last_request_time
            if elapsed >= self.min_interval:
                self._last_request_time = now
                return True
            return False
