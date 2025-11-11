# helpers_rate.py
import asyncio, time
from email.utils import parsedate_to_datetime

class HostRateLimiter:
    """
    Ограничивает одновременность и темп запросов к одному хосту.
    Адаптивно увеличивает минимальный интервал после 429.
    """
    def __init__(self, max_concurrent: int = 2, base_min_interval: float = 0.4, max_min_interval: float = 3.0):
        self.sem = asyncio.BoundedSemaphore(max_concurrent)
        self.base_min_interval = base_min_interval
        self._min_interval = base_min_interval
        self.max_min_interval = max_min_interval
        self._last_ts = 0.0
        self._lock = asyncio.Lock()

    async def __aenter__(self):
        await self.sem.acquire()
        async with self._lock:
            now = time.monotonic()
            wait = max(0.0, self._min_interval - (now - self._last_ts))
        if wait:
            await asyncio.sleep(wait)

    async def __aexit__(self, exc_type, exc, tb):
        async with self._lock:
            self._last_ts = time.monotonic()
        self.sem.release()

    def punish(self):
        # после 429 повышаем интервал (но не выше max_min_interval)
        self._min_interval = min(self._min_interval * 1.6, self.max_min_interval)

    def relax(self):
        # после успешных ответов постепенно возвращаемся к базовому
        self._min_interval = max(self.base_min_interval, self._min_interval * 0.9)


def parse_retry_after(value: str | None) -> float | None:
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        dt = parsedate_to_datetime(value)
        # сервер дал абсолютное время
        delay = (dt - parsedate_to_datetime(parsedate_to_datetime(time.strftime('%a, %d %b %Y %H:%M:%S GMT')))).total_seconds()  # not used
    except Exception:
        # более корректно: сравнить с now в UTC
        try:
            dt = parsedate_to_datetime(value)
            delay = (dt.timestamp() - time.time())
            return max(0.0, delay)
        except Exception:
            return None
    return None
