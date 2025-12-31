import logging
import threading
from collections import deque
from datetime import datetime
from typing import Deque, Iterable, List


class RingBufferLogHandler(logging.Handler):
    """
    Simple in-memory ring buffer for log records.
    Stores the formatted message so we can return strings quickly.
    Thread-safe for writes/reads.
    """

    def __init__(self, capacity: int = 5000) -> None:
        super().__init__()
        self.capacity = max(1, capacity)
        self._buffer: Deque[str] = deque(maxlen=self.capacity)
        self._lock = threading.Lock()
        self.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            with self._lock:
                self._buffer.append(msg)
        except Exception:  # pragma: no cover - best-effort logging
            self.handleError(record)

    def tail(self, n: int) -> List[str]:
        with self._lock:
            if n <= 0:
                return []
            return list(list(self._buffer)[-n:])

    def all(self) -> List[str]:
        with self._lock:
            return list(self._buffer)


_handler: RingBufferLogHandler | None = None


def get_log_buffer() -> RingBufferLogHandler:
    """
    Singleton accessor to ensure a single handler instance.
    """
    global _handler
    if _handler is None:
        _handler = RingBufferLogHandler(capacity=5000)
        root = logging.getLogger("ai_crypto_trader")
        root.addHandler(_handler)
        if root.level == logging.NOTSET:
            root.setLevel(logging.INFO)
    return _handler
