"""Thread-safe in-memory storage for the IPC server."""

from __future__ import annotations

from collections import deque
from threading import Condition, RLock
from typing import Any


class KeyValueStore:
    """A process-local key-value store protected by a reentrant lock."""

    def __init__(self) -> None:
        self._values: dict[str, Any] = {}
        self._lock = RLock()

    def put(self, key: str, value: Any) -> None:
        """Insert or replace a value."""
        with self._lock:
            self._values[key] = value

    def get(self, key: str) -> Any:
        """Return a value, or None when the key does not exist."""
        with self._lock:
            return self._values.get(key)

    def delete(self, key: str) -> bool:
        """Delete a key and return whether it existed."""
        with self._lock:
            if key not in self._values:
                return False
            del self._values[key]
            return True

    def contains(self, key: str) -> bool:
        """Return whether a key exists."""
        with self._lock:
            return key in self._values


class TaskQueue:
    """A thread-safe FIFO queue that can block while waiting for a task."""

    def __init__(self) -> None:
        self._items: deque[Any] = deque()
        self._condition = Condition()

    def enqueue(self, item: Any) -> None:
        """Add an item and wake one waiting consumer."""
        with self._condition:
            self._items.append(item)
            self._condition.notify()

    def dequeue(self, timeout: float | None = None) -> Any:
        """Remove and return the next item, waiting until one is available."""
        with self._condition:
            available = self._condition.wait_for(
                lambda: bool(self._items), timeout)
            if not available:
                raise TimeoutError("no queued item became available")
            return self._items.popleft()
