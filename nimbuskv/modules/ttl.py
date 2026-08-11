"""ttl.py - NimbusKV v3 TTL / expiry.

TTL is deliberately a thin layer on top of :class:`~.reactive_core.ReactiveStore`
rather than baked into the core -- ``ReactiveStore`` knows nothing about
time; this module just schedules a
``store.delete(key, _event="expire")`` call at the right moment.

If you're using :class:`nimbuskv.NimbusKV`, you never touch this module
directly -- ``ttl=`` on ``set()`` handles it for you. Read this if you're
building something custom on :class:`~.reactive_core.ReactiveStore` and
want expiry too.
"""
import heapq
import logging
import threading
import time
from typing import List, Optional, Tuple

from .reactive_core import ReactiveStore

_log = logging.getLogger("nimbuskv")


class ExpiryScheduler(threading.Thread):
    """Background thread that deletes keys at their TTL deadline.

    Uses a min-heap of ``(when, key, generation)`` plus a condition
    variable, so the thread sleeps until the next actual expiry instead
    of polling in a loop.

    Generations exist to solve a real bug class: if a key is
    rescheduled (e.g. ``set(k, v, ttl=10)`` followed later by
    ``set(k, v2, ttl=3)``), there would otherwise be two heap entries
    for the same key. Without a generation check, the stale ``ttl=10``
    entry could fire *after* the key had already been legitimately
    reset or reused, deleting it incorrectly. Each call to
    :meth:`schedule` or :meth:`cancel` bumps the key's generation; the
    scheduler only actually expires a key if the popped entry's
    generation still matches the current one -- stale entries are
    silently skipped.

    Example:
        >>> store = ReactiveStore()
        >>> scheduler = ExpiryScheduler(store)
        >>> scheduler.start()
        >>> store.set("temp", "value")
        >>> scheduler.schedule("temp", time.time() + 5)  # expires in 5s
    """

    def __init__(self, store: ReactiveStore):
        """
        Args:
            store: The :class:`~.reactive_core.ReactiveStore` this
                scheduler will call ``.delete(key, _event="expire")``
                on when a key's TTL elapses.
        """
        super().__init__(daemon=True, name="NimbusKV-ExpiryScheduler")
        self._store = store
        self._heap: List[Tuple[float, str, int]] = []
        self._generation: dict = {}
        self._cv = threading.Condition()
        self._stop = False

    def schedule(self, key: str, when: float) -> None:
        """Schedule ``key`` to expire at absolute time ``when`` (as
        returned by ``time.time()``).

        Calling this again for the same key before it expires replaces
        the previous schedule -- the old deadline is invalidated via
        the generation counter, not left to fire alongside the new one.

        Args:
            key: The key to expire.
            when: Absolute Unix timestamp (``time.time() + ttl_seconds``)
                at which the key should be deleted.
        """
        with self._cv:
            gen = self._generation.get(key, 0) + 1
            self._generation[key] = gen
            heapq.heappush(self._heap, (when, key, gen))
            self._cv.notify()

    def cancel(self, key: str) -> None:
        """Cancel any pending expiry for ``key`` (e.g. it was
        overwritten with no TTL, or deleted manually before its TTL
        elapsed). Safe to call even if nothing was scheduled."""
        with self._cv:
            self._generation[key] = self._generation.get(key, 0) + 1
            self._cv.notify()

    def run(self) -> None:
        """Scheduler main loop. Not called directly -- started via
        :meth:`threading.Thread.start` (NimbusKV does this for you)."""
        while True:
            with self._cv:
                if self._stop:
                    return
                if not self._heap:
                    self._cv.wait()
                    continue
                when, key, gen = self._heap[0]
                wait = when - time.time()
                if wait > 0:
                    self._cv.wait(timeout=wait)
                    continue
                heapq.heappop(self._heap)
                is_current = self._generation.get(key) == gen
            if not is_current:
                continue  # stale entry -- superseded by a later schedule/cancel
            try:
                self._store.delete(key, _event="expire")
            except Exception:
                # Expiry firing must never kill the scheduler thread --
                # an error here (e.g. from a subscriber callback raising
                # inside delete()'s notify) would otherwise silently
                # stop every other key from ever expiring again.
                _log.exception("error while expiring key=%r", key)

    def stop(self) -> None:
        """Stop the background thread. Called automatically by
        ``NimbusKV.stop()`` -- you don't normally need to call this
        yourself."""
        with self._cv:
            self._stop = True
            self._cv.notify()
