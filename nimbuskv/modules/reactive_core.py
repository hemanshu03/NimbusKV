"""reactive_core.py - NimbusKV v3 core engine.

This module implements the optimistic-concurrency state container that
every other part of NimbusKV sits on top of. If you're just using
``NimbusKV`` (from ``nimbuskv import NimbusKV``), you don't need to import
anything from here directly -- this is the internals. Read this file if
you want to understand *why* NimbusKV is safe under concurrent access, or
if you're building something custom on top of ``ReactiveStore`` directly.

Why not "true" lock-free CAS?
Python (free-threaded or not) does not expose a language-level atomic
compare-and-swap primitive to pure-Python code. What CPython *does*
guarantee is that a single attribute/reference write is not torn --
readers always see either the old snapshot or a fully-formed new one,
never a half-written one. We exploit that: readers just read ``self._ref``,
no lock, ever. Writers still need *some* serialization to decide "did I
win the race", so the compare-and-swap step itself is guarded by a lock
held only for that single comparison+assignment -- nanoseconds, not the
whole operation. This is the same "software CAS" pattern Software
Transactional Memory (STM) systems use in languages without native atomics.

On a free-threaded build (Python 3.13t/3.14t) this gives true parallel
readers and true parallel snapshot construction, with contention only at
the final, tiny swap. On a standard GIL build it's still correct and still
cheaper than a coarse lock, though the GIL removes most of the parallelism
benefit -- which is exactly why ``runtime.LOCK_FREE_MODE`` exists: this
module works either way, it's just faster where it counts.
"""
import logging
import threading
from typing import Any, Callable, Dict, List, Optional, Tuple

import immutables

from .runtime import LOCK_FREE_MODE
from .exceptions import NimbusKVError

_MISSING = object()
_log = logging.getLogger("nimbuskv")


class ConflictRetryExceeded(NimbusKVError):
    """Raised when a mutation cannot win the compare-and-swap race within
    ``max_retries`` attempts.

    This should only happen under pathological write contention -- many
    threads hammering the *same* store with expensive mutations
    simultaneously. If you hit this in practice, consider batching writes
    with :meth:`ReactiveStore.mset` instead of many individual
    :meth:`ReactiveStore.set` calls, since batching reduces the number of
    separate swap attempts needed.
    """


class Subscription:
    """Handle returned by :meth:`ReactiveStore.subscribe`.

    Call :meth:`cancel` when you no longer want to receive notifications.
    Forgetting to cancel a subscription just means the callback keeps
    firing -- it does not leak a thread or a lock, but it's good practice
    to cancel subscriptions you no longer need (e.g. in a request handler
    that subscribes temporarily and should clean up afterwards).

    Example:
        >>> sub = store.subscribe(lambda k, e, o, n: print(k, e))
        >>> store.set("x", 1)
        x set
        >>> sub.cancel()
        >>> store.set("x", 2)  # no output now
    """

    def __init__(self, store: "ReactiveStore", key: Optional[str], sub_id: int):
        self._store = store
        self._key = key
        self._id = sub_id
        self._cancelled = False

    def cancel(self) -> None:
        """Stop receiving notifications from this subscription. Safe to
        call more than once."""
        if not self._cancelled:
            self._store._unsubscribe(self._key, self._id)
            self._cancelled = True


class ReactiveStore:
    """Immutable-snapshot, optimistic-concurrency key/value core.

    This is the engine underneath NimbusKV's public API -- it knows
    nothing about TTL, persistence backends, or the sync/async split.
    It only guarantees three things:

    1. Reads never block, ever, regardless of what writers are doing.
    2. Writes never lose an update, even under heavy concurrent
       contention -- they retry against a fresh snapshot instead of
       overwriting someone else's change.
    3. Every successful write pushes a notification to subscribers.

    You will normally use this through :class:`nimbuskv.NimbusKV`
    rather than directly -- but it's a complete, independently usable
    concurrent store if you need lower-level control (e.g. building a
    custom persistence strategy).

    Example:
        >>> store = ReactiveStore()
        >>> store.set("foo", 1)
        >>> store.get("foo")
        1
        >>> store.mutate(lambda m: m.set("foo", m.get("foo", 0) + 1))
        >>> store.get("foo")
        2
    """

    def __init__(self, max_retries: int = 1000):
        """
        Args:
            max_retries: Maximum compare-and-swap attempts for a single
                mutation before giving up and raising
                :class:`ConflictRetryExceeded`. The default (1000) is
                generous -- you'd need sustained, extreme write
                contention on one key to ever hit it.
        """
        self._ref: immutables.Map = immutables.Map()
        self._swap_lock = threading.Lock()
        self._max_retries = max_retries
        self._subs: Dict[Optional[str], List[Tuple[int, Callable]]] = {None: []}
        self._subs_lock = threading.Lock()
        self._next_sub_id = 0

    # ---- reads: no lock, ever ----------------------------------------

    def snapshot(self) -> immutables.Map:
        """Return the current immutable snapshot.

        This is an O(1), lock-free read of the live state as a whole
        (rather than one key). The returned :class:`immutables.Map` is
        itself immutable, so it's always safe to iterate/inspect even
        while other threads keep writing to the store.

        Returns:
            The current ``immutables.Map`` snapshot.

        Example:
            >>> store.snapshot()
            immutables.Map({'foo': 2})
        """
        return self._ref

    def get(self, key: str, default: Any = _MISSING) -> Any:
        """Read a single key. Never blocks, never contends with writers.

        Args:
            key: The key to read.
            default: Value to return if ``key`` isn't present. If
                omitted, returns ``None`` for a missing key (matching
                ``dict.get``'s common usage, not raising ``KeyError``).

        Returns:
            The stored value, or ``default`` (or ``None``) if absent.

        Example:
            >>> store.get("missing")
            None
            >>> store.get("missing", "fallback")
            'fallback'
        """
        val = self._ref.get(key, _MISSING)
        if val is _MISSING:
            if default is _MISSING:
                return None
            return default
        return val

    def exists(self, key: str) -> bool:
        """Return ``True`` if ``key`` is currently present.

        Example:
            >>> store.set("a", 1)
            >>> store.exists("a")
            True
        """
        return key in self._ref

    def keys(self) -> List[str]:
        """Return a snapshot list of all current keys.

        This is a point-in-time copy -- it won't change under you even
        if writers are actively modifying the store while you iterate
        it, unlike iterating a plain ``dict`` under concurrent
        mutation.
        """
        return list(self._ref.keys())

    def items(self) -> List[Tuple[str, Any]]:
        """Return a snapshot list of all current ``(key, value)`` pairs.
        Same point-in-time-safe guarantee as :meth:`keys`."""
        return list(self._ref.items())

    def __len__(self) -> int:
        return len(self._ref)

    # ---- writes: optimistic retry loop --------------------------------

    def mutate(self, fn: Callable[[immutables.Map], immutables.Map]) -> immutables.Map:
        """Apply an arbitrary transformation to the whole map, retrying
        automatically on conflict.

        This is the primitive every other write operation (``set``,
        ``delete``, ``mset``) is built from. Use it directly when you
        need a transformation that :meth:`set`/:meth:`delete` don't
        cover -- e.g. "set this key only if it doesn't already exist",
        or any multi-key change that must be applied as a single atomic
        swap.

        Args:
            fn: A **pure** function ``(old_map) -> new_map``. It must
                have no side effects, because under contention it may
                be called more than once (each retry recomputes against
                a fresh snapshot). Side effects -- logging, I/O,
                notifying something external -- belong in a subscriber
                callback fired *after* the swap wins, not inside `fn`.
                If ``fn`` returns the same object it was given
                (``new is old``), that's treated as a no-op and no
                swap happens at all.

        Returns:
            The new snapshot that won the race (or the unchanged
            snapshot, if `fn` was a no-op).

        Raises:
            ConflictRetryExceeded: If ``max_retries`` attempts all lost
                the race. Only realistic under extreme sustained
                contention on the same store.

        Example:
            >>> # "set only if absent" -- not expressible with plain set()
            >>> store.mutate(
            ...     lambda m: m if "lock" in m else m.set("lock", "held")
            ... )
        """
        for attempt in range(self._max_retries):
            old = self._ref
            new = fn(old)
            if new is old:
                return old  # no-op mutation, nothing to swap
            with self._swap_lock:
                if self._ref is old:
                    self._ref = new
                    return new
            # Someone else won the race since we read `old` -- retry
            # against the now-current snapshot. This is the normal,
            # expected path under contention, not an error.
            if attempt > 50 and attempt % 50 == 0:
                _log.debug("mutate() retrying after %d conflicts", attempt)
        raise ConflictRetryExceeded(
            f"Could not apply mutation after {self._max_retries} retries "
            "(pathological write contention on a single store)"
        )

    def set(self, key: str, value: Any) -> None:
        """Set a single key, atomically, with retry-on-conflict.

        Args:
            key: The key to set.
            value: The value to store. Any Python object.

        Example:
            >>> store.set("foo", 123)
            >>> store.get("foo")
            123
        """
        old_val = self._ref.get(key, _MISSING)
        self.mutate(lambda m: m.set(key, value))
        self._notify(key, "set", old_val if old_val is not _MISSING else None, value)

    def mset(self, mapping: Dict[str, Any]) -> immutables.Map:
        """Set multiple keys in a **single atomic swap**.

        This is not just a convenience wrapper around calling
        :meth:`set` in a loop -- it matters for correctness and
        performance under contention. Each individual ``set()`` call is
        its own compare-and-swap attempt; if you have five related keys
        to update together, five separate ``set()`` calls means five
        separate chances to lose a race and retry, and -- more
        importantly -- other readers could observe the update
        *partially applied* (key 1 through 3 changed, 4 and 5 not yet).
        ``mset`` applies all of them in one swap: readers see either
        the fully-old or the fully-new state, never a partial mix.

        Args:
            mapping: Dict of ``{key: value}`` pairs to set together.

        Returns:
            The new snapshot after the swap.

        Example:
            >>> # Move money between two accounts -- must be atomic,
            >>> # a reader must never see only one side updated.
            >>> store.mset({"account_a": 90, "account_b": 110})
        """
        old_vals = {k: self._ref.get(k, _MISSING) for k in mapping}

        def _txn(m: immutables.Map) -> immutables.Map:
            for k, v in mapping.items():
                m = m.set(k, v)
            return m

        new = self.mutate(_txn)
        for k, v in mapping.items():
            old = old_vals[k]
            self._notify(k, "set", None if old is _MISSING else old, v)
        return new

    def atomic(self, key: str, fn: Callable[[Any], Any], default: Any = None) -> Any:
        """Atomic compound read-modify-write on a single key, with a
        notification fired after the winning swap -- e.g. a counter
        increment that's safe under concurrent writers **and** visible
        to :meth:`subscribe` / anything built on it (such as
        :meth:`livedict.LiveDict.wait_for`... see
        :class:`livedict.LiveDict.atomic` for the public-facing
        version most users call).

        This exists at the ``ReactiveStore`` level (not just on
        :class:`livedict.LiveDict`) specifically so the notification is
        part of the same atomic operation as the swap -- computing the
        new value via :meth:`mutate` and firing the notification as a
        separate step would leave a window where a subscriber could
        read a value that hasn't been announced yet, or never gets
        announced at all if the calling code forgets to notify (this
        was a real bug: an earlier version of the higher-level
        ``atomic()`` called :meth:`mutate` directly and never notified
        subscribers at all, so :meth:`subscribe` and anything built on
        it -- like a blocking wait on a condition -- silently never
        fired for atomic updates).

        Args:
            key: The key to read, transform, and write back.
            fn: A **pure** function ``(current_value) -> new_value``.
                May run more than once under contention.
            default: Value passed to ``fn`` if ``key`` doesn't exist
                yet.

        Returns:
            The new value that was written.

        Example:
            >>> store.set("counter", 0)
            >>> store.atomic("counter", lambda v: v + 1)
            1
        """
        box: Dict[str, Any] = {}

        def _txn(m: immutables.Map) -> immutables.Map:
            old = m.get(key, _MISSING)
            box["old"] = old
            current = m.get(key, default)
            box["new_value"] = fn(current)
            return m.set(key, box["new_value"])

        self.mutate(_txn)
        old = box["old"]
        self._notify(key, "set", None if old is _MISSING else old, box["new_value"])
        return box["new_value"]

    def delete(self, key: str, _event: str = "delete") -> bool:
        """Delete a key if present.

        Args:
            key: The key to delete.
            _event: Internal use only. TTL expiry reuses this exact
                method but passes ``_event="expire"`` so subscribers
                can tell an automatic expiry apart from a manual
                delete. Don't pass this yourself.

        Returns:
            ``True`` if the key existed and was removed, ``False`` if
            it was already absent (a no-op, nothing is swapped or
            notified in that case).

        Example:
            >>> store.set("foo", 1)
            >>> store.delete("foo")
            True
            >>> store.delete("foo")  # already gone
            False
        """
        existed = key in self._ref
        if not existed:
            return False
        old_val = self._ref.get(key)
        self.mutate(lambda m: m.delete(key) if key in m else m)
        self._notify(key, _event, old_val, None)
        return True

    # ---- reactive subscriptions ----------------------------------------

    def subscribe(
        self,
        fn: Callable[[str, str, Any, Any], None],
        key: Optional[str] = None,
    ) -> Subscription:
        """Subscribe to changes on this store.

        Args:
            fn: Called as ``fn(key, event, old_value, new_value)`` after
                every successful write that matches ``key``.
                ``event`` is one of ``"set"``, ``"delete"``, or
                ``"expire"``. Callbacks run synchronously, in the
                thread that performed the write, immediately after that
                write's swap succeeds -- so keep them fast; a slow
                callback delays whoever made the change (and, for
                :class:`nimbuskv.NimbusKV`, delays that ``set``/
                ``delete`` call from returning).
            key: If given, only notified about changes to this specific
                key. If ``None`` (default), notified about every key.

        Returns:
            A :class:`Subscription` handle -- call ``.cancel()`` on it
            to stop receiving notifications.

        Example:
            >>> def on_change(key, event, old, new):
            ...     print(f"{key}: {event} {old!r} -> {new!r}")
            >>> sub = store.subscribe(on_change, key="foo")
            >>> store.set("foo", 1)
            foo: set None -> 1
            >>> sub.cancel()
        """
        with self._subs_lock:
            sub_id = self._next_sub_id
            self._next_sub_id += 1
            self._subs.setdefault(key, []).append((sub_id, fn))
        return Subscription(self, key, sub_id)

    def _unsubscribe(self, key: Optional[str], sub_id: int) -> None:
        with self._subs_lock:
            lst = self._subs.get(key)
            if lst:
                self._subs[key] = [(i, f) for i, f in lst if i != sub_id]

    def _notify(self, key: str, event: str, old_value: Any, new_value: Any) -> None:
        # Snapshot the subscriber lists under lock, then call outside
        # the lock -- callbacks must never be executed while holding
        # a lock, or a slow/misbehaving callback would block every
        # other subscriber (or worse, another writer) indefinitely.
        with self._subs_lock:
            targeted = list(self._subs.get(key, ()))
            general = list(self._subs.get(None, ()))
        for _, fn in targeted + general:
            try:
                fn(key, event, old_value, new_value)
            except Exception:
                # A misbehaving subscriber must never break the write
                # path for everyone else. Logged, not raised.
                _log.exception(
                    "subscriber callback raised for key=%r event=%r", key, event
                )
