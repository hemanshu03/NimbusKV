# Changelog

## [3.0.0] - 2026-08-10

**Project renamed from `livedict` to `nimbuskv`.** The import path is now `from nimbuskv import NimbusKV` (previously `from livedict import LiveDict`). See the migration note in the README for details. The old `livedict` package on PyPI will remain published as a deprecated stub pointing here.

**Rewrite - new architecture, not a patch on v2.**

Added
* `ReactiveStore`: immutable-snapshot, optimistic-concurrency core (lock-free reads always; lock-free writes on free-threaded Python via runtime detection, correct retry-based writes on standard GIL Python).
* `runtime.py`: one-time interpreter detection (`LOCK_FREE_MODE`, `gil_enabled()`, `is_free_threaded_build()`).
* `subscribe()`: real push-based reactivity (`set`/`delete`/`expire` events), replacing v2's fire-and-forget callback registration.
* `atomic()`: safe compound read-modify-write, replacing manual `lock()`/`unlock()`.
* `mset()`/`mget()`: batched multi-key atomic write + convenience multi-key read.
* `wait_for()`: block a thread until a condition on a key holds, via subscriptions -- no polling.
* Context manager support (`with NimbusKV() as ld:`).
* `py.typed` marker (PEP 561) for type-checker support.
* Genuinely non-blocking async API for the in-memory path (no `run_in_executor` used when there's no real I/O).
* Persistence backends (`NullBackend`/`SQLiteBackend`/`RedisBackend`) rebuilt as write-through mirrors with startup rehydration, decoupled from the hot read/write path.
* Full docstrings (Google-style, with runnable examples) on every public class and method.

Fixed
* `ExpiryScheduler` now uses a per-key generation counter -- rescheduling a key's TTL correctly invalidates the previous deadline instead of leaving a stale heap entry that could (harmlessly, but wastefully) fire later.
* Subscriber callbacks that raise no longer propagate into the write path or the scheduler thread; they're logged via the `nimbuskv` logger instead.

Removed
* Sandbox module - v2's was a timeout wrapper, not real isolation; removed rather than kept under a misleading name. Planned for a future release with actual isolation.
* Manual `lock()`/`unlock()` - replaced by `atomic()`.
* Fake-async (`run_in_executor` wrapping non-I/O calls) for the memory backend.

Breaking
* This is not backwards compatible with v2's API by design (`work_mode=` constructor arg removed in favor of `set`/`aset` naming; callback registration replaced by `subscribe()`).


All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project follows Semantic Versioning.

## Release: [2.0.2] - 2025-10-05 - latest
### Changed
- The callback handler was calling the callbacks 2 times.I messed up by adding another loop if there is any RunTimeError at _run_async in the _trigger_event function.. fixed it.

## Release: [2.0.1] - 2025-10-05
### Changed
- setup.py had a major requirement issue that was fixed. (I had mistakenly added sqlite3 as a requirement but it's not needed since Python 3.6+ has included it.)

## Release: [2.0.0] - 2025-10-04
### Added
- Comprehensive Google-style docstrings added to modules, classes, and functions.
- Sandbox functionality is now active and maintained within the package.

### Changed
- Project version bumped to `2.0.0`.
- Packaging files (`setup.py`, `pyproject.toml`) updated for PyPI release.

### Removed
- Cryptography-related code has been removed from the package. Encryption and cryptographic responsibilities are now delegated to the user; the library no longer provides built-in cryptographic primitives.

### Notes
- The `sandbox` module is live and behaves according to the code in this release; users should review sandbox behavior for their environment and use cases.

## Release: [v1.0.4] - 2025-08-11

### Added
* Pydantic-based configuration models for storage/backends/monitoring and bucket policies.
* New persistence backends:
  - SQLite backend with short-lived connections and indexing.
  - File-backed object store for external object storage emulation.
  - Improved InMemory backend used for testing and simple runs.
* CipherAdapter with AES-GCM (cryptography) and deterministic base64 XOR fallback.
* Heap-based expiry monitor with efficient wake-ups and rebuild heuristics.
* Bucket semantics, bucket policies, and hybrid routing placeholders.
* Better backend fallbacks and tolerant lookup behavior (any-bucket fallback for SQLite/File).
* Expanded API and documentation covering rotation, hooks, and persistence options.

### Changed
* Packaging bumped to `1.0.4-release` to mark the release-ready core implementation.
* README and packaging metadata updated to reflect new dependencies and features.

### Fixed
* Backend expiry handling improved to avoid stale reads.
* sqlite backend uses short-lived sqlite3 connections to avoid file locks on Windows.
* File backend tolerant lookup fixes and safer cleanup logic.

---
## Prior patch notes (v1.0.4 and earlier)
No previous changes. This is the first version of NimbusKV's official release.

