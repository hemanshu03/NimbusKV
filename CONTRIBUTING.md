# Contributing to NimbusKV

Thanks for considering it. NimbusKV is a solo-maintained project, so contributions - bug reports, PRs, and design feedback - genuinely help.

## Before you start

For anything beyond a small fix (typo, docstring, a single bug), open an issue first describing what you want to change and why. NimbusKV's core is an optimistic-concurrency engine - changes there need to be reasoned about carefully, so it's worth agreeing on the approach before writing code.

## Setup

```bash
git clone https://github.com/hemanshu03/NimbusKV.git
cd NimbusKV
pip install -e ".[dev]"
```

This installs NimbusKV in editable mode plus `pytest`, `pytest-asyncio`, and `fakeredis` for the test suite.

## Running tests

```bash
pytest tests/ -v
```

If you have a free-threaded Python build (3.13t/3.14t) available, please also run the suite there - that's the runtime this library's core promise is actually about, and standard-Python-only testing can't catch every concurrency issue.

```bash
python testfile.py
```

runs the same checks as an end-to-end script, plus the OLD-lock-vs-NEW-optimistic benchmark (section `[8]`) - useful for seeing whether a change affects performance under contention.

## Code style

- Every public class/method needs a docstring: what it does, `Args`/`Returns`/`Raises` where relevant, and a runnable `Example`. Look at any existing method in `nimbuskv/modules/nimbuskv_v3.py` for the expected format.
- Mutations passed to `ReactiveStore.mutate()` (or anything built on it) must be pure functions - no side effects, since they may be retried under contention. If you're adding a new compound operation, make sure this holds.
- Keep the persistence backend contract intact: `PersistenceBackend` implementations are write-through mirrors only, never read from directly by the hot path. Don't add a fast-path read from a backend, even as an optimization - it breaks the lock-free read guarantee.

## What's especially useful right now

- **Free-threaded (3.13t/3.14t) testing and bug reports.** This is the newest, least-battle-tested part of the runtime story, and the area most likely to have real bugs no one's found yet.
- **Real sandboxing design.** A previous "sandbox" feature was removed because it wasn't actually isolating anything (just a timeout wrapper). If you have experience with subprocess isolation or WASM-based sandboxing in Python, this would be a genuinely valuable contribution.
- **Additional persistence backends** beyond Memory/SQLite/Redis, following the `PersistenceBackend` interface.

## Reporting bugs

Open a [GitHub issue](https://github.com/hemanshu03/NimbusKV/issues) with:
- What you expected vs. what happened
- A minimal reproduction if possible
- Your Python version, and whether it's a free-threaded build (`python -c "import sys; print(sys._is_gil_enabled())"` if on 3.13+)

## License

By contributing, you agree your contributions are licensed under the same GPL-3.0 license as the rest of the project.
