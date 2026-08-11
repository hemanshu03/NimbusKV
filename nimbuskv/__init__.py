"""nimbuskv - reactive, TTL-aware, optimistic-concurrency key/value store.

Adapts automatically between standard GIL Python and free-threaded Python
(3.13t/3.14t) using the same code path.
"""
from .modules import *
from .modules import __all__

__version__ = "3.0.1"
