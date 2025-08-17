"""Compatibility package for build tools expecting `sql_generator`.

This shim exists so tooling that looks for a package named
`sql_generator` finds a module. It attempts to import the project's
actual code under the `src` package but remains empty on failure.
"""

from importlib import import_module

try:
    _mod = import_module("src")
    for k in dir(_mod):
        if not k.startswith("_"):
            globals()[k] = getattr(_mod, k)
except Exception:
    # Keep shim minimal; build just needs the package folder present.
    pass
