"""Parity / anti-drift tests for the PySpark console entry points.

1. Every ``[project.scripts]`` entry in ``pyproject.toml`` must resolve to an importable
   ``module:function`` (so a renamed module can never leave a dangling console script).
2. Every script name must be documented in ``python/README.md`` (so the entry-point table cannot
   silently drift from the code).

Pure-Python: modules are imported but their ``main()`` is never called, so no Spark session starts.
"""

from __future__ import annotations

import importlib
from pathlib import Path

import pytest

_PY_ROOT = Path(__file__).resolve().parent.parent
_PYPROJECT = _PY_ROOT / "pyproject.toml"
_README = _PY_ROOT / "README.md"


def _load_scripts() -> dict[str, str]:
    tomllib = pytest.importorskip("tomllib")
    data = tomllib.loads(_PYPROJECT.read_text())
    return data.get("project", {}).get("scripts", {})


def test_pyproject_has_scripts():
    assert _load_scripts(), "no [project.scripts] found in pyproject.toml"


@pytest.mark.parametrize("name,target", sorted(_load_scripts().items()))
def test_entry_point_is_importable(name: str, target: str):
    module_path, _, func_name = target.partition(":")
    module = importlib.import_module(module_path)
    func = getattr(module, func_name, None)
    assert callable(func), f"{name}: {target} does not resolve to a callable"


def test_every_script_is_documented_in_readme():
    readme = _README.read_text()
    missing = [name for name in _load_scripts() if name not in readme]
    assert not missing, f"scripts missing from python/README.md entry table: {missing}"
