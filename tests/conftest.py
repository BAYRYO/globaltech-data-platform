import os

import pytest


@pytest.fixture(autouse=True)
def _isolated_environment(monkeypatch):
    """Keep GTDP_* variables of the developer shell out of every test."""
    for name in list(os.environ):
        if name.startswith("GTDP_"):
            monkeypatch.delenv(name)
