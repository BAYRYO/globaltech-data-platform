from pathlib import Path

import pytest

from gtdp.datagen.__main__ import main

KIT = Path(__file__).resolve().parents[2] / "data" / "kit"


@pytest.fixture(scope="session")
def kit_dir() -> Path:
    return KIT


@pytest.fixture(scope="session")
def generate_ci():
    """Run `generate --profile ci` through the command line and return the profile root."""

    def run(out: Path, seed: int = 42) -> Path:
        args = ["generate", "--profile", "ci", "--seed", str(seed), "--out", str(out)]
        assert main([*args, "--kit", str(KIT)]) == 0
        return out / "ci"

    return run


@pytest.fixture(scope="session")
def ci_root(tmp_path_factory: pytest.TempPathFactory, generate_ci) -> Path:
    return generate_ci(tmp_path_factory.mktemp("generated"))
