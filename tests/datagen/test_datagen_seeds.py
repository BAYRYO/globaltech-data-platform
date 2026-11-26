"""Generation is sane by construction: every seed generates, then passes the verifier."""

import os
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import pytest

from gtdp.datagen.generate import generate
from gtdp.datagen.model import PROFILES
from gtdp.datagen.verify import verify

KIT = Path(__file__).resolve().parents[2] / "data" / "kit"
CASES = [("dev", 92)] + [("ci", seed) for seed in range(400)]


def generate_and_verify(case: tuple[str, int], out: Path) -> str | None:
    """None when the profile generates and verifies, else what went wrong."""
    profile, seed = case
    root = out / str(os.getpid())
    try:
        generate(PROFILES[profile], seed, root, KIT)
    except Exception as error:
        return f"{profile} seed {seed}: generate raised {error!r}"
    problems = verify(root / profile).problems
    return f"{profile} seed {seed}: {problems[:3]}" if problems else None


@pytest.mark.slow
def test_every_seed_generates_and_verifies(tmp_path):
    with ProcessPoolExecutor() as pool:
        results = pool.map(generate_and_verify, CASES, [tmp_path] * len(CASES), chunksize=8)
        assert [failure for failure in results if failure] == []
