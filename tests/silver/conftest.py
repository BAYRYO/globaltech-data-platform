import pytest

from .lakes import DAYS, SALT, Lake, bronze_lake, copy_of


@pytest.fixture(autouse=True)
def _salt(monkeypatch):
    monkeypatch.setenv("GTDP_PSEUDO_SALT", SALT)


@pytest.fixture(scope="session")
def seed42(spark, tmp_path_factory) -> Lake:
    """Bronze J1 to J3 of seed 42, shared: tests copy it with ``fresh_lake``, never write to it."""
    with pytest.MonkeyPatch.context() as patch:
        patch.setenv("GTDP_PSEUDO_SALT", SALT)
        return bronze_lake(spark, tmp_path_factory.mktemp("seed42"), 42)


@pytest.fixture
def fresh_lake(seed42, tmp_path) -> Lake:
    """Bronze J1 to J3 of seed 42, no silver yet, for a test that writes."""
    return copy_of(seed42, tmp_path)


@pytest.fixture(scope="session")
def published(seed42, tmp_path_factory) -> Lake:
    """Silver of J1, J2 then J3, all sources, on a copy of seed 42: read only."""
    with pytest.MonkeyPatch.context() as patch:
        patch.setenv("GTDP_PSEUDO_SALT", SALT)
        lake = copy_of(seed42, tmp_path_factory.mktemp("published"))
        for day in DAYS:
            lake.run("silver", day)
    return lake
