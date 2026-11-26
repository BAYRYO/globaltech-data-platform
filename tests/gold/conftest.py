import pytest

from silver.lakes import DAYS, SALT, Lake, bronze_lake

from .silver_lake import SilverLake


@pytest.fixture
def silver_lake(spark, lake_config) -> SilverLake:
    return SilverLake(spark, lake_config)


@pytest.fixture(scope="session")
def generated(spark, tmp_path_factory) -> Lake:
    """The ci drops of seed 42 through bronze, then silver and gold day after day: read only."""
    with pytest.MonkeyPatch.context() as patch:
        patch.setenv("GTDP_PSEUDO_SALT", SALT)
        lake = bronze_lake(spark, tmp_path_factory.mktemp("gold-generated"), 42)
        for day in DAYS:
            lake.run("silver", day)
            lake.run("gold", day)
    return lake
