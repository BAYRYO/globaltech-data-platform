"""KAN-34, KAN-35: the products kept per country and the moving average window are settings."""

import pytest

from gtdp.config import ConfigError, load_config


@pytest.mark.parametrize(
    ("key", "variable"), [("top_n", "TOP_N"), ("moving_average_days", "MOVING_AVERAGE_DAYS")]
)
def test_a_gold_setting_can_be_overridden_per_run(monkeypatch, key, variable):
    assert getattr(load_config("local", overrides={f"gold.{key}": "10"}).gold, key) == 10

    monkeypatch.setenv(f"GTDP_GOLD__{variable}", "3")
    assert getattr(load_config("local").gold, key) == 3


@pytest.mark.parametrize("key", ["top_n", "moving_average_days"])
def test_a_gold_setting_must_be_at_least_one(key):
    with pytest.raises(ConfigError, match=key):
        load_config("local", overrides={f"gold.{key}": "0"})
