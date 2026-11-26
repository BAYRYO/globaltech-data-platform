"""KAN-34: the number of products kept per country is configuration."""

import pytest

from gtdp.config import ConfigError, load_config


def test_top_n_can_be_overridden_per_run(monkeypatch):
    assert load_config("local", overrides={"gold.top_n": "10"}).gold.top_n == 10

    monkeypatch.setenv("GTDP_GOLD__TOP_N", "3")
    assert load_config("local").gold.top_n == 3


def test_top_n_must_keep_at_least_one_rank():
    with pytest.raises(ConfigError, match="top_n"):
        load_config("local", overrides={"gold.top_n": "0"})
