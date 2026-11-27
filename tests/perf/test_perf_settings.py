"""The benchmark folders and warm-up rounds come from the [perf] section of the configuration."""

import pytest

from gtdp.config import ConfigError, load_config


def test_the_benchmark_folders_come_from_the_configuration(monkeypatch):
    perf = load_config("cluster").perf
    assert perf.storage == "/benchmarks/storage"
    assert perf.warmup_rounds == 2

    assert load_config("cluster", overrides={"perf.storage": "/tmp/s"}).perf.storage == "/tmp/s"
    monkeypatch.setenv("GTDP_PERF__TABLES", "hdfs://namenode:8020/user/etl/t")
    assert load_config("cluster").perf.tables == "hdfs://namenode:8020/user/etl/t"


def test_a_negative_number_of_warm_up_rounds_is_refused():
    with pytest.raises(ConfigError, match="warmup_rounds"):
        load_config("cluster", overrides={"perf.warmup_rounds": "-1"})
