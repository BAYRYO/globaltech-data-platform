import json
import logging
from dataclasses import asdict
from pathlib import Path

import pytest

from gtdp.config import DEV_SALT, ConfigError, Env, load_config

REPO = Path(__file__).resolve().parents[2]

PATHS = """
[paths]
landing = "hdfs://nn:8020/lake/landing"
bronze = "hdfs://nn:8020/lake/bronze/"
silver = "hdfs://nn:8020/lake/silver"
gold = "hdfs://nn:8020/lake/gold"
streaming = "hdfs://nn:8020/lake/streaming"
ops = "lake/ops"
checkpoints = "/abs/checkpoints"
"""


@pytest.fixture
def conf_dir(tmp_path):
    conf = tmp_path / "conf"
    (conf / "env").mkdir(parents=True)
    (conf / "app.toml").write_text(
        '[app]\nname = "common"\ntimezone = "UTC"\n'
        "[spark]\nshuffle_partitions = 10\nadaptive = true\n" + PATHS
    )
    (conf / "env" / "local.toml").write_text('[app]\nname = "from-env-file"\n')
    (conf / "env" / "cluster.toml").write_text("")
    return conf


def write_env_file(conf_dir, text):
    (conf_dir / "env" / "local.toml").write_text(text)


def test_each_layer_overrides_the_previous_one(conf_dir, monkeypatch):
    write_env_file(conf_dir, "[spark]\nshuffle_partitions = 20\nadaptive = false\n")
    monkeypatch.setenv("GTDP_SPARK__SHUFFLE_PARTITIONS", "30")
    monkeypatch.setenv("GTDP_APP__NAME", "from-env-var")

    cfg = load_config("local", conf_dir, overrides={"spark.shuffle_partitions": "40"})

    assert cfg.env is Env.LOCAL
    assert cfg.app.timezone == "UTC"  # app.toml only
    assert cfg.spark.adaptive is False  # env file
    assert cfg.app.name == "from-env-var"  # env var
    assert cfg.spark.shuffle_partitions == 40  # explicit override


def test_text_overrides_are_typed_like_the_files(conf_dir, monkeypatch):
    monkeypatch.setenv("GTDP_SPARK__ADAPTIVE", "False")

    cfg = load_config("local", conf_dir, overrides={"spark.master": "local[3]"})

    assert cfg.spark.adaptive is False
    assert cfg.spark.master == "local[3]"


def test_optional_key_defaults_to_none(conf_dir):
    assert load_config("local", conf_dir).spark.master is None


@pytest.mark.parametrize(
    "spark_table",
    [
        'shuffle_partitions = "ten"\nadaptive = true',
        "shuffle_partitions = true\nadaptive = true",
        "shuffle_partitions = 10\nadaptive = 1",
        "shuffle_partitions = 0\nadaptive = true",
        "shuffle_partitions = 10\nadaptive = true\nshufle = 3",
    ],
    ids=["str-for-int", "bool-for-int", "int-for-bool", "out-of-range", "unknown-key"],
)
def test_invalid_file_value_fails_at_load(conf_dir, spark_table):
    write_env_file(conf_dir, f"[spark]\n{spark_table}\n")

    with pytest.raises(ConfigError):
        load_config("local", conf_dir)


@pytest.mark.parametrize(
    ("variable", "value"),
    [
        ("GTDP_SPARK__SHUFFLE_PARTITIONS", "many"),
        ("GTDP_SPARK__ADAPTIVE", "yes please"),
        ("GTDP_SPARK__SHUFLE_PARTITIONS", "3"),
        ("GTDP_NOPE__KEY", "x"),
        ("GTDP_PSEUDONYMIZATION__SALT", "must-come-from-GTDP_PSEUDO_SALT"),
    ],
)
def test_invalid_environment_override_fails_at_load(conf_dir, monkeypatch, variable, value):
    monkeypatch.setenv(variable, value)

    with pytest.raises(ConfigError, match=variable):
        load_config("local", conf_dir)


def test_invalid_explicit_override_fails_at_load(conf_dir):
    with pytest.raises(ConfigError, match="paths.lake"):
        load_config("local", conf_dir, overrides={"paths.lake": "/x"})


def test_unknown_section_in_file_fails_at_load(conf_dir):
    write_env_file(conf_dir, '[pseudonymization]\nsalt = "committed-secret"\n')

    with pytest.raises(ConfigError, match="pseudonymization"):
        load_config("local", conf_dir)


def test_missing_key_fails_at_load(conf_dir):
    (conf_dir / "app.toml").write_text('[app]\nname = "x"\n[spark]\nshuffle_partitions = 1\n')

    with pytest.raises(ConfigError, match="missing key"):
        load_config("local", conf_dir)


def test_unknown_env_fails_at_load(conf_dir):
    with pytest.raises(ConfigError, match="local, cluster"):
        load_config("prod", conf_dir)


def test_cluster_without_salt_fails_at_load(conf_dir):
    with pytest.raises(ConfigError, match="GTDP_PSEUDO_SALT"):
        load_config("cluster", conf_dir)


def test_salt_comes_from_environment_and_never_prints(conf_dir, monkeypatch):
    monkeypatch.setenv("GTDP_PSEUDO_SALT", "s3cret-salt")

    cfg = load_config("cluster", conf_dir)

    assert cfg.pseudonymization.salt.reveal() == "s3cret-salt"
    assert "s3cret-salt" not in repr(cfg)
    assert "s3cret-salt" not in json.dumps(asdict(cfg), default=str)


def test_local_without_salt_uses_dev_salt_with_warning(conf_dir, caplog):
    with caplog.at_level(logging.WARNING):
        cfg = load_config("local", conf_dir)

    assert cfg.pseudonymization.salt.reveal() == DEV_SALT
    assert "GTDP_PSEUDO_SALT" in caplog.text


def test_local_paths_become_file_uris_under_the_conf_parent(conf_dir):
    paths = load_config("local", conf_dir).paths

    assert paths.ops == (conf_dir.parent / "lake" / "ops").as_uri()
    assert paths.checkpoints == "file:///abs/checkpoints"
    assert paths.bronze == "hdfs://nn:8020/lake/bronze"


def test_git_sha_comes_from_environment(conf_dir, monkeypatch):
    assert load_config("local", conf_dir).git_sha is None
    monkeypatch.setenv("GTDP_GIT_SHA", "abc123")
    assert load_config("local", conf_dir).git_sha == "abc123"


def test_conf_dir_comes_from_environment(conf_dir, monkeypatch):
    monkeypatch.setenv("GTDP_CONF_DIR", str(conf_dir))

    assert load_config("local").app.name == "from-env-file"


def test_shipped_cluster_config_points_every_area_to_hdfs(monkeypatch):
    monkeypatch.setenv("GTDP_PSEUDO_SALT", "x")

    paths = asdict(load_config("cluster").paths)

    assert paths["bronze"] == "hdfs://namenode:8020/datalake/bronze"
    assert paths["checkpoints"] == "hdfs://namenode:8020/datalake/streaming/sensors/checkpoints"
    assert all(uri.startswith("hdfs://namenode:8020/datalake/") for uri in paths.values())


def test_shipped_local_config_keeps_the_lake_inside_the_repository():
    cfg = load_config("local")

    assert cfg.spark.master == "local[*]"
    assert cfg.paths.ops == (REPO / "data" / "lake" / "ops").as_uri()
