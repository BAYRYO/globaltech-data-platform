import re
import shutil
from pathlib import Path

import pytest

from gtdp.config import ConfigError, load_config

REPO = Path(__file__).resolve().parents[2]


@pytest.fixture
def conf_dir(tmp_path):
    conf = tmp_path / "conf"
    shutil.copytree(REPO / "conf", conf)
    return conf


def with_streaming(conf_dir: Path, thresholds: str) -> Path:
    """Replace the shipped [streaming.thresholds] table with ``thresholds``."""
    app = conf_dir / "app.toml"
    text = app.read_text(encoding="utf-8")
    head = text[: text.index("[streaming.thresholds]")]
    app.write_text(f"{head}[streaming.thresholds]\n{thresholds}\n", encoding="utf-8")
    return conf_dir


def test_shipped_thresholds_are_the_data_contract_alert_limits():
    streaming = load_config("local").streaming

    assert streaming is not None
    assert streaming.thresholds == {
        "temperature": {"above": 85.0},
        "vibration": {"above": 7.1},
        "pressure": {"below": 4.0, "above": 8.0},
    }


def test_streaming_keys_take_environment_and_explicit_overrides(conf_dir, monkeypatch):
    monkeypatch.setenv("GTDP_STREAMING__TRIGGER_INTERVAL_S", "1")

    cfg = load_config("local", conf_dir, overrides={"streaming.stop_after_idle_triggers": "3"})

    assert cfg.streaming.trigger_interval_s == 1
    assert cfg.streaming.stop_after_idle_triggers == 3


def test_a_configuration_without_streaming_section_has_no_streaming_settings(conf_dir):
    app = conf_dir / "app.toml"
    text = app.read_text(encoding="utf-8")
    app.write_text(text[: text.index("[streaming]")], encoding="utf-8")

    assert load_config("local", conf_dir).streaming is None


@pytest.mark.parametrize(
    ("thresholds", "message"),
    [
        ("", "no metric"),
        ("temperature = 85.0", "expected a table"),
        ("temperature = { over = 85.0 }", "unknown directions ['over']"),
        ('temperature = { above = "85" }', "expected a number"),
        ("temperature = { above = true }", "expected a number"),
        ("pressure = { below = 8.0, above = 4.0 }", "below must be lower than above"),
    ],
    ids=["empty", "not-a-table", "unknown-direction", "string", "bool", "inverted"],
)
def test_invalid_thresholds_fail_at_load(conf_dir, thresholds, message):
    with pytest.raises(ConfigError, match=re.escape(message)):
        load_config("local", with_streaming(conf_dir, thresholds))


@pytest.mark.parametrize(
    "override",
    [
        "streaming.max_files_per_trigger=0",
        "streaming.state_partitions=0",
        "streaming.trigger_interval_s=-1",
        "streaming.thresholds=85",
    ],
)
def test_invalid_streaming_override_fails_at_load(conf_dir, override):
    key, _, value = override.partition("=")

    with pytest.raises(ConfigError):
        load_config("local", conf_dir, overrides={key: value})
