import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from gtdp.datagen.__main__ import main
from gtdp.datagen.replay import replay


def sensor_files(tmp_path: Path, count: int = 4) -> Path:
    source = tmp_path / "source"
    source.mkdir()
    for index in range(count):
        name = f"sensors_20260101T0000{index * 5:02d}.jsonl"
        (source / name).write_text(f'{{"event_id":"S{index}"}}\n' * 3, encoding="utf-8")
    (source / "_MANIFEST.json").write_text("{}", encoding="utf-8")
    return source


def names(directory: Path) -> list[str]:
    return sorted(path.name for path in directory.iterdir())


def sensor_names(directory: Path) -> list[str]:
    return sorted(path.name for path in directory.glob("sensors_*.jsonl"))


def published_names(*args, **kwargs) -> list[str]:
    return [path.name for path in replay(*args, **kwargs)]


def test_replay_publishes_one_complete_file_per_interval_in_name_order(tmp_path):
    source = sensor_files(tmp_path)
    expected = sensor_names(source)
    watched = tmp_path / "incoming"
    now = [1000.0]
    seen_at_each_tick = []

    def sleep(seconds: float) -> None:
        present = names(watched)
        assert present == expected[: len(present)]
        assert all((watched / n).read_bytes() == (source / n).read_bytes() for n in present)
        seen_at_each_tick.append((seconds, len(present)))
        now[0] += seconds

    published = published_names(source, str(watched), 5, clock=lambda: now[0], sleep=sleep)

    assert published == expected
    assert seen_at_each_tick == [(5, 1), (5, 2), (5, 3)]
    assert names(watched) == expected


def test_replay_resumes_after_a_partial_run(tmp_path):
    source = sensor_files(tmp_path)
    watched = tmp_path / "incoming"
    args = ["replay-sensors", "--src", str(source), "--dst", str(watched), "--interval", "0"]

    assert main([*args, "--limit", "2"]) == 0
    assert len(names(watched)) == 2
    assert published_names(source, str(watched), 0) == sensor_names(source)[2:]
    assert names(watched) == sensor_names(source)


def test_replay_prints_the_files_and_events_it_published(tmp_path, capsys):
    source = sensor_files(tmp_path)
    watched = tmp_path / "incoming"
    args = ["replay-sensors", "--src", str(source), "--dst", str(watched), "--interval", "0"]

    assert main([*args, "--limit", "3"]) == 0

    assert capsys.readouterr().out.startswith(f"published 3 files, 9 events to {watched} (")


def test_replay_streams_a_generated_sensor_day(ci_root, tmp_path):
    day = ci_root / "sensors" / "2026-01-01"
    watched = tmp_path / "incoming"

    published = published_names(day, str(watched), 0)

    assert len(published) == 48
    assert names(watched) == sorted(published)
    assert all((watched / n).read_bytes() == (day / n).read_bytes() for n in published)


def test_hdfs_replay_puts_into_a_sibling_staging_dir_then_moves(tmp_path):
    source = sensor_files(tmp_path, count=3)
    files = sensor_names(source)
    calls = []

    def run(command: list[str], **kwargs) -> subprocess.CompletedProcess:
        calls.append(command)
        listing = f"hdfs:///landing/incoming/{files[0]}\n" if command[2] == "-ls" else ""
        return subprocess.CompletedProcess(command, 0, stdout=listing)

    published = published_names(source, "hdfs:///landing/incoming/", 0, run=run)

    staging, watched = "hdfs:///landing/incoming_staging", "hdfs:///landing/incoming"
    assert published == files[1:]
    assert calls == [
        ["hdfs", "dfs", "-mkdir", "-p", staging, watched],
        ["hdfs", "dfs", "-ls", "-C", watched],
        ["hdfs", "dfs", "-put", "-f", str(source / files[1]), f"{staging}/{files[1]}"],
        ["hdfs", "dfs", "-mv", f"{staging}/{files[1]}", f"{watched}/{files[1]}"],
        ["hdfs", "dfs", "-put", "-f", str(source / files[2]), f"{staging}/{files[2]}"],
        ["hdfs", "dfs", "-mv", f"{staging}/{files[2]}", f"{watched}/{files[2]}"],
    ]


@pytest.mark.parametrize(("sig", "status"), [(signal.SIGINT, 130), (signal.SIGTERM, 143)])
def test_a_stop_signal_ends_the_replay_with_its_summary_and_no_traceback(tmp_path, sig, status):
    source = sensor_files(tmp_path, count=20)
    watched = tmp_path / "incoming"
    command = [sys.executable, "-m", "gtdp.datagen", "replay-sensors"]
    args = ["--src", str(source), "--dst", str(watched), "--interval", "0.5"]
    replaying = subprocess.Popen(
        [*command, *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)},
    )
    deadline = time.monotonic() + 30
    while len(sensor_names(watched) if watched.exists() else []) < 2:
        assert time.monotonic() < deadline, "the replay published nothing"
        time.sleep(0.05)

    replaying.send_signal(sig)
    out, err = replaying.communicate(timeout=30)

    published = sensor_names(watched)
    assert replaying.returncode == status
    assert 2 <= len(published) < 20
    expected = f"published {len(published)} files, {3 * len(published)} events to {watched}"
    assert out.startswith(f"{expected}, stopped by {sig.name} (")
    assert "Traceback" not in err
