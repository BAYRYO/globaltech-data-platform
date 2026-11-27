"""A fake ``docker`` on PATH for the DataNode failure scripts, after review B of PR #24.

The fake keeps the cluster in files: running and created containers, the NameNode exclude file,
the spare's volume. It answers what the scripts and ``scripts/cluster/client.sh`` send, so a
scenario runs in seconds without a cluster. Its poller writes one timeline row every 0.2 s from
that state: a DataNode is ``live`` when its container runs, ``dead`` otherwise, with
``/decommissioned`` while the exclude file names it; blocks are under-replicated while one of
datanode1 to datanode3 is stopped and the spare is not live.

Hooks, shell conditions on the state folder ``$S``: ``POLLER_DIES_WHEN`` ends the poller,
``REPORT_FAILS_WHEN`` makes ``hdfs dfsadmin -report`` fail. ``GOLD_SECONDS`` lists how long each
gold run takes, the last value for the runs after it (1 s).
"""

import os
import shutil
import stat
import subprocess
from dataclasses import dataclass
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]

DOCKER = r"""#!/usr/bin/env bash
set -u
S=$FAKE
running() { [[ -e $S/running/$1 ]]; }
cmd=$1
shift
echo "$cmd $*" >>"$S/calls"
case $cmd in
  network) exit 0 ;;
  inspect)
    if [[ $1 == -f ]]; then fmt=$2 name=$3; else fmt= name=$1; fi
    [[ -e $S/created/$name ]] || exit 1
    if [[ $fmt == *Running* ]]; then
      if running "$name"; then echo true; else echo false; fi
    elif running "$name"; then echo running
    else echo exited
    fi ;;
  stop) rm -f "$S/running/$1"; echo "stop $1" >>"$S/log"; sleep 0.5 ;;
  start) touch "$S/running/$1"; echo "start $1" >>"$S/log"; sleep 0.5 ;;
  exec)
    if [[ $1 == -i ]]; then
      cat >"$S/exclude"; echo "exclude [$(tr '\n' ' ' <"$S/exclude")]" >>"$S/log"
    else
      cat "$S/exclude"
    fi ;;
  compose)
    if [[ " $* " == *" up "* ]]; then
      touch "$S/running/gtdp-datanode4-1" "$S/created/gtdp-datanode4-1" "$S/volume"
    elif [[ " $* " == *" rm "* ]]; then
      rm -f "$S/running/gtdp-datanode4-1" "$S/created/gtdp-datanode4-1"
    fi
    echo "compose $*" >>"$S/log"; sleep 0.5 ;;
  volume) [[ $1 == rm && -e $S/volume ]] && rm "$S/volume"; echo "volume $*" >>"$S/log" ;;
  ps | logs) exit 0 ;;
  run)
    while [[ $1 != gtdp/hadoop-spark:* ]]; do shift; done
    shift
    exec bash "$S/cluster.sh" "$@" ;;
  *) echo "fake docker: unexpected $cmd" >&2; exit 1 ;;
esac
"""

CLUSTER = r"""set -u
S=$FAKE
running() { [[ -e $S/running/gtdp-$1-1 ]]; }
excluded() { grep -qx "$1" "$S/exclude" 2>/dev/null; }
state() {
  local h=$1 s b
  if [[ ! -e $S/created/gtdp-$h-1 ]]; then echo "absent,"; return; fi
  if running "$h"; then s=live b=100; else s=dead b=0; fi
  if excluded "$h"; then s=$s/decommissioned; fi
  echo "$s,$b"
}
row() {
  local live=0 dead=0 under=0 cells="" h st
  for h in datanode1 datanode2 datanode3 datanode4; do
    st=$(state "$h")
    cells+=",$st"
    case $st in live*) live=$((live + 1)) ;; dead*) dead=$((dead + 1)) ;; esac
  done
  for h in datanode1 datanode2 datanode3; do
    if ! running "$h" && [[ $(state datanode4) != live,* ]]; then under=5; fi
  done
  echo "$(date -u +%Y-%m-%dT%H:%M:%SZ),$live,$dead,0,$under,0,0,0,0,0,0,0,0$cells,"
}
case "$1 ${2:-}" in
  "python3 -c") echo /opt/hadoop/etc/hadoop/dfs.exclude ;;
  "python3 scripts/resilience/hdfs_watch.py")
    case $3 in
      poll)
        out=$5 stop=$9
        cols=time,live,dead,decommissioning,under_replicated,missing,corrupt,pending_deletion
        cols+=,ec_low_redundancy,ec_missing,ec_corrupt,pending_reconstruction,excess
        for h in datanode1 datanode2 datanode3 datanode4; do cols+=,$h,${h}_blocks; done
        echo "$cols,error" >"$out"
        while [[ ! -e $stop ]]; do
          if [[ -n ${POLLER_DIES_WHEN:-} ]] && eval "$POLLER_DIES_WHEN"; then exit 1; fi
          row >>"$out"
          sleep 0.2
        done ;;
      wait-state) echo "t watching"; sleep 0.5; echo "t is PUBLISHING" ;;
      state) state "$4" | cut -d, -f1 ;;
      *) echo summary ;;
    esac ;;
  "bash -c") echo "t watching"; sleep 0.5; echo "t first file" ;;
  "yarn application") echo "Total number of applications: 0" ;;
  "hdfs dfs")
    case $* in
      *-test*) exit 1 ;;
      *_state.json*) echo '{"status": "PUBLISHED"}' ;;
      *) echo ok ;;
    esac ;;
  "hdfs dfsadmin")
    if [[ $3 == -report && -n ${REPORT_FAILS_WHEN:-} ]] && eval "$REPORT_FAILS_WHEN"; then
      echo "report: Call From client to namenode:8020 failed" >&2
      exit 1
    fi
    echo ok ;;
  "bash scripts/cluster/submit.sh")
    if [[ $3 == gold ]]; then
      read -ra seconds <<<"${GOLD_SECONDS:-1}"
      run=$(cat "$S/gold-runs" 2>/dev/null || echo 0)
      echo $((run + 1)) >"$S/gold-runs"
      echo "Submitted application_1_0001"; sleep "${seconds[run]:-${seconds[-1]}}"
    else
      echo '{}' >"$5"
    fi ;;
  *) echo ok ;;
esac
"""

SCRIPTS = (
    "scripts/cluster/lib.sh",
    "scripts/cluster/client.sh",
    "scripts/cluster/submit.sh",
    "scripts/resilience/lib.sh",
    "scripts/resilience/datanode-failure.sh",
    "scripts/resilience/gold-write-under-failure.sh",
)


@dataclass(frozen=True)
class FakeCluster:
    root: Path
    state: Path
    bin: Path

    def environ(self, **env: str) -> dict[str, str]:
        return {k: v for k, v in os.environ.items() if not k.startswith("GTDP_")} | {
            "PATH": f"{self.bin}{os.pathsep}{os.environ['PATH']}",
            "FAKE": str(self.state),
            **env,
        }

    def command(self, script: str, out: str) -> list[str]:
        return ["bash", f"scripts/resilience/{script}", "--out", f"output/{out}"]

    def run(self, script: str, out: str, **env: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            self.command(script, out),
            cwd=self.root,
            env=self.environ(**env),
            capture_output=True,
            text=True,
            timeout=300,
            check=False,
        )

    def running(self, node: str) -> bool:
        return (self.state / "running" / f"gtdp-{node}-1").exists()

    def created(self, node: str) -> bool:
        return (self.state / "created" / f"gtdp-{node}-1").exists()

    @property
    def exclude(self) -> str:
        return (self.state / "exclude").read_text().strip()

    @property
    def spare_volume(self) -> bool:
        return (self.state / "volume").exists()

    def events(self, out: str) -> list[str]:
        lines = (self.root / "output" / out / "events.csv").read_text().splitlines()
        return [line.split(",", 1)[1] for line in lines]


@pytest.fixture
def cluster(tmp_path: Path) -> FakeCluster:
    """A copy of the scripts, a fake docker, and 3 running DataNodes."""
    root = tmp_path / "repo"
    for rel in SCRIPTS:
        dest = root / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(REPO / rel, dest)
        dest.chmod(dest.stat().st_mode | stat.S_IXUSR)
    state = tmp_path / "state"
    for sub in ("running", "created"):
        (state / sub).mkdir(parents=True)
    for name in ("namenode", "datanode1", "datanode2", "datanode3"):
        (state / "running" / f"gtdp-{name}-1").touch()
        (state / "created" / f"gtdp-{name}-1").touch()
    (state / "exclude").write_text("")
    (state / "cluster.sh").write_text(CLUSTER)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    docker = bin_dir / "docker"
    docker.write_text(DOCKER)
    docker.chmod(0o755)
    return FakeCluster(root, state, bin_dir)
