"""Command line: generate, verify, replay-sensors."""

import argparse
import sys
import time
from collections import Counter
from pathlib import Path

from gtdp.datagen.generate import generate
from gtdp.datagen.model import PROFILES
from gtdp.datagen.replay import replay
from gtdp.datagen.verify import Report, verify


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m gtdp.datagen", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    gen = commands.add_parser("generate", help="write the J1/J2/J3 drops and the sensor day")
    gen.add_argument("--profile", choices=sorted(PROFILES), default="dev")
    gen.add_argument("--seed", type=int, default=42)
    gen.add_argument("--out", type=Path, default=Path("data/generated"))
    gen.add_argument("--kit", type=Path, default=Path("data/kit"), help="kit directory")

    check = commands.add_parser("verify", help="recount every anomaly and compare to manifests")
    check.add_argument("--path", type=Path, required=True, help="data/generated/<profile>")

    rep = commands.add_parser("replay-sensors", help="publish one sensor file per interval")
    rep.add_argument("--src", type=Path, required=True)
    rep.add_argument("--dst", required=True, help="local directory or hdfs:///path")
    rep.add_argument("--interval", type=float, default=5.0)
    rep.add_argument("--limit", type=int, help="replay only the first N files")
    return parser


def _print_report(report: Report) -> None:
    sensor_files, sensor_rows, sensor_codes = 0, 0, Counter()
    for label, counted in report.checked:
        if label.startswith("sensors/"):
            sensor_files += 1
            sensor_rows += counted.rows
            sensor_codes += counted.anomalies
        else:
            print(f"{label:44} {counted.rows:>10}  {dict(counted.anomalies)}")
    if sensor_files:
        print(f"{f'sensors ({sensor_files} files)':44} {sensor_rows:>10}  {dict(sensor_codes)}")
    for problem in report.problems:
        print(f"PROBLEM {problem}", file=sys.stderr)


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    started = time.monotonic()
    status = 0
    if args.command == "generate":
        files = generate(PROFILES[args.profile], args.seed, args.out, args.kit)
        summary = f"{len(files)} files, {sum(f.rows for f in files)} rows"
    elif args.command == "verify":
        report = verify(args.path)
        _print_report(report)
        status = 1 if report.problems else 0
        summary = f"{len(report.problems)} problems" if status else "OK"
    else:
        published = replay(args.src, args.dst, args.interval, args.limit)
        summary = f"published {len(published)} files to {args.dst}"
    print(f"{summary} ({time.monotonic() - started:.1f} s)")
    return status


if __name__ == "__main__":
    sys.exit(main())
