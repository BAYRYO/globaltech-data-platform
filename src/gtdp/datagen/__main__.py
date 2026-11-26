"""Command line: generate."""

import argparse
import sys
import time
from pathlib import Path

from gtdp.datagen.generate import generate
from gtdp.datagen.model import PROFILES


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m gtdp.datagen", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    gen = commands.add_parser("generate", help="write the J1/J2/J3 drops and the sensor day")
    gen.add_argument("--profile", choices=sorted(PROFILES), default="dev")
    gen.add_argument("--seed", type=int, default=42)
    gen.add_argument("--out", type=Path, default=Path("data/generated"))
    gen.add_argument("--kit", type=Path, default=Path("data/kit"), help="kit directory")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    started = time.monotonic()
    files = generate(PROFILES[args.profile], args.seed, args.out, args.kit)
    summary = f"{len(files)} files, {sum(f.rows for f in files)} rows"
    print(f"{summary} ({time.monotonic() - started:.1f} s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
