"""spark-submit entrypoint.

    run.py <job> --env {local,cluster} [--date YYYY-MM-DD] [--source NAME] [--force]
               [--set SECTION.KEY=VALUE ...]
    run.py list

Works as ``python -m gtdp.jobs.run`` and as a file given to spark-submit with
``--py-files dist/gtdp.zip``. Exit codes: 0 success, 1 job failure, 2 usage or configuration error.
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Sequence
from dataclasses import asdict
from datetime import date

from gtdp.audit import audited_run
from gtdp.config import ConfigError, Env, load_config
from gtdp.jobs.catalog import JOBS
from gtdp.jobs.registry import JobContext
from gtdp.log import bind, configure_logging
from gtdp.spark import build_spark

# Not __name__: it is "__main__" when spark-submit runs this file.
logger = logging.getLogger("gtdp.jobs.run")


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "list":
        for spec in JOBS.values():
            print(f"{spec.name:<24} {spec.layer.value:<10} {spec.description}")
        return 0

    configure_logging()
    spec = JOBS[args.command]
    with bind(job=spec.name):
        try:
            cfg = load_config(args.env, overrides=dict(args.overrides))
        except ConfigError as exc:
            logger.error("invalid configuration: %s", exc)
            return 2
        logger.info("configuration loaded", extra={"config": asdict(cfg)})
        spark = build_spark(cfg, f"{cfg.app.name}-{spec.name}")
        try:
            with audited_run(spark, cfg, spec.name, spec.layer, args.source, args.date) as run:
                if args.force:
                    run.metrics["force"] = True
                spec.func(JobContext(spark, cfg, run, force=args.force))
        except Exception:
            logger.exception("job failed")
            return 1
        finally:
            spark.stop()
    return 0


def _parser() -> argparse.ArgumentParser:
    options = argparse.ArgumentParser(add_help=False)
    options.add_argument("--env", required=True, choices=[env.value for env in Env])
    options.add_argument("--date", type=_iso_date, help="ingestion date, YYYY-MM-DD")
    options.add_argument("--source", help="source name, for example orders")
    options.add_argument(
        "--force",
        action="store_true",
        help="process --date even when the target already holds a later day",
    )
    options.add_argument(
        "--set",
        dest="overrides",
        action="append",
        default=[],
        type=_override,
        metavar="SECTION.KEY=VALUE",
        help="override one configuration key; repeatable",
    )
    parser = argparse.ArgumentParser(prog="run.py", description="Run a GTDP Spark job.")
    commands = parser.add_subparsers(dest="command", required=True, metavar="JOB")
    commands.add_parser("list", help="print the registered jobs")
    for spec in JOBS.values():
        commands.add_parser(
            spec.name, parents=[options], help=spec.description, description=spec.description
        )
    return parser


def _iso_date(text: str) -> date:
    try:
        return date.fromisoformat(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"expected YYYY-MM-DD, got {text!r}") from None


def _override(text: str) -> tuple[str, str]:
    key, sep, value = text.partition("=")
    if not sep or "." not in key:
        raise argparse.ArgumentTypeError(f"expected SECTION.KEY=VALUE, got {text!r}")
    return key, value


if __name__ == "__main__":
    sys.exit(main())
