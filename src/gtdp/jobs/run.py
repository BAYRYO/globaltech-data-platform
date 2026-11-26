"""spark-submit entrypoint.

    run.py <job> --env {local,cluster} [--date YYYY-MM-DD] [--source NAME] [--force]
               [--set SECTION.KEY=VALUE ...]
    run.py list

Works as ``python -m gtdp.jobs.run`` and as a file given to spark-submit with
``--py-files dist/gtdp.zip``. Apart from ``list`` and the usage messages, it writes only JSON log
lines. SIGTERM and SIGINT stop the job and give the run a ``STOPPED`` audit row; a second signal
kills the process at once. A signal that arrives while ``gtdp.io`` swaps a table takes effect when
the swap ends. ``EXIT_CODES`` lists the exit codes.
"""

from __future__ import annotations

import argparse
import logging
import signal
import sys
from collections.abc import Sequence
from dataclasses import asdict
from datetime import date
from types import FrameType

from pyspark import SparkContext

from gtdp.audit import RunStopped, audited_run
from gtdp.config import ConfigError, Env, load_config
from gtdp.jobs.catalog import JOBS
from gtdp.jobs.registry import JobContext, JobSpec
from gtdp.log import bind, configure_logging
from gtdp.spark import build_spark

# Not __name__: it is "__main__" when spark-submit runs this file.
logger = logging.getLogger("gtdp.jobs.run")

EXIT_CODES = """exit codes:
  0    the job succeeded
  1    the job failed
  2    usage error, invalid configuration, or Spark could not start
  130  stopped by SIGINT (Ctrl+C)
  143  stopped by SIGTERM
"""
STOP_SIGNALS = (signal.SIGTERM, signal.SIGINT)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "list":
        for spec in JOBS.values():
            print(f"{spec.name:<24} {spec.layer.value:<10} {spec.description}")
        return 0

    configure_logging()
    spec = JOBS[args.command]
    previous = {sig: signal.getsignal(sig) for sig in STOP_SIGNALS}
    _stop_on_signals()
    with bind(job=spec.name):
        try:
            return _run(spec, args)
        except RunStopped as stop:
            logger.warning("run stopped", extra={"signal": stop.signal.name})
            return 128 + stop.signal
        finally:
            for sig, handler in previous.items():
                signal.signal(sig, handler)


def _run(spec: JobSpec, args: argparse.Namespace) -> int:
    try:
        cfg = load_config(args.env, overrides=dict(args.overrides))
    except ConfigError as exc:
        logger.error("invalid configuration: %s", exc)
        return 2
    logger.info("configuration loaded", extra={"config": asdict(cfg)})
    try:
        spark = build_spark(cfg, f"{cfg.app.name}-{spec.name}")
    except Exception:
        logger.exception("could not start Spark")
        return 2
    _stop_on_signals()  # Starting Spark installs PySpark's own SIGINT handler.
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


def _stop_on_signals() -> None:
    for sig in STOP_SIGNALS:
        signal.signal(sig, _raise_stopped)


def _raise_stopped(signum: int, frame: FrameType | None) -> None:
    for sig in STOP_SIGNALS:
        signal.signal(sig, signal.SIG_DFL)
    _drop_py4j_connection()
    raise RunStopped(signal.Signals(signum))


def _drop_py4j_connection() -> None:
    """Close this thread's py4j connection: the signal may have cut a call short.

    PySpark cleans up after an interrupted call with more calls on the same connection. They would
    wait for the interrupted call to end, then read its answer as their own. Once the connection
    is closed, the next call opens a new one.
    """
    client = getattr(SparkContext._gateway, "_gateway_client", None)
    connection = getattr(client, "get_thread_connection", lambda: None)()
    if connection is not None:
        connection.close()


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
    parser = argparse.ArgumentParser(
        prog="run.py",
        description="Run a GTDP Spark job.",
        epilog=EXIT_CODES,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    commands = parser.add_subparsers(dest="command", required=True, metavar="JOB")
    commands.add_parser("list", help="print the registered jobs")
    for spec in JOBS.values():
        commands.add_parser(
            spec.name,
            parents=[options],
            help=spec.description,
            description=spec.description,
            epilog=EXIT_CODES,
            formatter_class=argparse.RawDescriptionHelpFormatter,
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
