import logging

from gtdp.log import bind, configure_logging


def test_records_are_json_lines_with_run_context_and_extra_fields(stderr_lines):
    configure_logging()
    with bind(run_id="r-1", job="smoke"):
        logging.getLogger("gtdp.test").info("rows written: %d", 3, extra={"table": "orders"})

    [entry] = stderr_lines()
    assert entry["level"] == "INFO"
    assert entry["logger"] == "gtdp.test"
    assert entry["message"] == "rows written: 3"
    assert entry["run_id"] == "r-1"
    assert entry["job"] == "smoke"
    assert entry["table"] == "orders"
    assert entry["ts"].endswith("+00:00")


def test_context_ends_with_the_block(stderr_lines):
    configure_logging()
    with bind(run_id="r-1"):
        pass
    logging.getLogger("gtdp.test").warning("after")

    [entry] = stderr_lines()
    assert entry["run_id"] is None
    assert entry["job"] is None


def test_configuring_again_keeps_one_line_per_record(stderr_lines):
    configure_logging()
    configure_logging()
    logging.getLogger("gtdp.test").info("once")

    assert [entry["message"] for entry in stderr_lines()] == ["once"]


def test_exception_traceback_is_a_field(stderr_lines):
    configure_logging()
    try:
        raise RuntimeError("boom")
    except RuntimeError:
        logging.getLogger("gtdp.test").exception("job failed")

    [entry] = stderr_lines()
    assert entry["level"] == "ERROR"
    assert "RuntimeError: boom" in entry["exc"]


def test_py4j_chatter_is_hidden(stderr_lines):
    configure_logging()
    logging.getLogger("py4j.clientserver").info("Received command c on object id p0")

    assert stderr_lines() == []
