import json
import logging

import pytest


@pytest.fixture
def stderr_lines(capsys):
    """Configure logging in the test body: capsys swaps sys.stderr between setup and call."""
    root = logging.getLogger()
    handlers, level = list(root.handlers), root.level

    def read():
        return [json.loads(line) for line in capsys.readouterr().err.splitlines()]

    yield read
    root.handlers[:] = handlers
    root.setLevel(level)
