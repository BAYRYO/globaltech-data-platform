import hashlib
import importlib.util
import sys
import threading
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
ARCHIVE = "spark/spark-3.5.3/spark-3.5.3-bin-without-hadoop.tgz"
CONTENT = b"spark distribution bytes\n" * 1000


@pytest.fixture(scope="module")
def fetch():
    spec = importlib.util.spec_from_file_location("fetch", REPO / "docker" / "fetch.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def mirrors(tmp_path):
    """Three mirrors on one local server: ``gone`` lacks the archive, ``corrupt`` serves other
    bytes under its name, ``good`` serves it."""
    root = tmp_path / "www"
    for name, body in (("corrupt", b"truncated"), ("good", CONTENT)):
        archive = root / name / ARCHIVE
        archive.parent.mkdir(parents=True)
        archive.write_bytes(body)
    (root / "gone").mkdir()
    handler = partial(SimpleHTTPRequestHandler, directory=str(root))
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    yield {name: f"{base}/{name}" for name in ("gone", "corrupt", "good")}
    server.shutdown()


def sha512(data: bytes) -> str:
    return hashlib.sha512(data).hexdigest()


def test_the_archive_comes_from_the_first_mirror_that_serves_its_checksum(
    fetch, mirrors, tmp_path, capsys
):
    dest = tmp_path / "spark.tgz"
    order = (mirrors["gone"], mirrors["corrupt"], mirrors["good"])

    fetch.fetch(ARCHIVE, sha512(CONTENT), str(dest), mirrors=order, pause=0)

    assert dest.read_bytes() == CONTENT
    assert f"verified {mirrors['good']}/{ARCHIVE}" in capsys.readouterr().out


def test_no_mirror_with_the_checksum_stops_the_build(fetch, mirrors, tmp_path):
    order = (mirrors["gone"], mirrors["corrupt"])

    with pytest.raises(SystemExit, match="no mirror served"):
        fetch.fetch(ARCHIVE, sha512(CONTENT), str(tmp_path / "spark.tgz"), mirrors=order, pause=0)
