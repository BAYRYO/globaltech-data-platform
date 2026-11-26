import pytest
from stream_helpers import lake


@pytest.fixture
def stream_lake(tmp_path):
    return lake(tmp_path / "lake")
