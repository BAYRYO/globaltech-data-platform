import gtdp


def test_package_exposes_semantic_version():
    major, minor, patch = gtdp.__version__.split(".")
    assert all(part.isdigit() for part in (major, minor, patch))
