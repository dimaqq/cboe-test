from pathlib import Path

import pytest

from cboe_test.parser import parse


@pytest.fixture
def testfile(request):
    return open(request.config.rootdir / "tests/data/pitch_example_data.txt")


def test_parse_smoke(testfile):
    list(parse(testfile))
