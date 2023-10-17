from pathlib import Path

import pytest

from cboe_test.parser import parse, top_volume


@pytest.fixture
def testfile(request):
    return open(request.config.rootdir / "tests/data/pitch_example_data.txt")


def test_parse_smoke(testfile):
    list(parse(testfile))


def test_top_volume_smoke(testfile):
    assert top_volume(parse(testfile)) == [
        (5000, "OIH"),
        (2000, "SPY"),
        (1209, "DRYS"),
        (577, "ZVZZT"),
        (495, "AAPL"),
        (400, "UYG"),
        (400, "PTR"),
        (320, "FXP"),
        (229, "DIA"),
        (210, "BAC"),
    ]
