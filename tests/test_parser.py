from pathlib import Path

import pytest

from cboe_test.parser import parse, top_volume, Trade, Order, OrderCancel, OrderExecuted


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


def test_trade():
    assert top_volume([Trade(0, 0, 42, "A", 0, 0)]) == [(42, "A")]
    assert top_volume(
        [
            Trade(0, 0, 42, "A", 0, 0),
            Trade(0, 0, 1, "A", 0, 0),
        ]
    ) == [(43, "A")]


def test_order_cancel():
    store = {}
    assert not top_volume(
        [Order(0, 42, 0, 100, 0, 0), OrderCancel(0, 42, 100)], store=store
    )
    assert not store

    store = {}
    assert not top_volume(
        [Order(0, 42, 0, 100, 0, 0), OrderCancel(0, 42, 50)], store=store
    )
    assert store == {42: Order(0, 42, 0, 50, 0, 0)}


def test_order_execute():
    store = {}
    assert top_volume(
        [Order(0, 42, 0, 100, 0, 0), OrderExecuted(0, 42, 100, 0)], store=store
    ) == [(100, 0)]
    assert not store

    store = {}
    assert top_volume(
        [Order(0, 42, 0, 100, 0, 0), OrderExecuted(0, 42, 50, 0)], store=store
    ) == [(50, 0)]
    assert store == {42: Order(0, 42, 0, 50, 0, 0)}
