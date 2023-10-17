from dataclasses import dataclass
from decimal import Decimal
from collections import Counter
import logging
from typing import Self, Iterable
import struct
import warnings


def soup(line):
    if line.startswith("S"):
        return line[1:].strip()
    else:
        # R -- client heartbeat
        # H -- server heartbeat
        # + -- debug packet
        # A -- login accepted
        # J -- login rejected
        # L -- login request
        # U -- unsequenced data
        # O -- logout request
        logging.warning("Ignoring SOUP frame %r", line)


# execution id first char:
# 0: internal match
# C: auction fill
# M: market close trade
# R: routed trade


@dataclass
class Order:
    ts: str
    id: str  # b36-encoded
    side: str  # "B" buy or "S" sell
    count: int
    symbol: str
    price: Decimal

    @classmethod
    def from_short(cls, raw: str) -> Self:
        bits = [
            b.decode("ascii")
            for b in struct.unpack("8s1s12s1s6s6s10s1s", raw.encode("ascii"))
        ]
        bits[4] = int(bits[4])
        bits[5] = bits[5].strip()
        bits[6] = Decimal(f"{bits[6][:6]}.{bits[6][6:]}")
        assert bits[7] == "Y"
        assert bits[1] == "A"
        del bits[7]
        del bits[1]
        return cls(*bits)

    @classmethod
    def from_long(cls, raw: str) -> Self:
        raise NotImplementedError("FIXME")


@dataclass
class OrderCancel:
    ts: str
    id: str
    count: int

    @classmethod
    def from_raw(cls, raw: str) -> Self:
        bits = [
            b.decode("ascii") for b in struct.unpack("8s1s12s6s", raw.encode("ascii"))
        ]
        bits[3] = int(bits[3])
        assert bits[1] == "X"
        del bits[1]
        return cls(*bits)


@dataclass
class Trade:
    ts: str
    id: str  # b36-encoded
    count: int
    symbol: str
    price: Decimal
    eid: str  # b36-encoded

    @classmethod
    def from_raw(cls, raw: str) -> Self:
        bits = [
            b.decode("ascii")
            for b in struct.unpack("8s1s12s1s6s6s10s12s", raw.encode("ascii"))
        ]
        bits[4] = int(bits[4])
        bits[5] = bits[5].strip()
        bits[6] = Decimal(f"{bits[6][:6]}.{bits[6][6:]}")
        if bits[3] != "B":
            # Huh, the spec states that this field is always "B"...
            # Yet, there are records with an "S" in the test data.
            warnings.warn(f"Unexpected Trade side indicator {bits[3]!r}")
        del bits[3]
        assert bits[1] == "P"
        del bits[1]
        return cls(*bits)


@dataclass
class OrderExecuted:
    ts: str
    id: str  # b36-encoded
    count: int
    eid: str  # b36-encoded

    @classmethod
    def from_raw(cls, raw: str) -> Self:
        bits = [
            b.decode("ascii")
            for b in struct.unpack("8s1s12s6s12s", raw.encode("ascii"))
        ]
        bits[3] = int(bits[3])
        assert bits[1] == "E"
        del bits[1]
        return cls(*bits)


def parse(file: Iterable[str]):
    for line in file:
        line = soup(line)
        if not line:
            continue
        match line[8:9]:
            case "A":
                yield Order.from_short(line)
            case "d":
                yield Order.from_long(line)
            case "X":
                yield OrderCancel.from_raw(line)
            case "P":
                yield Trade.from_raw(line)
            case "E":
                yield OrderExecuted.from_raw(line)
            case _:
                logging.warning("Unknown data %r", line)


def top_volume(events: Iterable, n=10):
    totals: Counter[str] = Counter()
    book: dict[str, Order] = dict()
    for e in events:
        match e:
            case Trade():
                totals[e.symbol] += e.count
            case OrderExecuted():
                order = book.get(e.id)
                if not order:
                    warnings.warn(f"Missing order {e.id!r}")
                    continue
                totals[order.symbol] += e.count
                order.count -= e.count
                if not order.count:
                    del book[e.id]
                elif order.count < 0:
                    warnings.warn("Negative shares")
                    del book[e.id]
            case OrderCancel():
                order = book.get(e.id)
                if not order:
                    warnings.warn(f"Missing order {e.id!r}")
                    continue
                order.count -= e.count
                if not order.count:
                    del book[e.id]
                elif order.count < 0:
                    warnings.warn("Negative shares")
                    del book[e.id]
            case Order():
                if e.id in book:
                    warnings.warn(f"Duplicate order {e.id!r}")
                    continue
                book[e.id] = e

    return sorted(((v, k) for k, v in totals.items()), reverse=True)[:n]
