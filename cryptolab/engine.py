"""Integrate account stream rates across event boundaries and detect intermediate balance insolvency."""

from .common import (
    number,
    integer,
    identity,
    records,
    chronology,
    allocate,
    result,
    validate_document,
)
from decimal import Decimal


def _analyze(d):
    balances = {identity(k): integer(v) for k, v in d["balances"].items()}
    initial = sum(balances.values())
    streams = records(d["streams"], "id")
    times = {integer(d["start"]), integer(d["end"])}
    start = d["start"]
    end = d["end"]
    if end < start:
        raise ValueError("invalid evaluation interval")
    for s in streams:
        if (
            s["sender"] not in balances
            or s["receiver"] not in balances
            or s["sender"] == s["receiver"]
        ):
            raise ValueError("invalid stream accounts")
        left = integer(s["start"])
        right = integer(s["end"])
        integer(s["rate"], 1)
        if right <= left:
            raise ValueError("invalid stream interval")
        if start < left < end:
            times.add(left)
        if start < right < end:
            times.add(right)
    trace = []
    first = None
    ordered = sorted(times)
    for left, right in zip(ordered, ordered[1:]):
        rates = {k: 0 for k in balances}
        for s in streams:
            if s["start"] <= left < s["end"]:
                rates[s["sender"]] -= s["rate"]
                rates[s["receiver"]] += s["rate"]
        for account, rate in rates.items():
            if rate < 0 and balances[account] + rate * (right - left) < 0:
                crossing = left + max(balances[account], 0) // (-rate) + 1
                first = crossing if first is None else min(first, crossing)
            balances[account] += rate * (right - left)
        trace.append(
            {
                "start": left,
                "end": right,
                "net_rates": rates,
                "balances": dict(balances),
            }
        )
    return {
        "balances": balances,
        "first_insolvent_second": first,
        "conserved": sum(balances.values()) == initial,
        "trace": trace,
    }


def analyze(document):
    validate_document(document)
    try:
        return result(_analyze(document))
    except (KeyError, TypeError, ArithmeticError, RecursionError) as error:
        raise ValueError("invalid domain input: " + str(error)) from error
