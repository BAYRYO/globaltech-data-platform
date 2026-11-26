"""Hadoop Streaming reducer and combiner of Lot 6: sums `country<TAB>amount<TAB>lines` records.

Usage: python3 reducer.py [--combine] < records sorted by country

As a combiner (--combine) it writes the sums in the mapper's format, amounts in 1e-5 EUR, so
Hadoop may run it any number of times on any subset of a country's records. As the reducer it
writes `country<TAB>revenue_eur<TAB>completed_lines`, the revenue rounded half up to the cent.
"""

from __future__ import annotations

import sys
from collections.abc import Iterable, Iterator
from itertools import groupby

UNITS_PER_CENT = 1000


def euros(units: int) -> str:
    """A non-negative amount of 1e-5 EUR as euros, rounded half up to the cent."""
    cents = (units + UNITS_PER_CENT // 2) // UNITS_PER_CENT
    return f"{cents // 100}.{cents % 100:02d}"


def reduce_records(records: Iterable[str], combine: bool) -> Iterator[str]:
    """One output line per country of ``records``, which arrive grouped by country."""
    rows = (record.rstrip("\n").split("\t") for record in records)
    for country, group in groupby(rows, key=lambda row: row[0]):
        total = lines = 0
        for _, amount, count in group:
            total += int(amount)
            lines += int(count)
        yield f"{country}\t{total if combine else euros(total)}\t{lines}\n"


def main(args: list[str]) -> int:
    if args not in ([], ["--combine"]):
        sys.exit(__doc__)
    sys.stdout.writelines(reduce_records(sys.stdin, combine=bool(args)))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
