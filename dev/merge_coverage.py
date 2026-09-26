#!/usr/bin/env python3
"""Merge the Go coverage profiles of several runs into one and say what each added."""

import argparse
from decimal import Decimal
from pathlib import Path
import sys


def read(path):
    """A profile as {block: (statements, count)}. Identical blocks are counted
    once at the highest count seen, which is what running the same code twice
    means."""
    blocks = {}
    for line in Path(path).read_text().splitlines():
        if not line or line.startswith('mode:'):
            continue
        where, statements, count = line.rsplit(' ', 2)
        held = blocks.get(where, (0, 0))
        blocks[where] = (int(statements), max(int(count), held[1]))
    return blocks


def files(blocks):
    """The blocks of each file, so that profiles can be compared where they
    describe the same file and carried over where only one of them does."""
    out = {}
    for where in blocks:
        out.setdefault(where.split(':')[0], set()).add(where)
    return out


def share(blocks):
    total = sum(statements for statements, _ in blocks.values())
    covered = sum(statements for statements, count in blocks.values() if count)
    return covered, total, round(100 * covered / total, 1) if total else 0.0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('profiles', nargs='+')
    parser.add_argument('--output', required=True)
    parser.add_argument('--minimum', type=Decimal, default=Decimal(0))
    args = parser.parse_args()
    merged = {}
    for path in args.profiles:
        blocks = read(path)

        if not blocks:
            sys.exit(f'{path} carries no measured block')

        # Every profile has a line for every block of its binary, run or not,
        # so where two profiles describe the same file they describe the same
        # blocks. Blocks that differ mean the sources differ, and adding those
        # up would invent a number that describes neither. A file only one
        # binary contains, such as the scaffolding that refuses system calls,
        # is carried over as it stands: it is code, and it is measured.
        for name, held in files(merged).items():
            found = files(blocks).get(name)

            if found is not None and found != held:
                sys.exit(f'{path} was measured on other sources: {name} has other blocks')

        covered, total, percent = share(blocks)
        print(f'{Path(path).name}: {percent}% ({covered}/{total} statements)')

        for where, (statements, count) in blocks.items():
            held = merged.get(where, (0, 0))
            merged[where] = (statements, max(count, held[1]))
    covered, total, percent = share(merged)
    Path(args.output).write_text(
        'mode: atomic\n' + ''.join(f'{where} {statements} {count}\n'
                                   for where, (statements, count) in sorted(merged.items())))
    print(f'together: {percent}% ({covered}/{total} statements)')

    if not total or 100 * covered < args.minimum * total:
        sys.exit(f'coverage {covered}/{total} statements is below {args.minimum}%')


if __name__ == '__main__':
    main()
