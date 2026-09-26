#!/usr/bin/env python3
"""Every point the chaos build can refuse has to be asked about somewhere.

A point that is declared and armed but never consulted refuses nothing, and
nothing says so: the run comes back green and quieter than it claims to be.
"""

from pathlib import Path
import re
import sys


def main():
    root = Path(__file__).resolve().parent.parent
    declared = set(re.findall(r'^\t"([^"]+)":', (root / 'syscalls_chaos.go').read_text(), re.M))

    if not declared:
        sys.exit('no chaos points are declared')

    asked = set()

    for path in sorted(root.glob('*.go')):
        source = path.read_text()
        asked |= set(re.findall(r'failing\("([^"]+)"\)', source))

        for read, write in re.findall(r'sys\.connection\([^,]+, "([^"]+)", "([^"]+)"\)', source):
            asked |= {read, write}

    silent = sorted(declared - asked)

    if silent:
        sys.exit('nothing ever asks about: ' + ', '.join(silent))

    print(f'{len(declared)} chaos points, every one asked about')


if __name__ == '__main__':
    main()
