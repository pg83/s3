#!/usr/bin/env python3
"""Validate the counters of every process, merge the coverage of one e2e run, and enforce the floor."""

import argparse
from pathlib import Path
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', required=True)
    # One suite is one part of what the binary does, and the paths a refused
    # binary walks are not in this one at all. What the whole of it has to
    # clear is checked where the runs are added up, in dev/merge_coverage.py.
    parser.add_argument('--minimum', type=float, default=0)
    parser.add_argument('dirs', nargs='+')
    args = parser.parse_args()
    directories = []
    for name in args.dirs:
        root = Path(name)
        # Every process the lab started has a directory of its own, and a
        # process that was killed rather than stopped left it empty; the short
        # lived commands a scenario runs itself leave their counters at the root.
        processes = sorted(p for p in root.iterdir() if p.is_dir()) if root.is_dir() else []
        for process in processes:
            if not list(process.glob('covcounters.*')):
                sys.exit(f'no counters in {process}: the process did not exit through os.Exit')
        covered = [p for p in [root, *processes] if list(p.glob('covcounters.*'))]
        if not covered:
            sys.exit(f'no coverage counters in {root}: the scenario did not run')
        directories.extend(str(p) for p in covered)
        print(f'{root.name}: {len(processes)} processes')
    subprocess.run(['go', 'tool', 'covdata', 'textfmt', '-i=' + ','.join(directories), '-o', args.output], check=True)
    summary = subprocess.run(['go', 'tool', 'cover', f'-func={args.output}'], check=True, capture_output=True, text=True).stdout
    print(summary)
    total = float(summary.strip().splitlines()[-1].split()[-1].rstrip('%'))
    if total < args.minimum:
        sys.exit(f'coverage {total}% is below {args.minimum}%')


if __name__ == '__main__':
    main()
