"""Test lab: the s3 binary under test, run as local processes."""

import os
import subprocess
import sys

BINARY = os.environ.get("S3_TEST_BINARY") or os.path.join(os.path.dirname(__file__), "..", "s3")


def run(*args, check=False):
    return subprocess.run([BINARY, *args], capture_output=True, text=True, check=check)


def fail(msg):
    print(msg, file=sys.stderr)
    sys.exit(1)
