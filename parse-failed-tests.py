#!/usr/bin/env python3
"""Parse failed-test info from ctest -V or run-parallel-tests.py output.

Two input formats are auto-detected:

* ctest -V: per-test output is prefixed with the test ID (e.g. "14: ..."),
  with failures marked by a "14: TEST FAILURE" block followed by
  "14: Test command: ...", "14: Log path: ..." lines.

* run-parallel-tests.py: each failing case emits a single line like
  "[12/345] FAIL tests-pgwrapper/foo-test::Suite.Case (12.3s) -> /<build_root>/yb-test-logs/parallel-tests/foo-test/Suite.Case.log"

Output is the same in both cases — one block per "logical test" suitable
for piping into rerun-failed-tests.py:

    #<id>\\t<name>
    \\t<exe_rel> --gtest_filter=<gfilter>
    \\t\\t<log_rel>

Usage: parse-failed-tests.py [path/to/output]  (defaults to ./ctest.out)
"""

from __future__ import annotations

import argparse
import re
import sys
from collections import defaultdict
from pathlib import Path

# ctest -V format
# "97/593 Test #14: client_ql-tablet-test ...***Failed  455.66 sec"
SUMMARY_RE = re.compile(r"^\s*\d+/\d+\s+Test\s+#(\d+):\s+(\S+).*\*\*\*Failed")
# "14: TEST FAILURE"
FAILURE_RE = re.compile(r"^(\d+):\s*TEST FAILURE\s*$")
# "14: Test command: <exe> ... --gtest_filter=QLTabletTest.TruncateTableDuringLongRead"
TEST_CMD_RE = re.compile(r"^(\d+):\s*Test command:\s+(\S+)(?:.*--gtest_filter=(\S+))?")
# "14: Log path: /..."
LOG_PATH_RE = re.compile(r"^(\d+):\s*Log path:\s*(\S.*)$")

# run-parallel-tests.py format
# "[12/345] FAIL tests-pgwrapper/foo-test::Suite.Case (12.3s) -> /<build_root>/yb-test-logs/parallel-tests/foo-test/Suite.Case.log"
PARALLEL_FAIL_RE = re.compile(
    r"^\[\d+/\d+\]\s+FAIL\s+(\S+)::(\S+)\s+\(\S+\)\s+->\s+(\S.*\S)\s*$"
)
# log path is "<build_root>/yb-test-logs/parallel-tests/<binary>/<case>.log";
# everything left of the marker is the build_root.
PARALLEL_LOG_MARKER = "/yb-test-logs/parallel-tests/"


def trim_to_build(path: str) -> str:
    """Strip everything before the leading 'build/' segment in path."""
    idx = path.find("/build/")
    return path[idx + 1:] if idx >= 0 else path


def parse_parallel(lines: list[str]) -> dict[int, dict]:
    """Parse run-parallel-tests.py FAIL lines. Failures from the same binary
    are grouped under one entry, matching ctest's "one ctest test = one
    binary" structure."""
    by_binary: dict[str, list[tuple[str, str]]] = {}
    order: list[str] = []
    for line in lines:
        m = PARALLEL_FAIL_RE.match(line)
        if not m:
            continue
        binary_rel = m.group(1)   # "tests-<subdir>/<binary>"
        case = m.group(2)         # "Suite.Case"
        log_abs = m.group(3)      # absolute log path

        idx = log_abs.find(PARALLEL_LOG_MARKER)
        if idx >= 0:
            build_root = log_abs[:idx]
            exe = f"{build_root}/{binary_rel}"
        else:
            # Fallback: emit binary_rel as-is; rerun will try to resolve it
            # relative to cwd.
            exe = binary_rel

        run_cmd = f"{trim_to_build(exe)} --gtest_filter={case}"
        log_rel = trim_to_build(log_abs)
        if binary_rel not in by_binary:
            order.append(binary_rel)
            by_binary[binary_rel] = []
        by_binary[binary_rel].append((run_cmd, log_rel))

    out: dict[int, dict] = {}
    for tid, binary_rel in enumerate(order, start=1):
        bin_name = binary_rel.rsplit("/", 1)[-1]
        out[tid] = {"name": bin_name, "failures": by_binary[binary_rel]}
    return out


def parse_ctest(lines: list[str]) -> dict[int, dict]:
    names: dict[int, str] = {}
    failures: dict[int, list[tuple[str, str]]] = defaultdict(list)
    # tid -> (run_command, log_path?) for the most recent unmatched TEST FAILURE
    # block. run_command is None until we see the Test command line.
    pending: dict[int, str | None] = {}

    for line in lines:
        m = SUMMARY_RE.match(line)
        if m:
            names[int(m.group(1))] = m.group(2)
            continue

        m = FAILURE_RE.match(line)
        if m:
            pending[int(m.group(1))] = None
            continue

        m = TEST_CMD_RE.match(line)
        if m:
            tid = int(m.group(1))
            if tid in pending:
                exe = trim_to_build(m.group(2))
                gfilter = m.group(3)
                pending[tid] = f"{exe} --gtest_filter={gfilter}" if gfilter else exe
            continue

        m = LOG_PATH_RE.match(line)
        if m:
            tid = int(m.group(1))
            if tid in pending:
                run_cmd = pending.pop(tid) or ""
                failures[tid].append((run_cmd, trim_to_build(m.group(2).strip())))

    # Only include tests ctest has marked ***Failed in its summary. Tests that
    # emitted TEST FAILURE blocks for individual gtest cases but haven't yet
    # produced a summary line are still running.
    return {tid: {"name": names[tid], "failures": failures.get(tid, [])} for tid in names}


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("input", nargs="?", default="ctest.out",
                    help="ctest output file (default: ctest.out)")
    args = ap.parse_args(argv[1:])

    path = Path(args.input)
    if not path.is_file():
        print(f"error: file not found: {path}", file=sys.stderr)
        return 2

    lines = path.read_bytes().decode("utf-8", errors="replace").splitlines()

    # Auto-detect format: if any PARALLEL_FAIL_RE matches, use the parallel
    # parser; otherwise fall back to ctest.
    if any(PARALLEL_FAIL_RE.match(line) for line in lines):
        failed = parse_parallel(lines)
    else:
        failed = parse_ctest(lines)

    if not failed:
        print("No failed tests found.")
        return 0

    for tid in sorted(failed):
        entry = failed[tid]
        print(f"#{tid}\t{entry['name']}")
        if entry["failures"]:
            for run_cmd, log in entry["failures"]:
                print(f"\t{run_cmd or '(no run command captured)'}")
                print(f"\t\t{log}")
        else:
            print("\t(no failure details captured)")

    print(f"\n{len(failed)} failed test(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
