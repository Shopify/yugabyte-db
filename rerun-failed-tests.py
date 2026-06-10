#!/usr/bin/env python3
"""Run failed tests from parse-failed-tests.py output, one at a time.

Reads parse-failed-tests.py output (file or stdin), extracts the per-subtest
run commands, and runs each in sequence. For each test:

  * Tees stdout+stderr to <output_dir>/<gtest_filter>.log
  * Highlights interesting lines (e.g. "Webserver started. Bound to: ...")
  * Prints elapsed time on a single overwriting line
  * Ctrl-C kills the running test and moves to the next one
  * Ctrl-C twice within 1 second exits the runner

Usage:
  parse-failed-tests.py ctest.out | rerun-failed-tests.py -o logs_dir
  rerun-failed-tests.py -o logs_dir parsed.txt
"""

from __future__ import annotations

import argparse
import os
import re
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path

# parse-failed-tests.py emits a header line "#<id>\t<name>" then one or more
# subtest blocks: a single-tab "<exe> --gtest_filter=<name>" line followed by a
# double-tab log path line. We only need the run command lines.
HEADER_RE = re.compile(r"^#(\d+)\t(\S+)")
RUN_CMD_RE = re.compile(r"^\t(\S+)\s+--gtest_filter=(\S+)\s*$")

# Match "Webserver started. Bound to: <url>" lines, optionally prefixed with a
# role tag like "[m-1]" or "[ts-2]" added by the mini-cluster log multiplexer.
WEBSERVER_RE = re.compile(
    r"^(?P<prefix>\[[a-z-]+\d+\])?\s*.*Webserver started\. Bound to:\s+(?P<url>\S+)"
)


def transform_for_display(line: str) -> str | None:
    """If `line` is interesting, return a paste-ready string; else None."""
    m = WEBSERVER_RE.search(line)
    if m:
        prefix = (m.group("prefix") + " ") if m.group("prefix") else ""
        url = m.group("url").split(",", 1)[0].rstrip("/,")
        return f"{prefix}{url}/threadz?group=all"
    return None


@dataclass
class Command:
    test_name: str  # ctest test name (e.g. client_ql-tablet-test)
    exe: str        # path to test binary, possibly relative
    gfilter: str    # gtest filter, e.g. QLTabletTest.TruncateTableDuringLongRead


def parse_input(stream) -> list[Command]:
    cmds: list[Command] = []
    current_name = "<unknown>"
    for line in stream:
        line = line.rstrip("\n")
        m = HEADER_RE.match(line)
        if m:
            current_name = m.group(2)
            continue
        m = RUN_CMD_RE.match(line)
        if m:
            cmds.append(Command(current_name, m.group(1), m.group(2)))
    return cmds


def kill_proc(proc: subprocess.Popen) -> None:
    """Best-effort terminate the test's process group."""
    try:
        pgid = os.getpgid(proc.pid)
    except ProcessLookupError:
        return
    try:
        os.killpg(pgid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(pgid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        proc.wait()


def reader_thread(proc: subprocess.Popen, log_file) -> None:
    """Tee subprocess output to log_file; print highlight lines to stdout."""
    assert proc.stdout is not None
    for raw in iter(proc.stdout.readline, b""):
        log_file.write(raw)
        log_file.flush()
        line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
        display = transform_for_display(line)
        if display is not None:
            # \033[K clears the elapsed-time line so it doesn't tail us
            print(f"\r\033[K  > {display}", flush=True)


def fmt_elapsed(seconds: float) -> str:
    s = int(seconds)
    h, s = divmod(s, 3600)
    m, s = divmod(s, 60)
    return f"{h:d}:{m:02d}:{s:02d}" if h else f"{m:d}:{s:02d}"


def derive_build_root(exe: str) -> str:
    """Given a test exe path like 'build/<variant>/tests-foo/bar', return the
    absolute path to 'build/<variant>'."""
    return str(Path(exe).resolve().parent.parent)


def run_one(cmd: Command, log_path: Path, skip_flag: list[bool]) -> tuple[str, float]:
    """Run one test command; return (status_label, elapsed_seconds)."""
    skip_flag[0] = False
    argv = [cmd.exe, f"--gtest_filter={cmd.gfilter}"]
    env = os.environ.copy()
    build_root = derive_build_root(cmd.exe)
    env["YB_BUILD_ROOT"] = build_root
    env["BUILD_ROOT"] = build_root
    start = time.monotonic()
    with open(log_path, "wb") as logf:
        proc = subprocess.Popen(
            argv,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=0,
            start_new_session=True,
            env=env,
        )
        reader = threading.Thread(target=reader_thread, args=(proc, logf), daemon=True)
        reader.start()

        while proc.poll() is None:
            if skip_flag[0]:
                kill_proc(proc)
                reader.join(timeout=2)
                elapsed = time.monotonic() - start
                print(f"\r\033[K  [killed after {fmt_elapsed(elapsed)}]", flush=True)
                return "killed", elapsed
            elapsed = time.monotonic() - start
            print(f"\r\033[K  [running {fmt_elapsed(elapsed)}]", end="", flush=True)
            time.sleep(1)

        reader.join(timeout=2)
        elapsed = time.monotonic() - start
        status = "passed" if proc.returncode == 0 else f"failed (exit={proc.returncode})"
        print(f"\r\033[K  [{status} after {fmt_elapsed(elapsed)}]", flush=True)
        return status, elapsed


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("input", nargs="?", help="parse-failed-tests.py output (default: stdin)")
    ap.add_argument("-o", "--output-dir", required=True, help="directory for per-test log files")
    args = ap.parse_args()

    if args.input:
        with open(args.input) as f:
            cmds = parse_input(f)
    else:
        cmds = parse_input(sys.stdin)

    if not cmds:
        print("error: no commands found in input", file=sys.stderr)
        return 1

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    last_sigint = [0.0]
    skip_flag = [False]

    def on_sigint(signum, frame):
        now = time.monotonic()
        if now - last_sigint[0] < 1.0:
            print("\n[runner] second Ctrl-C, exiting.", file=sys.stderr)
            sys.exit(130)
        last_sigint[0] = now
        skip_flag[0] = True
        print("\n[runner] Ctrl-C: killing current test (Ctrl-C again within 1s to exit)",
              file=sys.stderr)

    signal.signal(signal.SIGINT, on_sigint)

    print(f"Running {len(cmds)} test(s); logs in {out_dir}\n")
    summary: list[tuple[Command, str, float]] = []
    for idx, cmd in enumerate(cmds, 1):
        # Parameterized gtest filters contain '/' (e.g. Foo.Bar/0). Match yb's
        # test logs convention: replace '/' with '__' so it's a flat filename.
        safe_name = cmd.gfilter.replace("/", "__")
        log_path = out_dir / f"{safe_name}.log"
        print(f"[{idx}/{len(cmds)}] {cmd.test_name}  ::  {cmd.gfilter}")
        print(f"  cmd: {cmd.exe} --gtest_filter={cmd.gfilter}")
        print(f"  log: {log_path}")
        try:
            status, elapsed = run_one(cmd, log_path, skip_flag)
        except FileNotFoundError as e:
            print(f"\r\033[K  [error: {e}]", flush=True)
            status, elapsed = "missing", 0.0
        summary.append((cmd, status, elapsed))
        print()

    print("Summary:")
    for cmd, status, elapsed in summary:
        print(f"  {status:24s} {fmt_elapsed(elapsed):>8s}  {cmd.gfilter}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
