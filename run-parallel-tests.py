#!/usr/bin/env python3
"""Run YugabyteDB gtest cases in parallel, independent of yb_build.sh.

Discovers test binaries under <build_root>/tests-*, enumerates their gtest
cases, and dispatches case-level work across a worker pool. Each case is run
via build-support/run-test.sh so it picks up the same env/wrapping as a
normal test run.

This script is intentionally standalone: it does not require yb_build.sh
or ctest to be on the path. It only reads the build tree and shells out
to run-test.sh.
"""

from __future__ import annotations

import argparse
import dataclasses
import glob
import os
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
RUN_TEST_SH = REPO_ROOT / "build-support" / "run-test.sh"

# Groups of binaries whose cases must NOT run concurrently. Each inner list
# becomes one work item: all (filtered) cases across every binary in the
# group run sequentially in one worker slot. Singletons serialize cases
# within one binary; multi-entry groups serialize across binaries too.
# Match is EXACT on "tests-<subdir>/<binary>" (case.binary_rel); the dir
# prefix disambiguates if two test trees ship binaries with the same name.
#
# Add a group here (with a short comment explaining WHY) when you see cases
# failing only under parallelism, with rerun-individually success.
SERIAL_BINARIES: list[list[str]] = [
    # Cases write SQL output to a shared per-binary scratch path
    # ${BUILD_ROOT}/test_conflict_resolve_keys_verification_sql/.../target.out
    # Concurrent cases clobber target.out -> CompareFiles fails.
    ["tests-integration-tests/conflict_resolve_keys_verification-itest"],

    # Cases write CSV input to fixed shared path
    # /tmp/PgOpBufferingTest_copy_test.tmp. Different cases use tables with
    # different column counts -> concurrent writes cause COPY errors.
    ["tests-pgwrapper/pg_op_buffering-test"],

    # Each case spins up 3 yb-controller daemons in SetUp(). Parallel cases
    # across these binaries burst YBC startup-pings beyond the retry budget.
    # One group => one worker slot shared across all three binaries.
    [
        "tests-tools/yb-backup-test",
        "tests-tools/yb-backup-cross-feature-test",
        "tests-tools/yb-backup-during-ddl-test",
    ],

    # rpc-test cases failed under parallelism, passed on rerun-individually.
    ["tests-rpc/rpc-test"],

    # these are long running tail tests
    ["tests-pgwrapper/alter_schema_abort_txn-test"],
    ["tests-pgwrapper/pg_tablet_shutdown-test"],
    ["tests-integration-tests/basic_upgrade-test"],
    ["tests-master/master-test"],
]


@dataclasses.dataclass
class Case:
    binary: Path
    suite_dot_case: str  # "Suite.Case"

    @property
    def binary_rel(self) -> str:
        # "tests-<subdir>/<binary>" — enough to reconstruct the exe path
        # given the build root, so consumers of our output don't have to
        # glob the filesystem.
        return f"{self.binary.parent.name}/{self.binary.name}"

    @property
    def display(self) -> str:
        return f"{self.binary_rel}::{self.suite_dot_case}"


@dataclasses.dataclass
class Result:
    case: Case
    exit_code: int
    duration_sec: float
    log_path: Path


@dataclasses.dataclass
class WorkItem:
    """One dispatch unit submitted to the worker pool.

    - Parallel: len(cases) == 1 — one worker slot runs one case.
    - Serial:   len(cases) >  1 — one worker slot runs ALL listed cases
      sequentially. Cases may span multiple binaries when grouped via
      SERIAL_BINARIES.
    """
    cases: list[Case]

    @property
    def serial(self) -> bool:
        return len(self.cases) > 1


@dataclasses.dataclass
class TestSpec:
    """A --test argument, e.g. 'cql-test:Read,Write' or just 'cql-test'."""
    binary_substr: str
    case_substrs: list[str] | None  # None means "all cases of this binary"


def parse_test_spec(s: str) -> TestSpec:
    if ":" in s:
        bin_part, case_part = s.split(":", 1)
        cases = [c.strip() for c in case_part.split(",") if c.strip()]
        return TestSpec(bin_part.strip(), cases or None)
    return TestSpec(s.strip(), None)


def select_binaries(binaries: list[Path],
                    specs: list[TestSpec]) -> list[tuple[Path, list[str] | None]]:
    """Pair each binary with the case substrings to keep.

    Returns (binary, None) for "keep all cases", (binary, [substrs]) for
    "keep cases matching any substr". Binaries that no spec matches are
    dropped. If specs is empty, every binary is kept with all cases.
    """
    if not specs:
        return [(b, None) for b in binaries]
    out: list[tuple[Path, list[str] | None]] = []
    for b in binaries:
        case_filters: list[str] = []
        keep_all = False
        matched = False
        for spec in specs:
            if spec.binary_substr in b.name:
                matched = True
                if spec.case_substrs is None:
                    keep_all = True
                else:
                    case_filters.extend(spec.case_substrs)
        if matched:
            out.append((b, None if keep_all else case_filters))
    return out


def discover_binaries(build_root: Path) -> list[Path]:
    """Return absolute paths of every test binary under build_root/tests-*."""
    binaries: list[Path] = []
    for tests_dir in sorted(build_root.glob("tests-*")):
        if not tests_dir.is_dir():
            continue
        for entry in sorted(tests_dir.iterdir()):
            if entry.is_file() and os.access(entry, os.X_OK):
                binaries.append(entry)
    return binaries


def enumerate_cases(binary: Path) -> list[str]:
    """Return ["Suite.Case", ...] for one binary via --gtest_list_tests."""
    try:
        out = subprocess.check_output(
            [str(binary), "--gtest_list_tests"],
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=30,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return []

    cases: list[str] = []
    current_suite: str | None = None
    for line in out.splitlines():
        if not line:
            continue
        if not line.startswith(" "):
            current_suite = line.split("#", 1)[0].strip().rstrip(".")
        elif current_suite:
            name = line.split("#", 1)[0].strip()
            if name:
                cases.append(f"{current_suite}.{name}")
    return cases


def run_one(case: Case, results_dir: Path) -> Result:
    case_dir = results_dir / case.binary.name
    case_dir.mkdir(parents=True, exist_ok=True)
    # Parameterized gtest cases contain '/' (e.g. "Suite.Case/Param").
    # Sanitize for the log filename only; the case arg to run-test.sh
    # keeps the original form.
    safe = case.suite_dot_case.replace("/", "_")
    log_path = case_dir / f"{safe}.log"
    # run-test.sh derives BUILD_ROOT (and YB_COMPILER_TYPE, YB_LINKING_TYPE,
    # YB_TARGET_ARCH, etc.) from CWD via handle_build_root_from_current_dir.
    # Binary lives at $BUILD_ROOT/tests-*/, so cwd=$BUILD_ROOT.
    build_root = case.binary.parent.parent
    log_rel = os.path.relpath(log_path, build_root)
    print(f"  START {case.display}\n    {log_rel}", flush=True)
    start = time.monotonic()
    with log_path.open("wb") as log_fh:
        proc = subprocess.run(
            [str(RUN_TEST_SH), str(case.binary), case.suite_dot_case],
            stdout=log_fh,
            stderr=subprocess.STDOUT,
            cwd=str(build_root),
        )
    return Result(case, proc.returncode, time.monotonic() - start, log_path)


def run_work_item(item: WorkItem, results_dir: Path) -> list[Result]:
    """Run every case of one work item. For serial items, cases run
    sequentially inside this single worker slot."""
    results: list[Result] = []
    for case in item.cases:
        results.append(run_one(case, results_dir))
    return results


def build_queue(selections: list[tuple[Path, list[str] | None]],
                serial_groups: list[list[str]],
                workers: int) -> list[WorkItem]:
    binaries = [b for b, _ in selections]
    with ThreadPoolExecutor(max_workers=workers) as pool:
        enumerated = list(pool.map(enumerate_cases, binaries))

    per_binary: dict[str, tuple[Path, list[str]]] = {}
    for (binary, case_filters), cases in zip(selections, enumerated):
        if case_filters is None:
            filtered = cases
        else:
            wanted = set(case_filters)
            filtered = [sc for sc in cases if sc in wanted]
        if filtered:
            key = f"{binary.parent.name}/{binary.name}"
            per_binary[key] = (binary, filtered)

    grouped_names: set[str] = set()
    queue: list[WorkItem] = []

    for group in serial_groups:
        group_cases: list[Case] = []
        for name in group:
            grouped_names.add(name)
            if name in per_binary:
                binary, cases = per_binary[name]
                group_cases.extend(Case(binary, sc) for sc in cases)
        if group_cases:
            queue.append(WorkItem(cases=group_cases))

    # Parallel binaries ordered by descending case count so long-pole
    # binaries claim worker slots early. Within a binary, cases keep their
    # gtest enumeration order.
    parallel_binaries = [(name, *per_binary[name]) for name in per_binary
                         if name not in grouped_names]
    parallel_binaries.sort(key=lambda nbc: (-len(nbc[2]), nbc[0]))
    for _, binary, cases in parallel_binaries:
        for sc in cases:
            queue.append(WorkItem(cases=[Case(binary, sc)]))

    # Stable sort: serial WorkItems float to the front (largest first),
    # parallel WorkItems keep the order built above.
    queue.sort(key=lambda wi: -len(wi.cases) if wi.serial else 0)
    return queue


def cleanup_tmp() -> None:
    """Remove YB scratch directories left in /tmp after a run.

    Targets exactly three patterns:
      - /tmp/.yb.*            (port-allocator residue, one dir per ip:port)
      - /tmp/yb-port-locks
      - /tmp/yb_completed_tests
    """
    paths = (glob.glob("/tmp/.yb.*")
             + ["/tmp/yb-port-locks", "/tmp/yb_completed_tests"])
    removed = 0
    for p in paths:
        if not os.path.exists(p):
            continue
        try:
            shutil.rmtree(p)
            removed += 1
        except OSError as e:
            print(f"  cleanup: could not remove {p}: {e}", flush=True)
    print(f"Cleaned up {removed} /tmp entries.", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-root", type=Path,
                        default=REPO_ROOT / "build" / "latest",
                        help="Build directory (default: build/latest)")
    parser.add_argument("-j", "--workers", type=int, default=os.cpu_count() or 8,
                        help="Parallel worker count")
    parser.add_argument("--test", action="append", default=None,
                        metavar="BINARY[:Suite.Case1,Suite.Case2,...]",
                        help="Select a binary (substring match on basename) "
                             "and optionally specific cases (EXACT match on "
                             "Suite.Case). Repeatable. Without any --test, "
                             "all binaries and cases run.")
    parser.add_argument("--results-dir", type=Path, default=None,
                        help="Where to write per-case logs (default: "
                             "<build_root>/yb-test-logs/parallel-tests)")
    parser.add_argument("--print-queue", action="store_true",
                        help="Print the dispatch queue (binary<TAB>Suite.Case) "
                             "and exit without running anything.")
    args = parser.parse_args()

    specs = [parse_test_spec(t) for t in (args.test or [])]

    build_root = args.build_root.resolve()
    if not build_root.is_dir():
        print(f"build root not found: {build_root}", file=sys.stderr)
        return 2

    results_dir = (args.results_dir
                   or build_root / "yb-test-logs" / "parallel-tests").resolve()
    results_dir.mkdir(parents=True, exist_ok=True)

    print(f"Discovering binaries under {build_root}...", flush=True)
    binaries = discover_binaries(build_root)
    print(f"  {len(binaries)} binaries", flush=True)

    selections = select_binaries(binaries, specs)
    if specs:
        print(f"  {len(selections)} matched by --test specs", flush=True)

    print("Enumerating cases...", flush=True)
    queue = build_queue(selections, SERIAL_BINARIES, args.workers)
    total_cases = sum(len(wi.cases) for wi in queue)
    print(f"  {total_cases} cases in {len(queue)} work items "
          f"({sum(1 for wi in queue if wi.serial)} serial)", flush=True)

    if args.print_queue:
        for idx, wi in enumerate(queue):
            marker = "serial" if wi.serial else "parallel"
            for case in wi.cases:
                print(f"{marker}\t{idx}\t{case.binary_rel}\t"
                      f"{case.suite_dot_case}")
            if wi.serial:
                print("---")
        return 0

    if not queue:
        return 0

    print(f"Running with {args.workers} workers, logs in {results_dir}",
          flush=True)
    failed: list[Result] = []
    done = 0
    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(run_work_item, wi, results_dir): wi for wi in queue}
        for fut in as_completed(futures):
            for res in fut.result():
                done += 1
                if res.exit_code != 0:
                    failed.append(res)
                    print(f"[{done}/{total_cases}] FAIL {res.case.display} "
                          f"({res.duration_sec:.1f}s) -> {res.log_path}",
                          flush=True)
                elif done % 25 == 0:
                    print(f"[{done}/{total_cases}] ok, "
                          f"{len(failed)} failed so far", flush=True)

    elapsed = time.monotonic() - started
    print(f"\nDone in {elapsed:.1f}s. {done - len(failed)} passed, "
          f"{len(failed)} failed.")
    by_binary: dict[str, list[Result]] = {}
    for r in failed:
        by_binary.setdefault(r.case.binary_rel, []).append(r)
    for binary in sorted(by_binary):
        print(f"  {binary}")
        for r in by_binary[binary]:
            print(f"    {r.case.suite_dot_case}")
    cleanup_tmp()
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

