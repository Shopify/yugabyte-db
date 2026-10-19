# Copyright (c) YugabyteDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
# in compliance with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations
# under the License.

"""Process-tree CPU accounting on Linux/macOS, including children reaped between samples."""

import ctypes
import errno
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time


class _RUsageInfoV2(ctypes.Structure):
    # sys/resource.h, RUSAGE_INFO_V2. CPU durations use Mach absolute ticks.
    _fields_ = [("uuid", ctypes.c_uint8 * 16)] + [
        (name, ctypes.c_uint64) for name in (
            "user_time", "system_time", "pkg_idle_wkups", "interrupt_wkups", "pageins",
            "wired_size", "resident_size", "phys_footprint", "proc_start_abstime",
            "proc_exit_abstime", "child_user_time", "child_system_time",
            "child_pkg_idle_wkups", "child_interrupt_wkups", "child_pageins",
            "child_elapsed_abstime", "diskio_bytesread", "diskio_byteswritten")]


if sys.platform == "darwin":
    _libproc = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
    _libproc.proc_pid_rusage.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_void_p]
    _libproc.proc_pid_rusage.restype = ctypes.c_int

    class _Timebase(ctypes.Structure):
        _fields_ = [("numer", ctypes.c_uint32), ("denom", ctypes.c_uint32)]

    _timebase = _Timebase()
    _libsystem = ctypes.CDLL("/usr/lib/libSystem.B.dylib")
    _libsystem.mach_timebase_info.argtypes = [ctypes.POINTER(_Timebase)]
    _libsystem.mach_timebase_info.restype = ctypes.c_int
    if _libsystem.mach_timebase_info(ctypes.byref(_timebase)) != 0:
        raise RuntimeError("Cannot read Mach clock conversion")
    _seconds_per_tick = _timebase.numer / _timebase.denom / 1e9


def process_cpu(pid):
    """Return self CPU, reaped-child CPU, and process birth identifier."""
    if sys.platform == "darwin":
        usage = _RUsageInfoV2()
        if _libproc.proc_pid_rusage(pid, 2, ctypes.byref(usage)) != 0:
            error = ctypes.get_errno()
            raise OSError(error, os.strerror(error))
        return ((usage.user_time + usage.system_time) * _seconds_per_tick,
                (usage.child_user_time + usage.child_system_time) * _seconds_per_tick,
                usage.proc_start_abstime)
    if sys.platform.startswith("linux"):
        stat = Path(f"/proc/{pid}/stat").read_text()
        # comm can contain spaces and parentheses; fields below start at field 3 (state).
        fields = stat[stat.rindex(")") + 2:].split()
        ticks = os.sysconf("SC_CLK_TCK")
        return ((int(fields[11]) + int(fields[12])) / ticks,
                (int(fields[13]) + int(fields[14])) / ticks, int(fields[19]))
    raise RuntimeError("CPU accounting requires Linux or macOS")


def process_tree(roots):
    output = subprocess.check_output(
        ["ps", "-axo", "pid=,ppid=,rss=,state=,comm="], text=True)
    processes = {}
    children = {}
    for line in output.splitlines():
        pid, parent, rss, state, command = line.split(maxsplit=4)
        pid, parent = int(pid), int(parent)
        processes[pid] = {"pid": pid, "ppid": parent, "rss_bytes": int(rss) * 1024,
                          "command": Path(command).name, "state": state}
        children.setdefault(parent, []).append(pid)
    pending = list(roots)
    result = []
    seen = set()
    # Read ancestors before children. If a child is reaped during the walk, mark the sample
    # incomplete instead of silently dropping its CPU or counting it twice.
    for pid in pending:
        if pid in seen:
            continue
        seen.add(pid)
        row = processes.get(pid)
        if row is None:
            continue
        pending.extend(children.get(pid, []))
        if "Z" in row["state"]:
            row["cpu_available"] = False
        else:
            try:
                own, reaped, birth = process_cpu(pid)
                row.update(cpu_seconds=own, reaped_cpu_seconds=reaped, birth=birth,
                           cpu_available=True)
            except OSError as error:
                if error.errno not in (errno.ESRCH, errno.ENOENT):
                    raise
                row["cpu_available"] = False
        result.append(row)
    return result


def snapshot(roots):
    root_ids = roots()
    rows = process_tree(root_ids) if root_ids else []
    available = all(row["cpu_available"] for row in rows)
    available = available and set(root_ids).issubset(row["pid"] for row in rows)
    return {"monotonic_seconds": time.monotonic(), "complete": available,
            "cpu_seconds": sum(row.get("cpu_seconds", 0) + row.get("reaped_cpu_seconds", 0)
                               for row in rows),
            "rss_sum_bytes": sum(row["rss_bytes"] for row in rows), "processes": rows}


class CpuWindow:
    """CPU-seconds / wall-seconds is average cores, not percent of the whole machine."""

    def __init__(self, roots, trace_path, interval=0.5):
        self.roots = roots
        self.trace_path = trace_path
        self.interval = interval
        self.samples = []
        self.errors = []
        self.stop_event = threading.Event()

    def take_sample(self, boundary=False):
        for attempt in range(5 if boundary else 1):
            value = snapshot(self.roots)
            if value["complete"]:
                break
            if boundary:
                time.sleep(0.01)
        self.samples.append(value)
        self.trace.write(json.dumps(value, sort_keys=True) + "\n")
        self.trace.flush()

    def _run(self):
        try:
            while not self.stop_event.wait(self.interval):
                self.take_sample()
        except Exception as error:
            self.errors.append(error)

    def __enter__(self):
        self.trace = self.trace_path.open("w", encoding="utf-8")
        try:
            self.take_sample(boundary=True)
            self.thread = threading.Thread(target=self._run, name="cpu-sampler", daemon=True)
            self.thread.start()
        except BaseException:
            self.trace.close()
            raise
        return self

    def __exit__(self, kind, value, traceback):
        self.stop_event.set()
        self.thread.join()
        try:
            self.take_sample(boundary=True)
        finally:
            self.trace.close()
        if kind is None and self.errors:
            raise RuntimeError(f"CPU sampler failed: {self.errors[0]}")

    def summary(self):
        first, last = self.samples[0], self.samples[-1]
        if not first["complete"] or not last["complete"]:
            raise RuntimeError("Incomplete CPU accounting at a phase boundary; inspect the trace")
        wall = last["monotonic_seconds"] - first["monotonic_seconds"]
        cpu = last["cpu_seconds"] - first["cpu_seconds"]
        if cpu < 0:
            raise RuntimeError("Process-tree CPU counter regressed; inspect the trace")
        rates = []
        regressions = 0
        for before, after in zip(self.samples, self.samples[1:]):
            if before["complete"] and after["complete"]:
                elapsed = after["monotonic_seconds"] - before["monotonic_seconds"]
                delta = after["cpu_seconds"] - before["cpu_seconds"]
                if delta >= 0:
                    rates.append(delta / elapsed)
                else:
                    regressions += 1
        return {"cpu_seconds": cpu, "cpu_window_seconds": wall,
                "average_cores": cpu / wall, "average_cpu_percent": 100 * cpu / wall,
                "peak_sampled_cores": max(rates) if rates else None,
                "peak_rss_sum_bytes": max(s["rss_sum_bytes"] for s in self.samples),
                "cpu_samples": len(self.samples), "counter_regressions": regressions,
                "incomplete_samples": sum(not s["complete"] for s in self.samples)}
