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

import importlib.util
import json
import os
from pathlib import Path
import resource
import subprocess
import sys

import pytest


spec = importlib.util.spec_from_file_location(
    "local_runtime_cpu_usage", Path(__file__).with_name("cpu_usage.py"))
cpu_usage = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cpu_usage)


class TestLocalRuntimeCpu:
    def test_kernel_cpu_units_match_wait4(self):
        before = cpu_usage.process_cpu(os.getpid())
        usage_before = resource.getrusage(resource.RUSAGE_CHILDREN)
        code = ("import time; start=time.process_time();\n"
                "while time.process_time()-start < 0.1: pass")
        for _ in range(2):
            subprocess.run([sys.executable, "-c", code], check=True, timeout=10)
        usage_after = resource.getrusage(resource.RUSAGE_CHILDREN)
        after = cpu_usage.process_cpu(os.getpid())
        expected = (usage_after.ru_utime + usage_after.ru_stime -
                    usage_before.ru_utime - usage_before.ru_stime)
        assert expected >= 0.19
        assert after[1] - before[1] == pytest.approx(expected, abs=0.03)
        assert after[2] == before[2]

    def test_linux_stat_with_parentheses_in_name(self, monkeypatch):
        fields = ["0"] * 20
        fields[0] = "S"
        fields[11:15] = ["200", "100", "500", "200"]
        fields[19] = "45678"
        stat = "123 (a name with ) parentheses) " + " ".join(fields)
        monkeypatch.setattr(cpu_usage.sys, "platform", "linux")
        monkeypatch.setattr(cpu_usage.Path, "read_text", lambda self: stat)
        monkeypatch.setattr(cpu_usage.os, "sysconf", lambda name: 100)
        assert cpu_usage.process_cpu(123) == (3.0, 7.0, 45678)

    def test_descendants_and_reaped_cpu(self, monkeypatch):
        monkeypatch.setattr(cpu_usage.subprocess, "check_output", lambda *a, **kw:
                            "10 1 100 S root\n20 10 20 S child\n30 1 300 S unrelated\n")
        usages = {10: (1.0, 2.0, 10), 20: (3.0, 4.0, 20)}
        monkeypatch.setattr(cpu_usage, "process_cpu", lambda pid: usages[pid])
        result = cpu_usage.snapshot(lambda: [10])
        assert result["complete"]
        assert result["cpu_seconds"] == 10.0
        assert result["rss_sum_bytes"] == 120 * 1024
        assert [p["pid"] for p in result["processes"]] == [10, 20]

    def test_vanished_child_marks_sample_incomplete(self, monkeypatch):
        monkeypatch.setattr(cpu_usage.subprocess, "check_output", lambda *a, **kw:
                            "10 1 100 S root\n20 10 20 S child\n")

        def usage(pid):
            if pid == 20:
                raise ProcessLookupError(3, "gone")
            return 1.0, 0.0, 10

        monkeypatch.setattr(cpu_usage, "process_cpu", usage)
        assert not cpu_usage.snapshot(lambda: [10])["complete"]

    def test_window_counts_short_lived_backends(self, tmp_path):
        helper = r'''
import json, resource, subprocess, sys
print("ready", flush=True)
input()
before = resource.getrusage(resource.RUSAGE_CHILDREN)
code = "import time; start=time.process_time();\nwhile time.process_time()-start < 0.1: pass"
for _ in range(3):
    subprocess.run([sys.executable, "-c", code], check=True)
after = resource.getrusage(resource.RUSAGE_CHILDREN)
print(json.dumps(after.ru_utime + after.ru_stime - before.ru_utime - before.ru_stime), flush=True)
input()
'''
        process = subprocess.Popen([sys.executable, "-c", helper], stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE, text=True)
        try:
            assert process.stdout.readline().strip() == "ready"
            with cpu_usage.CpuWindow(lambda: [process.pid], tmp_path / "cpu.jsonl", 0.05) as window:
                process.stdin.write("go\n")
                process.stdin.flush()
                expected = json.loads(process.stdout.readline())
            summary = window.summary()
            assert expected >= 0.29
            assert summary["cpu_seconds"] >= expected - 0.03
            assert summary["cpu_seconds"] <= expected + 0.2
            assert summary["average_cpu_percent"] > 0
            samples = [json.loads(line)
                       for line in (tmp_path / "cpu.jsonl").read_text().splitlines()]
            assert len(samples) >= 2
            assert samples[-1]["complete"]
        finally:
            process.kill()
            process.wait(timeout=10)
            process.stdin.close()
            process.stdout.close()

    def test_incomplete_boundary_rejected(self, tmp_path):
        window = cpu_usage.CpuWindow(lambda: [], tmp_path / "unused.jsonl")
        window.samples = [{"complete": False}, {"complete": True}]
        with pytest.raises(RuntimeError, match="Incomplete CPU accounting"):
            window.summary()

    def test_peak_unknown_without_complete_intervals(self, tmp_path):
        window = cpu_usage.CpuWindow(lambda: [], tmp_path / "unused.jsonl")
        window.samples = [
            {"complete": True, "cpu_seconds": 0, "monotonic_seconds": 0, "rss_sum_bytes": 100},
            {"complete": False, "cpu_seconds": 1, "monotonic_seconds": 2, "rss_sum_bytes": 100},
            {"complete": True, "cpu_seconds": 2, "monotonic_seconds": 4, "rss_sum_bytes": 100}]
        result = window.summary()
        assert result["average_cores"] == 0.5
        assert result["peak_sampled_cores"] is None
        assert result["incomplete_samples"] == 1
