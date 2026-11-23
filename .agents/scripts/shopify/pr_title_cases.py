#!/usr/bin/env python3
"""Run the PR title policy workflow against its fixture cases. Requires Python 3.10+.

Each case in .github/tests/pr-title-cases.json describes a pull request and the expected
outcome of every step of the pr-title job in .github/workflows/pr-title.yml. The runner
expands each step's env from the case, runs the script with `bash -e` as GitHub does for
a step without an explicit shell on Linux, and compares outcomes, exit codes, and output.
It interprets only the workflow's jobs subtree, with a parser for the YAML subset that
file uses, and rejects anything it cannot interpret instead of guessing. Exit status:
0 all cases match, 1 a mismatch, 2 invalid input, unsupported syntax, or a step timeout.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile
import textwrap

GITHUB = Path(__file__).resolve().parents[3] / ".github"
JOB = "pr-title"
EXPRESSIONS = {
    "github.event.pull_request.title": ("pull_request", "title"),
    "github.event.pull_request.body": ("pull_request", "body"),
    "github.event.pull_request.number": ("pull_request", "number"),
    "github.base_ref": ("pull_request", "base_ref"),
    "github.head_ref": ("pull_request", "head_ref"),
    "github.event.action": ("action",),
}
EXPRESSION = re.compile(r"\$\{\{\s*(.*?)\s*\}\}")
KEY = re.compile(r"([A-Za-z0-9_.-]+):(?:\s+(.*))?$")
# Other top-level keys change how steps run (env, defaults) or are unknown. Only the
# jobs subtree is parsed; the bodies of the allowed keys, such as `on:`, are blanked.
TOP_LEVEL_KEYS = {"name", "run-name", "on", "permissions", "concurrency", "jobs"}
JOB_KEYS = {"name", "timeout-minutes", "permissions", "concurrency"}
STEP_KEYS = {"id", "env", "run", "timeout-minutes"}
PULL_REQUEST_KEYS = {"number", "title", "body", "base_ref", "head_ref", "labels"}


class RunnerError(ValueError):
    """The inputs cannot be interpreted or a step did not finish; exit status 2."""


def require(condition: object, message: str) -> None:
    if not condition:
        raise RunnerError(message)


def indent(line: str) -> int:
    return len(line) - len(line.lstrip(" "))


def skip(lines: list[str], i: int) -> int:
    """Index of the next line that is neither blank nor a comment."""
    while i < len(lines) and (not lines[i].strip() or lines[i].lstrip().startswith("#")):
        i += 1
    return i


def uncommented(text: str) -> str:
    """Drop a trailing comment from an unquoted value; a bare comment leaves it empty."""
    if text.startswith("#"):
        return ""
    if text and text[0] not in "'\"":
        return text.split(" #", 1)[0].rstrip()
    return text


def scalar(text: str, i: int) -> str:
    quote = text[:1]
    if quote in ("'", '"'):
        inner = text[1:-1]
        malformed = len(text) < 2 or text[-1] != quote or (
            inner.count("'") % 2 if quote == "'" else '"' in inner or "\\" in inner)
        require(not malformed, f"workflow line {i + 1}: unsupported scalar {text}")
        return inner.replace("''", "'") if quote == "'" else inner
    require(text and quote not in "|>&*!%@`" and not text.startswith("- "),
            f'workflow line {i + 1}: unsupported scalar "{text}"')
    return text


def parse_literal(lines: list[str], i: int, level: int, newline: str) -> tuple[str, int]:
    start = i
    while i < len(lines) and (not lines[i].strip() or indent(lines[i]) > level):
        i += 1
    block = lines[start:i]
    while block and not block[-1].strip():
        block.pop()
    require(block, f"workflow line {start}: empty literal block")
    width = indent(next(line for line in block if line.strip()))
    require(all(not line.strip() or indent(line) >= width for line in block),
            f"workflow line {start}: inconsistent literal block indentation")
    return "\n".join(line[width:] for line in block) + newline, i


def parse_sequence(lines: list[str], i: int, level: int) -> tuple[list, int]:
    items = []
    while (i := skip(lines, i)) < len(lines) and indent(lines[i]) >= level:
        if indent(lines[i]) == level and not lines[i].lstrip().startswith("- "):
            break  # the enclosing mapping continues at this indentation
        require(indent(lines[i]) == level, f"workflow line {i + 1}: expected a sequence item")
        lines[i] = " " * (level + 2) + lines[i].lstrip()[2:]
        text = uncommented(lines[i].strip())
        if KEY.match(text):
            item, i = parse_mapping(lines, i, indent(lines[i]))
        else:
            item, i = scalar(text, i), i + 1
        items.append(item)
    return items, i


def parse_mapping(lines: list[str], i: int, level: int) -> tuple[dict, int]:
    mapping = {}
    while (i := skip(lines, i)) < len(lines) and indent(lines[i]) >= level:
        match = KEY.match(lines[i].strip())
        require(indent(lines[i]) == level and match,
                f"workflow line {i + 1}: expected a mapping key")
        key, rest = match.group(1), uncommented((match.group(2) or "").strip())
        require(key not in mapping, f'workflow line {i + 1}: duplicate key "{key}"')
        if not rest:
            j = skip(lines, i + 1)
            require(j < len(lines) and (indent(lines[j]) > level or (
                indent(lines[j]) == level and lines[j].lstrip().startswith("- "))),
                f'workflow line {i + 1}: key "{key}" has no value')
            parse = parse_sequence if lines[j].lstrip().startswith("- ") else parse_mapping
            mapping[key], i = parse(lines, j, indent(lines[j]))
        elif rest in ("|", "|-"):
            mapping[key], i = parse_literal(lines, i + 1, level, "\n" if rest == "|" else "")
        else:
            mapping[key], i = scalar(rest, i), i + 1
    return mapping, i


def parse_workflow(text: str) -> dict:
    """The top-level mapping with only the jobs subtree parsed: other top-level values
    become placeholders and their bodies are blanked before parsing. The subtree uses
    block mappings and sequences, single-line plain or quoted scalars, and | or |-
    literal blocks; flow collections stay opaque."""
    kept, in_jobs = [], False
    for line in text.splitlines():
        if line[:1] not in ("", " ", "#", "-"):
            match = KEY.match(line.rstrip())
            in_jobs = bool(match) and match.group(1) == "jobs"
            kept.append(line if in_jobs or not match else f"{match.group(1)}: skipped")
        else:
            kept.append(line if in_jobs else "")
    return parse_mapping(kept, 0, 0)[0]


def check_keys(mapping: object, label: str, required: set, optional: set = frozenset(),
               unknown: str = "unknown") -> None:
    require(isinstance(mapping, dict), f"{label}: expected a mapping")
    for key in sorted(required):
        require(key in mapping, f'{label}: missing key "{key}"')
    for key in mapping:
        require(key in required or key in optional, f'{label}: {unknown} key "{key}"')


def read_text(path: Path, label: str) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as error:
        detail = getattr(error, "strerror", None) or "not valid UTF-8"
        raise RunnerError(f"cannot read {label} {path}: {detail}") from error


def load_workflow(path: Path) -> list[dict]:
    """The steps of the pr-title job, each with name, run, and optional env."""
    document = parse_workflow(read_text(path, "workflow"))
    check_keys(document, "workflow", {"jobs"}, TOP_LEVEL_KEYS, "unsupported")
    jobs = document["jobs"]
    require(isinstance(jobs, dict) and JOB in jobs, f'workflow: no job named "{JOB}"')
    job = jobs[JOB]
    check_keys(job, f'workflow: job "{JOB}"', {"runs-on", "steps"}, JOB_KEYS, "unsupported")
    require(isinstance(job["runs-on"], str) and job["runs-on"].startswith("ubuntu"),
            f'workflow: job "{JOB}": runs-on must be an ubuntu runner, not "{job["runs-on"]}"')
    require(isinstance(job["steps"], list) and job["steps"],
            f'workflow: job "{JOB}": steps must be a non-empty list')
    for index, step in enumerate(job["steps"], 1):
        require(isinstance(step, dict) and isinstance(step.get("name"), str),
                f"workflow: step {index}: name is required")
        label = f'workflow: step "{step["name"]}"'
        check_keys(step, label, {"name"}, STEP_KEYS, "unsupported")
        require(isinstance(step.get("run"), str) and step["run"].strip(),
                f"{label}: run is required")
        env = step.get("env", {})
        require(isinstance(env, dict) and all(isinstance(v, str) for v in env.values()),
                f"{label}: env must map names to strings")
        for name, text in env.items():
            for match in EXPRESSION.finditer(text):
                require(match.group(1) in EXPRESSIONS,
                        f'{label}: env {name}: unsupported expression "{match.group(0)}"')
            require("${{" not in EXPRESSION.sub("", text),
                    f"{label}: env {name}: malformed expression")
        for key in [key for key in step if key not in ("name", "env")]:
            require("${{" not in str(step[key]),
                    f"{label}: {key} must not contain ${{{{ }}}} expressions")
    return job["steps"]


def load_cases(path: Path, steps: list[dict]) -> list[dict]:
    """The fixture cases, each checked against the schema and the workflow's steps."""
    try:
        document = json.loads(read_text(path, "cases"))
    except json.JSONDecodeError as error:
        raise RunnerError(f"cannot read cases {path}: invalid JSON "
                          f"(line {error.lineno} column {error.colno})") from error
    check_keys(document, "fixture", {"schema_version", "cases"})
    require(type(document["schema_version"]) is int and document["schema_version"] == 1,
            "fixture: schema_version must be 1")
    cases = document["cases"]
    require(isinstance(cases, list) and cases, "fixture: cases must be a non-empty list")
    names = set()
    for index, case in enumerate(cases, 1):
        check_keys(case, f"fixture: case {index}", {"name", "action", "pull_request", "expect"})
        name = case["name"]
        require(isinstance(name, str) and name and name not in names,
                f"fixture: case {index}: name must be a unique non-empty string")
        names.add(name)
        label = f'case "{name}"'
        require(isinstance(case["action"], str) and case["action"],
                f"{label}: action must be a non-empty string")
        pull_request = case["pull_request"]
        check_keys(pull_request, f"{label}: pull_request", PULL_REQUEST_KEYS)
        require(type(pull_request["number"]) is int,
                f"{label}: pull_request.number must be an integer")
        for key in ("title", "body", "base_ref", "head_ref"):
            require(isinstance(pull_request[key], str),
                    f"{label}: pull_request.{key} must be a string")
        check_keys(case["expect"], f"{label}: expect", {"steps"})
        expected = case["expect"]["steps"]
        require(isinstance(expected, list) and len(expected) == len(steps),
                f'{label}: expect.steps must list the {len(steps)} steps of job "{JOB}" in order')
        for position, (step, item) in enumerate(zip(steps, expected), 1):
            step_label = f'{label}: step "{step["name"]}"'
            check_keys(item, step_label, {"job", "step", "outcome"},
                       {"exit_code", "stdout_contains"})
            require(item["job"] == JOB and item["step"] == step["name"],
                    f'{label}: expect.steps[{position}] names job "{item["job"]}" step '
                    f'"{item["step"]}"; the workflow has job "{JOB}" step "{step["name"]}"')
            require(item["outcome"] in ("pass", "fail", "skipped"),
                    f"{step_label}: outcome must be pass, fail, or skipped")
            if "exit_code" in item:
                require(item["outcome"] == "fail",
                        f"{step_label}: exit_code is only allowed with outcome fail")
                require(type(item["exit_code"]) is int and 1 <= item["exit_code"] <= 255,
                        f"{step_label}: exit_code must be an integer from 1 to 255")
            if "stdout_contains" in item:
                require(isinstance(item["stdout_contains"], str) and item["stdout_contains"],
                        f"{step_label}: stdout_contains must be a non-empty string")
    return cases


def expand(value: str, case: dict) -> str:
    def lookup(match: re.Match) -> str:
        result = case
        for key in EXPRESSIONS[match.group(1)]:
            result = result[key]
        return str(result)
    return EXPRESSION.sub(lookup, value)


def run_step(step: dict, case: dict, workdir: Path, timeout: float) -> dict:
    """Run one step as GitHub runs a step without an explicit shell on Linux: `bash -e`
    on a script kept outside the workspace, which starts empty and persists per case."""
    label = f'case "{case["name"]}": step "{step["name"]}"'
    workspace, script = workdir / "workspace", workdir / "step.sh"
    env = {"PATH": "/usr/bin:/bin", "HOME": str(workdir), "TMPDIR": str(workdir),
           "LANG": "C.UTF-8", "CI": "true", "GITHUB_ACTIONS": "true",
           "GITHUB_BASE_REF": case["pull_request"]["base_ref"],
           "GITHUB_HEAD_REF": case["pull_request"]["head_ref"]}
    env.update({name: expand(value, case) for name, value in step.get("env", {}).items()})
    try:
        workspace.mkdir(exist_ok=True)
        script.write_text(step["run"], encoding="utf-8")
        process = subprocess.Popen(
            ["bash", "-e", str(script)], cwd=workspace, env=env, stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, errors="replace",
            start_new_session=True)
    except (OSError, ValueError) as error:
        detail = getattr(error, "strerror", None) or error
        raise RunnerError(f"{label}: cannot run the step: {detail}") from error
    try:
        stdout, stderr = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.communicate()
        raise RunnerError(f"{label}: timed out after {timeout:g} seconds") from None
    return {"outcome": "pass" if process.returncode == 0 else "fail",
            "exit_code": process.returncode, "stdout": stdout, "stderr": stderr}


def run_case(case: dict, steps: list[dict], timeout: float = 10.0) -> list[dict]:
    """Outcomes of the job's steps for one case; steps after a failure are skipped."""
    results = []
    with tempfile.TemporaryDirectory() as directory:
        for step in steps:
            if results and results[-1]["outcome"] != "pass":
                results.append({"outcome": "skipped"})
            else:
                results.append(run_step(step, case, Path(directory).resolve(), timeout))
    return results


def describe(item: dict) -> str:
    text = item["outcome"]
    if text == "fail" and item.get("exit_code"):
        text += f" (exit {item['exit_code']})"
    if item.get("stdout_contains"):
        text += f' with stdout containing "{item["stdout_contains"]}"'
    return text


def report(case: dict, steps: list[dict], results: list[dict]) -> bool:
    """Print the first mismatching step of a case with its output; True if there is one."""
    for step, expected, actual in zip(steps, case["expect"]["steps"], results):
        if (expected["outcome"] == actual["outcome"]
                and expected.get("exit_code") in (None, actual.get("exit_code"))
                and expected.get("stdout_contains", "") in actual.get("stdout", "")):
            continue
        print(f'case "{case["name"]}": step "{step["name"]}": '
              f"expected {describe(expected)}, got {describe(actual)}")
        output = actual.get("stdout", "") + actual.get("stderr", "")
        if output.strip():
            print(textwrap.indent(output.rstrip("\n"), "    "))
        return True
    return False


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workflow", type=Path, default=GITHUB / "workflows/pr-title.yml")
    parser.add_argument("--cases", type=Path, default=GITHUB / "tests/pr-title-cases.json")
    args = parser.parse_args(argv)
    try:
        steps = load_workflow(args.workflow)
        cases = load_cases(args.cases, steps)
        failed = sum(report(case, steps, run_case(case, steps)) for case in cases)
    except RunnerError as error:
        print(f"Error: {error}", file=sys.stderr)
        return 2
    if failed:
        print(f"{failed} of {len(cases)} cases failed")
        return 1
    print(f"{len(cases)} cases passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
