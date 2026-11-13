"""Exercise the PR title case runner with synthetic workflows and fixtures.

Workflows and fixtures are written to temporary directories and run scripts are
executed by the bash found through PATH=/usr/bin:/bin. The scripts stay portable to
bash 3.2 on macOS, whose regex engine lacks \\S, so they never depend on the
repository's own policy.
"""

from contextlib import redirect_stderr, redirect_stdout
from copy import deepcopy
import io
import json
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

from pr_title_cases import RunnerError, load_cases, load_workflow, main, run_case, run_step


WORKFLOW = r"""name: Synthetic title policy

# Comments and blank lines are part of the subset the runner reads.
on:
  pull_request:
    types: [opened, edited]
    paths-ignore:
      - 'docs/**'

jobs:
  pr-title:
    runs-on: ubuntu-latest
    steps:
      - name: Check title
        env:
          PR_TITLE: ${{ github.event.pull_request.title }}
        run: |
          # Titles lead with a bracketed lowercase area tag.
          TITLE_RE='^\[[a-z]+\] '

          if [[ ! "$PR_TITLE" =~ $TITLE_RE ]]; then
            echo "::error ::Title must start with an area tag."
            exit 1
          fi
          echo "Title is valid: $PR_TITLE"
      - name: Check base
        env:
          BASE_REF: ${{ github.base_ref }}
          PR_BODY: ${{ github.event.pull_request.body }}
        run: |
          if [[ "$BASE_REF" == "frozen" ]]; then
            echo "::error ::Base branch is frozen."
            exit 3
          fi
          echo "Base is open: $BASE_REF"
          printf 'Body: %s\n' "$PR_BODY"
      - name: Report
        env:
          PR_NUMBER: ${{ github.event.pull_request.number }}
          HEAD_REF: ${{ github.head_ref }}
          ACTION: ${{ github.event.action }}
        run: |
          echo "PR #$PR_NUMBER $ACTION from $HEAD_REF"
          echo "GITHUB_BASE_REF=$GITHUB_BASE_REF GITHUB_HEAD_REF=$GITHUB_HEAD_REF"
"""
STEP_NAMES = ("Check title", "Check base", "Report")

# Upstream YAML style the runner must tolerate outside the jobs subtree, plus the
# in-subtree details it must interpret: comments after values, |- chomping, '' escapes,
# a same-indentation sequence, and the harmless job and step keys.
DETAILED_WORKFLOW = r"""--- # document start
name: "Shopify's policy" # quoted top-level values are not interpreted
on:
  workflow_dispatch:
  pull_request:
    types:
      - opened
      - edited
    paths-ignore: [
      'docs/**',
      README.md
    ]
# A comment in column 0 inside a skipped body.
  push:
    branches:
    - main
permissions:
  contents: read
jobs:
  pr-title:
    concurrency: title-${{ github.ref }}
    runs-on: ubuntu-latest # pinned by policy
    steps: # in order
    - name: 'It''s quoted'
      id: quoted
      timeout-minutes: 1
      env: # inputs
        PR_TITLE: ${{ github.event.pull_request.title }} # the title
      run: |- # no trailing newline
        printf '%s' "$PR_TITLE"
    timeout-minutes: 5
"""


def workflow_text(steps, runs_on="ubuntu-latest"):
    """A complete workflow around step blocks produced by step_block."""
    return ("name: Synthetic\non:\n  pull_request:\n    types: [opened]\njobs:\n  pr-title:\n"
            f"    runs-on: {runs_on}\n    steps:\n" + "".join(steps))


def step_block(name, script, **env):
    lines = [f"      - name: {name}"]
    if env:
        lines.append("        env:")
        lines += [f"          {key}: {value}" for key, value in env.items()]
    lines.append("        run: |")
    lines += [f"          {line}" if line else "" for line in script.splitlines()]
    return "\n".join(lines) + "\n"


def step(outcome, **extra):
    return {"outcome": outcome, **extra}


def case(name, steps, names=STEP_NAMES, **changes):
    pull_request = {"number": 7, "title": "[area] Valid title", "body": "", "base_ref": "main",
                    "head_ref": "topic", "labels": []} | changes
    return {
        "name": name, "action": "opened", "pull_request": pull_request,
        "expect": {"steps": [{"job": "pr-title", "step": step_name, **outcome}
                             for step_name, outcome in zip(names, steps)]},
    }


ALL_PASS = case("good", [step("pass"), step("pass"), step("pass")])


class RunnerTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.workflow = self.write("pr-title.yml", WORKFLOW)

    def write(self, name, content):
        path = self.directory / name
        text = content if isinstance(content, str) else json.dumps(content)
        path.write_text(text, encoding="utf-8")
        return path

    def fixture(self, *cases, **changes):
        return self.write("cases.json", {"schema_version": 1, "cases": list(cases)} | changes)

    def run_main(self, cases, workflow=None):
        stdout, stderr = io.StringIO(), io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            status = main(["--workflow", str(workflow or self.workflow), "--cases", str(cases)])
        return status, stdout.getvalue(), stderr.getvalue()

    def test_every_case_passes_when_all_step_outcomes_match(self):
        fixture = self.fixture(
            case("valid title on an open base", [
                step("pass", stdout_contains="Title is valid: [area] Valid title"),
                step("pass", stdout_contains="Base is open: main"),
                step("pass", stdout_contains="PR #7 opened from topic"),
            ]),
            case("missing area tag fails the title check", [
                step("fail", exit_code=1,
                     stdout_contains="::error ::Title must start with an area tag."),
                step("skipped"), step("skipped"),
            ], title="No tag here"),
            case("frozen base fails with its own exit code", [
                step("pass"), step("fail", exit_code=3), step("skipped"),
            ], base_ref="frozen"),
            case("a failure without exit_code accepts any nonzero status", [
                step("pass"), step("fail"), step("skipped"),
            ], base_ref="frozen"),
        )
        self.assertEqual((0, "4 cases passed\n", ""), self.run_main(fixture))

    def test_a_passing_step_that_was_expected_to_fail_is_reported_with_its_output(self):
        fixture = self.fixture(case("tagged title is rejected", [
            step("fail", exit_code=1), step("skipped"), step("skipped")]))
        self.assertEqual((1, (
            'case "tagged title is rejected": step "Check title": '
            "expected fail (exit 1), got pass\n"
            "    Title is valid: [area] Valid title\n"
            "1 of 1 cases failed\n"), ""), self.run_main(fixture))

    def test_an_unexpected_exit_code_is_reported(self):
        fixture = self.fixture(
            case("frozen base exits with status 1", [
                step("pass"), step("fail", exit_code=1), step("skipped")], base_ref="frozen"),
            case("valid title on an open base", [step("pass"), step("pass"), step("pass")]),
        )
        status, stdout, _ = self.run_main(fixture)
        self.assertEqual(1, status)
        self.assertIn('case "frozen base exits with status 1": step "Check base": '
                      "expected fail (exit 1), got fail (exit 3)\n", stdout)
        self.assertTrue(stdout.endswith("1 of 2 cases failed\n"), stdout)

    def test_missing_stdout_text_is_reported_once_per_case(self):
        fixture = self.fixture(case("base check names the branch", [
            step("pass"), step("pass", stdout_contains="Base is open: dev"),
            step("pass", stdout_contains="never compared")]))
        self.assertEqual((1, (
            'case "base check names the branch": step "Check base": '
            'expected pass with stdout containing "Base is open: dev", got pass\n'
            "    Base is open: main\n    Body: \n"
            "1 of 1 cases failed\n"), ""), self.run_main(fixture))

    def test_steps_after_a_failure_are_skipped_and_not_run(self):
        marker = self.directory / "second-step-ran"
        workflow = self.write("marker.yml", WORKFLOW.replace(
            'echo "Base is open: $BASE_REF"', f'touch "{marker}"'))
        results = run_case(
            case("untagged", [step("fail"), step("skipped"), step("skipped")], title="untagged"),
            load_workflow(workflow))
        self.assertEqual(["fail", "skipped", "skipped"], [item["outcome"] for item in results])
        self.assertEqual(1, results[0]["exit_code"])
        self.assertEqual("::error ::Title must start with an area tag.\n", results[0]["stdout"])
        self.assertEqual({"outcome": "skipped"}, results[1])
        self.assertFalse(marker.exists())

    def test_env_values_are_expanded_from_the_case(self):
        results = run_case(
            case("expanded", [step("pass"), step("pass"), step("pass")], title="[area] Hello",
                 body="Line one\nLine two", base_ref="release", head_ref="feature", number=42)
            | {"action": "edited"},
            load_workflow(self.workflow))
        self.assertEqual("Title is valid: [area] Hello\n", results[0]["stdout"])
        self.assertEqual("Base is open: release\nBody: Line one\nLine two\n", results[1]["stdout"])
        self.assertEqual("PR #42 edited from feature\n"
                         "GITHUB_BASE_REF=release GITHUB_HEAD_REF=feature\n", results[2]["stdout"])

    def test_run_scripts_execute_with_errexit_but_without_pipefail(self):
        workflow = self.write("strict.yml", workflow_text([
            step_block("Strict", "false | true\necho reached\nfalse\necho unreachable")]))
        results = run_case(case("strict", [step("fail")], ("Strict",)), load_workflow(workflow))
        self.assertEqual([{"outcome": "fail", "exit_code": 1, "stdout": "reached\n", "stderr": ""}],
                         results)

    def test_steps_run_in_an_empty_workspace_with_a_minimal_linux_style_environment(self):
        workflow = self.write("env.yml", workflow_text([step_block("Inspect", "\n".join([
            'echo "CI=$CI GITHUB_ACTIONS=$GITHUB_ACTIONS LANG=$LANG PATH=$PATH"',
            'echo "LEAK=${PR_TITLE_CASES_LEAK:-unset}"',
            '[[ "$HOME" == "$TMPDIR" && "$PWD" == "$HOME/workspace" && -z "$(ls -A)" ]] \\',
            '  && echo "cwd is an empty workspace under HOME"',
            'echo "$HOME"',
        ]))]))
        with patch.dict(os.environ, {"PR_TITLE_CASES_LEAK": "leaked"}):
            results = run_case(case("env", [step("pass")], ("Inspect",)), load_workflow(workflow))
        lines = results[0]["stdout"].splitlines()
        self.assertEqual(["CI=true GITHUB_ACTIONS=true LANG=C.UTF-8 PATH=/usr/bin:/bin",
                          "LEAK=unset", "cwd is an empty workspace under HOME"], lines[:3])
        self.assertNotEqual(os.path.expanduser("~"), lines[3])
        self.assertFalse(Path(lines[3]).exists(), "the per-case directory is removed afterwards")

    def test_a_step_script_that_cannot_be_written_stops_the_run(self):
        steps = load_workflow(self.workflow)
        with self.assertRaises(RunnerError) as raised:
            run_step(steps[0], ALL_PASS, self.directory / "missing", 1.0)
        self.assertEqual('case "good": step "Check title": cannot run the step: '
                         "No such file or directory", str(raised.exception))

    def test_literal_run_blocks_keep_comments_and_blank_lines(self):
        steps = load_workflow(self.workflow)
        self.assertEqual(list(STEP_NAMES), [item["name"] for item in steps])
        self.assertEqual({"PR_TITLE": "${{ github.event.pull_request.title }}"}, steps[0]["env"])
        self.assertEqual(
            "# Titles lead with a bracketed lowercase area tag.\nTITLE_RE='^\\[[a-z]+\\] '\n\n"
            'if [[ ! "$PR_TITLE" =~ $TITLE_RE ]]; then\n'
            '  echo "::error ::Title must start with an area tag."\n  exit 1\nfi\n'
            'echo "Title is valid: $PR_TITLE"\n', steps[0]["run"])
        self.assertEqual(["PR_NUMBER", "HEAD_REF", "ACTION"], list(steps[2]["env"]))

    def test_only_the_jobs_subtree_is_interpreted_and_yaml_details_are_handled(self):
        steps = load_workflow(self.write("detailed.yml", DETAILED_WORKFLOW))
        self.assertEqual([{
            "name": "It's quoted", "id": "quoted", "timeout-minutes": "1",
            "env": {"PR_TITLE": "${{ github.event.pull_request.title }}"},
            "run": 'printf \'%s\' "$PR_TITLE"',
        }], steps)
        results = run_case(case("quoted", [step("pass")], ("It's quoted",), title="[x] T"), steps)
        self.assertEqual("[x] T", results[0]["stdout"])

    def test_a_list_in_column_zero_under_on_is_skipped(self):
        text = workflow_text([step_block("Listed", "echo hi")]).replace(
            "on:\n  pull_request:\n    types: [opened]\n", "on:\n- pull_request\n- push\n")
        self.assertEqual([{"name": "Listed", "run": "echo hi\n"}],
                         load_workflow(self.write("listed.yml", text)))

    def test_unsupported_workflow_syntax_is_rejected(self):
        violations = [
            (workflow_text([step_block("Folded", "echo hi").replace("run: |", "run: >")]),
             'workflow line 10: unsupported scalar ">"'),
            (workflow_text([step_block("Kept", "echo hi").replace("run: |", "run: |+")]),
             'workflow line 10: unsupported scalar "|+"'),
            (workflow_text([step_block("Anchored", "echo hi").replace(
                "run: |", "run: &script |")]), 'workflow line 10: unsupported scalar "&script |"'),
            (workflow_text([step_block("Odd", "echo hi").replace("Odd", "'it's'")]),
             "workflow line 9: unsupported scalar 'it's'"),
            (workflow_text([step_block('"a\\tb"', "echo hi")]),
             'workflow line 9: unsupported scalar "a\\tb"'),
            (workflow_text([step_block("Flow", "echo hi"), "        env: { A: b }\n"]),
             'workflow: step "Flow": env must map names to strings'),
            (workflow_text([step_block("Twice", "echo hi"), "        run: |\n          echo\n"]),
             'workflow line 12: duplicate key "run"'),
            (workflow_text([step_block("Mac", "echo hi")], runs_on="macos-latest"),
             'workflow: job "pr-title": runs-on must be an ubuntu runner, not "macos-latest"'),
            (workflow_text([step_block("Boxed", "echo hi")]).replace(
                "    steps:", "    container: ubuntu\n    steps:"),
             'workflow: job "pr-title": unsupported key "container"'),
            ("env:\n  A: b\n" + workflow_text([step_block("Top", "echo hi")]),
             'workflow: unsupported key "env"'),
            ("defaults:\n  run:\n    shell: sh\n" + workflow_text([step_block("Top", "echo hi")]),
             'workflow: unsupported key "defaults"'),
            ("name: Jobless\non: push\n", 'workflow: missing key "jobs"'),
            (workflow_text([step_block("Other", "echo hi")]).replace("pr-title:", "lint:"),
             'workflow: no job named "pr-title"'),
            (workflow_text(["      - run: echo hi\n"]), "workflow: step 1: name is required"),
            (workflow_text(["      - name: Silent\n"]), 'workflow: step "Silent": run is required'),
        ]
        for text, message in violations:
            with self.subTest(message=message):
                with self.assertRaises(RunnerError) as raised:
                    load_workflow(self.write("bad.yml", text))
                self.assertEqual(message, str(raised.exception))

    def test_unsupported_step_keys_are_rejected(self):
        for key, value in [("if", "always()"), ("uses", "actions/checkout@v6"), ("shell", "sh"),
                           ("with", "\n          path: ."), ("working-directory", "."),
                           ("continue-on-error", "true"), ("outputs", "none")]:
            with self.subTest(key=key):
                text = WORKFLOW.replace("      - name: Report\n",
                                        f"      - name: Report\n        {key}: {value}\n")
                with self.assertRaises(RunnerError) as raised:
                    load_workflow(self.write("bad.yml", text))
                self.assertEqual(f'workflow: step "Report": unsupported key "{key}"',
                                 str(raised.exception))

    def test_expressions_outside_env_values_are_rejected(self):
        for text, message in [
            (WORKFLOW.replace('echo "Title is valid: $PR_TITLE"',
                              'echo "Title is valid: ${{ github.event.pull_request.title }}"'),
             'workflow: step "Check title": run must not contain ${{ }} expressions'),
            (WORKFLOW.replace("      - name: Report\n",
                              "      - name: Report\n        id: ${{ github.sha }}\n"),
             'workflow: step "Report": id must not contain ${{ }} expressions'),
        ]:
            with self.subTest(message=message):
                with self.assertRaises(RunnerError) as raised:
                    load_workflow(self.write("bad.yml", text))
                self.assertEqual(message, str(raised.exception))

    def test_unsupported_expressions_in_env_values_are_rejected(self):
        for text, message in [
            (WORKFLOW.replace("${{ github.head_ref }}", "${{ secrets.TOKEN }}"),
             'workflow: step "Report": env HEAD_REF: '
             'unsupported expression "${{ secrets.TOKEN }}"'),
            (WORKFLOW.replace("${{ github.head_ref }}", "${{ github.head_ref"),
             'workflow: step "Report": env HEAD_REF: malformed expression'),
            (WORKFLOW.replace("${{ github.base_ref }}", "${{github.event.pull_request.labels}}"),
             'workflow: step "Check base": env BASE_REF: '
             'unsupported expression "${{github.event.pull_request.labels}}"'),
        ]:
            with self.subTest(message=message):
                with self.assertRaises(RunnerError) as raised:
                    load_workflow(self.write("bad.yml", text))
                self.assertEqual(message, str(raised.exception))

    def test_fixture_schema_violations_are_rejected(self):
        def mutated(**changes):
            document = {"schema_version": 1, "cases": [deepcopy(ALL_PASS)]}
            for path, value in changes.items():
                target = document
                keys = path.split("__")
                for key in keys[:-1]:
                    target = target[int(key) if key.isdigit() else key]
                if value is KeyError:
                    del target[keys[-1]]
                else:
                    target[keys[-1]] = value
            return document

        violations = [
            (mutated(schema_version=2), "fixture: schema_version must be 1"),
            (mutated(schema_version=True), "fixture: schema_version must be 1"),
            (mutated(notes=""), 'fixture: unknown key "notes"'),
            (mutated(cases=[]), "fixture: cases must be a non-empty list"),
            (mutated(cases__0__labels=[]), 'fixture: case 1: unknown key "labels"'),
            (mutated(cases__0__expect=KeyError, cases__0__action=KeyError),
             'fixture: case 1: missing key "action"'),
            ({"schema_version": 1, "cases": [ALL_PASS, ALL_PASS]},
             "fixture: case 2: name must be a unique non-empty string"),
            (mutated(cases__0__action=""), 'case "good": action must be a non-empty string'),
            (mutated(cases__0__pull_request__number="7"),
             'case "good": pull_request.number must be an integer'),
            (mutated(cases__0__pull_request__draft=False),
             'case "good": pull_request: unknown key "draft"'),
            (mutated(cases__0__pull_request__body=None),
             'case "good": pull_request.body must be a string'),
            (mutated(cases__0__expect__status="pass"), 'case "good": expect: unknown key "status"'),
            (mutated(cases__0__expect__steps__0__outcome="error"),
             'case "good": step "Check title": outcome must be pass, fail, or skipped'),
            (mutated(cases__0__expect__steps__0__exit_code=1),
             'case "good": step "Check title": exit_code is only allowed with outcome fail'),
            (mutated(cases__0__expect__steps__0__outcome="fail",
                     cases__0__expect__steps__0__exit_code=0),
             'case "good": step "Check title": exit_code must be an integer from 1 to 255'),
            (mutated(cases__0__expect__steps__0__outcome="fail",
                     cases__0__expect__steps__0__exit_code=256),
             'case "good": step "Check title": exit_code must be an integer from 1 to 255'),
            (mutated(cases__0__expect__steps__0__outcome="fail",
                     cases__0__expect__steps__0__exit_code=True),
             'case "good": step "Check title": exit_code must be an integer from 1 to 255'),
            (mutated(cases__0__expect__steps__0__stdout_contains=""),
             'case "good": step "Check title": stdout_contains must be a non-empty string'),
            (mutated(cases__0__expect__steps__0__stderr_contains="x"),
             'case "good": step "Check title": unknown key "stderr_contains"'),
            (mutated(cases__0__expect__steps__0__job="lint"),
             'case "good": expect.steps[1] names job "lint" step "Check title"; '
             'the workflow has job "pr-title" step "Check title"'),
            (mutated(cases__0__expect__steps=ALL_PASS["expect"]["steps"][:2]),
             'case "good": expect.steps must list the 3 steps of job "pr-title" in order'),
        ]
        steps = load_workflow(self.workflow)
        for document, message in violations:
            with self.subTest(message=message):
                with self.assertRaises(RunnerError) as raised:
                    load_cases(self.write("bad.json", document), steps)
                self.assertEqual(message, str(raised.exception))

    def test_a_renamed_or_added_workflow_step_is_reported(self):
        fixture = self.fixture(ALL_PASS)
        renamed = self.write("renamed.yml", WORKFLOW.replace(
            "- name: Check base", "- name: Check base ref"))
        self.assertEqual((2, "", 'Error: case "good": expect.steps[2] names job "pr-title" step '
                          '"Check base"; the workflow has job "pr-title" step "Check base ref"\n'),
                         self.run_main(fixture, renamed))
        extended = self.write("extended.yml", WORKFLOW + step_block("Extra", "echo extra"))
        self.assertEqual((2, "", 'Error: case "good": expect.steps must list the 4 steps of job '
                          '"pr-title" in order\n'), self.run_main(fixture, extended))

    def test_a_step_that_exceeds_the_timeout_stops_the_run_and_its_children(self):
        workflow = self.write("slow.yml", workflow_text([step_block("Stall", "sleep 30")]))
        started = time.monotonic()
        with self.assertRaises(RunnerError) as raised:
            run_case(case("stalls", [step("pass")], ("Stall",)), load_workflow(workflow),
                     timeout=0.5)
        self.assertEqual('case "stalls": step "Stall": timed out after 0.5 seconds',
                         str(raised.exception))
        self.assertLess(time.monotonic() - started, 15, "sleep must die with its process group")

    def test_unreadable_inputs_exit_with_status_2(self):
        fixture = self.fixture(ALL_PASS)
        missing = self.directory / "missing.yml"
        self.assertEqual((2, "", f"Error: cannot read workflow {missing}: "
                          "No such file or directory\n"), self.run_main(fixture, missing))
        broken = self.write("broken.json", "{")
        self.assertEqual((2, "", f"Error: cannot read cases {broken}: "
                          "invalid JSON (line 1 column 2)\n"), self.run_main(broken))


if __name__ == "__main__":
    unittest.main()
