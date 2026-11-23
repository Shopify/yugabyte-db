"""Exercise `upstream.py submit` against throwaway repositories, an in-memory GitHub and a
recording tool runner.

Tests never read or write a real checkout and never call GitHub or the network: the fork and
upstream are bare repositories under a temporary directory, `gh` answers from canned responses,
and `gh stack`, `create-pr.sh`, `issues.py` and the editor are recorded with only the minimal git
side effect they would have.
"""

import io
import json
import os
from pathlib import Path
import shlex
import subprocess
import unittest
from unittest.mock import patch

from scm import Error, Tool
from upstream import Cli
from upstream_submit import (ISSUES_SCRIPT, GitHub, commit_message, link_fork_references,
                             normalize_newlines, slug, strip_shopify_context, summary_section)
import testfixtures


FORK = "Shopify/yugabyte-db"
UPSTREAM = "yugabyte/yugabyte-db"
FORK_ISSUE = "https://github.com/Shopify/yugabyte-db/issues/"
FORK_PULL = "https://github.com/Shopify/yugabyte-db/pull/"
UPSTREAM_ISSUE = "https://github.com/yugabyte/yugabyte-db/issues/"
UPSTREAM_PULL = "https://github.com/yugabyte/yugabyte-db/pull/"
PR_FIELDS = ("number,title,body,state,baseRefName,mergeCommit,labels,reviews,reviewDecision,url,"
             "author")
UPSTREAM_FIELDS = "number,state,mergedAt,mergeCommit,url,title,headRefName"
TITLE = "[#70] DocDB: Expose the refresh batch metric"
BRANCH = "feature-stack/Shopify/expose-the-refresh-batch-metric"
CHILD = "feature-stack/Shopify/use-the-refresh-batch-metric-in-the-cache"
BODY = (f"Refs {FORK_ISSUE}70\n\n## Summary\n\nExpose the refresh batch metric. Fixes #69, "
        "follows #68.\n\n## Test plan\n\n- unit tests\n")
SUMMARY = (f"Expose the refresh batch metric. {FORK_ISSUE}69, follows {FORK_ISSUE}68.\n\n"
           f"Shopify fork PR: {FORK_PULL}71\n")
ISSUE_TITLE = "DocDB: The refresh batch metric is missing"
ISSUE_BODY = ("### Description\n\nThe metric is missing.\n\n## Shopify context\n\nShopify needs "
              "it for its dashboards.\n\n### Issue Type\n\nkind/bug\n\n"
              "<!-- shopify-yb-publication:0123abcd -->\n")
PUBLIC_ISSUE_BODY = "### Description\n\nThe metric is missing.\n\n### Issue Type\n\nkind/bug\n"
PRIVATE_ISSUE = "https://github.com/owner/private-tracker/issues/9"
MISSING = "a" * 40
MERGED_LIST = ("pr", "list", "--repo", FORK, "--state", "merged", "-L", "5000", "--json",
               "number,title,mergeCommit,mergedAt,labels,author,url,baseRefName")
PROMPT_PLAN = "Test plan file path, e to write one in $EDITOR, q to quit: "
PROMPT_PUBLISH = "Publish? [y/N] "


def git(path, *args):
    result = subprocess.run(["git", "-C", path, *args], capture_output=True, text=True)
    if result.returncode:
        raise AssertionError(f"git {' '.join(args)} failed: {result.stderr}")
    return result.stdout.strip()


def read_file(value):
    with open(value, encoding="utf-8") as source:
        return ("file", source.read())


class FakeGitHub(GitHub):
    """Answers gh by its exact argv from canned responses and records every write in `log`.

    A canned value is returned as JSON (or as is when it is a string); an Error is raised.
    `repo set-default --view` answers per working directory. Anything else fails the test.
    """

    WRITES = {("issue", "create"), ("issue", "comment"), ("pr", "comment")}

    def __init__(self, log):
        super().__init__(executable="/nonexistent/gh")
        self.log = log
        self.responses = {}
        self.default_repos = {}
        self.created_issue = UPSTREAM_ISSUE + "38140"
        self.failing = set()  # (verb, subcommand) writes that fail

    def answer(self, *args, value):
        self.responses[tuple(args)] = value

    def run(self, *args, cwd=None):
        if args[:3] == ("repo", "set-default", "--view"):
            name = self.default_repos.get(os.path.realpath(cwd))
            if name is None:
                raise Error("Failed to run `gh repo set-default --view`\nno default repository "
                            "has been set\n")
            return name + "\n"
        if args[:2] == ("repo", "set-default"):
            self.log.append(("gh", *args, ("cwd", os.path.realpath(cwd))))
            self.default_repos[os.path.realpath(cwd)] = args[2]
            return ""
        if args[:2] in self.failing:
            raise Error(f"Failed to run `gh {' '.join(args[:2])}`\nHTTP 502\n")
        if args[:2] in self.WRITES:
            inlined = [read_file(value) if previous == "--body-file" else value
                       for previous, value in zip(("",) + args, args)]
            self.log.append(("gh", *inlined))
            return self.created_issue + "\n" if args[:2] == ("issue", "create") else ""
        if args == MERGED_LIST:
            return json.dumps([value for key, value in self.responses.items()
                               if key[:2] == ("pr", "view") and key[4] == FORK
                               and isinstance(value, dict) and value.get("state") == "MERGED"])
        if args not in self.responses:
            raise AssertionError(f"unexpected gh call: {args}")
        value = self.responses[args]
        if isinstance(value, Exception):
            raise value
        return value if isinstance(value, str) else json.dumps(value)


class FakeTool(Tool):
    """Records `gh stack`, `create-pr.sh`, `issues.py` and editor runs with their minimal git side
    effect; anything else (`git clone`) runs for real and is recorded too."""

    def __init__(self, log):
        self.log = log
        self.stacks = []  # [{"repo": path, "trunk": "master", "branches": [...]}]
        self.remote_stacks = {}  # parent PR url -> branches of its GitHub stack
        self.create_pr_code = 0
        self.pr_url = UPSTREAM_PULL + "38200"
        self.pull_requests = []  # (branch, message, parent sha) at each create-pr.sh run
        self.private_issues = {}
        self.editor_text = None
        self.editor_effect = None  # called with the editor's argv, as the developer's pause
        self.stack_installed = True
        self.gh_missing = False
        self.link_fails = False

    @staticmethod
    def result(argv, code=0, stdout=""):
        return subprocess.CompletedProcess(argv, code, stdout, "")

    def run(self, argv, cwd=None, echo=None, check=True):
        argv = list(argv)
        if argv == ["gh", "stack", "--help"]:
            if self.gh_missing:
                raise Error("gh was not found")
            return self.result(argv, 0 if self.stack_installed else 1)
        if argv[:2] == ["gh", "stack"]:
            return self.stack(argv, cwd, check)
        if os.path.basename(argv[0]) == "create-pr.sh":
            return self.create_pr(argv, cwd, echo)
        if len(argv) > 1 and os.path.basename(argv[1]) == "issues.py":
            self.log.append(("issues.py", *argv[2:]))
            if argv[2] == "inspect":
                return self.result(argv, stdout=json.dumps(self.private_issues[argv[4]]))
            if self.link_fails:
                raise Error("Failed to run `issues.py link`\nboom\n")
            return self.result(argv, stdout="{}\n")
        self.log.append(("run", *argv))
        return super().run(argv, cwd=cwd, echo=echo, check=check)

    def interactive(self, argv, cwd=None):
        self.log.append(("editor", *argv[:-1]))
        if self.editor_text is not None:
            Path(argv[-1]).write_text(self.editor_text)
        if self.editor_effect is not None:
            self.editor_effect()
        return 0

    def current(self, repo):
        branch = git(repo, "symbolic-ref", "--short", "HEAD")
        return next((stack for stack in self.stacks if stack["repo"] == os.path.realpath(repo)
                     and branch in stack["branches"]), None)

    def stack(self, argv, cwd, check):
        self.log.append(tuple(argv))
        command, rest, repo = argv[2], argv[3:], os.path.realpath(cwd)
        if command == "init":
            if len(rest) == 1:
                git(cwd, "checkout", "--quiet", "-b", rest[0], "master")
            else:
                git(cwd, "checkout", "--quiet", rest[0])
                git(cwd, "checkout", "--quiet", "-b", rest[1])
            self.stacks.append({"repo": repo, "trunk": "master", "branches": list(rest)})
        elif command == "rebase":
            remote = rest[rest.index("--remote") + 1]
            git(cwd, "fetch", "--quiet", remote)
            git(cwd, "branch", "--quiet", "-f", "master", f"{remote}/master")
            git(cwd, "rebase", "--quiet", "master")
        elif command == "checkout":
            branches = self.remote_stacks[rest[0]]
            for branch in branches:
                git(cwd, "fetch", "--quiet", "upstream",
                    f"+refs/heads/{branch}:refs/remotes/upstream/{branch}")
                if not git(cwd, "branch", "--list", branch):
                    git(cwd, "branch", "--quiet", "--no-track", branch, f"upstream/{branch}")
            git(cwd, "checkout", "--quiet", branches[-1])
            if self.current(cwd) is None:
                self.stacks.append({"repo": repo, "trunk": "master", "branches": list(branches)})
        elif command == "add":
            stack = self.current(cwd)
            git(cwd, "checkout", "--quiet", "-b", rest[0])
            stack["branches"].append(rest[0])
        elif command == "view":
            stack = self.current(cwd)
            if stack is None:
                if check:
                    raise Error("not in a stack")
                return self.result(argv, 1)
            view = {"trunk": stack["trunk"], "branches": [
                {"name": name, "isMerged": False, "pr": None} for name in stack["branches"]]}
            return self.result(argv, stdout=json.dumps(view))
        return self.result(argv)

    def create_pr(self, argv, cwd, echo):
        inlined = [read_file(value) if previous in ("-d", "-T", "-U") else value
                   for previous, value in zip([""] + argv[1:], argv[1:])]
        self.log.append(("create-pr.sh", *inlined))
        self.pull_requests.append((git(cwd, "symbolic-ref", "--short", "HEAD"),
                                   git(cwd, "log", "-1", "--format=%B"),
                                   git(cwd, "rev-parse", "HEAD^")))
        output = f">>> creating PR\n{self.pr_url}\n>>> done: {self.pr_url}\n"
        if self.create_pr_code:
            output = ">>> lint\n"
        if echo is not None:
            echo.write(output)
        return self.result(argv, self.create_pr_code, output)


def fork_pr(number, title, merge, body=BODY, state="MERGED", base="master-shopify", labels=(),
            reviews=(("reviewer", "APPROVED"),)):
    return {"number": number, "title": title, "body": body, "state": state, "baseRefName": base,
            "mergeCommit": {"oid": merge} if merge else None,
            "labels": [{"name": label} for label in labels],
            "reviews": [{"author": {"login": login}, "state": state} for login, state in reviews],
            "reviewDecision": "APPROVED", "url": FORK_PULL + str(number),
            "author": {"login": "priya"}}


def comment(body, created_at="2026-11-02T00:00:00Z"):
    return {"body": body, "created_at": created_at}


def branch_pr(number, state, merge=None, fork_pr=71, owner="yugabyte"):
    """A `gh pr list --head` entry; by default the PR this tool opened for fork PR `fork_pr`."""
    return {"number": number, "url": UPSTREAM_PULL + str(number), "state": state,
            "mergeCommit": {"oid": merge} if merge else None,
            "headRepository": {"name": "yugabyte-db"}, "headRepositoryOwner": {"login": owner},
            "body": f"## Summary\n\nText.\n\nShopify fork PR: {FORK_PULL}{fork_pr}\n\n"
                    "## Test plan\n\n- t\n"}


def upstream_pr(number, state, head=BRANCH, merge=None):
    return {"number": number, "state": state, "url": UPSTREAM_PULL + str(number),
            "title": "Expose the refresh batch metric", "headRefName": head,
            "mergedAt": "2026-11-12T08:00:00Z" if state == "MERGED" else None,
            "mergeCommit": {"oid": merge} if merge else None}


class SubmitTestCase(testfixtures.RepoTestCase):
    """The fixture repository authors both histories: upstream `master` is its first commit, and
    the fork's `master-shopify` adds the squash landing of fork PR #71. The developer's upstream
    clone has the remote `upstream`."""

    def setUp(self):
        super().setUp()
        self.log = []
        remotes = os.path.join(self.tmpdir, "remotes")
        self.upstream_url = os.path.join(remotes, "yugabyte", "yugabyte-db.git")
        self.fork_url = os.path.join(remotes, "Shopify", "yugabyte-db.git")
        self.fixture.init_bare(self.upstream_url)
        self.fixture.init_bare(self.fork_url)
        self.base = self.write({"src/metric.cc": "base\n", "src/cache.cc": "base\n"},
                               "Upstream base")
        self.fixture.git("push", "--quiet", self.upstream_url, "main:refs/heads/master")
        self.fixture.git("checkout", "--quiet", "-b", "master-shopify")
        self.landing = self.write({"src/metric.cc": "metric\n"},
                                  "Expose the refresh batch metric (#71)\n\nFixes #69 and see #68.")
        self.publish_fork()
        self.dev = os.path.join(self.tmpdir, "work", "yugabyte-db")
        os.makedirs(os.path.dirname(self.dev))
        git(self.tmpdir, "clone", "--quiet", "-o", "upstream", self.upstream_url, self.dev)
        git(self.dev, "config", "gh-stack.remote", "upstream")
        self.plan = os.path.join(self.tmpdir, "testplan.md")
        Path(self.plan).write_text("- ran the metric tests on the branch\n")
        self.github = FakeGitHub(self.log)
        self.github.default_repos[os.path.realpath(self.dev)] = UPSTREAM
        self.tool = FakeTool(self.log)
        self.answer_pr(fork_pr(71, TITLE, self.landing))
        self.answer_comments(71, [])
        self.answer_comments(70, [])
        self.github.answer("api", "orgs/Shopify/teams/yugabyte-code-reviewers/memberships/reviewer",
                           value={"state": "active"})
        self.github.answer("auth", "status", value="")
        self.github.answer("issue", "view", "70", "--repo", FORK, "--json", "title,body,labels",
                           value={"title": ISSUE_TITLE, "body": ISSUE_BODY,
                                  "labels": [{"name": "kind/bug"}, {"name": "Yugabyte Contrib"},
                                             {"name": "area/docdb"}]})
        self.github.answer("label", "list", "--repo", UPSTREAM, "--limit", "1000", "--json", "name",
                           value=[{"name": "area/docdb"}, {"name": "kind/bug"},
                                  {"name": "area/ysql"}])
        self.answer_branch_prs(BRANCH, [])
        self.answer_branch_prs(CHILD, [])

    def write(self, files, message):
        for name, content in files.items():
            path = Path(self.fixture.path, name)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content)
        self.fixture.git("add", "-A")
        self.fixture.git("commit", "--quiet", "-m", message)
        return self.fixture.git("rev-parse", "HEAD")

    def publish_fork(self):
        self.fixture.git("push", "--quiet", "--force", self.fork_url,
                         "master-shopify:refs/heads/master-shopify")

    def answer_pr(self, pr):
        self.github.answer("pr", "view", str(pr["number"]), "--repo", FORK, "--json", PR_FIELDS,
                           value=pr)

    def answer_comments(self, number, comments):
        self.github.answer("api", f"repos/{FORK}/issues/{number}/comments", "--paginate",
                           "--slurp", value=[comments])

    def answer_branch_prs(self, branch, prs):
        self.github.answer("pr", "list", "--repo", UPSTREAM, "--head", branch, "--state", "all",
                           "--json", "number,url,state,mergeCommit,headRepository,"
                           "headRepositoryOwner,body", value=prs)

    def answer_upstream(self, view):
        self.github.answer("pr", "view", str(view["number"]), "--repo", UPSTREAM, "--json",
                           UPSTREAM_FIELDS, value=view)

    def submit(self, *argv, answers=None, cwd=None, urls=True):
        out, err = io.StringIO(), io.StringIO()
        extra = ["--fork-url", self.fork_url, "--upstream-url", self.upstream_url] if urls else []
        code = Cli.run(["submit", *argv, *extra], github=self.github, out=out, err=err,
                       tool=self.tool, interactive=answers is not None,
                       stdin=io.StringIO("".join(f"{answer}\n" for answer in answers or [])),
                       cwd=cwd or os.path.join(self.tmpdir, "work"), program="upstream.py",
                       workdir=os.path.join(self.tmpdir, "scratch"))
        return code, out.getvalue(), err.getvalue()

    def rerun(self, number, *options):
        return shlex.join(["upstream.py", "submit", str(number), "--repo-path", self.dev,
                           "--fork-url", self.fork_url, "--upstream-url", self.upstream_url,
                           *options])

    def publish(self, *argv, number=71):
        code, out, err = self.submit(str(number), "--repo-path", self.dev, *argv,
                                     "--test-plan", self.plan, "--yes")
        self.assertEqual((0, ""), (code, err), out)
        return out

    def writes(self):
        return [entry for entry in self.log
                if entry[0] in ("create-pr.sh", "issues.py") or entry[1:3] in (
                    ("issue", "create"), ("issue", "comment"), ("pr", "comment"))]

    def head(self, ref="HEAD"):
        return git(self.dev, "rev-parse", ref)

    def created_pull_request(self, issue, title, summary=SUMMARY, *flags):
        return ("create-pr.sh", "-i", issue, "-t", title, "-d", ("file", summary),
                "-T", ("file", "- ran the metric tests on the branch\n"), *flags)

    @staticmethod
    def association(number):
        """The `Upstream issue:` comment on fork issue #70."""
        return ("gh", "issue", "comment", "70", "--repo", FORK, "--body",
                f"Upstream issue: {UPSTREAM_ISSUE}{number}")

    def recorded(self, number=71, url=UPSTREAM_PULL + "38200"):
        return ("gh", "pr", "comment", str(number), "--repo", FORK, "--body", f"Upstream: {url}")


class HappyPathTest(SubmitTestCase):
    def test_two_non_interactive_runs_build_then_publish_every_record_in_order(self):
        code, out, err = self.submit("71", "--repo-path", self.dev)

        self.assertEqual((4, ""), (code, err), out)
        built = self.head()
        self.assertEqual([
            f"Built {BRANCH} at {built} in {self.dev}. Build it and run the change's tests there; "
            "that run is the test plan.",
            "Write what ran to a file and rerun:",
            "  " + self.rerun(71, "--test-plan", "testplan.md", "--yes"),
        ], out.splitlines()[-3:])
        self.assertEqual([], self.writes())
        self.assertEqual([("gh", "stack", "init", BRANCH),
                          ("gh", "stack", "rebase", "--remote", "upstream")], self.log)
        self.assertEqual((BRANCH, self.base), (git(self.dev, "symbolic-ref", "--short", "HEAD"),
                                               self.head("HEAD^")))

        out = self.publish()

        self.assertEqual([
            ("gh", "issue", "create", "--repo", UPSTREAM, "--title", ISSUE_TITLE, "--body-file",
             ("file", PUBLIC_ISSUE_BODY), "--label", "kind/bug,area/docdb"),
            ("gh", "issue", "comment", "70", "--repo", FORK, "--body",
             f"Upstream issue: {UPSTREAM_ISSUE}38140"),
            self.created_pull_request("38140", "DocDB: Expose the refresh batch metric"),
            self.recorded(),
        ], self.writes())
        self.assertEqual([(BRANCH, "[#38140] DocDB: Expose the refresh batch metric\n\n"
                           f"{FORK_ISSUE}69 and see {FORK_ISSUE}68.\n\n"
                           f"(cherry picked from commit {self.landing})", self.base)],
                         self.tool.pull_requests)
        self.assertEqual(1, sum(entry[:3] == ("gh", "stack", "init") for entry in self.log))
        self.assertEqual(f"Opened {UPSTREAM_PULL}38200", out.splitlines()[-1])

    def test_a_draft_passes_dash_capital_d_and_prints_the_ready_command(self):
        out = self.publish("--draft", "--upstream-issue", "38136")

        self.assertEqual(self.created_pull_request(
            "38136", "DocDB: Expose the refresh batch metric", SUMMARY, "-D"), self.writes()[1])
        self.assertEqual([f"Opened {UPSTREAM_PULL}38200 as a draft; mark it ready with:",
                          "  gh pr ready 38200 -R yugabyte/yugabyte-db"], out.splitlines()[-2:])

    def test_the_interactive_run_asks_for_the_test_plan_and_one_confirmation(self):
        code, out, err = self.submit("71", "--repo-path", self.dev, answers=[self.plan, "y"])

        self.assertEqual((0, ""), (code, err), out)
        self.assertIn(PROMPT_PLAN, out)
        self.assertIn(PROMPT_PUBLISH, out)
        self.assertIn(f'  Upstream issue: create "{ISSUE_TITLE}" with labels kind/bug, area/docdb',
                      out)
        self.assertEqual(4, len(self.writes()))

    def test_declining_to_publish_writes_nothing_and_stops_with_the_rerun_command(self):
        code, out, err = self.submit("71", "--repo-path", self.dev, answers=[self.plan, "n"])

        self.assertEqual((4, ""), (code, err), out)
        self.assertEqual([], self.writes())
        # The captured output has no newline after the prompt: a terminal echoes the answer.
        self.assertEqual([PROMPT_PUBLISH + "Not published. To publish later:",
                          "  " + self.rerun(71, "--test-plan", self.plan, "--yes")],
                         out.splitlines()[-2:])

    def test_e_writes_the_test_plan_in_the_editor(self):
        self.tool.editor_text = f"## What ran on {BRANCH}\n\n- the metric tests passed\n"

        with patch.dict(os.environ, {"EDITOR": "fake-editor --wait"}):
            code, out, err = self.submit("71", "--repo-path", self.dev, answers=["e", "y"])

        self.assertEqual((0, ""), (code, err), out)
        self.assertEqual(("editor", "fake-editor", "--wait"),
                         next(entry for entry in self.log if entry[0] == "editor"))
        self.assertEqual(("file", self.tool.editor_text),
                         next(entry for entry in self.writes()
                              if entry[0] == "create-pr.sh")[8])

    def test_a_non_interactive_run_with_a_test_plan_previews_and_stops_without_yes(self):
        code, out, err = self.submit("71", "--repo-path", self.dev, "--test-plan", self.plan)

        self.assertEqual((4, ""), (code, err), out)
        self.assertEqual([], self.writes())
        self.assertIn(f"  Pull request: [#?] DocDB: Expose the refresh batch metric (the new "
                      "upstream issue's number)", out)
        self.assertEqual(["To publish, rerun:",
                          "  " + self.rerun(71, "--test-plan", self.plan, "--yes")],
                         out.splitlines()[-2:])

    def test_yes_without_a_test_plan_is_a_usage_error(self):
        self.assertEqual((2, "", "--yes needs --test-plan\n"),
                         self.submit("71", "--repo-path", self.dev, "--yes"))


class CreatePullRequestTest(SubmitTestCase):
    def test_a_lint_failure_says_to_fix_it_as_a_new_commit_and_records_nothing(self):
        self.tool.create_pr_code = 3

        code, out, err = self.submit("71", "--repo-path", self.dev, "--upstream-issue", "38136",
                                     "--test-plan", self.plan, "--yes")

        self.assertEqual((1, "create-pr.sh: lint failed; fix as a new commit, rerun\n"),
                         (code, err))
        self.assertEqual([self.association(38136), "create-pr.sh"],
                         [self.writes()[0], self.writes()[1][0]])

    def test_a_rerun_after_a_lint_fix_keeps_the_fix_commit_and_opens_the_pr(self):
        self.tool.create_pr_code = 3
        self.submit("71", "--repo-path", self.dev, "--upstream-issue", "38136",
                    "--test-plan", self.plan, "--yes")
        Path(self.dev, "src", "metric.cc").write_text("metric, linted\n")
        git(self.dev, "commit", "--quiet", "-am", "Fix lint")
        self.tool.create_pr_code = 0

        self.publish("--upstream-issue", "38136")

        self.assertEqual("Fix lint", self.tool.pull_requests[-1][1])
        self.assertEqual(self.recorded(), self.writes()[-1])


class PickAboveTest(SubmitTestCase):
    """The branch holds the pick, citing fork issue #70 in Phase A, and a commit above it."""

    def setUp(self):
        super().setUp()
        self.assertEqual(4, self.submit("71", "--repo-path", self.dev)[0])
        Path(self.dev, "notes.txt").write_text("notes\n")
        git(self.dev, "add", "notes.txt")
        git(self.dev, "commit", "--quiet", "-m", "Add notes")

    def test_an_issue_to_create_is_refused_before_any_write_when_the_pick_is_not_on_top(self):
        code, out, err = self.submit("71", "--repo-path", self.dev, "--test-plan", self.plan,
                                     "--yes")

        self.assertEqual((1, f"The cherry-pick of {self.landing[:9]} is not the top commit of "
                             f"{BRANCH}, and its subject must cite the upstream issue this run "
                             "creates; remove the commits above it and rerun, or create the "
                             "issue first and pass --upstream-issue\n"), (code, err))
        self.assertEqual([], self.writes())

    def test_a_pick_below_the_top_with_another_issue_in_its_subject_is_refused(self):
        code, out, err = self.submit("71", "--repo-path", self.dev, "--upstream-issue", "38136",
                                     "--test-plan", self.plan, "--yes")

        self.assertEqual((1, f"The cherry-pick of {self.landing[:9]} is not the top commit of "
                             f"{BRANCH} and its subject is not [#38136] DocDB: Expose the refresh "
                             "batch metric; reword it by hand, then rerun\n"), (code, err))
        self.assertEqual([], self.writes())


class CheckoutTest(SubmitTestCase):
    def test_the_working_directory_is_used_when_its_default_repository_is_upstream(self):
        code, out, err = self.submit("71", cwd=self.dev)

        self.assertEqual((4, ""), (code, err), out)
        self.assertEqual("  " + self.rerun(71, "--test-plan", "testplan.md", "--yes"),
                         out.splitlines()[-1])

    def test_a_working_directory_without_upstream_as_its_default_repository_is_not_used(self):
        del self.github.default_repos[os.path.realpath(self.dev)]
        cwd = os.path.join(self.dev, "src")

        code, out, err = self.submit("71", cwd=cwd)

        self.assertEqual((4, ""), (code, err), out)
        self.assertEqual(f"No {UPSTREAM} checkout found in {cwd}. Rerun with an existing "
                         "checkout or a directory to clone into:", out.splitlines()[0])
        self.assertEqual([], self.log)
        self.assertNotIn(BRANCH, git(self.dev, "branch", "--list"))

    def test_a_named_checkout_without_a_default_repository_gets_it_set(self):
        del self.github.default_repos[os.path.realpath(self.dev)]

        code, out, err = self.submit("71", "--repo-path", self.dev)

        self.assertEqual((4, ""), (code, err), out)
        self.assertEqual(("gh", "repo", "set-default", UPSTREAM,
                          ("cwd", os.path.realpath(self.dev))), self.log[0])

    def test_a_discovered_checkout_without_gh_stack_remote_is_refused_not_changed(self):
        git(self.dev, "config", "--unset", "gh-stack.remote")

        code, out, err = self.submit("71", cwd=self.dev)

        self.assertEqual((1, f"gh-stack.remote is not set in {self.dev}; run `git config "
                             "gh-stack.remote upstream` there first, or name the checkout with "
                             "--repo-path\n"), (code, err))
        self.assertIsNone(subprocess.run(["git", "-C", self.dev, "config", "gh-stack.remote"],
                                         capture_output=True, text=True).stdout.strip() or None)

    def test_a_missing_gh_is_reported_as_missing_gh(self):
        self.tool.gh_missing = True

        code, out, err = self.submit("71", "--clone-to", os.path.join(self.tmpdir, "clone"))

        self.assertEqual((1, "gh was not found; install gh and run `gh auth login`\n"),
                         (code, err))
        self.assertEqual([], self.log)

    def test_unreadable_files_are_refused_before_any_checkout_or_build(self):
        missing = os.path.join(self.tmpdir, "missing.md")
        for option, what in (("--test-plan", "test plan"), ("--upgrade", "upgrade")):
            with self.subTest(option=option):
                code, out, err = self.submit("71", option, missing)

                self.assertEqual((1, "", f"The {what} file {missing} is missing or empty\n"),
                                 (code, out, err))
                self.assertEqual([], self.log)

    def test_a_bad_branch_name_is_a_usage_error_before_any_checkout(self):
        code, out, err = self.submit("71", "--branch", "Metric_Name")

        self.assertEqual((2, "", "--branch Metric_Name is not a lowercase hyphenated name\n"),
                         (code, out, err))
        self.assertEqual([], self.log)

    def test_a_missing_gh_stack_is_refused_before_any_clone(self):
        self.tool.stack_installed = False

        code, out, err = self.submit("71", "--clone-to", os.path.join(self.tmpdir, "clone"))

        self.assertEqual((1, "gh stack is not installed; install it as stack-prs § Setup "
                             "describes: gh extension install github/gh-stack\n"), (code, err))
        self.assertEqual([], self.log)

    def test_the_checkout_stop_repeats_every_option_given(self):
        self.tool.private_issues[PRIVATE_ISSUE] = {"public_issues": []}
        options = ["--parent", "70", "--upstream-issue", "38136", "--private-issue",
                   PRIVATE_ISSUE, "--branch", "metric", "--draft"]
        tail = ["--fork-url", self.fork_url, "--upstream-url", self.upstream_url, "--no-fetch",
                "--test-plan", self.plan, "--yes"]

        code, out, err = self.submit("71", *options, "--no-fetch", "--test-plan", self.plan,
                                     "--yes")

        self.assertEqual((4, ""), (code, err), out)
        self.assertEqual([
            "  " + shlex.join(["upstream.py", "submit", "71", "--repo-path", "<checkout>",
                               *options, *tail]),
            "  " + shlex.join(["upstream.py", "submit", "71", "--clone-to",
                               os.path.join(self.tmpdir, "yugabyte-db-upstream"), *options,
                               *tail]),
        ], out.splitlines()[1:])

    def test_without_a_checkout_a_non_interactive_run_stops_with_both_rerun_forms(self):
        proposed = os.path.join(self.tmpdir, "yugabyte-db-upstream")

        code, out, err = self.submit("71")

        self.assertEqual((4, ""), (code, err), out)
        self.assertEqual([
            f"No {UPSTREAM} checkout found in {os.path.join(self.tmpdir, 'work')}. Rerun with "
            "an existing checkout or a directory to clone into:",
            "  " + shlex.join(["upstream.py", "submit", "71", "--repo-path", "<checkout>",
                               "--fork-url", self.fork_url, "--upstream-url", self.upstream_url]),
            "  " + shlex.join(["upstream.py", "submit", "71", "--clone-to", proposed,
                               "--fork-url", self.fork_url, "--upstream-url", self.upstream_url]),
        ], out.splitlines())
        self.assertEqual([], self.log)

    def test_the_prompt_takes_the_path_of_an_existing_checkout(self):
        code, out, err = self.submit("71", answers=["1", self.dev, "q"])

        self.assertEqual((4, ""), (code, err), out)
        self.assertIn("  2  clone yugabyte/yugabyte-db to "
                      f"{os.path.join(self.tmpdir, 'yugabyte-db-upstream')}", out)
        self.assertEqual(BRANCH, git(self.dev, "symbolic-ref", "--short", "HEAD"))

    def test_the_prompt_clones_upstream_and_sets_it_up_for_gh_stack(self):
        proposed = os.path.join(self.tmpdir, "yugabyte-db-upstream")

        code, out, err = self.submit("71", answers=["2", "q"])

        self.assertEqual((4, ""), (code, err), out)
        self.assertEqual([("run", "git", "clone", "-o", "upstream", self.upstream_url, proposed),
                          ("gh", "repo", "set-default", UPSTREAM,
                           ("cwd", os.path.realpath(proposed)))], self.log[:2])
        self.assertEqual("upstream", git(proposed, "config", "gh-stack.remote"))
        self.assertEqual(BRANCH, git(proposed, "symbolic-ref", "--short", "HEAD"))

    def test_clone_to_clones_without_asking(self):
        target = os.path.join(self.tmpdir, "elsewhere", "upstream")

        code, out, err = self.submit("71", "--clone-to", target)

        self.assertEqual((4, ""), (code, err), out)
        self.assertEqual(("run", "git", "clone", "-o", "upstream", self.upstream_url, target),
                         self.log[0])

    def test_quitting_at_the_checkout_prompt_stops(self):
        code, out, err = self.submit("71", answers=["q"])

        self.assertEqual((4, ""), (code, err), out)
        self.assertEqual([], self.log)

    def test_a_dirty_work_tree_is_refused(self):
        Path(self.dev, "src", "metric.cc").write_text("local edit\n")

        self.assertEqual((1, "", f"{self.dev} has uncommitted changes to tracked files; commit or "
                                 "stash them first\n"),
                         self.submit("71", "--repo-path", self.dev))

    def test_a_checkout_without_an_upstream_remote_is_refused(self):
        git(self.dev, "remote", "rename", "upstream", "mine")
        git(self.dev, "remote", "set-url", "mine", "https://example.invalid/owner/fork.git")

        self.assertEqual((1, "", f"No remote of {self.dev} points at {UPSTREAM}; add one, as "
                                 "stack-prs § Setup describes\n"),
                         self.submit("71", "--repo-path", self.dev))

    def test_a_default_repository_other_than_upstream_is_refused(self):
        self.github.default_repos[os.path.realpath(self.dev)] = FORK

        self.assertEqual((1, "", f"gh's default repository in {self.dev} is {FORK}; run `gh repo "
                                 f"set-default {UPSTREAM}` there first\n"),
                         self.submit("71", "--repo-path", self.dev))


class PreconditionTest(SubmitTestCase):
    def refused(self, *argv, number="71"):
        code, out, err = self.submit(number, "--repo-path", self.dev, *argv)
        self.assertEqual((1, ""), (code, out))
        self.assertEqual([], self.log)
        return err

    def test_an_unmerged_fork_pr_is_refused(self):
        self.answer_pr(fork_pr(71, TITLE, None, state="OPEN"))

        self.assertEqual("Fork PR #71 is OPEN, not merged: a fork PR merges into master-shopify "
                         "before it is submitted upstream\n", self.refused())

    def test_a_fork_pr_without_an_approval_from_the_reviewers_team_is_refused(self):
        self.answer_pr(fork_pr(71, TITLE, self.landing,
                               reviews=(("outsider", "APPROVED"), ("reviewer", "COMMENTED"))))
        self.github.answer("api", "orgs/Shopify/teams/yugabyte-code-reviewers/memberships/outsider",
                           value=Error("Failed to run `gh api`\ngh: Not Found (HTTP 404)\n"))

        self.assertEqual("Fork PR #71 has no approving review from a "
                         "Shopify/yugabyte-code-reviewers member, or this login cannot read the "
                         "team's membership\n", self.refused())

    def test_fork_only_work_is_refused(self):
        self.answer_pr(fork_pr(71, "[#45][shopify-only] Build: Share the helpers", self.landing,
                               labels=["upstream: skip"]))

        self.assertEqual("Fork PR #71 is fork-only work (upstream: skip): it does not go "
                         "upstream; remove the marker that is wrong before submitting\n",
                         self.refused())

    def test_a_shopify_only_title_without_the_label_is_refused_as_a_disagreement(self):
        self.answer_pr(fork_pr(71, "[#45][shopify-only] Build: Share the helpers", self.landing))

        self.assertEqual("Fork PR #71 is fork-only work (#71 is titled [shopify-only] but has no "
                         "upstream: skip label: the label decides, so it counts as "
                         "upstream-bound): it does not go upstream; remove the marker that is "
                         "wrong before submitting\n", self.refused())

    def test_a_title_without_an_issue_reference_is_refused(self):
        self.answer_pr(fork_pr(71, "DocDB: Expose the refresh batch metric", self.landing))

        self.assertEqual("Fork PR #71's title DocDB: Expose the refresh batch metric does not "
                         "follow [#N] Component: Short description\n", self.refused())

    def test_a_component_create_pr_cannot_take_is_refused_in_phase_a(self):
        for component in ("DocDB,YSQL", "yb-master", "build_tools"):
            with self.subTest(component=component):
                self.answer_pr(fork_pr(71, f"[#70] {component}: Expose the refresh batch metric",
                                       self.landing))

                self.assertEqual(f"Fork PR #71's component {component} is not one create-pr.sh "
                                 "accepts (a letter, then letters and digits); open the upstream "
                                 "PR by hand as shopify-upstream-pr Step 5 describes\n",
                                 self.refused())

    def already_submitted(self, view):
        self.answer_comments(71, [comment(f"Upstream: {UPSTREAM_PULL}38200")])
        self.answer_upstream(view)
        return self.submit("71", "--repo-path", self.dev)

    def test_a_fork_pr_with_an_open_upstream_pr_is_reported_and_nothing_is_done(self):
        self.assertEqual((0, f"Fork PR #71 is already submitted upstream: {UPSTREAM_PULL}38200 "
                             "(open); nothing to do\n", ""),
                         self.already_submitted(upstream_pr(38200, "OPEN")))
        self.assertEqual([], self.log)

    def test_a_fork_pr_whose_upstream_pr_landed_is_reported_and_nothing_is_done(self):
        self.assertEqual((0, f"Fork PR #71 is already submitted upstream: {UPSTREAM_PULL}38200 "
                             "(landed on upstream master); nothing to do\n", ""),
                         self.already_submitted(upstream_pr(38200, "MERGED", merge=self.base)))
        self.assertEqual([], self.log)

    def test_an_upstream_pr_closed_without_merging_is_not_silently_replaced(self):
        self.answer_comments(71, [comment(f"Upstream: {UPSTREAM_PULL}38200")])
        self.answer_upstream(upstream_pr(38200, "CLOSED"))

        self.assertEqual(f"Fork PR #71's upstream PR {UPSTREAM_PULL}38200 was closed without "
                         "merging; note the outcome on the fork PR and raise it with the team\n",
                         self.refused())

    def test_a_restructured_upstream_pr_gets_a_replacement_and_one_new_record(self):
        self.answer_comments(71, [comment(f"Upstream: {UPSTREAM_PULL}38190")])
        self.answer_upstream(upstream_pr(38190, "MERGED", merge=MISSING))
        self.answer_branch_prs(BRANCH, [branch_pr(38190, "MERGED", MISSING)])

        out = self.publish("--upstream-issue", "38136")

        self.assertIn(f"Fork PR #71's upstream PR {UPSTREAM_PULL}38190 was merged without "
                      "landing on upstream master: it was restructured. After the build and the "
                      "test plan, this run opens a replacement PR and records it in a new "
                      "Upstream: comment.", out)
        self.assertEqual(self.association(38136), self.writes()[0])
        self.assertEqual(["gh", "create-pr.sh", "gh"], [entry[0] for entry in self.writes()])
        self.assertEqual(self.recorded(), self.writes()[2])

    def test_a_replacement_create_pr_refuses_points_at_the_by_hand_replay(self):
        self.answer_comments(71, [comment(f"Upstream: {UPSTREAM_PULL}38190")])
        self.answer_upstream(upstream_pr(38190, "MERGED", merge=MISSING))
        self.tool.create_pr_code = 1

        code, out, err = self.submit("71", "--repo-path", self.dev, "--upstream-issue", "38136",
                                     "--test-plan", self.plan, "--yes")

        self.assertEqual((1, "create-pr.sh refused (exit 1); its message above says why. A "
                             "replacement for a restructured upstream PR is opened by replaying "
                             "create-pr.sh's calls by hand, as shopify-upstream-pr Step 5 "
                             "describes for a restructured layer\n"), (code, err))


class LandingTest(SubmitTestCase):
    def add_pr(self, number, title, merge):
        self.answer_pr(fork_pr(number, title, merge))
        self.answer_comments(number, [])
        self.answer_branch_prs("feature-stack/Shopify/" + slug(title.split(": ", 1)[1]), [])
        self.publish_fork()

    def test_a_merge_landing_is_refused(self):
        self.fixture.git("checkout", "--quiet", "-b", "feature")
        self.write({"src/cache.cc": "one\n"}, "first")
        self.write({"src/cache.cc": "two\n"}, "second")
        self.fixture.git("checkout", "--quiet", "master-shopify")
        self.fixture.git("merge", "--quiet", "--no-ff", "-m", "Merge pull request #73", "feature")
        merge = self.fixture.git("rev-parse", "HEAD")
        self.add_pr(73, "[#70] DocDB: Cache the refresh batch metric", merge)

        code, out, err = self.submit("73", "--repo-path", self.dev)

        self.assertEqual((1, f"Fork PR #73 landed on master-shopify as {merge[:9]}, not a single "
                             "squash commit; cherry-pick it by hand as shopify-upstream-pr Step 3 "
                             "describes\n"), (code, err))

    def replayed_stack(self, *numbers):
        """A sync's single-parent replay of a stack landing, naming `numbers` in its trailer."""
        landing = self.write({"src/cache.cc": "layers\n"},
                             "Merge pull request #42 from Shopify/stack\n\nShopify-PR: "
                             + " ".join(f"#{n}" for n in numbers))
        self.add_pr(41, "[#70] DocDB: Cache the refresh batch metric layer one", MISSING)
        self.add_pr(42, "[#70] DocDB: Cache the refresh batch metric layer two", MISSING)
        return landing

    def test_a_replayed_stack_landing_is_refused_for_each_of_its_prs(self):
        landing = self.replayed_stack(41, 42)
        for number, other in ((41, 42), (42, 41)):
            with self.subTest(number=number):
                code, out, err = self.submit(str(number), "--repo-path", self.dev)

                self.assertEqual((1, f"Fork PR #{number} landed on master-shopify as "
                                     f"{landing[:9]} together with #{other}: the stack landed "
                                     "whole; cherry-pick it by hand as shopify-upstream-pr Step 3 "
                                     "describes\n"), (code, err))
                self.assertEqual([], [entry for entry in self.log if entry[:2] == ("gh", "stack")])

    def test_a_replayed_landing_of_one_pr_is_accepted(self):
        landing = self.replayed_stack(41)

        self.publish("--upstream-issue", "38136", number=41)

        self.assertIn(f"(cherry picked from commit {landing})", self.tool.pull_requests[0][1])

    def test_a_landing_another_pr_records_as_its_merge_commit_is_refused(self):
        landing = self.write({"src/cache.cc": "layers\n"},
                             "Cache the refresh batch metric\n\nShopify-PR: #41")
        self.add_pr(41, "[#70] DocDB: Cache the refresh batch metric layer one", MISSING)
        self.add_pr(42, "[#70] DocDB: Cache the refresh batch metric layer two", landing)

        code, out, err = self.submit("41", "--repo-path", self.dev)

        self.assertEqual((1, f"Fork PR #41 landed on master-shopify as {landing[:9]} together "
                             "with #42: the stack landed whole; cherry-pick it by hand as "
                             "shopify-upstream-pr Step 3 describes\n"), (code, err))
        self.assertEqual([], [entry for entry in self.log if entry[:2] == ("gh", "stack")])

    def test_a_reverted_pr_and_its_revert_are_refused(self):
        fixed = self.write({"src/cache.cc": "fixed\n"}, "Fix the cache (#12)")
        self.fixture.git("revert", "--no-commit", fixed)
        self.fixture.git("commit", "--quiet", "-m",
                         f'Revert "Fix the cache" (#15)\n\nThis reverts commit {fixed}.')
        reverting = self.fixture.git("rev-parse", "HEAD")
        self.add_pr(12, "[#70] DocDB: Fix the cache", fixed)
        self.add_pr(15, '[#70] DocDB: Revert "Fix the cache"', reverting)

        code, out, err = self.submit("12", "--repo-path", self.dev)
        self.assertEqual((1, "Fork PR #12 was reverted in the fork by #15; the fork carries "
                             "nothing from it, so it does not go upstream\n"), (code, err))
        code, out, err = self.submit("15", "--repo-path", self.dev)
        self.assertEqual((1, "Fork PR #15 reverts fork PR #12, which was never submitted; the "
                             "fork carries nothing from the pair, so it does not go upstream\n"),
                         (code, err))
        self.assertEqual([], [entry for entry in self.log if entry[:2] == ("gh", "stack")])

    def test_a_pr_without_a_landing_is_refused(self):
        self.add_pr(75, "[#70] DocDB: Cache the refresh batch metric", MISSING)

        code, out, err = self.submit("75", "--repo-path", self.dev)

        self.assertEqual((1, f"Fork PR #75 has no commit on master-shopify's first-parent line "
                             f"since upstream {self.base[:9]}; nothing to cherry-pick\n"),
                         (code, err))

    def test_a_proto_change_needs_upgrade_notes_before_anything_is_published(self):
        landing = self.write({"src/meta.proto": "message Meta {}\n"},
                             "Add the meta message (#74)")
        self.add_pr(74, "[#70] DocDB: Add the refresh batch meta message", landing)

        code, out, err = self.submit("74", "--repo-path", self.dev, "--test-plan", self.plan,
                                     "--yes")

        self.assertEqual((1, "The commit changes .proto files (src/meta.proto); pass --upgrade "
                             "<file> with upgrade and rollback notes. Rerun with them:\n  "
                             + self.rerun(74, "--upgrade", "upgrade.md", "--test-plan", self.plan,
                                          "--yes") + "\n"), (code, err))
        self.assertEqual([], self.writes())

    def test_an_interactive_proto_change_is_refused_before_the_test_plan_prompt(self):
        landing = self.write({"src/meta.proto": "message Meta {}\n"},
                             "Add the meta message (#74)")
        self.add_pr(74, "[#70] DocDB: Add the refresh batch meta message", landing)

        code, out, err = self.submit("74", "--repo-path", self.dev, answers=["e", "y"])

        self.assertEqual((1, "The commit changes .proto files (src/meta.proto); pass --upgrade "
                             "<file> with upgrade and rollback notes. Rerun with them:\n  "
                             + self.rerun(74, "--upgrade", "upgrade.md") + "\n"), (code, err))
        self.assertNotIn(PROMPT_PLAN, out)
        self.assertEqual([], [entry for entry in self.log if entry[0] == "editor"])

    def test_a_proto_change_passes_its_upgrade_notes(self):
        landing = self.write({"src/meta.proto": "message Meta {}\n"},
                             "Add the meta message (#74)")
        self.add_pr(74, "[#70] DocDB: Add the refresh batch meta message", landing)
        upgrade = os.path.join(self.tmpdir, "upgrade.md")
        Path(upgrade).write_text("New optional message; old binaries ignore it.\n")

        self.publish("--upgrade", upgrade, "--upstream-issue", "38136", number=74)

        self.assertEqual(("-U", ("file", "New optional message; old binaries ignore it.\n")),
                         self.writes()[1][9:11])

    def test_the_rerun_command_asks_for_upgrade_notes_when_protos_change(self):
        landing = self.write({"src/meta.proto": "message Meta {}\n"},
                             "Add the meta message (#74)")
        self.add_pr(74, "[#70] DocDB: Add the refresh batch meta message", landing)

        code, out, err = self.submit("74", "--repo-path", self.dev)

        self.assertEqual((1, "  " + self.rerun(74, "--upgrade", "upgrade.md", "--test-plan",
                                               "testplan.md", "--yes")),
                         (code, err.splitlines()[-1]))

    def test_a_sync_replayed_landing_is_picked_instead_of_the_merge_commit(self):
        self.fixture.git("checkout", "--quiet", "-b", "replayed", self.base)
        replayed = self.write({"src/metric.cc": "metric\n"},
                              "Expose the refresh batch metric (#71)\n\nFixes #69 and see #68.")
        self.fixture.git("branch", "--quiet", "-f", "master-shopify", replayed)
        self.publish_fork()

        self.publish("--upstream-issue", "38136")

        self.assertIn(f"(cherry picked from commit {replayed})", self.tool.pull_requests[0][1])


class IssueTest(SubmitTestCase):
    def pull_request_issue(self):
        return next(entry for entry in self.writes() if entry[0] == "create-pr.sh")[2]

    def test_a_refs_line_naming_an_upstream_issue_is_reused(self):
        body = BODY.replace(f"{FORK_ISSUE}70", f"{UPSTREAM_ISSUE}38130")
        self.answer_pr(fork_pr(71, "[#38130] DocDB: Expose the refresh batch metric",
                               self.landing, body))

        self.publish()

        self.assertEqual("38130", self.pull_request_issue())
        self.assertEqual(2, len(self.writes()))

    def test_an_upstream_issue_comment_on_the_fork_issue_is_reused(self):
        self.answer_comments(70, [comment(f"Upstream issue: {UPSTREAM_ISSUE}38131",
                                          "2026-11-01T00:00:00Z"),
                                  comment(f"Upstream issue: {UPSTREAM_ISSUE}38135")])

        self.publish()

        self.assertEqual("38135", self.pull_request_issue())
        self.assertEqual(2, len(self.writes()))

    def test_upstream_issue_overrides_the_lookup(self):
        self.publish("--upstream-issue", "38136")

        self.assertEqual("38136", self.pull_request_issue())

    def test_an_upstream_issue_recorded_in_the_private_tracker_is_reused(self):
        self.tool.private_issues[PRIVATE_ISSUE] = {
            "public_issues": [f"{FORK_ISSUE}70", f"{UPSTREAM_ISSUE}38137"]}

        self.publish("--private-issue", PRIVATE_ISSUE)

        self.assertEqual(("issues.py", "inspect", "--issue", PRIVATE_ISSUE), self.log[0])
        self.assertEqual("38137", self.pull_request_issue())

    def test_a_created_issue_is_recorded_on_the_fork_issue_and_in_the_private_tracker(self):
        self.tool.private_issues[PRIVATE_ISSUE] = {"public_issues": [f"{FORK_ISSUE}70"]}

        self.publish("--private-issue", PRIVATE_ISSUE)

        self.assertEqual([
            ("gh", "issue", "create", "--repo", UPSTREAM, "--title", ISSUE_TITLE, "--body-file",
             ("file", PUBLIC_ISSUE_BODY), "--label", "kind/bug,area/docdb"),
            ("gh", "issue", "comment", "70", "--repo", FORK, "--body",
             f"Upstream issue: {UPSTREAM_ISSUE}38140"),
            ("issues.py", "link", "--issue", PRIVATE_ISSUE, "--public-issue",
             f"{UPSTREAM_ISSUE}38140"),
        ], self.writes()[1:4])

    def test_a_reused_upstream_issue_missing_from_the_private_tracker_is_linked(self):
        self.answer_comments(70, [comment(f"Upstream issue: {UPSTREAM_ISSUE}38135")])
        self.tool.private_issues[PRIVATE_ISSUE] = {"public_issues": [f"{FORK_ISSUE}70"]}

        self.publish("--private-issue", PRIVATE_ISSUE)

        self.assertEqual([("issues.py", "inspect", "--issue", PRIVATE_ISSUE),
                          ("issues.py", "link", "--issue", PRIVATE_ISSUE, "--public-issue",
                           f"{UPSTREAM_ISSUE}38135")],
                         [entry for entry in self.writes() if entry[0] == "issues.py"])

    def test_an_upstream_issue_already_in_the_private_tracker_is_not_linked_again(self):
        self.tool.private_issues[PRIVATE_ISSUE] = {"public_issues": [f"{UPSTREAM_ISSUE}38136"]}

        self.publish("--upstream-issue", "38136", "--private-issue", PRIVATE_ISSUE)

        self.assertEqual([("issues.py", "inspect", "--issue", PRIVATE_ISSUE)],
                         [entry for entry in self.writes() if entry[0] == "issues.py"])

    def test_a_failed_fork_issue_record_after_creating_the_issue_says_how_to_rerun(self):
        self.github.failing.add(("issue", "comment"))

        code, out, err = self.submit("71", "--repo-path", self.dev, "--test-plan", self.plan,
                                     "--yes")

        self.assertEqual(1, code)
        self.assertTrue(err.endswith(
            f"Created {UPSTREAM_ISSUE}38140 but did not record it; record it with `gh issue "
            f"comment 70 --repo {FORK} --body 'Upstream issue: {UPSTREAM_ISSUE}38140'`, then "
            "rerun with `--upstream-issue 38140`\n"), err)
        self.assertEqual(["gh"], [entry[0] for entry in self.writes()])

    def test_a_failed_private_link_after_creating_the_issue_says_how_to_rerun(self):
        self.tool.private_issues[PRIVATE_ISSUE] = {"public_issues": []}
        self.tool.link_fails = True

        code, out, err = self.submit("71", "--repo-path", self.dev, "--private-issue",
                                     PRIVATE_ISSUE, "--test-plan", self.plan, "--yes")

        self.assertEqual(1, code)
        self.assertTrue(err.endswith(f"Created {UPSTREAM_ISSUE}38140 but did not record it in "
                                     "the private tracker; rerun with `--upstream-issue "
                                     "38140`\n"), err)

    def test_a_created_issue_without_shared_labels_passes_no_label_option(self):
        self.github.answer("label", "list", "--repo", UPSTREAM, "--limit", "1000", "--json",
                           "name", value=[{"name": "area/ysql"}])

        self.publish()

        self.assertEqual(("gh", "issue", "create", "--repo", UPSTREAM, "--title", ISSUE_TITLE,
                          "--body-file", ("file", PUBLIC_ISSUE_BODY)), self.writes()[0])

    def test_a_refs_line_that_disagrees_with_the_title_is_refused(self):
        self.answer_pr(fork_pr(71, "[#72] DocDB: Expose the refresh batch metric", self.landing))

        code, out, err = self.submit("71", "--repo-path", self.dev)

        self.assertEqual((1, f"Fork PR #71's title cites #72 but its Refs line names "
                             f"{FORK_ISSUE}70\n"), (code, err))


class StackTest(SubmitTestCase):
    """Fork PR #72 is the layer above #71, whose upstream PR #38150 is open on BRANCH."""

    def setUp(self):
        super().setUp()
        self.child = self.write({"src/cache.cc": "uses the metric\n"},
                                "Use the refresh batch metric in the cache (#72)")
        self.publish_fork()
        self.fixture.git("checkout", "--quiet", "-b", "parent", self.base)
        self.write({"src/metric.cc": "metric\n"}, "[#38130] DocDB: Expose the refresh batch metric")
        self.fixture.git("push", "--quiet", self.upstream_url, f"parent:refs/heads/{BRANCH}")
        self.parent_tip = self.fixture.git("rev-parse", "HEAD")
        self.answer_pr(fork_pr(72, "[#70] DocDB: Use the refresh batch metric in the cache",
                               self.child, base="priya/expose-metric"))
        self.answer_comments(72, [])
        self.github.answer("pr", "list", "--repo", FORK, "--state", "merged", "--head",
                           "priya/expose-metric", "--json", "number,url",
                           value=[{"number": 71, "url": FORK_PULL + "71"}])
        self.answer_comments(71, [comment(f"Upstream: {UPSTREAM_PULL}38150")])
        self.answer_upstream(upstream_pr(38150, "OPEN"))
        self.github.answer("api", f"repos/{UPSTREAM}/pulls/38150", "--jq", ".stack",
                           value="null\n")

    def stacked_summary(self):
        return (f"Expose the refresh batch metric. {FORK_ISSUE}69, follows {FORK_ISSUE}68.\n\n"
                f"Shopify fork PR: {FORK_PULL}72\n"
                f"Stacked on #38150, based on {BRANCH}, so the diff is exactly this PR's commit.\n")

    def test_a_layer_above_an_open_upstream_pr_outside_a_github_stack_starts_one(self):
        self.publish("--upstream-issue", "38136", number=72)

        self.assertEqual([("gh", "stack", "init", BRANCH, CHILD)],
                         [entry for entry in self.log if entry[:2] == ("gh", "stack")])
        self.assertEqual((CHILD, self.parent_tip), (self.tool.pull_requests[0][0],
                                                    self.tool.pull_requests[0][2]))
        self.assertEqual(self.created_pull_request(
            "38136", "DocDB: Use the refresh batch metric in the cache", self.stacked_summary()),
            self.writes()[1])
        self.assertEqual(self.recorded(72), self.writes()[2])

    def test_a_layer_above_a_parent_in_a_github_stack_is_added_on_its_top(self):
        self.github.answer("api", f"repos/{UPSTREAM}/pulls/38150", "--jq", ".stack",
                           value='{"number": 3}\n')
        self.tool.remote_stacks[UPSTREAM_PULL + "38150"] = [BRANCH]

        self.publish("--upstream-issue", "38136", number=72)

        self.assertEqual([("gh", "stack", "checkout", UPSTREAM_PULL + "38150"),
                          ("gh", "stack", "view", "--json"), ("gh", "stack", "add", CHILD)],
                         [entry for entry in self.log if entry[:2] == ("gh", "stack")])
        self.assertEqual(self.parent_tip, self.tool.pull_requests[0][2])

    def test_a_github_stack_whose_top_is_not_the_parent_is_refused(self):
        self.github.answer("api", f"repos/{UPSTREAM}/pulls/38150", "--jq", ".stack",
                           value='{"number": 3}\n')
        self.fixture.git("push", "--quiet", self.upstream_url,
                         "parent:refs/heads/feature-stack/Shopify/other")
        self.tool.remote_stacks[UPSTREAM_PULL + "38150"] = [BRANCH, "feature-stack/Shopify/other"]

        code, out, err = self.submit("72", "--repo-path", self.dev)

        self.assertEqual((1, f"{BRANCH}'s GitHub stack has feature-stack/Shopify/other above it: "
                             "decide the arrangement with the team, one chain in dependency "
                             "order\n"), (code, err))

    def test_a_landed_parent_bases_the_change_on_master(self):
        self.answer_upstream(upstream_pr(38150, "MERGED", merge=self.base))

        self.publish("--upstream-issue", "38136", number=72)

        self.assertEqual(("gh", "stack", "init", CHILD), self.log[0])
        self.assertEqual(self.base, self.tool.pull_requests[0][2])

    def refused(self, *argv):
        code, out, err = self.submit("72", "--repo-path", self.dev, *argv)
        self.assertEqual(1, code, out)
        self.assertEqual([], [entry for entry in self.log if entry[:2] == ("gh", "stack")])
        return err

    def test_a_restructured_parent_is_refused(self):
        self.answer_upstream(upstream_pr(38150, "MERGED", merge=MISSING))

        self.assertEqual(f"Parent fork PR #71's upstream PR {UPSTREAM_PULL}38150 was merged but "
                         "has not landed on upstream master: it was restructured; wait for the "
                         "replacement Upstream: comment on #71\n", self.refused())

    def test_an_unsubmitted_parent_is_refused(self):
        self.answer_comments(71, [])

        self.assertEqual("Parent fork PR #71 has no upstream PR yet: submit it first\n",
                         self.refused())

    def test_a_fork_only_parent_is_refused(self):
        self.answer_pr(fork_pr(71, "[#45][shopify-only] Build: Share the helpers", self.landing,
                               labels=["upstream: skip"]))

        self.assertEqual("Parent fork PR #71 is fork-only work: this change cannot go upstream "
                         "as is; stop and raise it with the team\n", self.refused())

    def test_several_parents_are_refused(self):
        self.assertEqual("Several parents (#70, #71): decide the arrangement with the team, one "
                         "chain in dependency order\n",
                         self.refused("--parent", "70", "--parent", "71"))

    def test_an_explicit_parent_overrides_the_base_branch(self):
        self.answer_pr(fork_pr(72, "[#70] DocDB: Use the refresh batch metric in the cache",
                               self.child))

        self.publish("--parent", "71", "--upstream-issue", "38136", number=72)

        self.assertEqual(("gh", "stack", "init", BRANCH, CHILD), self.log[0])


class RerunTest(SubmitTestCase):
    def test_a_branch_that_already_has_the_pick_is_reused(self):
        self.assertEqual(4, self.submit("71", "--repo-path", self.dev)[0])
        built = self.head()
        self.log.clear()

        code, out, err = self.submit("71", "--repo-path", self.dev)

        self.assertEqual((4, ""), (code, err), out)
        self.assertEqual(([("gh", "stack", "view", "--json")], built), (self.log, self.head()))
        self.assertIn(f"{BRANCH} already has the cherry-pick of {self.landing[:9]}", out)

    def test_a_branch_with_other_commits_is_refused(self):
        git(self.dev, "checkout", "--quiet", "-b", BRANCH)
        Path(self.dev, "other.txt").write_text("other\n")
        git(self.dev, "add", "other.txt")
        git(self.dev, "commit", "--quiet", "-m", "Something else")

        code, out, err = self.submit("71", "--repo-path", self.dev)

        self.assertEqual((1, f"{BRANCH} exists without the cherry-pick of {self.landing[:9]}; "
                             "pass --branch to name another branch\n"), (code, err))

    def test_an_existing_pull_request_for_the_branch_is_recorded_not_reopened(self):
        self.answer_branch_prs(BRANCH, [branch_pr(38201, "OPEN")])

        self.publish("--upstream-issue", "38136")

        self.assertEqual([self.association(38136), self.recorded(url=UPSTREAM_PULL + "38201")],
                         self.writes())

    def test_an_existing_pull_request_needs_no_test_plan(self):
        self.answer_branch_prs(BRANCH, [branch_pr(38201, "OPEN")])

        code, out, err = self.submit("71", "--repo-path", self.dev, "--upstream-issue", "38136",
                                     answers=["y"])

        self.assertEqual((0, ""), (code, err), out)
        self.assertNotIn(PROMPT_PLAN, out)
        self.assertEqual([self.association(38136), self.recorded(url=UPSTREAM_PULL + "38201")],
                         self.writes())

    def test_declining_to_record_an_existing_pull_request_prints_the_record_commands(self):
        self.answer_branch_prs(BRANCH, [branch_pr(38201, "OPEN")])
        self.tool.private_issues[PRIVATE_ISSUE] = {"public_issues": []}

        code, out, err = self.submit("71", "--repo-path", self.dev, "--upstream-issue", "38136",
                                     "--private-issue", PRIVATE_ISSUE, answers=["n"])

        self.assertEqual((4, ""), (code, err), out)
        self.assertEqual([
            PROMPT_PUBLISH + "Not recorded. To record it, run:",
            "  " + shlex.join(self.association(38136)),
            "  " + shlex.join(["python3", str(ISSUES_SCRIPT), "link", "--issue", PRIVATE_ISSUE,
                               "--public-issue", f"{UPSTREAM_ISSUE}38136"]),
            "  " + shlex.join(["gh", "pr", "comment", "71", "--repo", FORK, "--body",
                               f"Upstream: {UPSTREAM_PULL}38201"]),
        ], out.splitlines()[-4:])
        self.assertEqual([("issues.py", "inspect", "--issue", PRIVATE_ISSUE)], self.writes())

    def test_an_existing_pull_request_without_a_known_upstream_issue_is_refused_first(self):
        self.answer_branch_prs(BRANCH, [branch_pr(38201, "OPEN")])

        code, out, err = self.submit("71", "--repo-path", self.dev)

        self.assertEqual((1, "", f"{UPSTREAM_PULL}38201 exists for {BRANCH}, but no upstream "
                                 "issue is known for it; pass --upstream-issue\n"),
                         (code, out, err))
        self.assertEqual([], self.writes())

    def test_yes_without_a_test_plan_records_an_existing_pull_request(self):
        self.answer_branch_prs(BRANCH, [branch_pr(38201, "OPEN")])

        code, out, err = self.submit("71", "--repo-path", self.dev, "--upstream-issue", "38136",
                                     "--yes")

        self.assertEqual((0, ""), (code, err), out)
        self.assertEqual([self.association(38136), self.recorded(url=UPSTREAM_PULL + "38201")],
                         self.writes())

    def test_an_existing_pull_request_is_recorded_without_building_a_local_branch(self):
        self.answer_branch_prs(BRANCH, [branch_pr(38201, "OPEN")])

        self.publish("--upstream-issue", "38136")

        self.assertEqual([], [entry for entry in self.log if entry[:2] == ("gh", "stack")])
        self.assertEqual("", git(self.dev, "branch", "--list", BRANCH))

    def test_branch_names_the_branch_with_or_without_its_prefix(self):
        for name in ("metric", "feature-stack/Shopify/metric"):
            with self.subTest(name=name):
                self.answer_branch_prs("feature-stack/Shopify/metric", [])
                git(self.dev, "checkout", "--quiet", "master")
                if git(self.dev, "branch", "--list", "feature-stack/Shopify/metric"):
                    git(self.dev, "branch", "--quiet", "-D", "feature-stack/Shopify/metric")

                self.publish("--branch", name, "--upstream-issue", "38136")

                self.assertEqual("feature-stack/Shopify/metric", self.tool.pull_requests[-1][0])

    def test_an_existing_pull_request_without_a_terminal_stops_with_the_record_command(self):
        self.answer_branch_prs(BRANCH, [branch_pr(38201, "OPEN")])

        code, out, err = self.submit("71", "--repo-path", self.dev, "--upstream-issue", "38136")

        self.assertEqual((4, ""), (code, err), out)
        self.assertEqual(["To record it, run:", "  " + shlex.join(self.association(38136)),
                          "  " + shlex.join(["gh", "pr", "comment", "71", "--repo", FORK,
                                             "--body", f"Upstream: {UPSTREAM_PULL}38201"])],
                         out.splitlines()[-3:])
        self.assertEqual([], self.writes())

    def test_a_landed_pull_request_for_the_branch_is_recorded(self):
        self.answer_branch_prs(BRANCH, [branch_pr(38201, "MERGED", self.base)])

        self.publish("--upstream-issue", "38136")

        self.assertEqual([self.association(38136), self.recorded(url=UPSTREAM_PULL + "38201")],
                         self.writes())

    def test_a_restructured_pull_request_for_the_branch_gets_a_replacement(self):
        self.answer_branch_prs(BRANCH, [branch_pr(38201, "MERGED", MISSING)])

        self.publish("--upstream-issue", "38136")

        self.assertEqual(self.association(38136), self.writes()[0])
        self.assertEqual(["gh", "create-pr.sh", "gh"], [entry[0] for entry in self.writes()])
        self.assertEqual(self.recorded(), self.writes()[2])

    def test_a_closed_pull_request_for_the_branch_is_refused(self):
        self.answer_branch_prs(BRANCH, [branch_pr(38199, "CLOSED")])

        code, out, err = self.submit("71", "--repo-path", self.dev, "--upstream-issue", "38136",
                                     "--test-plan", self.plan, "--yes")

        self.assertEqual((1, f"A closed upstream PR {UPSTREAM_PULL}38199 exists for {BRANCH}; "
                             "pass --branch to open a new one\n"), (code, err))
        self.assertEqual([], self.writes())

    def test_a_title_that_gives_no_branch_name_needs_branch(self):
        self.answer_pr(fork_pr(71, "[#70] DocDB: 日本語のタイトルを追加します", self.landing))

        code, out, err = self.submit("71", "--repo-path", self.dev)

        self.assertEqual((1, "The title's description gives no branch name; pass --branch "
                             "<name>\n"), (code, err))


class ProvenanceTest(SubmitTestCase):
    """Finding 1: a PR on the branch is this contribution's only when it says so."""

    def test_an_unrelated_pull_request_on_the_branch_is_refused_and_nothing_recorded(self):
        self.answer_branch_prs(BRANCH, [branch_pr(38201, "OPEN", fork_pr=99)])

        code, out, err = self.submit("71", "--repo-path", self.dev, "--upstream-issue", "38136",
                                     "--test-plan", self.plan, "--yes")

        self.assertEqual((1, f"{BRANCH} already has PR {UPSTREAM_PULL}38201 for another change; "
                             "pass --branch <other name>\n"), (code, err))
        self.assertEqual([], self.writes())

    def test_a_fork_pr_number_that_only_starts_the_same_is_another_change(self):
        self.answer_branch_prs(BRANCH, [branch_pr(38201, "OPEN", fork_pr=710)])

        code, out, err = self.submit("71", "--repo-path", self.dev, "--upstream-issue", "38136",
                                     "--test-plan", self.plan, "--yes")

        self.assertEqual(1, code, err)
        self.assertEqual([], self.writes())

    def test_the_matching_pull_request_is_recorded(self):
        self.answer_branch_prs(BRANCH, [branch_pr(38201, "OPEN")])

        self.publish("--upstream-issue", "38136")

        self.assertEqual(self.recorded(url=UPSTREAM_PULL + "38201"), self.writes()[-1])
        self.assertNotIn("create-pr.sh", [entry[0] for entry in self.writes()])

    def test_a_pull_request_from_another_repository_with_the_branch_name_is_ignored(self):
        self.answer_branch_prs(BRANCH, [branch_pr(38201, "OPEN", owner="someone")])

        self.publish("--upstream-issue", "38136")

        self.assertIn("create-pr.sh", [entry[0] for entry in self.writes()])
        self.assertEqual(self.recorded(), self.writes()[-1])


class CrlfTest(SubmitTestCase):
    """GitHub keeps the CRLF line endings of bodies and comments edited in the web UI."""

    def test_a_crlf_fork_pr_and_fork_issue_publish_like_lf_ones(self):
        self.answer_pr(fork_pr(71, TITLE, self.landing, BODY.replace("\n", "\r\n")))
        self.github.answer("issue", "view", "70", "--repo", FORK, "--json", "title,body,labels",
                           value={"title": ISSUE_TITLE, "body": ISSUE_BODY.replace("\n", "\r\n"),
                                  "labels": [{"name": "kind/bug"}]})

        self.publish()

        self.assertEqual(("file", PUBLIC_ISSUE_BODY), self.writes()[0][8])
        self.assertEqual(("file", SUMMARY), self.writes()[2][6])

    def test_a_crlf_upstream_issue_comment_is_recognised(self):
        self.answer_comments(70, [comment(f"Upstream issue: {UPSTREAM_ISSUE}38135\r\nThanks")])

        self.publish()

        self.assertEqual("38135", next(entry for entry in self.writes()
                                       if entry[0] == "create-pr.sh")[2])
        self.assertNotIn("issue", [entry[1] for entry in self.writes() if entry[0] == "gh"])

    def test_a_crlf_upstream_comment_is_recognised(self):
        self.answer_comments(71, [comment(f"Thanks!\r\nUpstream: {UPSTREAM_PULL}38200\r\n")])
        self.answer_upstream(upstream_pr(38200, "OPEN"))

        self.assertEqual((0, f"Fork PR #71 is already submitted upstream: {UPSTREAM_PULL}38200 "
                             "(open); nothing to do\n", ""),
                         self.submit("71", "--repo-path", self.dev))

    def test_a_crlf_body_on_the_existing_pull_request_is_this_contributions(self):
        pr = branch_pr(38201, "OPEN")
        pr["body"] = pr["body"].replace("\n", "\r\n")
        self.answer_branch_prs(BRANCH, [pr])

        self.publish("--upstream-issue", "38136")

        self.assertEqual(self.recorded(url=UPSTREAM_PULL + "38201"), self.writes()[-1])


class PlacementTest(SubmitTestCase):
    """Finding 2: a reused branch must sit on the planned base."""

    def test_an_empty_branch_at_the_planned_base_is_picked_again(self):
        git(self.dev, "branch", "--quiet", BRANCH, "master")
        self.tool.stacks.append({"repo": os.path.realpath(self.dev), "trunk": "master",
                                 "branches": [BRANCH]})

        code, out, err = self.submit("71", "--repo-path", self.dev)

        self.assertEqual((4, ""), (code, err), out)
        self.assertEqual(self.base, self.head("HEAD^"))
        self.assertIn(f"(cherry picked from commit {self.landing})",
                      git(self.dev, "log", "-1", "--format=%B"))

    def test_an_empty_branch_at_another_base_is_refused(self):
        self.fixture.git("checkout", "--quiet", "main")
        self.write({"src/other.cc": "upstream moved\n"}, "Upstream moves on")
        self.fixture.git("push", "--quiet", self.upstream_url, "main:refs/heads/master")
        git(self.dev, "fetch", "--quiet", "upstream")  # the remote's master is the planned base
        git(self.dev, "branch", "--quiet", BRANCH, self.base)
        self.tool.stacks.append({"repo": os.path.realpath(self.dev), "trunk": "master",
                                 "branches": [BRANCH]})

        code, out, err = self.submit("71", "--repo-path", self.dev)

        self.assertEqual((1, f"{BRANCH} exists but is not placed on master; this run does not "
                             f"restack it. Remove it: on {BRANCH} run `gh stack unstack --local`, "
                             f"switch to another branch, run `git branch -D {BRANCH}`, then "
                             "rerun\n"), (code, err))
        self.assertEqual(self.base, self.head(BRANCH))

    def test_a_branch_untracked_by_gh_stack_is_refused(self):
        git(self.dev, "branch", "--quiet", BRANCH, "master")

        code, out, err = self.submit("71", "--repo-path", self.dev)

        self.assertEqual(1, code)
        self.assertTrue(err.startswith(f"{BRANCH} exists but is not placed on master;"), err)


class SnapshotPlacementTest(SubmitTestCase):
    """The tool's upstream snapshot is at A while gh stack rebased the branch onto upstream B."""

    def setUp(self):
        super().setUp()
        self.assertEqual(4, self.submit("71", "--repo-path", self.dev)[0])
        self.fixture.git("checkout", "--quiet", "main")
        self.write({"src/other.cc": "upstream moved\n"}, "Upstream moves on")
        self.fixture.git("push", "--quiet", self.upstream_url, "main:refs/heads/master")
        # What `gh stack rebase --remote upstream` does: fetch, then replay the layer.
        git(self.dev, "fetch", "--quiet", "upstream")
        git(self.dev, "rebase", "--quiet", "upstream/master")
        self.moved = self.head("upstream/master")

    def test_a_branch_rebased_onto_the_remotes_newer_master_is_reused(self):
        code, out, err = self.submit("71", "--repo-path", self.dev, "--no-fetch")

        self.assertEqual((4, ""), (code, err), out)
        self.assertIn(f"{BRANCH} already has the cherry-pick of {self.landing[:9]}.", out)
        self.assertEqual(self.moved, self.head(f"{BRANCH}^"))

    def test_a_refusal_with_a_stale_snapshot_says_to_fetch(self):
        self.tool.stacks.clear()  # gh stack no longer tracks the branch

        code, out, err = self.submit("71", "--repo-path", self.dev, "--no-fetch")

        self.assertEqual((1, f"{BRANCH} is built on upstream/master, which is ahead of this "
                             "run's upstream snapshot refs/shopify/upstream-master; rerun without "
                             f"--no-fetch. If it is still refused, remove it: on {BRANCH} run "
                             "`gh stack unstack --local`, switch to another branch, run "
                             f"`git branch -D {BRANCH}`, then rerun\n"), (code, err))

    def test_a_pick_on_a_commit_upstream_does_not_have_is_still_misplaced(self):
        git(self.dev, "reset", "--quiet", "--hard", self.base)
        Path(self.dev, "local.txt").write_text("local only\n")
        git(self.dev, "add", "local.txt")
        git(self.dev, "commit", "--quiet", "-m", "Local commit")
        git(self.dev, "cherry-pick", "-x", self.landing)

        code, out, err = self.submit("71", "--repo-path", self.dev, "--no-fetch")

        self.assertEqual((1, f"{BRANCH} exists but is not placed on master; this run does not "
                             f"restack it. Remove it: on {BRANCH} run `gh stack unstack --local`, "
                             f"switch to another branch, run `git branch -D {BRANCH}`, then "
                             "rerun\n"), (code, err))


class TrackingRefTest(SubmitTestCase):
    """The upstream remote's tracking master: fetched with the snapshot, needed only to judge a
    branch that already exists."""

    def move_upstream(self):
        self.fixture.git("checkout", "--quiet", "main")
        self.write({"src/other.cc": "upstream moved\n"}, "Upstream moves on")
        self.fixture.git("push", "--quiet", self.upstream_url, "main:refs/heads/master")
        return self.fixture.git("rev-parse", "HEAD")

    def test_a_new_upstream_remote_without_a_tracking_ref_builds_the_branch(self):
        git(self.dev, "update-ref", "-d", "refs/remotes/upstream/master")

        code, out, err = self.submit("71", "--repo-path", self.dev)

        self.assertEqual((4, ""), (code, err), out)
        self.assertEqual((self.base, self.base),
                         (self.head("refs/remotes/upstream/master"), self.head(f"{BRANCH}^")))

    def test_fetching_updates_the_tracking_ref_before_an_existing_branch_is_judged(self):
        self.assertEqual(4, self.submit("71", "--repo-path", self.dev)[0])
        moved = self.move_upstream()
        self.log.clear()

        code, out, err = self.submit("71", "--repo-path", self.dev)

        self.assertEqual((4, ""), (code, err), out)
        self.assertEqual(moved, self.head("refs/remotes/upstream/master"))
        self.assertEqual([("gh", "stack", "view", "--json")], self.log)

    def test_without_fetching_a_missing_tracking_ref_for_an_existing_branch_says_to_fetch(self):
        self.assertEqual(4, self.submit("71", "--repo-path", self.dev)[0])
        git(self.dev, "update-ref", "-d", "refs/remotes/upstream/master")

        code, out, err = self.submit("71", "--repo-path", self.dev, "--no-fetch")

        self.assertEqual((1, "refs/remotes/upstream/master does not resolve to a commit in "
                             f"{self.dev}; fetch it with `git fetch upstream master`\n"),
                         (code, err))

    def test_a_pick_on_the_snapshot_is_accepted_when_the_tracking_ref_is_stale(self):
        self.assertEqual(4, self.submit("71", "--repo-path", self.dev)[0])
        moved = self.move_upstream()
        git(self.dev, "fetch", "--quiet", self.upstream_url,
            "+refs/heads/master:refs/shopify/upstream-master")
        git(self.dev, "rebase", "--quiet", moved)

        code, out, err = self.submit("71", "--repo-path", self.dev, "--no-fetch")

        self.assertEqual((4, ""), (code, err), out)
        self.assertEqual((self.base, moved), (self.head("refs/remotes/upstream/master"),
                                              self.head(f"{BRANCH}^")))
        self.assertIn(f"{BRANCH} already has the cherry-pick of {self.landing[:9]}.", out)


class StackPlacementTest(SubmitTestCase):
    def setUp(self):
        super().setUp()
        child = self.write({"src/cache.cc": "uses the metric\n"},
                           "Use the refresh batch metric in the cache (#72)")
        self.publish_fork()
        self.fixture.git("checkout", "--quiet", "-b", "parent", self.base)
        self.write({"src/metric.cc": "metric\n"}, "[#38130] DocDB: Expose the refresh batch metric")
        self.fixture.git("push", "--quiet", self.upstream_url, f"parent:refs/heads/{BRANCH}")
        self.answer_pr(fork_pr(72, "[#70] DocDB: Use the refresh batch metric in the cache",
                               child))
        self.answer_comments(72, [])
        self.answer_comments(71, [comment(f"Upstream: {UPSTREAM_PULL}38150")])
        self.answer_upstream(upstream_pr(38150, "OPEN"))
        self.github.answer("api", f"repos/{UPSTREAM}/pulls/38150", "--jq", ".stack",
                           value="null\n")

    def refusal(self):
        return (1, f"{CHILD} exists but is not placed on {BRANCH}; this run does not restack it. "
                   f"Remove it: on {CHILD} run `gh stack unstack --local`, switch to another "
                   f"branch, run `git branch -D {CHILD}`, then rerun\n")

    def test_a_stacked_branch_that_gh_stack_lists_as_the_bottom_layer_is_refused(self):
        self.assertEqual(4, self.submit("72", "--repo-path", self.dev, "--parent", "71")[0])
        stack = next(stack for stack in self.tool.stacks if CHILD in stack["branches"])
        stack["branches"] = [CHILD]

        code, out, err = self.submit("72", "--repo-path", self.dev, "--parent", "71")

        self.assertEqual(self.refusal(), (code, err))

    def test_a_branch_built_on_master_is_refused_when_a_parent_is_named_on_the_rerun(self):
        self.assertEqual(4, self.submit("72", "--repo-path", self.dev)[0])
        built = self.head(CHILD)

        code, out, err = self.submit("72", "--repo-path", self.dev, "--parent", "71")

        self.assertEqual((1, f"{CHILD} exists but is not placed on {BRANCH}; this run does not "
                             f"restack it. Remove it: on {CHILD} run `gh stack unstack --local`, "
                             f"switch to another branch, run `git branch -D {CHILD}`, then "
                             "rerun\n"), (code, err))
        self.assertEqual(built, self.head(CHILD))


class IndexSafetyTest(SubmitTestCase):
    """Finding 3: the developer's pause cannot change what is published."""

    def test_a_tree_made_dirty_during_the_pause_stops_phase_b_before_any_write(self):
        def stage_an_edit():
            Path(self.dev, "src", "cache.cc").write_text("staged during the pause\n")
            git(self.dev, "add", "src/cache.cc")
        self.tool.editor_text = f"## What ran on {BRANCH}\n\n- the metric tests passed\n"
        self.tool.editor_effect = stage_an_edit

        with patch.dict(os.environ, {"EDITOR": "fake-editor"}):
            code, out, err = self.submit("71", "--repo-path", self.dev, answers=["e", "y"])

        self.assertEqual((1, f"{self.dev} has uncommitted changes to tracked files; commit or "
                             "stash them first\n"), (code, err))
        self.assertEqual([], self.writes())
        self.assertEqual(git(self.dev, "rev-parse", f"{self.landing}^{{tree}}"),
                         git(self.dev, "rev-parse", "HEAD^{tree}"))


class MovedCheckoutTest(SubmitTestCase):
    """What create-pr.sh publishes is what the preview showed: the planned branch at its sha."""

    def pause(self, effect):
        built = []

        def record_then_move():
            built.append(self.head())
            effect()
        self.tool.editor_text = f"## What ran on {BRANCH}\n\n- the metric tests passed\n"
        self.tool.editor_effect = record_then_move
        with patch.dict(os.environ, {"EDITOR": "fake-editor"}):
            code, out, err = self.submit("71", "--repo-path", self.dev, "--upstream-issue",
                                         "38136", answers=["e", "y"])
        self.assertEqual([], self.writes())
        return code, err, built[0]

    def commit(self, name):
        Path(self.dev, name).write_text("during the pause\n")
        git(self.dev, "add", name)
        git(self.dev, "commit", "--quiet", "-m", f"Add {name}")
        return self.head()

    def test_switching_to_another_branch_with_the_pick_is_refused_before_any_write(self):
        other = "feature-stack/Shopify/other"
        moved = []

        def switch():
            git(self.dev, "checkout", "--quiet", "-b", other)
            moved.append(self.commit("other.txt"))

        code, err, built = self.pause(switch)

        self.assertEqual((1, f"The checkout moved from {BRANCH}@{built[:9]} to "
                             f"{other}@{moved[0][:9]} while you were deciding; rerun to preview "
                             "again\n"), (code, err))

    def test_a_commit_added_to_the_planned_branch_is_refused_before_any_write(self):
        moved = []

        code, err, built = self.pause(lambda: moved.append(self.commit("extra.txt")))

        self.assertEqual((1, f"The checkout moved from {BRANCH}@{built[:9]} to "
                             f"{BRANCH}@{moved[0][:9]} while you were deciding; rerun to preview "
                             "again\n"), (code, err))


class AssociationTest(SubmitTestCase):
    """Finding 4: the fork issue names its upstream issue whichever way it was found."""

    def test_upstream_issue_writes_the_association_once_before_the_pull_request(self):
        self.publish("--upstream-issue", "38136")

        self.assertEqual([self.association(38136), "create-pr.sh", "gh"],
                         [self.writes()[0], self.writes()[1][0], self.writes()[2][0]])

    def test_a_rerun_does_not_duplicate_the_association(self):
        self.answer_comments(70, [comment(f"Upstream issue: {UPSTREAM_ISSUE}38136")])

        self.publish("--upstream-issue", "38136")

        self.assertNotIn(self.association(38136), self.writes())

    def test_an_issue_from_the_private_tracker_is_associated(self):
        self.tool.private_issues[PRIVATE_ISSUE] = {"public_issues": [f"{UPSTREAM_ISSUE}38137"]}

        self.publish("--private-issue", PRIVATE_ISSUE)

        self.assertIn(self.association(38137), self.writes())

    def test_the_records_of_an_existing_pull_request_include_the_association(self):
        self.answer_branch_prs(BRANCH, [branch_pr(38201, "OPEN")])

        self.publish("--upstream-issue", "38136")

        self.assertEqual([self.association(38136), self.recorded(url=UPSTREAM_PULL + "38201")],
                         self.writes())


class DirectUpstreamIssueTest(SubmitTestCase):
    """Finding 5: --private-issue also applies when the Refs line cites the upstream issue."""

    def setUp(self):
        super().setUp()
        body = BODY.replace(f"{FORK_ISSUE}70", f"{UPSTREAM_ISSUE}38130")
        self.answer_pr(fork_pr(71, "[#38130] DocDB: Expose the refresh batch metric",
                               self.landing, body))

    def private_writes(self):
        return [entry for entry in self.writes() if entry[0] == "issues.py"]

    def test_a_cited_upstream_issue_missing_from_the_private_tracker_is_linked(self):
        self.tool.private_issues[PRIVATE_ISSUE] = {"public_issues": []}

        self.publish("--private-issue", PRIVATE_ISSUE)

        self.assertEqual([("issues.py", "inspect", "--issue", PRIVATE_ISSUE),
                          ("issues.py", "link", "--issue", PRIVATE_ISSUE, "--public-issue",
                           f"{UPSTREAM_ISSUE}38130")], self.private_writes())

    def test_a_cited_upstream_issue_already_in_the_private_tracker_is_not_linked(self):
        self.tool.private_issues[PRIVATE_ISSUE] = {"public_issues": [f"{UPSTREAM_ISSUE}38130"]}

        self.publish("--private-issue", PRIVATE_ISSUE)

        self.assertEqual([("issues.py", "inspect", "--issue", PRIVATE_ISSUE)],
                         self.private_writes())


class TextTest(unittest.TestCase):
    def test_bare_fork_numbers_become_fork_urls_without_their_closing_keyword(self):
        self.assertEqual(
            f"{FORK_ISSUE}1, {FORK_ISSUE}2 and {FORK_ISSUE}3 ({FORK_ISSUE}4) also "
            f"{FORK_ISSUE}5 {FORK_ISSUE}6 https://github.com/x/y/pull/7#8 a#9 &#10; "
            "issue #x",
            link_fork_references("Fixes #1, closes #2 and Resolved: #3 (#4) also fixed #5 #6 "
                                 "https://github.com/x/y/pull/7#8 a#9 &#10; issue #x"))

    def test_the_commit_message_takes_the_new_subject_and_keeps_the_pick_line(self):
        message = ("Expose the metric (#71)\n\nCloses #69.\n\nCo-authored-by: A <a@example.invalid>"
                   "\n\n(cherry picked from commit " + "b" * 40 + ")\n")

        self.assertEqual(f"[#38140] DocDB: Expose the metric\n\n{FORK_ISSUE}69.\n\n"
                         "Co-authored-by: A <a@example.invalid>\n\n(cherry picked from commit "
                         + "b" * 40 + ")\n",
                         commit_message(message, "[#38140] DocDB: Expose the metric"))

    def test_the_shopify_context_section_and_publication_marker_are_stripped(self):
        self.assertEqual(PUBLIC_ISSUE_BODY, strip_shopify_context(ISSUE_BODY))
        self.assertEqual("### Description\n\nText.\n",
                         strip_shopify_context("### Description\n\nText.\n\n## Shopify context\n\n"
                                               "Why.\n"))

    def test_the_summary_section_ends_at_the_next_heading_of_its_level(self):
        body = "Refs x\n\n## Summary\n\nOne.\n\n### Detail\n\nTwo.\n\n## Test plan\n\n- t\n"

        self.assertEqual("One.\n\n### Detail\n\nTwo.\n", summary_section(body))
        self.assertIsNone(summary_section("Refs x\n\nNo sections.\n"))

    def test_crlf_and_lone_cr_bodies_read_like_lf_bodies(self):
        body = "Refs x\n\n## Summary\n\nOne.\n\n## Test plan\n\n- t\n"
        for newline in ("\r\n", "\r"):
            with self.subTest(newline=repr(newline)):
                self.assertEqual("One.\n", summary_section(body.replace("\n", newline)))
                self.assertEqual(PUBLIC_ISSUE_BODY,
                                 strip_shopify_context(ISSUE_BODY.replace("\n", newline)))
                self.assertEqual("a\nb\n", normalize_newlines(f"a{newline}b{newline}"))

    def test_the_slug_is_lowercase_hyphenated_and_at_most_48_characters(self):
        self.assertEqual("fix-the-tablet-split-race-in-the-catalog-manager",
                         slug("Fix the tablet-split race in the catalog manager's queue"))
        self.assertEqual("use-utf-8-names", slug("  Use UTF-8 names!  "))


class CliTest(testfixtures.RepoTestCase):
    def test_importing_the_module_has_no_side_effects(self):
        home, cwd = os.path.join(self.tmpdir, "home"), os.path.join(self.tmpdir, "cwd")
        os.makedirs(home)
        os.makedirs(cwd)
        directory = os.path.dirname(os.path.abspath(__file__))
        code = f"import sys; sys.path.insert(0, {directory!r}); import upstream_submit"

        result = subprocess.run(["python3", "-I", "-B", "-c", code], cwd=cwd, capture_output=True,
                                text=True, env={**os.environ, "HOME": home})

        self.assertEqual((0, "", ""), (result.returncode, result.stdout, result.stderr))
        self.assertEqual(([], []), (os.listdir(home), os.listdir(cwd)))

    def test_submit_help_documents_its_options(self):
        out = io.StringIO()

        self.assertEqual(0, Cli.run(["submit", "--help"], out=out))
        for option in ("--repo-path", "--clone-to", "--parent", "--upstream-issue",
                       "--private-issue", "--test-plan", "--upgrade", "--branch", "--draft",
                       "--yes", "--fork-url", "--upstream-url", "--no-fetch"):
            self.assertIn(option, out.getvalue())

    def test_a_command_is_required_and_names_both(self):
        err = io.StringIO()

        self.assertEqual(2, Cli.run([], out=io.StringIO(), err=err))
        self.assertEqual("A command is required: status, submit or sync\n", err.getvalue())


if __name__ == "__main__":
    unittest.main()
