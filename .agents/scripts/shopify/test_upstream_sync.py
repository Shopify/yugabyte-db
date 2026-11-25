"""Exercise `upstream.py sync` against throwaway repositories and an in-memory GitHub.

Tests never read or write a real checkout and never call GitHub or the network: upstream and the
fork are bare repositories under a temporary directory, the fork checkout is cloned from the fork,
and `gh` answers from canned responses keyed by its exact arguments.
"""

from datetime import datetime, timezone
import io
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import unittest
from unittest.mock import patch

from scm import Error, GitError, Repository
from upstream import Cli, attribute, dropped, shopify_prs, sync_kind
from upstream_sync import Checkout, GitHub, range_diff_section
import testfixtures


FORK = "Shopify/yugabyte-db"
UPSTREAM = "yugabyte/yugabyte-db"
FORK_PULL = "https://github.com/Shopify/yugabyte-db/pull/"
UPSTREAM_PULL = "https://github.com/yugabyte/yugabyte-db/pull/"
UPSTREAM_COMMIT = "https://github.com/yugabyte/yugabyte-db/commit/"
MERGED_FIELDS = "number,title,mergeCommit,mergedAt,labels,author,url,baseRefName"
UPSTREAM_FIELDS = "number,state,mergedAt,mergeCommit,url,title,headRefName"
MISSING = "a" * 40
SKIP = "upstream: skip"
T0, DAY = 1793534400, 86400  # 2026-11-01T12:00:00Z
NOW = datetime(2026, 11, 19, 12, tzinfo=timezone.utc)
PR_URL = FORK_PULL + "200"
SCRIPT = str(Path(__file__).resolve().with_name("upstream.py"))
OPEN_PR_FIELDS = "number,url,baseRefName,headRepository,headRepositoryOwner"
PROMPT = "Push shopify-sync/{short} and open its PR against master-shopify? [y/N] "
LANDING = ("Do not use the merge button: squash, merge-commit and rebase all undo the sync. It "
           "lands by fast-forwarding master-shopify to this head once Binks and a "
           "yugabyte-code-reviewers member have approved it. Land it with "
           "`python3 .agents/scripts/shopify/upstream.py sync --land <pr>`, which checks approval, "
           "Binks, CI and the fast-forward first. Break-glass, skipping those checks: "
           "`git push origin {head}:master-shopify`.")
SECTIONS = ["## Summary", "## Upgrade/Rollback safety", "## Upstream", "## Dropped", "## Carried",
            "## Range-diff", "## Warnings", "## Landing"]
# upstream's .github/workflows/pr-upgrade.yml, run on the lower-cased body.
UPGRADE_CHECK = re.compile(r"upgrade.*(downgrade|rollback)\ssafety")
# The [shopify-only] form of the title grammar in .github/workflows/pr-title.yml.
TITLE = re.compile(r"\[shopify-only\] [a-zA-Z0-9_-]+(,[a-zA-Z0-9_-]+)*: \S.{8,98}\S")


def git(path, *args, env=None):
    result = subprocess.run(["git", "-C", path, *args], capture_output=True, text=True,
                            env={**os.environ, **(env or {})})
    if result.returncode:
        raise AssertionError(f"git {' '.join(args)} failed: {result.stderr}")
    return result.stdout.strip()


def dates(timestamp):
    return {"GIT_AUTHOR_DATE": f"{timestamp} +0000", "GIT_COMMITTER_DATE": f"{timestamp} +0000"}


def read_file(value):
    with open(value, encoding="utf-8") as source:
        return ("file", source.read())


class FakeGitHub(GitHub):
    """Answers gh by its exact argv from canned responses and records every write in `log`.

    A canned value is returned as JSON (or as is when it is a string); an Error is raised.
    Anything else fails the test.
    """

    WRITES = {("label", "create"), ("pr", "create"), ("pr", "edit")}

    def __init__(self, log):
        super().__init__(executable="/nonexistent/gh")
        self.log = log
        self.responses = {}

    def answer(self, *args, value):
        self.responses[tuple(args)] = value

    def run(self, *args, cwd=None):
        if args[:2] in self.WRITES:
            inlined = [read_file(value) if previous == "--body-file" else value
                       for previous, value in zip(("",) + args, args)]
            self.log.append(("gh", *inlined))
            return PR_URL + "\n" if args[:2] == ("pr", "create") else ""
        if args not in self.responses:
            raise AssertionError(f"unexpected gh call: {args}")
        value = self.responses[args]
        if isinstance(value, Exception):
            raise value
        return value if isinstance(value, str) else json.dumps(value)


def fork_pr(number, title, merge, labels=()):
    return {"number": number, "title": title, "mergeCommit": {"oid": merge},
            "mergedAt": "2026-11-01T12:00:00Z", "labels": [{"name": label} for label in labels],
            "author": {"login": "priya"}, "url": FORK_PULL + str(number),
            "baseRefName": "master-shopify"}


def open_pr(number, owner="Shopify", base="master-shopify"):
    """A `gh pr list --head` entry: by default the sync PR from the fork itself."""
    return {"number": number, "url": FORK_PULL + str(number), "baseRefName": base,
            "headRepository": {"name": "yugabyte-db"}, "headRepositoryOwner": {"login": owner}}


class Deciding(io.StringIO):
    """Standard input whose answer arrives after `effect` runs, as if the developer acted in
    the worktree while the question was open."""

    def __init__(self, effect, answer):
        super().__init__()
        self.effect = effect
        self.answer = answer

    def readline(self, *args):
        self.effect()
        return self.answer + "\n"


def upstream_pr(number, state, merge=None):
    return {"number": number, "state": state, "url": UPSTREAM_PULL + str(number),
            "title": f"Upstream {number}", "headRefName": "feature-stack/Shopify/x",
            "mergedAt": "2026-11-03T12:00:00Z" if state == "MERGED" else None,
            "mergeCommit": {"oid": merge} if merge else None}


class SyncTestCase(testfixtures.RepoTestCase):
    """The fixture repository authors both histories. Upstream `master` (`main` here) is a base,
    then #69's change as yugabyte#38110, then an unrelated change. The fork's `master-shopify`
    adds the squash landings of #69 and #70, a GitHub-style merge of upstream master, the stack
    merge of #41 and #42, and the squash landings of #71 to #74. #69 landed upstream and is
    dropped; #70 is not submitted, #41 and #42 are fork-only, #71 is open upstream, #72 closed,
    #73 restructured and #74's upstream PR cannot be read: all of those are carried."""

    def setUp(self):
        super().setUp()
        self.log = []
        self.github = FakeGitHub(self.log)
        remotes = os.path.join(self.tmpdir, "remotes")
        self.upstream_url = os.path.join(remotes, "yugabyte", "yugabyte-db.git")
        self.fork_url = os.path.join(remotes, "Shopify", "yugabyte-db.git")
        self.fixture.init_bare(self.upstream_url)
        self.fixture.init_bare(self.fork_url)
        self.base = self.write({"src/cache.cc": "base\n", "src/batch.cc": "base\n",
                                "src/metric.cc": "base\n", "src/restart.cc": "base\n",
                                "src/retry.cc": "base\n", "src/backoff.cc": "base\n"},
                               "Upstream base", T0)
        self.fixture.git("checkout", "--quiet", "-b", "master-shopify")
        self.l69 = self.write({"src/cache.cc": "refresh\n"}, "Refresh the catalog cache (#69)",
                              T0 + DAY)
        self.l70 = self.write({"src/batch.cc": "cap\n"},
                              "Cap the refresh batch (#70)\n\nKeeps each batch under the limit.",
                              T0 + DAY, author="Priya")
        self.fixture.git("checkout", "--quiet", "main")
        self.u1 = self.write({"src/cache.cc": "refresh\n"}, "Refresh the catalog cache (#38110)",
                             T0 + 2 * DAY)
        self.fixture.git("checkout", "--quiet", "master-shopify")
        self.upstream_merge = self.merge("main", "Merge branch 'yugabyte:master' into "
                                         "master-shopify", T0 + 3 * DAY)
        self.fixture.git("checkout", "--quiet", "-b", "stack")
        self.write({"src/layer1.cc": "one\n"}, "Add the first layer", T0 + 3 * DAY)
        self.write({"src/layer2.cc": "two\n"}, "Add the second layer", T0 + 3 * DAY)
        self.fixture.git("checkout", "--quiet", "master-shopify")
        self.stack = self.merge("stack", "Merge pull request #42 from Shopify/stack", T0 + 4 * DAY)
        self.l71 = self.write({"src/metric.cc": "metric\n"}, "Expose the batch metric (#71)",
                              T0 + 5 * DAY)
        self.l72 = self.write({"src/restart.cc": "restart\n"}, "Fix the restart race (#72)",
                              T0 + 5 * DAY)
        self.l73 = self.write({"src/retry.cc": "retry\n"}, "Retry the refresh (#73)",
                              T0 + 5 * DAY)
        self.l74 = self.write({"src/backoff.cc": "backoff\n"}, "Tune the retry backoff (#74)",
                              T0 + 5 * DAY)
        self.old = self.l74
        self.publish_fork()
        self.fixture.git("checkout", "--quiet", "main")
        self.to = self.upstream_commit({"src/other.cc": "other\n"}, "Upstream change", T0 + 6 * DAY)
        self.dev = os.path.join(self.tmpdir, "work", "yugabyte-db")
        os.makedirs(os.path.dirname(self.dev))
        git(self.fork_url, "symbolic-ref", "HEAD", "refs/heads/master-shopify")
        git(self.tmpdir, "clone", "--quiet", self.fork_url, self.dev)
        self.github.answer("auth", "status", value="")
        self.merged = [
                fork_pr(69, "[#38100] DocDB: Refresh the catalog cache", self.l69),
                fork_pr(70, "[#38101] DocDB: Cap the refresh batch", self.l70),
                fork_pr(41, "[#45][shopify-only] Build: Add the first layer", self.stack, [SKIP]),
                fork_pr(42, "[#45][shopify-only] Build: Add the second layer", self.stack, [SKIP]),
                fork_pr(71, "[#38102] DocDB: Expose the batch metric", self.l71),
                fork_pr(72, "[#38103] DocDB: Fix the restart race", self.l72),
                fork_pr(73, "[#38104] DocDB: Retry the refresh", self.l73),
                fork_pr(74, "[#38105] DocDB: Tune the retry backoff", self.l74)]
        self.answer_merged()
        for number, upstream in ((69, 38110), (70, None), (71, 38131), (72, 38140), (73, 38150),
                                 (74, 38160)):
            body = [{"body": f"Upstream: {UPSTREAM_PULL}{upstream}",
                     "created_at": "2026-11-02T00:00:00Z"}] if upstream else []
            self.github.answer("api", f"repos/{FORK}/issues/{number}/comments", "--paginate",
                               "--slurp", value=[body])
        for view in (upstream_pr(38110, "MERGED", self.u1), upstream_pr(38131, "OPEN"),
                     upstream_pr(38140, "CLOSED"), upstream_pr(38150, "MERGED", MISSING)):
            self.answer_upstream(view)
        self.answer_upstream(upstream_pr(38160, "OPEN"),
                             Error("Failed to run `gh pr view`\nno pull requests found\n"))
        self.answer_labels([SKIP])
        self.answer_open_prs([])

    # Fixture authoring.

    def write(self, files, message, timestamp, author=None):
        for name, content in files.items():
            path = Path(self.fixture.path, name)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content)
        self.fixture.git("add", "-A")
        env = dates(timestamp)
        if author:
            env["GIT_AUTHOR_NAME"] = author
        self.fixture.git("commit", "--quiet", "-m", message, env=env)
        return self.fixture.git("rev-parse", "HEAD")

    def merge(self, branch, message, timestamp):
        self.fixture.git("merge", "--quiet", "--no-ff", "-m", message, branch, env=dates(timestamp))
        return self.fixture.git("rev-parse", "HEAD")

    def publish_fork(self):
        self.fixture.git("push", "--quiet", "--force", self.fork_url,
                         "master-shopify:refs/heads/master-shopify")

    def upstream_commit(self, files, message, timestamp=T0 + 7 * DAY):
        """Commits on upstream master and pushes it; the fixture stays on `main`."""
        sha = self.write(files, message, timestamp)
        self.fixture.git("push", "--quiet", "--force", self.upstream_url, "main:refs/heads/master")
        self.to = sha
        self.answer_open_prs([])
        return sha

    def answer_merged(self, *added):
        """The merged fork PRs, with `added` PRs (and no comments on them) appended."""
        self.merged += added
        for pr in added:
            self.github.answer("api", f"repos/{FORK}/issues/{pr['number']}/comments",
                               "--paginate", "--slurp", value=[[]])
        self.github.answer("pr", "list", "--repo", FORK, "--state", "merged", "-L", "5000",
                           "--json", MERGED_FIELDS, value=self.merged)

    def answer_upstream(self, view, value=None):
        self.github.answer("pr", "view", str(view["number"]), "--repo", UPSTREAM, "--json",
                           UPSTREAM_FIELDS, value=view if value is None else value)

    def answer_labels(self, names):
        self.github.answer("label", "list", "--repo", FORK, "--limit", "1000", "--json", "name",
                           value=[{"name": name} for name in names])

    def answer_open_prs(self, prs):
        self.github.answer("pr", "list", "--repo", FORK, "--head", self.branch, "--state", "open",
                           "--json", OPEN_PR_FIELDS, value=prs)

    # Names.

    @property
    def short(self):
        return self.to[:12]

    @property
    def branch(self):
        return f"shopify-sync/{self.short}"

    @property
    def worktree(self):
        return os.path.join(os.path.realpath(self.tmpdir), "work", f"yugabyte-db-sync-{self.short}")

    @property
    def title(self):
        return f"[shopify-only] Build: Sync master-shopify to upstream {self.short}"

    def rerun(self, *options, skip=None):
        skipping = ["--skip", f"'#{skip}'"] if skip else []
        return " ".join(["upstream.py", "sync", "--continue", *skipping, "--to", self.to,
                         "--repo-path", self.dev, "--upstream-url", self.upstream_url, *options])

    # Running.

    def sync(self, *argv, answers=None, stdin=None, program="upstream.py", repo_path=None):
        """Runs sync; `program` None is the real default, `stdin` replaces the answers."""
        out, err = io.StringIO(), io.StringIO()
        interactive = answers is not None or stdin is not None
        if stdin is None:
            stdin = io.StringIO("".join(f"{answer}\n" for answer in answers or []))
        options = {} if program is None else {"program": program}
        code = Cli.run(["sync", *argv, "--repo-path", repo_path or self.dev, "--upstream-url",
                        self.upstream_url], github=self.github, out=out, err=err, now=NOW,
                       interactive=interactive, stdin=stdin, cwd=os.path.join(self.tmpdir, "work"),
                       workdir=os.path.join(self.tmpdir, "scratch"), **options)
        return code, out.getvalue(), err.getvalue()

    def add_landing(self, files, subject, number, timestamp=T0 + 6 * DAY):
        """Lands fork PR `number` on master-shopify as `subject`, attributed by its merge commit."""
        self.fixture.git("checkout", "--quiet", "master-shopify")
        sha = self.write(files, subject, timestamp)
        self.publish_fork()
        self.fixture.git("checkout", "--quiet", "main")
        self.answer_merged(fork_pr(number, f"[#38108] DocDB: {subject}", sha))
        return sha

    def published(self, *argv):
        code, out, err = self.sync(*argv, "--yes")
        self.assertEqual((0, ""), (code, err), out)
        return out

    # Inspection.

    def candidate(self):
        """The candidate's first-parent commits after the upstream commit, oldest first."""
        return Repository(self.dev).first_parent_commits(self.to, self.branch)

    def message(self, sha):
        return Repository(self.dev).message(sha)

    def raw_message(self, sha):
        """The message exactly as the commit object stores it."""
        return git(self.dev, "cat-file", "commit", sha).split("\n\n", 1)[1] + "\n"

    def writes(self):
        return [entry for entry in self.log if entry[0] == "gh"]

    def body(self):
        created = [entry for entry in self.log if entry[1:3] in (("pr", "create"), ("pr", "edit"))]
        entry = created[-1]
        return entry[entry.index("--body-file") + 1][1]

    @staticmethod
    def section(body, heading):
        start = body.index(heading + "\n") + len(heading) + 1
        end = body.find("\n## ", start)
        return body[start:end if end != -1 else len(body)].strip("\n")

    def remote_head(self):
        result = subprocess.run(["git", "-C", self.fork_url, "rev-parse", "--verify", "--quiet",
                                 f"refs/heads/{self.branch}"], capture_output=True, text=True)
        return result.stdout.strip() or None

    def local_branches(self):
        return git(self.dev, "for-each-ref", "--format=%(refname:short)", "refs/heads").split()

    def resolve_in_worktree(self, path, content):
        Path(self.worktree, path).write_text(content)
        git(self.worktree, "add", path)
        git(self.worktree, "cherry-pick", "--continue", env={"GIT_EDITOR": "true"})


class HappyPathTest(SyncTestCase):
    def test_a_sync_builds_the_candidate_and_opens_its_draft_pr(self):
        out = self.published()

        commits = self.candidate()
        merge = commits[0]
        self.assertEqual([self.to, self.old], merge["parents"])
        self.assertEqual("sync", sync_kind(Repository(self.dev), merge, self.to))
        self.assertEqual(f"Sync master-shopify to upstream {self.short}\n\n"
                         f"Old-master-shopify: {self.old}\n"
                         "Dropped: #69 (upstream #38110)\n", merge["message"])
        self.assertEqual([(69, 38110)], dropped(merge["message"]))
        originals = [self.l70, self.stack, self.l71, self.l72, self.l73, self.l74]
        replayed = [commit["sha"] for commit in commits[1:]]
        self.assertEqual([self.message(sha) for sha in originals[:1] + originals[2:]],
                         [self.message(sha) for sha in replayed[:1] + replayed[2:]])
        self.assertEqual(self.raw_message(self.stack) + "\nShopify-PR: #41 #42\n",
                         self.raw_message(replayed[1]))
        self.assertEqual([41, 42], shopify_prs(self.message(replayed[1])))
        self.assertTrue(all(len(c["parents"]) == 1 for c in commits[1:]))
        self.assertEqual(git(self.dev, "log", "-1", "--format=%an %aI", self.l70),
                         git(self.dev, "log", "-1", "--format=%an %aI", replayed[0]))
        head = replayed[-1]
        self.assertEqual(self.branch, git(self.worktree, "symbolic-ref", "--short", "HEAD"))
        self.assertEqual(head, self.remote_head())
        self.assertEqual("", git(self.dev, "status", "--porcelain"))
        self.assertEqual("master-shopify", git(self.dev, "symbolic-ref", "--short", "HEAD"))
        body = self.body()
        self.assertEqual([
            ("gh", "label", "create", "sync", "--repo", FORK, "--description",
             "Rebuild of master-shopify on a newer upstream commit", "--color", "0E8A16"),
            ("gh", "pr", "create", "--repo", FORK, "--base", "master-shopify", "--head",
             self.branch, "--draft", "--title", self.title, "--body-file", ("file", body),
             "--label", "upstream: skip", "--label", "sync"),
        ], self.writes())
        self.assertRegex(self.title, TITLE)
        self.assertEqual(SECTIONS, [line for line in body.splitlines() if line.startswith("## ")])
        self.assertEqual(LANDING.format(head=head), self.section(body, "## Landing"))
        self.assertEqual(f"| Fork PR | Upstream PR |\n| --- | --- |\n"
                         f"| [#69]({FORK_PULL}69) | [yugabyte#38110]({UPSTREAM_PULL}38110) |",
                         self.section(body, "## Dropped"))
        rows = self.section(body, "## Carried").splitlines()
        self.assertEqual(["| Fork PR | Subject | Old → new | Conflict |",
                          "| --- | --- | --- | --- |",
                          f"| [#70]({FORK_PULL}70) | Cap the refresh batch (#70) | "
                          f"`{self.l70[:12]}` → `{replayed[0][:12]}` | no |",
                          f"| [#41]({FORK_PULL}41) [#42]({FORK_PULL}42) | Merge pull request #42 "
                          f"from Shopify/stack | `{self.stack[:12]}` → `{replayed[1][:12]}` "
                          "| no |"],
                         rows[:4])
        self.assertEqual(8, len(rows))
        upstream = self.section(body, "## Upstream")
        self.assertIn(f"[`{self.to}`]({UPSTREAM_COMMIT}{self.to})", upstream)
        self.assertIn("2026-11-07", upstream)
        self.assertIn("Upstream change", upstream)
        self.assertIn(f"`{self.old}`", upstream)
        range_diff = self.section(body, "## Range-diff")
        self.assertEqual(6, range_diff.count("### "))
        self.assertIn("Cap the refresh batch (#70)", range_diff)
        self.assertIn("Shopify-PR: #41 #42", range_diff)
        self.assertEqual("- #74: could not read upstream PR yugabyte#38160: no pull requests found",
                         self.section(body, "## Warnings"))
        self.assertEqual([f"Opened {PR_URL} as a draft.",
                          f"Review it with Binks before requesting review: "
                          f"devx binks review run {PR_URL}",
                          f"Worktree: {self.worktree}"], out.splitlines()[-3:])

    def test_the_preview_lists_what_the_sync_carries_and_drops(self):
        out = self.published()

        replayed = [commit["sha"] for commit in self.candidate()[1:]]
        lines = out.splitlines()
        start = lines.index(f"Candidate {self.branch} at {replayed[-1][:12]} in {self.worktree}:")
        self.assertEqual([
            f"  Upstream: {self.to[:12]} 2026-11-07 Upstream change",
            f"  Old master-shopify: {self.old[:12]}",
            "  Carried (6):",
            f"    #70  Cap the refresh batch (#70)  {self.l70[:12]} -> {replayed[0][:12]}",
            f"    #41 #42  Merge pull request #42 from Shopify/stack  {self.stack[:12]} -> "
            f"{replayed[1][:12]}",
            f"    #71  Expose the batch metric (#71)  {self.l71[:12]} -> {replayed[2][:12]}",
            f"    #72  Fix the restart race (#72)  {self.l72[:12]} -> {replayed[3][:12]}",
            f"    #73  Retry the refresh (#73)  {self.l73[:12]} -> {replayed[4][:12]}",
            f"    #74  Tune the retry backoff (#74)  {self.l74[:12]} -> {replayed[5][:12]}",
            "  Dropped (1):",
            "    #69 -> yugabyte#38110",
            "  Conflicts resolved: none",
            "  Warnings (1):",
            "    #74: could not read upstream PR yugabyte#38160: no pull requests found",
        ], lines[start + 1:start + 15])

    def test_the_interactive_run_asks_once_before_pushing(self):
        code, out, err = self.sync(answers=["y"])

        self.assertEqual((0, ""), (code, err), out)
        self.assertIn(PROMPT.format(short=self.short), out)
        self.assertEqual(self.candidate()[-1]["sha"], self.remote_head())

    def test_declining_stops_with_the_rerun_command_and_writes_nothing(self):
        code, out, err = self.sync(answers=["n"])

        self.assertEqual((4, ""), (code, err), out)
        self.assertTrue(out.endswith(f"{PROMPT.format(short=self.short)}Not pushed. To push it and "
                                     f"open its PR later:\n  {self.rerun('--yes')}\n"), out)
        self.assertEqual(([], None), (self.writes(), self.remote_head()))

    def test_without_a_terminal_or_yes_it_stops_with_the_rerun_command(self):
        code, out, err = self.sync()

        self.assertEqual((4, ""), (code, err), out)
        self.assertEqual([f"To push {self.branch} and open its PR, rerun:",
                          "  " + self.rerun("--yes")], out.splitlines()[-2:])
        self.assertEqual(([], None), (self.writes(), self.remote_head()))
        self.assertEqual(7, len(self.candidate()))

        out = self.published("--continue")

        self.assertEqual(self.candidate()[-1]["sha"], self.remote_head())
        self.assertEqual(7, len(self.candidate()))

    def test_a_range_diff_that_fails_becomes_a_warning_and_the_run_completes(self):
        original = Checkout.range_diff

        def range_diff(checkout, args):
            if "--right-only" in args:
                raise GitError("Failed to run `git range-diff`\nerror: unknown option "
                               "`diff-merges'\n")
            return original(checkout, args)

        with patch.object(Checkout, "range_diff", range_diff):
            self.published()

        replayed = self.candidate()[2]["sha"]
        command = (f"git range-diff --diff-merges=first-parent --right-only {self.stack}^1.."
                   f"{self.stack} {replayed}^..{replayed}")
        body = self.body()
        self.assertIn("### #41 #42 Merge pull request #42 from Shopify/stack\n\nNot available: "
                      f"git range-diff failed here; see it with `{command}`.",
                      self.section(body, "## Range-diff"))
        self.assertIn("- the range-diff of #41 #42 failed (error: unknown option 'diff-merges'; "
                      "merge-landing range-diffs need git 2.48 or later); see it with "
                      f"`{command}`", self.section(body, "## Warnings"))
        self.assertEqual(self.candidate()[-1]["sha"], self.remote_head())

    def test_status_on_the_candidate_reports_carried_and_adopted_prs(self):
        self.published()
        out, err = io.StringIO(), io.StringIO()

        code = Cli.run(["status", "--repo-path", self.dev, "--no-fetch", "--fork-ref", self.branch,
                        "--upstream-ref", self.to], github=self.github, out=out, err=err, now=NOW)

        self.assertEqual((0, ""), (code, err.getvalue()), out.getvalue())
        lines = out.getvalue().splitlines()
        self.assertTrue(lines[0].startswith(f"{self.branch} {self.candidate()[-1]['sha'][:9]}: "
                                            f"6 carried commits since upstream {self.to[:9]}"),
                        lines[0])
        self.assertTrue(lines[0].endswith("(sync)"), lines[0])
        self.assertEqual([
            "Not submitted (2)",
            f"  #70  18d   {'[#38101] DocDB: Cap the refresh batch':<60} priya",
            f"  #74  18d   {'[#38105] DocDB: Tune the retry backoff':<60} priya",
            "Open upstream (1)", "  #71  yugabyte#38131  Upstream 38131",
            "Landed upstream, dropped at the next sync (0)",
            "Restructured upstream, awaiting a replacement Upstream: comment (1)",
            "  #73  yugabyte#38150  merged 2026-11-03",
            "Closed upstream without merging, needs a disposition (1)",
            "  #72  yugabyte#38140  Upstream 38140",
            "Fork-only (2)", "  #41 #42",
            "Reverted in the fork (0)",
            "Adopted by earlier syncs (1)", "  #69  yugabyte#38110  dropped 2026-11-19",
            "Unidentified commits (0)", "",
            "Warnings", "  #74: could not read upstream PR yugabyte#38160: no pull requests found",
        ], [re.sub(r"dropped \d{4}-\d\d-\d\d", "dropped 2026-11-19", line) for line in lines[2:]])

    def test_a_replayed_landing_without_a_pr_number_gets_a_shopify_pr_trailer(self):
        landing = self.add_landing({"src/size.cc": "size\n"}, "Tune the cache size", 76)

        self.published()

        commits = self.candidate()
        replayed = commits[-1]["sha"]
        self.assertEqual(self.raw_message(landing) + "\nShopify-PR: #76\n",
                         self.raw_message(replayed))
        landed, unidentified = attribute(commits[1:], self.merged)
        self.assertEqual(([replayed], [commits[2]["sha"]], []),
                         (landed[76], landed[41], unidentified))

    def upgrade(self):
        body = self.body()
        self.assertRegex(body.lower(), UPGRADE_CHECK)
        return self.section(body, "## Upgrade/Rollback safety")

    def tail(self, old):
        return (f"The previous `master-shopify` tip `{old[:12]}` stays reachable as the sync "
                "merge's second parent, named by its `Old-master-shopify:` trailer.")

    def test_the_body_answers_upstreams_upgrade_check_without_proto_changes(self):
        self.published()

        self.assertEqual("No `.proto` file changes in this diff. " + self.tail(self.old),
                         self.upgrade())

    def test_the_upgrade_section_names_upstreams_proto_changes_and_a_clean_proto_replay(self):
        self.upstream_commit({"src/yb/common/common.proto": "message A {}\n",
                              "src/yb/master/master_ddl.proto": "message B {}\n"},
                             "Upstream proto change")
        meta = self.add_landing({"src/yb/meta.proto": "message Meta {}\n"},
                                "Add the meta message", 77)

        self.published()

        self.assertEqual(
            "2 `.proto` files change in this diff: 2 in upstream's commits "
            f"`{self.u1[:12]}..{self.to[:12]}`, reviewed and merged in yugabyte/yugabyte-db, "
            "which this sync takes as they are (`src/yb/common/common.proto`, "
            "`src/yb/master/master_ddl.proto`). #77 replays its original landing without "
            "conflict (range-diff below), so that fork PR's upgrade and rollback notes still "
            "apply. " + self.tail(meta), self.upgrade())

    def test_the_upgrade_section_qualifies_a_proto_replay_adapted_in_a_conflict(self):
        meta = self.add_landing({"src/yb/meta.proto": "message Meta {}\n"},
                                "Add the meta message", 77)
        self.upstream_commit({"src/yb/meta.proto": "message Upstream {}\n"}, "Upstream meta")
        code, out, err = self.sync()
        self.assertEqual((4, ""), (code, err), out)
        self.resolve_in_worktree("src/yb/meta.proto", "message Meta {}\nmessage Upstream {}\n")

        self.published("--continue")

        self.assertEqual(
            "1 `.proto` file changes in this diff: 1 changed both in upstream's commits "
            f"`{self.u1[:12]}..{self.to[:12]}` and by a replayed landing, accounted for below "
            "(`src/yb/meta.proto`). #77 was replayed "
            "with a conflict resolution; its original notes may not describe the resolved "
            "schema: review its range-diff and add notes here before landing. "
            + self.tail(meta), self.upgrade())

    def test_a_proto_conflict_resolved_in_upstreams_favour_is_reported_as_adapted(self):
        meta = self.add_landing({"src/yb/meta.proto": "message Meta {}\n",
                                 "src/yb/meta.cc": "meta\n"}, "Add the meta message", 77)
        self.upstream_commit({"src/yb/meta.proto": "message Upstream {}\n"}, "Upstream meta")
        code, out, err = self.sync()
        self.assertEqual((4, ""), (code, err), out)
        self.resolve_in_worktree("src/yb/meta.proto", "message Upstream {}\n")

        self.published("--continue")

        self.assertEqual(
            "1 `.proto` file changes in this diff: 1 in upstream's commits "
            f"`{self.u1[:12]}..{self.to[:12]}`, reviewed and merged in yugabyte/yugabyte-db, "
            "which this sync takes as they are (`src/yb/meta.proto`). #77 was replayed with a "
            "conflict resolution and its own `.proto` changes were resolved away in favour of "
            "upstream's; its original notes may not describe the resolved schema: review its "
            "range-diff and add notes here before landing. " + self.tail(meta), self.upgrade())

    def test_an_empty_proto_pick_is_reported_as_already_upstream(self):
        meta = self.add_landing({"src/yb/meta.proto": "message Meta {}\n"},
                                "Add the meta message", 77)
        self.upstream_commit({"src/yb/meta.proto": "message Meta {}\n"}, "Upstream meta")
        code, out, err = self.sync()
        self.assertEqual((4, ""), (code, err), out)
        git(self.worktree, "cherry-pick", "--skip")

        self.published("--continue")

        self.assertEqual(
            "No `.proto` file changes in this diff. #77, which changed `.proto` files, applied "
            "empty on upstream: upstream already contains its schema changes, and this sync "
            "carries nothing of it. " + self.tail(meta), self.upgrade())

    def test_the_upgrade_section_reports_a_dropped_proto_landing(self):
        meta = self.add_landing({"src/yb/meta.proto": "message Meta {}\n"},
                                "Add the meta message", 77)
        self.upstream_commit({"src/yb/meta.proto": "message Upstream {}\n"}, "Upstream meta")
        code, out, err = self.sync()
        self.assertEqual((4, ""), (code, err), out)
        git(self.worktree, "cherry-pick", "--abort")

        self.published("--continue", "--skip", "77")

        self.assertEqual(
            "1 `.proto` file changes in this diff: 1 in upstream's commits "
            f"`{self.u1[:12]}..{self.to[:12]}`, reviewed and merged in yugabyte/yugabyte-db, "
            "which this sync takes as they are (`src/yb/meta.proto`). #77, which changed "
            "`.proto` files, is dropped from the fork by this sync (it conflicted and was "
            "skipped), so the diff removes its schema changes. " + self.tail(meta),
            self.upgrade())

    def test_a_same_named_pr_from_another_fork_is_not_reused(self):
        self.answer_open_prs([open_pr(300, owner="someone"), open_pr(301, base="main")])

        self.published()

        self.assertEqual([("pr", "create")], [entry[1:3] for entry in self.writes()
                                              if entry[1] == "pr"])

    def test_a_commit_made_while_the_question_is_open_is_refused_and_nothing_is_pushed(self):
        def commit():
            Path(self.worktree, "src/sneak.cc").write_text("sneak\n")
            git(self.worktree, "add", "src/sneak.cc")
            git(self.worktree, "commit", "--quiet", "-m", "Sneak in")

        code, out, err = self.sync(stdin=Deciding(commit, "y"))

        head, moved = self.candidate()[-2]["sha"], self.candidate()[-1]["sha"]
        self.assertEqual((1, [], None), (code, self.writes(), self.remote_head()), out)
        self.assertIn(PROMPT.format(short=self.short), out)
        self.assertEqual(f"{self.branch} moved from {head[:12]} to {moved[:12]}: the candidate "
                         "changed while you were deciding; rerun with --continue\n", err)

    def test_large_range_diffs_keep_the_body_under_the_cap_and_the_pr_is_opened(self):
        big = "\n".join("x" * 100 for _ in range(250)) + "\n"

        with patch.object(Checkout, "range_diff", lambda checkout, args: big):
            self.published()

        body = self.body()
        self.assertLess(len(body), 60000)
        self.assertIn("Truncated at 200 of 250 lines", body)
        self.assertIn("Omitted to keep this body under 60,000 characters", body)
        self.assertEqual(LANDING.format(head=self.candidate()[-1]["sha"]),
                         self.section(body, "## Landing"))

    def test_a_body_over_the_cap_without_range_diffs_is_refused_before_pushing(self):
        self.answer_merged(*(fork_pr(number, "[#38109] DocDB: Lost on the way", MISSING)
                             for number in range(1000, 3000)))

        code, out, err = self.sync("--yes")

        size = len(Path(self.tmpdir, "scratch", "sync-body.md").read_text(encoding="utf-8"))
        self.assertEqual((1, [], None), (code, self.writes(), self.remote_head()), out)
        self.assertEqual(f"The PR body would be {size:,} characters, over the 60,000-character "
                         "cap even with every range-diff omitted; nothing was pushed. See "
                         f"{os.path.join(self.tmpdir, 'scratch', 'sync-body.md')} and rerun with "
                         "--continue once it is shorter\n", err)

    def test_a_rerun_with_the_pr_open_pushes_and_updates_its_body(self):
        self.published()
        self.answer_labels([SKIP, "sync"])
        self.answer_open_prs([open_pr(200)])
        self.log.clear()

        out = self.published("--continue")

        body = self.body()
        self.assertEqual([("gh", "pr", "edit", "200", "--repo", FORK, "--body-file",
                           ("file", body))], self.writes())
        self.assertEqual(self.candidate()[-1]["sha"], self.remote_head())
        self.assertIn(f"Updated the body of {PR_URL}.", out)

    def test_a_rerun_after_rewriting_the_pushed_candidate_is_refused(self):
        self.published()
        pushed = self.remote_head()
        self.answer_open_prs([open_pr(200)])
        git(self.worktree, "commit", "--quiet", "--amend", "-m",
            "Tune the retry backoff (#74)\n\nEdited.")
        self.log.clear()

        code, out, err = self.sync("--continue", "--yes")

        head = self.candidate()[-1]["sha"]
        self.assertEqual((1, []), (code, self.writes()), out)
        self.assertEqual(f"origin's {self.branch} ({pushed[:12]}) is not an ancestor of the "
                         f"candidate ({head[:12]}): the candidate was rewritten after it was "
                         "pushed, and sync never force-pushes. Run --abort and sync again, or "
                         "restore the pushed history in the worktree.\n", err)
        self.assertEqual(pushed, self.remote_head())


class RefusalTest(SyncTestCase):
    def test_nothing_to_sync_when_master_shopify_contains_the_upstream_commit(self):
        code, out, err = self.sync("--to", self.u1)

        self.assertEqual((0, f"master-shopify already contains upstream {self.u1[:12]}; nothing to "
                             "sync\n", ""), (code, out, err))
        self.assertFalse(os.path.exists(os.path.dirname(self.worktree) + "/yugabyte-db-sync-"
                                        + self.u1[:12]))

    def test_a_target_off_upstream_master_is_refused(self):
        code, out, err = self.sync("--to", self.l70)

        self.assertEqual((1, ""), (code, out))
        self.assertEqual(f"{self.l70} ({self.l70[:12]}) is not on upstream master "
                         f"(refs/shopify/upstream-master at {self.to[:12]})\n", err)

    def test_unidentified_landings_are_refused_before_anything_is_built(self):
        self.fixture.git("checkout", "--quiet", "master-shopify")
        typo = self.write({"docs/runbook.md": "typo\n"}, "Fix a typo in the runbook", T0 + 6 * DAY)
        self.fixture.git("checkout", "--quiet", "-b", "feature")
        self.write({"src/feature.cc": "f\n"}, "Add a feature", T0 + 6 * DAY)
        self.fixture.git("checkout", "--quiet", "master-shopify")
        merge = self.merge("feature", "Merge branch 'feature'", T0 + 6 * DAY)
        self.publish_fork()

        code, out, err = self.sync("--yes")

        self.assertEqual((1, ""), (code, out))
        self.assertEqual(f"2 commits on master-shopify since upstream {self.u1[:12]} belong to no "
                         "merged fork PR; attribute these landings first:\n"
                         f"  {typo[:12]}  Fix a typo in the runbook\n"
                         f"  {merge[:12]}  Merge branch 'feature' (a merge commit that is neither "
                         "a sync point nor a landing)\n", err)
        self.assertEqual(["master-shopify"], self.local_branches())
        self.assertFalse(os.path.exists(self.worktree))

    def test_a_sparse_checkout_is_refused(self):
        git(self.dev, "config", "core.sparseCheckout", "true")

        self.assertEqual((1, "", f"sync needs a full checkout; {self.dev} is sparse "
                                 "(core.sparseCheckout is true)\n"), self.sync("--yes"))

    def test_a_checkout_whose_origin_is_not_the_fork_is_refused(self):
        git(self.dev, "remote", "set-url", "origin", self.upstream_url)

        self.assertEqual((1, "", f"origin of {self.dev} is {self.upstream_url}, not "
                                 "Shopify/yugabyte-db; sync runs in a fork checkout\n"),
                         self.sync("--yes"))

    def test_continue_and_abort_together_are_a_usage_error(self):
        self.assertEqual((2, "", "Pass --continue or --abort, not both\n"),
                         self.sync("--continue", "--abort"))

    def test_continue_without_a_candidate_is_refused(self):
        self.assertEqual((1, "", f"There is no {self.branch} to continue; run sync without "
                                 "--continue\n"), self.sync("--continue", "--yes"))

    def test_an_existing_candidate_without_continue_is_refused(self):
        self.sync()

        code, out, err = self.sync("--yes")

        self.assertEqual((1, "", f"{self.branch} already exists; use --continue to resume or "
                                 "--abort to discard\n"), (code, out, err))

    def test_a_candidate_only_on_origin_is_refused_too_and_continue_rebuilds_its_worktree(self):
        self.published()
        git(self.dev, "worktree", "remove", self.worktree)
        git(self.dev, "branch", "--quiet", "-D", self.branch)
        self.answer_open_prs([open_pr(200)])

        self.assertEqual((1, "", f"{self.branch} already exists; use --continue to resume or "
                                 "--abort to discard\n"), self.sync("--yes"))

        out = self.published("--continue")

        self.assertEqual(self.remote_head(), git(self.worktree, "rev-parse", "HEAD"))
        self.assertIn("| [#70](" + FORK_PULL + "70) | Cap the refresh batch (#70) | ",
                      self.body())
        self.assertIn(" | ? |", self.body())
        self.assertIn(f"Updated the body of {PR_URL}.", out)

    def test_continue_refuses_a_candidate_checked_out_in_the_developers_checkout(self):
        self.sync()
        git(self.dev, "worktree", "remove", self.worktree)
        git(self.dev, "checkout", "--quiet", self.branch)

        self.assertEqual((1, "", f"{self.branch} is checked out in your checkout "
                                 f"{os.path.realpath(self.dev)}; check out another branch in "
                                 "your checkout, then rerun\n"),
                         self.sync("--continue", "--yes"))
        self.assertIsNone(self.remote_head())

    def test_abort_refuses_a_candidate_checked_out_in_the_developers_checkout(self):
        self.sync()
        git(self.dev, "worktree", "remove", self.worktree)
        git(self.dev, "checkout", "--quiet", self.branch)

        self.assertEqual((1, "", f"{self.branch} is checked out in your checkout "
                                 f"{os.path.realpath(self.dev)}; check out another branch in "
                                 "your checkout, then rerun\n"), self.sync("--abort"))
        self.assertEqual(self.branch, git(self.dev, "symbolic-ref", "--short", "HEAD"))

    def test_continue_after_master_shopify_moved_is_refused(self):
        self.sync()
        self.fixture.git("checkout", "--quiet", "master-shopify")
        moved = self.write({"src/late.cc": "late\n"}, "Land late (#75)", T0 + 7 * DAY)
        self.publish_fork()

        code, out, err = self.sync("--continue", "--yes")

        self.assertEqual((1, ""), (code, out))
        self.assertEqual(f"master-shopify moved from {self.old[:12]} to {moved[:12]} since "
                         f"{self.branch} was built; run --abort and sync again\n", err)

    def test_a_commit_on_the_candidate_that_replays_no_landing_is_refused(self):
        self.sync()
        Path(self.worktree, "src/fix.cc").write_text("fix\n")
        git(self.worktree, "add", "src/fix.cc")
        git(self.worktree, "commit", "--quiet", "-m", "Fix the build")
        extra = git(self.worktree, "rev-parse", "HEAD")

        code, out, err = self.sync("--continue", "--yes")

        self.assertEqual((1, None), (code, self.remote_head()), out)
        self.assertEqual(f"{extra[:12]} Fix the build on the candidate replays no planned landing; "
                         "sync carries only landings. Remove it (land a fix on master-shopify "
                         "first), then rerun with --continue\n", err)

    def test_a_rerun_without_fetching_reuses_the_fetched_refs(self):
        self.sync()

        code, out, err = self.sync("--continue", "--no-fetch", "--yes")

        self.assertEqual((0, ""), (code, err), out)
        self.assertEqual(self.candidate()[-1]["sha"], self.remote_head())


class StopTest(SyncTestCase):
    def test_a_conflict_stops_and_continue_finishes_after_it_is_resolved(self):
        self.upstream_commit({"src/batch.cc": "upstream cap\n"}, "Cap batches upstream")

        code, out, err = self.sync()

        self.assertEqual((4, ""), (code, err), out)
        self.assertEqual([
            f"Cherry-picking fork PR #70 ({self.l70[:12]} Cap the refresh batch (#70)) conflicts "
            "in:",
            "  src/batch.cc",
            "Resolve it, then continue:",
            f"  cd {self.worktree}",
            "  (resolve the conflicts)",
            "  git add <the resolved paths>",
            "  git cherry-pick --continue",
            "  " + self.rerun(),
            "To drop it instead, keeping upstream's version:",
            f"  git -C {self.worktree} cherry-pick --abort",
            "  " + self.rerun(skip=70),
        ], out.splitlines()[-11:])
        self.assertEqual(1, len(self.candidate()))

        code, out, err = self.sync("--continue", "--yes")

        self.assertEqual((4, ""), (code, err), out)
        self.assertIn("  src/batch.cc", out)

        self.resolve_in_worktree("src/batch.cc", "both\n")

        self.published("--continue")

        commits = self.candidate()
        self.assertEqual(7, len(commits))
        self.assertEqual(self.message(self.l70), self.message(commits[1]["sha"]))
        rows = self.section(self.body(), "## Carried").splitlines()
        self.assertTrue(rows[2].endswith(" | yes |"), rows[2])
        self.assertTrue(rows[3].endswith(" | no |"), rows[3])
        self.assertEqual(commits[-1]["sha"], self.remote_head())

    def test_a_conflicting_merge_landing_gets_its_trailer_on_continue(self):
        self.upstream_commit({"src/layer1.cc": "upstream layer\n"}, "Add a layer upstream")

        code, out, err = self.sync()

        self.assertEqual((4, ""), (code, err), out)
        self.assertIn("Cherry-picking fork PRs #41 #42", out)

        self.resolve_in_worktree("src/layer1.cc", "one\n")

        self.published("--continue")

        replayed = self.candidate()[2]["sha"]
        self.assertEqual(self.raw_message(self.stack) + "\nShopify-PR: #41 #42\n",
                         self.raw_message(replayed))

    def test_continue_after_aborting_a_conflicted_pick_picks_it_again(self):
        self.upstream_commit({"src/batch.cc": "upstream cap\n"}, "Cap batches upstream")
        self.sync()
        git(self.worktree, "cherry-pick", "--abort")

        code, out, err = self.sync("--continue", "--yes")

        self.assertEqual((4, ""), (code, err), out)
        self.assertEqual([f"Cherry-picking fork PR #70 ({self.l70[:12]} Cap the refresh batch "
                          "(#70)) conflicts in:", "  src/batch.cc"], out.splitlines()[-11:-9])
        self.assertEqual((1, None), (len(self.candidate()), self.remote_head()))

    def test_a_resolved_conflict_without_a_pr_number_gets_its_trailer_on_continue(self):
        landing = self.add_landing({"src/size.cc": "size\n"}, "Tune the cache size", 76)
        self.upstream_commit({"src/size.cc": "upstream size\n"}, "Size the cache upstream")
        self.assertEqual(4, self.sync()[0])
        self.resolve_in_worktree("src/size.cc", "both\n")

        self.published("--continue")

        commits = self.candidate()
        replayed = commits[-1]["sha"]
        self.assertEqual(self.raw_message(landing) + "\nShopify-PR: #76\n",
                         self.raw_message(replayed))
        landed, unidentified = attribute(commits[1:], self.merged)
        self.assertEqual(([replayed], []), (landed[76], unidentified))
        self.assertTrue(self.section(self.body(), "## Carried").splitlines()[-1]
                        .endswith(" | yes |"))

    def test_recovery_commands_name_the_script_and_the_checkout_absolutely(self):
        self.upstream_commit({"src/batch.cc": "upstream cap\n"}, "Cap batches upstream")

        code, out, err = self.sync(program=None, repo_path=os.path.relpath(self.dev))

        rerun = ["python3", SCRIPT, "sync", "--continue"]
        where = ["--to", self.to, "--repo-path", self.dev, "--upstream-url", self.upstream_url]
        self.assertEqual((4, ""), (code, err), out)
        self.assertEqual(["  " + shlex.join(rerun + where),
                          "To drop it instead, keeping upstream's version:",
                          f"  git -C {self.worktree} cherry-pick --abort",
                          "  " + shlex.join(rerun + ["--skip", "#70"] + where)],
                         out.splitlines()[-4:])

    def test_skip_drops_a_conflicting_landing_after_its_pick_is_aborted(self):
        self.upstream_commit({"src/batch.cc": "upstream cap\n"}, "Cap batches upstream")
        self.assertEqual(4, self.sync()[0])
        git(self.worktree, "cherry-pick", "--abort")

        self.published("--continue", "--skip", "#70")

        self.assertEqual(["Merge pull request #42 from Shopify/stack",
                          "Expose the batch metric (#71)", "Fix the restart race (#72)",
                          "Retry the refresh (#73)", "Tune the retry backoff (#74)"],
                         [commit["subject"] for commit in self.candidate()[1:]])
        body = self.body()
        self.assertNotIn(f"[#70]({FORK_PULL}70)", self.section(body, "## Carried"))
        self.assertIn(f"- fork PR #70 conflicted on upstream {self.short} and was skipped; "
                      "status reports it as neither carried nor adopted until a disposition "
                      "exists", self.section(body, "## Warnings"))
        self.assertEqual(self.candidate()[-1]["sha"], self.remote_head())

    def test_skip_of_a_pr_that_is_not_the_conflicting_pick_is_a_usage_error(self):
        self.upstream_commit({"src/batch.cc": "upstream cap\n"}, "Cap batches upstream")
        self.sync()
        git(self.worktree, "cherry-pick", "--abort")

        self.assertEqual((2, "", "--skip #71: fork PR #71 is not the conflicting pick this sync "
                                 "stopped on\n"), self.sync("--continue", "--skip", "71", "--yes"))
        self.assertEqual((1, None), (len(self.candidate()), self.remote_head()))

    def test_skip_without_continue_is_a_usage_error(self):
        self.assertEqual((2, "", "--skip needs --continue\n"), self.sync("--skip", "70"))

    def test_a_committed_replay_pairs_by_its_trailer_with_twins_carrying_their_own(self):
        self.fixture.git("checkout", "--quiet", "master-shopify")
        self.write({"src/version.cc": "2\n"}, "Bump the version\n\nShopify-PR: #80", T0 + 6 * DAY)
        self.write({"src/version.cc": "3\n"}, "Bump the version\n\nShopify-PR: #81", T0 + 6 * DAY)
        self.publish_fork()
        self.fixture.git("checkout", "--quiet", "main")
        self.upstream_commit({"src/version.cc": "upstream\n"}, "Version upstream")
        self.answer_merged(fork_pr(80, "[#38106] DocDB: Bump the version", MISSING),
                           fork_pr(81, "[#38107] DocDB: Bump the version again", MISSING))
        self.assertEqual(4, self.sync()[0])
        git(self.worktree, "cherry-pick", "--abort")
        self.assertEqual(4, self.sync("--continue", "--skip", "80")[0])
        self.resolve_in_worktree("src/version.cc", "3\n")

        self.published("--continue")

        replayed = self.candidate()[-1]
        self.assertEqual("Bump the version\n\nShopify-PR: #81\n",
                         self.raw_message(replayed["sha"]))
        body = self.body()
        carried = self.section(body, "## Carried")
        self.assertTrue(carried.splitlines()[-1].startswith(f"| [#81]({FORK_PULL}81) | Bump "
                                                            "the version | "), carried)
        self.assertTrue(carried.splitlines()[-1].endswith(" | yes |"), carried)
        self.assertNotIn(f"[#80]({FORK_PULL}80)", carried)
        self.assertIn(f"- fork PR #80 conflicted on upstream {self.short} and was skipped",
                      self.section(body, "## Warnings"))
        self.assertEqual(replayed["sha"], self.remote_head())

    def test_landings_that_share_a_subject_pair_by_their_recorded_replay(self):
        self.fixture.git("checkout", "--quiet", "master-shopify")
        l80 = self.write({"src/version.cc": "2\n"}, "Bump the version", T0 + 6 * DAY)
        l81 = self.write({"src/version.cc": "3\n"}, "Bump the version", T0 + 6 * DAY)
        self.publish_fork()
        self.fixture.git("checkout", "--quiet", "main")
        self.upstream_commit({"src/version.cc": "2\n"}, "Bump the version upstream")
        self.answer_merged(fork_pr(80, "[#38106] DocDB: Bump the version", l80),
                           fork_pr(81, "[#38107] DocDB: Bump the version again", l81))
        self.assertEqual(4, self.sync()[0])
        git(self.worktree, "cherry-pick", "--skip")
        self.assertEqual(4, self.sync("--continue")[0])

        self.published("--continue")

        replayed = self.candidate()[-1]["sha"]
        rows = self.section(self.body(), "## Carried").splitlines()
        self.assertEqual(f"| [#81]({FORK_PULL}81) | Bump the version | `{l81[:12]}` → "
                         f"`{replayed[:12]}` | no |", rows[-1])
        self.assertNotIn(f"[#80]({FORK_PULL}80)", self.section(self.body(), "## Carried"))
        self.assertEqual(replayed, self.remote_head())

    def test_an_empty_pick_stops_and_continue_after_skip_leaves_it_out(self):
        self.upstream_commit({"src/batch.cc": "cap\n"}, "Cap batches upstream")

        code, out, err = self.sync()

        self.assertEqual((4, ""), (code, err), out)
        self.assertEqual([
            f"fork PR #70 applies empty on upstream {self.short}; if upstream took an equivalent "
            f"fix, run `git cherry-pick --skip` in {self.worktree} and rerun with --continue; "
            "status will then report #70 as neither carried nor adopted until a disposition "
            "exists",
            "  " + self.rerun(),
        ], out.splitlines()[-2:])

        git(self.worktree, "cherry-pick", "--skip")
        self.published("--continue")

        commits = self.candidate()
        self.assertEqual(["Merge pull request #42 from Shopify/stack",
                          "Expose the batch metric (#71)", "Fix the restart race (#72)",
                          "Retry the refresh (#73)", "Tune the retry backoff (#74)"],
                         [commit["subject"] for commit in commits[1:]])
        body = self.body()
        self.assertNotIn("| [#70](", self.section(body, "## Carried"))
        self.assertIn(f"- fork PR #70 applies empty on upstream {self.short} and was skipped; "
                      "status reports it as neither carried nor adopted until a disposition "
                      "exists", self.section(body, "## Warnings"))
        out, err = io.StringIO(), io.StringIO()
        Cli.run(["status", "--repo-path", self.dev, "--no-fetch", "--fork-ref", self.branch,
                 "--upstream-ref", self.to], github=self.github, out=out, err=err, now=NOW)
        self.assertIn("  merged PR #70 is neither carried nor adopted", out.getvalue())

    def test_abort_removes_the_worktree_and_the_branch(self):
        self.upstream_commit({"src/batch.cc": "upstream cap\n"}, "Cap batches upstream")
        self.sync()

        code, out, err = self.sync("--abort")

        self.assertEqual((0, ""), (code, err), out)
        self.assertEqual([f"Removed the worktree {self.worktree}.",
                          f"Deleted the branch {self.branch}."], out.splitlines())
        self.assertFalse(os.path.exists(self.worktree))
        self.assertEqual(["master-shopify"], self.local_branches())
        self.assertNotIn(self.worktree, git(self.dev, "worktree", "list"))

    def test_abort_asks_before_deleting_a_pushed_candidate(self):
        self.published()

        code, out, err = self.sync("--abort", answers=["y"])

        self.assertEqual((0, ""), (code, err), out)
        self.assertTrue(out.endswith(f"Delete {self.branch} on origin? Its PR closes. [y/N] "
                                     f"Deleted {self.branch} on origin.\n"), out)
        self.assertIsNone(self.remote_head())
        self.assertEqual(["master-shopify"], self.local_branches())

    def test_abort_without_a_terminal_keeps_a_pushed_candidate(self):
        self.published()
        pushed = self.remote_head()

        code, out, err = self.sync("--abort")

        self.assertEqual((0, ""), (code, err), out)
        self.assertEqual(f"Kept {self.branch} on origin; delete it with `git push origin --delete "
                         f"{self.branch}`.", out.splitlines()[-1])
        self.assertEqual(pushed, self.remote_head())

    def test_abort_without_a_candidate_has_nothing_to_do(self):
        self.assertEqual((0, "There is no shopify-sync/ branch; nothing to abort.\n", ""),
                         self.sync("--abort"))


class RangeDiffSectionTest(unittest.TestCase):
    def test_each_range_diff_is_capped_at_200_lines(self):
        text = "\n".join(f"line {n}" for n in range(250))

        section = range_diff_section([("#70", "Fix", "git range-diff a^..a b^..b", text)], 60000)

        self.assertIn("line 199\n", section)
        self.assertNotIn("line 200\n", section)
        self.assertIn("Truncated at 200 of 250 lines; see the rest with "
                      "`git range-diff a^..a b^..b`.", section)

    def test_range_diffs_beyond_the_budget_are_omitted_with_their_command(self):
        text = "\n".join("x" * 90 for _ in range(150))

        section = range_diff_section([("#70", "Fix", "git range-diff a^..a b^..b", text),
                                      ("#71", "Fix", "git range-diff c^..c d^..d", text)], 20000)

        self.assertLess(len(section), 20000)
        self.assertIn("Omitted to keep this body under 60,000 characters; see it with "
                      "`git range-diff c^..c d^..d`.", section)

    def test_a_fence_is_longer_than_any_backtick_run_in_the_diff(self):
        section = range_diff_section([("#70", "Fix", "git range-diff a^..a b^..b",
                                       "has ``` inside")], 60000)

        self.assertIn("````\nhas ``` inside\n````", section)

    def test_a_failed_range_diff_counts_against_the_budget(self):
        text = "\n".join("x" * 90 for _ in range(100))

        section = range_diff_section([("#70", "Fix", "git range-diff a^..a b^..b", None),
                                      ("#71", "Fix", "git range-diff c^..c d^..d", text)], 9100)

        self.assertLess(len(section), 9100)
        self.assertIn("### #70 Fix\n\nNot available: git range-diff failed here; see it with "
                      "`git range-diff a^..a b^..b`.", section)
        self.assertIn("### #71 Fix\n\nOmitted to keep this body under 60,000 characters; see it "
                      "with `git range-diff c^..c d^..d`.", section)

    def test_entries_whose_placeholders_do_not_fit_are_omitted_in_one_block(self):
        text = "\n".join("x" * 90 for _ in range(30))
        entries = [(f"#{n}", "A carried landing with a long subject",
                    f"git range-diff {'a' * 40}^..{'a' * 40} {'b' * 40}^..{'b' * 40}", text)
                   for n in range(100, 160)]
        entries[0] = ("#100", "Short", "git range-diff a^..a b^..b", "1:  a = 1:  b Short")

        section = range_diff_section(entries, 3000)

        self.assertLess(len(section), 3000)
        self.assertTrue(section.startswith("### #100 Short\n\n```\n1:  a = 1:  b Short\n```"),
                        section)
        self.assertIn("### Omitted\n\nThe range-diffs of 59 replays (#101, #102, ", section)
        self.assertIn("#159) are omitted to keep this body under 60,000 characters. See each "
                      "with `git range-diff <old>^..<old> <new>^..<new>`, the old and new SHAs "
                      "from the Carried table (for a merge landing, `--diff-merges=first-parent "
                      "--right-only <old>^1..<old>`).", section)
        self.assertNotIn("see it with", section)


class CliTest(testfixtures.RepoTestCase):
    def test_sync_help_documents_its_options(self):
        out = io.StringIO()

        self.assertEqual(0, Cli.run(["sync", "--help"], out=out))
        for option in ("--to", "--continue", "--skip", "--abort", "--repo-path", "--no-fetch",
                       "--yes", "--upstream-url"):
            self.assertIn(option, out.getvalue())

    def test_importing_the_module_has_no_side_effects(self):
        home, cwd = os.path.join(self.tmpdir, "home"), os.path.join(self.tmpdir, "cwd")
        os.makedirs(home)
        os.makedirs(cwd)
        directory = os.path.dirname(os.path.abspath(__file__))
        code = f"import sys; sys.path.insert(0, {directory!r}); import upstream_sync"

        result = subprocess.run(["python3", "-I", "-B", "-c", code], cwd=cwd, capture_output=True,
                                text=True, env={**os.environ, "HOME": home})

        self.assertEqual((0, "", ""), (result.returncode, result.stdout, result.stderr))
        self.assertEqual(([], []), (os.listdir(home), os.listdir(cwd)))


if __name__ == "__main__":
    unittest.main()
