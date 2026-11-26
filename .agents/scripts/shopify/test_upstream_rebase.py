"""Exercise `upstream.py rebase-open-prs` against throwaway repositories and an in-memory GitHub.

The fork is a bare repository whose `master-shopify` has just landed a sync, with open PR
branches based on the master-shopify before it; the developer's checkout is cloned from it.
`gh` answers from canned responses keyed by its exact arguments, and keeps the PR comments it
was given. Every rebase is a real one in a real worktree; nothing touches the network.
"""

import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import unittest
from unittest.mock import patch

from scm import Error
from test_upstream_sync import NOW, dates, git
from upstream import Cli
from upstream_rebase import GitHub
import testfixtures


FORK = "Shopify/yugabyte-db"
FORK_PULL = "https://github.com/Shopify/yugabyte-db/pull/"
OPEN_FIELDS = ("number,baseRefName,headRefName,headRefOid,headRepository,headRepositoryOwner,"
               "labels,url")
LABEL = "no-auto-rebase"
LABEL_DESCRIPTION = "Not rebased by upstream.py rebase-open-prs after a sync"
T0, DAY = 1793534400, 86400  # 2026-11-01T12:00:00Z
TEN = "".join(f"{n}\n" for n in range(1, 11))


class FakeGitHub(GitHub):
    """Answers gh by its exact argv from canned responses, keeps the comments `pr comment`
    posts (served back by the comments query), and records every write in `log`."""

    def __init__(self):
        super().__init__(executable="/nonexistent/gh")
        self.responses = {}
        self.log = []
        self.posted = {}
        self.refuse_comments = False

    def answer(self, *args, value):
        self.responses[tuple(args)] = value

    def run(self, *args, cwd=None):
        if args[:2] in (("pr", "comment"), ("label", "create")):
            self.log.append(args)
            if args[:2] == ("pr", "comment") and self.refuse_comments:
                raise Error("Failed to run `gh pr comment`\nHTTP 403: Forbidden\n")
            if args[:2] == ("pr", "comment"):
                body = args[args.index("--body") + 1]
                self.posted.setdefault(int(args[2]), []).append(body)
            return ""
        if args[0] == "api" and args[1].startswith(f"repos/{FORK}/issues/"):
            number = int(args[1].split("/")[-2])
            return json.dumps([[{"body": body, "created_at": "2026-11-20T00:00:00Z"}
                                for body in self.posted.get(number, [])]])
        if args not in self.responses:
            raise AssertionError(f"unexpected gh call: {args}")
        value = self.responses[args]
        return value if isinstance(value, str) else json.dumps(value)


class RebaseTestCase(testfixtures.RepoTestCase):
    """Upstream `main` is a base, then an upstream change to src/d.cc and src/p.cc's line 3.
    The old master-shopify is the base plus the fork's landing #100. The sync merged it into the
    upstream change with `-s ours` and replayed #100, and that landed on master-shopify. Open:
    #301 (feature-a), and the stack #302 (stack-1) under #303 (stack-2), all on the old tip."""

    def setUp(self):
        super().setUp()
        self.github = FakeGitHub()
        self.fork_url = os.path.join(self.tmpdir, "remotes", "Shopify", "yugabyte-db.git")
        self.fixture.init_bare(self.fork_url)
        self.base = self.write({"src/a.cc": "base\n", "src/b.cc": "base\n", "src/c.cc": "base\n",
                                "src/d.cc": "base\n", "src/e.cc": "base\n", "src/p.cc": TEN},
                               "Upstream base", T0)
        self.fixture.git("checkout", "--quiet", "-b", "master-shopify")
        self.old_tip = self.write({"src/e.cc": "fork\n"}, "Keep the fork's tweak (#100)",
                                  T0 + DAY)
        self.heads = {}
        self.branch("feature-a", self.old_tip, {"src/a.cc": "a\n"}, "Change a")
        self.branch("stack-1", self.old_tip, {"src/b.cc": "b\n"}, "Change b")
        self.branch("stack-2", "stack-1", {"src/c.cc": "c\n"}, "Change c")
        self.fixture.git("checkout", "--quiet", "main")
        self.upstream = self.write({"src/d.cc": "upstream\n",
                                    "src/p.cc": TEN.replace("3\n", "three\n")},
                                   "Upstream change (#30000)", T0 + 3 * DAY)
        self.fixture.git("checkout", "--quiet", "-b", "sync")
        self.fixture.git("merge", "--quiet", "--no-ff", "-s", "ours", "-m",
                         f"Sync master-shopify to upstream {self.upstream[:12]}\n\n"
                         f"Old-master-shopify: {self.old_tip}", self.old_tip,
                         env=dates(T0 + 4 * DAY))
        self.fixture.git("cherry-pick", self.old_tip, env=dates(T0 + 4 * DAY))
        self.tip = self.fixture.git("rev-parse", "HEAD")
        self.fixture.git("push", "--quiet", self.fork_url, "sync:refs/heads/master-shopify")
        self.fixture.git("checkout", "--quiet", "main")
        git(self.fork_url, "symbolic-ref", "HEAD", "refs/heads/master-shopify")
        self.work = os.path.join(self.tmpdir, "work")
        self.dev = os.path.join(self.work, "yugabyte-db")
        os.makedirs(self.work)
        git(self.tmpdir, "clone", "--quiet", self.fork_url, self.dev)
        self.github.answer("auth", "status", value="")
        self.answer_labels(["upstream: skip", "sync", LABEL])
        self.prs = [self.pr(301, "feature-a"), self.pr(302, "stack-1"),
                    self.pr(303, "stack-2", base="stack-1")]
        self.answer_open()

    # Fixture authoring.

    def write(self, files, message, timestamp, author=None):
        for name, content in files.items():
            Path(self.fixture.path, name).parent.mkdir(parents=True, exist_ok=True)
            Path(self.fixture.path, name).write_text(content)
        self.fixture.git("add", "-A")
        env = dates(timestamp)
        if author:
            env["GIT_AUTHOR_NAME"] = author
        self.fixture.git("commit", "--quiet", "-m", message, env=env)
        return self.fixture.git("rev-parse", "HEAD")

    def branch(self, name, start, files, message, publish=True):
        """Commits `message` on a new branch `name` from `start` and pushes it to the fork."""
        self.fixture.git("checkout", "--quiet", "-b", name, start)
        self.heads[name] = self.write(files, message, T0 + 2 * DAY, author="Priya")
        if publish:
            self.fixture.git("push", "--quiet", self.fork_url, f"{name}:refs/heads/{name}")
        return self.heads[name]

    def move(self, name, files, message):
        """The author pushes another commit to `name` on origin."""
        self.fixture.git("checkout", "--quiet", name)
        self.heads[name] = self.write(files, message, T0 + 5 * DAY, author="Priya")
        self.fixture.git("push", "--quiet", self.fork_url, f"{name}:refs/heads/{name}")
        self.fixture.git("checkout", "--quiet", "main")

    def open_branch(self, number, name, start, files, base="master-shopify"):
        self.branch(name, start, files, f"Change {name}")
        self.fixture.git("checkout", "--quiet", "main")
        self.prs.append(self.pr(number, name, base=base))
        self.answer_open()

    @staticmethod
    def pr(number, head, base="master-shopify", labels=(), owner="Shopify"):
        return {"number": number, "baseRefName": base, "headRefName": head,
                "headRefOid": "0" * 40, "headRepository": {"name": "yugabyte-db"},
                "headRepositoryOwner": {"login": owner},
                "labels": [{"name": label} for label in labels], "url": FORK_PULL + str(number)}

    def answer_open(self):
        self.github.answer("pr", "list", "--repo", FORK, "--state", "open", "-L", "5000",
                           "--json", OPEN_FIELDS, value=self.prs)

    def answer_labels(self, names):
        self.github.answer("label", "list", "--repo", FORK, "--limit", "1000", "--json", "name",
                           value=[{"name": name} for name in names])

    def label(self, number, *labels):
        next(pr for pr in self.prs if pr["number"] == number)["labels"] = [
            {"name": label} for label in labels]
        self.answer_open()

    # Running and inspection.

    def rebase(self, *argv):
        out, err = io.StringIO(), io.StringIO()
        code = Cli.run(["rebase-open-prs", *argv, "--repo-path", self.dev], github=self.github,
                       out=out, err=err, now=NOW, interactive=False, program="upstream.py",
                       cwd=self.work, workdir=os.path.join(self.tmpdir, "scratch"))
        return code, out.getvalue(), err.getvalue()

    def completed(self, *argv):
        code, out, err = self.rebase(*argv)
        self.assertEqual((0, ""), (code, err), out)
        return out

    def origin(self, name):
        return git(self.fork_url, "rev-parse", f"refs/heads/{name}")

    def origin_refs(self):
        return git(self.fork_url, "for-each-ref", "--format=%(refname) %(objectname)")

    def writes(self):
        return list(self.github.log)

    def marker(self):
        return f"<!-- upstream.py rebase-open-prs {self.tip} -->"

    def rebased_comment(self, branch):
        return (f"{self.marker()}\nRebased onto master-shopify {self.tip[:12]} after the sync to "
                f"upstream {self.upstream[:12]}; the patch is unchanged. Before working on this "
                f"branch: `git fetch origin && git reset --hard origin/{branch}` when you have no "
                "local changes, or `gh stack sync --remote origin` when you do (it re-pushes the "
                "branch under a new sha).")

    def developer_state(self):
        return (git(self.dev, "rev-parse", "HEAD"), git(self.dev, "symbolic-ref", "HEAD"),
                git(self.dev, "status", "--porcelain"),
                git(self.dev, "for-each-ref", "--format=%(refname) %(objectname)", "refs/heads"))

    def assert_no_scratch_worktrees(self):
        self.assertEqual(["yugabyte-db"], os.listdir(self.work))
        listed = git(self.dev, "worktree", "list", "--porcelain").splitlines()
        self.assertEqual([f"worktree {os.path.realpath(self.dev)}"],
                         [line for line in listed if line.startswith("worktree ")])

    def lines(self, out, heading):
        """The entry lines under `heading (n):` in the human report."""
        rows = out.splitlines()
        start = next(k for k, row in enumerate(rows) if row.startswith(heading + " ("))
        end = start + 1
        while end < len(rows) and rows[end].startswith("  "):
            end += 1
        return rows[start + 1:end]

    def patch(self, sha):
        return git(self.fork_url, "show", "--format=", sha)


class CleanRebaseTest(RebaseTestCase):
    def test_a_clean_chain_is_rebased_pushed_with_a_lease_and_commented(self):
        shim = os.path.join(self.tmpdir, "shim")
        os.makedirs(shim)
        calls = os.path.join(self.tmpdir, "git-calls")
        Path(shim, "git").write_text(f"#!/bin/sh\nprintf '%s\\n' \"$*\" >> {calls}\n"
                                     f"exec {shutil.which('git')} \"$@\"\n")
        os.chmod(os.path.join(shim, "git"), 0o755)
        before = self.developer_state()

        with patch.dict(os.environ, {"PATH": f"{shim}{os.pathsep}{os.environ['PATH']}"}):
            out = self.completed("--push")

        new = {name: self.origin(name) for name in ("feature-a", "stack-1", "stack-2")}
        self.assertEqual(self.tip, git(self.dev, "rev-parse", f"{new['feature-a']}^"))
        self.assertEqual(new["stack-1"], git(self.dev, "rev-parse", f"{new['stack-2']}^"))
        self.assertEqual(self.tip, git(self.dev, "rev-parse", f"{new['stack-1']}^"))
        for name, sha in new.items():
            self.assertEqual(git(self.fork_url, "log", "-1", "--format=%an %ae %aI",
                                 self.heads[name]),
                             git(self.fork_url, "log", "-1", "--format=%an %ae %aI", sha))
            self.assertEqual(self.patch(self.heads[name]), self.patch(sha))
        pushes = [line for line in Path(calls).read_text().splitlines() if " push " in line]
        self.assertEqual([f"-C {os.path.realpath(self.dev)} push --quiet --force-with-lease="
                          f"refs/heads/{name}:{self.heads[name]} origin "
                          f"{new[name]}:refs/heads/{name}"
                          for name in ("feature-a", "stack-1", "stack-2")],
                         [line.replace(self.dev, os.path.realpath(self.dev)) for line in pushes])
        self.assertEqual([("pr", "comment", str(number), "--repo", FORK, "--body",
                           self.rebased_comment(name))
                          for number, name in ((301, "feature-a"), (302, "stack-1"),
                                               (303, "stack-2"))], self.writes())
        self.assertEqual([
            f"Rebasing the open PRs onto master-shopify {self.tip[:12]}, the sync to upstream "
            f"{self.upstream[:12]}.",
            "Rebased (3):",
            f"  #301 feature-a  {self.heads['feature-a'][:12]} -> {new['feature-a'][:12]}  "
            "pushed, commented",
            f"  #302 stack-1  {self.heads['stack-1'][:12]} -> {new['stack-1'][:12]}  "
            "pushed, commented",
            f"  #303 stack-2  {self.heads['stack-2'][:12]} -> {new['stack-2'][:12]}  "
            "pushed, commented",
            "Conflicted (0):", "Not pushed (0):", "Up to date (0):", "Skipped (0):"],
            out.splitlines())
        self.assertEqual(before, self.developer_state())
        self.assert_no_scratch_worktrees()

    def test_a_dry_run_reports_and_writes_nothing(self):
        refs, before = self.origin_refs(), self.developer_state()
        self.answer_labels([])

        out = self.completed()

        self.assertEqual(refs, self.origin_refs())
        self.assertEqual([], self.writes())
        self.assertEqual({}, self.github.posted)
        rows = self.lines(out, "Rebased")
        self.assertEqual(3, len(rows))
        self.assertTrue(rows[0].startswith(f"  #301 feature-a  {self.heads['feature-a'][:12]} -> "))
        self.assertTrue(all(row.endswith("  would push") for row in rows), rows)
        self.assertEqual("Dry run: nothing was pushed or commented; rerun with --push to push the "
                         "rebased branches and comment on their PRs.", out.splitlines()[-1])
        self.assertEqual(before, self.developer_state())
        self.assert_no_scratch_worktrees()

    def test_json_reports_the_same_groups(self):
        out = self.completed("--json")

        report = json.loads(out)
        self.assertEqual({"master_shopify": self.tip, "upstream": self.upstream[:12],
                          "push": False},
                         {key: report[key] for key in ("master_shopify", "upstream", "push")})
        self.assertEqual([301, 302, 303], [entry["number"] for entry in report["rebased"]])
        first = report["rebased"][0]
        self.assertEqual(("feature-a", "master-shopify", self.heads["feature-a"], "rebased",
                          False, False),
                         (first["branch"], first["base"], first["old"], first["status"],
                          first["pushed"], first["commented"]))
        self.assertEqual(self.tip, git(self.dev, "rev-parse", f"{first['new']}^"))
        self.assertEqual(([], [], [], []), (report["conflicted"], report["not_pushed"],
                                            report["up_to_date"], report["skipped"]))

    def test_pr_restricts_the_run_to_that_prs_chain(self):
        out = self.completed("--pr", "303", "--push")

        self.assertEqual(["#302", "#303"], [row.split()[0] for row in self.lines(out, "Rebased")])
        self.assertEqual(self.heads["feature-a"], self.origin("feature-a"))
        self.assertEqual([302, 303], sorted(self.github.posted))

    def test_a_pr_outside_every_chain_is_refused(self):
        code, out, err = self.rebase("--pr", "999")

        self.assertEqual((1, "#999 is not an open PR into master-shopify or stacked on one\n"),
                         (code, err), out)

    def test_a_rerun_for_the_same_tip_finds_every_branch_up_to_date(self):
        self.completed("--push")
        comments = dict(self.github.posted)

        out = self.completed("--push")

        self.assertEqual(comments, self.github.posted)
        self.assertEqual(["#301", "#302", "#303"],
                         [row.split()[0] for row in self.lines(out, "Up to date")])

    def test_a_pr_with_no_commits_of_its_own_is_left_alone(self):
        # A branch pushed at the old tip with nothing on it: nothing to rebase, nothing to push.
        self.fixture.git("checkout", "--quiet", "-b", "empty", self.old_tip)
        self.fixture.git("push", "--quiet", self.fork_url, "empty:refs/heads/empty")
        self.fixture.git("checkout", "--quiet", "main")
        self.prs.append(self.pr(350, "empty"))
        self.answer_open()

        out = self.completed("--push")

        self.assertIn("#350", " ".join(self.lines(out, "Up to date")))
        self.assertIn("no commits of its own", out)
        self.assertEqual(self.old_tip, git(self.fork_url, "rev-parse", "refs/heads/empty"))
        self.assertNotIn(350, dict(self.github.posted))

    def test_without_fetching_it_rebases_the_fetched_heads(self):
        out = self.completed("--no-fetch")

        self.assertEqual(3, len(self.lines(out, "Rebased")))


class ConflictTest(RebaseTestCase):
    def setUp(self):
        super().setUp()
        self.open_branch(311, "mid-1", self.old_tip, {"src/m1.cc": "one\n"})
        self.open_branch(312, "mid-2", "mid-1", {"src/d.cc": "fork\n"}, base="mid-1")
        self.open_branch(313, "mid-3", "mid-2", {"src/m3.cc": "three\n"}, base="mid-2")

    def test_a_conflict_mid_chain_stops_the_layers_above_and_leaves_them_untouched(self):
        before = self.developer_state()

        out = self.completed("--push")

        self.assertEqual(self.tip, git(self.dev, "rev-parse", f"{self.origin('mid-1')}^"))
        self.assertEqual((self.heads["mid-2"], self.heads["mid-3"]),
                         (self.origin("mid-2"), self.origin("mid-3")))
        self.assertEqual([f"  #312 mid-2  conflicts in src/d.cc (last changed by "
                          f"{self.upstream[:12]} Upstream change (#30000))  commented"],
                         self.lines(out, "Conflicted"))
        self.assertEqual(["  #313 mid-3  not rebased: #312 below it conflicted  commented"],
                         self.lines(out, "Not pushed"))
        self.assertEqual([
            f"{self.marker()}\nNot rebased onto master-shopify {self.tip[:12]} after the sync to "
            f"upstream {self.upstream[:12]}: `mid-2` conflicts with it in:\n\n"
            f"- `src/d.cc`, last changed there by {self.upstream[:12]} Upstream change (#30000)\n\n"
            "Rebase it yourself:\n\n"
            "1. `gh stack rebase --remote origin`\n"
            "2. Resolve the conflicts and `git add` the resolved files.\n"
            "3. `gh stack rebase --continue`\n"
            "4. `build-support/lint.sh --rev origin/master-shopify`\n"
            "5. `gh stack push --remote origin`\n\n"
            "A resolution changes the diff, so request a new Binks review."],
            self.github.posted[312])
        self.assertEqual([
            f"{self.marker()}\nNot rebased: #312 below it conflicted, so nothing was pushed for "
            f"master-shopify {self.tip[:12]}; once that is resolved, `gh stack rebase --remote "
            "origin` restacks this layer too."], self.github.posted[313])
        self.assertEqual(before, self.developer_state())
        self.assert_no_scratch_worktrees()

    def test_the_comment_marker_makes_a_rerun_for_the_same_tip_comment_once(self):
        self.open_branch(314, "clash", self.old_tip, {"src/d.cc": "clash\n"})
        self.completed("--push")

        out = self.completed("--push")

        self.assertEqual((1, 1, 1), tuple(len(self.github.posted[n]) for n in (312, 313, 314)))
        self.assertEqual([f"  #314 clash  conflicts in src/d.cc (last changed by "
                          f"{self.upstream[:12]} Upstream change (#30000))"],
                         self.lines(out, "Conflicted"))
        self.assertIn("  #311 mid-1  " + self.origin("mid-1")[:12], self.lines(out, "Up to date"))
        self.assertEqual(["  #312 mid-2  not rebased: not stacked on #311's head",
                          "  #313 mid-3  not rebased: #312 below it was not rebased"],
                         self.lines(out, "Not pushed"))
        self.assertEqual((self.heads["mid-2"], self.heads["mid-3"]),
                         (self.origin("mid-2"), self.origin("mid-3")))
        self.assert_no_scratch_worktrees()


class NotPushedTest(RebaseTestCase):
    def test_a_rebase_that_changes_the_patch_is_not_pushed(self):
        self.open_branch(321, "context", self.old_tip, {"src/p.cc": TEN.replace("5\n", "five\n")})
        self.open_branch(322, "context-2", "context", {"src/q.cc": "q\n"}, base="context")

        out = self.completed("--push")

        self.assertEqual((self.heads["context"], self.heads["context-2"]),
                         (self.origin("context"), self.origin("context-2")))
        rows = self.lines(out, "Not pushed")
        self.assertTrue(rows[0].startswith(f"  #321 context  {self.heads['context'][:12]} -> "),
                        rows)
        self.assertTrue(rows[0].endswith("  patch changed, not pushed  commented"), rows)
        self.assertEqual("  #322 context-2  not rebased: #321 below it changed its patch  "
                         "commented", rows[1])
        self.assertEqual([
            f"{self.marker()}\nNot pushed: `context` rebases onto master-shopify "
            f"{self.tip[:12]} without conflicts, but its patch changed: upstream touched its "
            "context, or a commit of it is already on master-shopify. Rebase it yourself (`gh "
            "stack rebase --remote origin`), check the diff, lint, `gh stack push --remote "
            "origin`, and request a new Binks review."],
            self.github.posted[321])

    def test_a_branch_moved_on_origin_fails_its_lease_and_the_layers_above_are_not_pushed(self):
        self.move("stack-1", {"src/b.cc": "b2\n"}, "Amend b")
        moved = self.origin("stack-1")

        out = self.completed("--push", "--no-fetch")

        self.assertEqual((moved, self.heads["stack-2"]),
                         (self.origin("stack-1"), self.origin("stack-2")))
        self.assertEqual(self.tip, git(self.dev, "rev-parse", f"{self.origin('feature-a')}^"))
        rows = self.lines(out, "Not pushed")
        self.assertTrue(rows[0].startswith("  #302 stack-1  "), rows)
        self.assertTrue(rows[0].endswith("  stack-1 moved on origin, not pushed  commented"), rows)
        self.assertEqual("  #303 stack-2  not pushed: #302 below it was not pushed  commented",
                         rows[1])
        self.assertEqual([
            f"{self.marker()}\nNot pushed: `stack-1` moved on origin while it was being rebased "
            f"onto master-shopify {self.tip[:12]}; rebase it yourself with `gh stack rebase "
            "--remote origin`, then `gh stack push --remote origin`."], self.github.posted[302])

    def test_a_branch_pushed_on_a_rerun_is_commented_despite_the_earlier_marker(self):
        self.move("feature-a", {"src/a.cc": "a2\n"}, "Amend a")
        self.completed("--push", "--no-fetch")

        self.completed("--push")

        self.assertEqual(self.tip, git(self.dev, "rev-parse", f"{self.origin('feature-a')}~2"))
        self.assertEqual(2, len(self.github.posted[301]))
        self.assertEqual(self.rebased_comment("feature-a"), self.github.posted[301][1])

    def test_a_layer_left_behind_by_its_lower_layer_is_not_rebased(self):
        self.move("stack-1", {"src/b.cc": "b2\n"}, "Amend b")

        out = self.completed("--push")

        self.assertEqual(self.heads["stack-2"], self.origin("stack-2"))
        self.assertEqual(self.tip, git(self.dev, "rev-parse", f"{self.origin('stack-1')}~2"))
        self.assertEqual(["  #303 stack-2  not rebased: not stacked on #302's head  commented"],
                         self.lines(out, "Not pushed"))
        self.assertEqual([
            f"{self.marker()}\nNot rebased onto master-shopify {self.tip[:12]}: `stack-2` is not "
            "stacked on the head of `stack-1`, the layer below it, so which commits are its own "
            "is unclear; restack it with `gh stack rebase --remote origin`, then `gh stack push "
            "--remote origin`."], self.github.posted[303])

    def test_a_push_origin_rejects_fails_the_run_after_reporting_it(self):
        hook = Path(self.fork_url, "hooks", "pre-receive")
        hook.write_text("#!/bin/sh\necho 'branch is frozen' >&2\nexit 1\n")
        hook.chmod(0o755)

        code, out, err = self.rebase("--push")

        self.assertEqual((1, ""), (code, err), out)
        self.assertEqual((self.heads["feature-a"], self.heads["stack-1"]),
                         (self.origin("feature-a"), self.origin("stack-1")))
        rows = self.lines(out, "Not pushed")
        self.assertEqual(["#301", "#302", "#303"], [row.split()[0] for row in rows])
        self.assertIn("push failed: ", rows[0])
        self.assertEqual("  #303 stack-2  not pushed: #302 below it was not pushed  commented",
                         rows[2])
        self.assertNotIn(301, self.github.posted)

    def test_a_failed_comment_is_a_warning_and_the_report_still_prints(self):
        self.github.refuse_comments = True

        code, out, err = self.rebase("--push")

        self.assertEqual((1, "Could not comment on #301: HTTP 403: Forbidden\n"),
                         (code, err.splitlines(keepends=True)[0]), out)
        self.assertEqual(self.tip, git(self.dev, "rev-parse", f"{self.origin('feature-a')}^"))
        self.assertEqual(["#301", "#302", "#303"],
                         [row.split()[0] for row in self.lines(out, "Rebased")])
        self.assertTrue(self.lines(out, "Rebased")[0].endswith("  pushed"))

    def test_a_branch_holding_a_merge_commit_is_not_rebased_whatever_the_config(self):
        git(self.dev, "config", "rebase.rebaseMerges", "true")
        self.fixture.git("checkout", "--quiet", "-b", "side", self.old_tip)
        self.write({"src/side.cc": "discarded\n"}, "The side the merge discards", T0 + 2 * DAY)
        self.branch("merged", self.old_tip, {"src/m.cc": "m\n"}, "Change m")
        self.fixture.git("merge", "--quiet", "--no-ff", "-s", "ours", "-m", "Merge side", "side",
                         env=dates(T0 + 2 * DAY))
        self.heads["merged"] = self.fixture.git("rev-parse", "HEAD")
        self.fixture.git("push", "--quiet", self.fork_url, "merged:refs/heads/merged")
        self.open_branch(371, "merged-2", "merged", {"src/m2.cc": "m2\n"}, base="merged")
        self.prs.append(self.pr(370, "merged"))
        self.answer_open()

        out = self.completed("--push")

        self.assertEqual((self.heads["merged"], self.heads["merged-2"]),
                         (self.origin("merged"), self.origin("merged-2")))
        self.assertEqual(["  #370 merged  holds a merge commit, not rebased  commented",
                          "  #371 merged-2  not rebased: #370 below it holds a merge commit  "
                          "commented"], self.lines(out, "Not pushed"))
        self.assertEqual([
            f"{self.marker()}\nNot rebased onto master-shopify {self.tip[:12]}: `merged` holds a "
            "merge commit, which rebase-open-prs does not replay; rebase it yourself with `gh "
            "stack rebase --remote origin`, check the diff, lint, `gh stack push --remote "
            "origin`, and request a new Binks review."], self.github.posted[370])
        self.assert_no_scratch_worktrees()

    def test_a_commit_without_a_patch_id_counts_as_a_changed_patch(self):
        self.branch("empty", self.old_tip, {"src/x.cc": "x\n"}, "Change x")
        self.fixture.git("commit", "--quiet", "--allow-empty", "-m", "Record nothing",
                         env=dates(T0 + 2 * DAY))
        self.heads["empty"] = self.fixture.git("rev-parse", "HEAD")
        self.fixture.git("push", "--quiet", self.fork_url, "empty:refs/heads/empty")
        self.fixture.git("checkout", "--quiet", "main")
        self.prs.append(self.pr(372, "empty"))
        self.answer_open()

        out = self.completed("--push")

        self.assertEqual(self.heads["empty"], self.origin("empty"))
        rows = self.lines(out, "Not pushed")
        self.assertTrue(rows[0].startswith("  #372 empty  "), rows)
        self.assertTrue(rows[0].endswith("  patch changed, not pushed  commented"), rows)

    def test_an_operational_rebase_failure_fails_the_run_without_commenting(self):
        git(self.dev, "config", "commit.gpgSign", "true")
        git(self.dev, "config", "gpg.program", "false")
        refs = self.origin_refs()

        code, out, err = self.rebase("--push")

        self.assertEqual(1, code, out)
        self.assertTrue(err.startswith("Failed to run `git rebase --onto "), err)
        self.assertIn("gpg", err)
        self.assertEqual((refs, [], {}), (self.origin_refs(), self.writes(), self.github.posted))
        self.assert_no_scratch_worktrees()


class SkipTest(RebaseTestCase):
    def test_an_opted_out_pr_is_skipped_commented_and_the_label_created(self):
        self.answer_labels(["sync"])
        self.label(301, LABEL)

        out = self.completed("--push")

        self.assertEqual(self.heads["feature-a"], self.origin("feature-a"))
        self.assertEqual(["  #301 feature-a  labelled no-auto-rebase  commented"],
                         self.lines(out, "Skipped"))
        self.assertEqual(("label", "create", LABEL, "--repo", FORK, "--description",
                          LABEL_DESCRIPTION, "--color", "FBCA04"), self.writes()[0])
        self.assertEqual([
            f"{self.marker()}\nNot rebased onto master-shopify {self.tip[:12]}: this PR is "
            "labelled `no-auto-rebase`; rebase it yourself with `gh stack rebase --remote "
            "origin` when you are ready."], self.github.posted[301])

    def test_an_existing_label_is_not_created_again(self):
        self.completed("--push")

        self.assertNotIn("create", [entry[1] for entry in self.writes()])

    def test_a_skipped_bottom_skips_the_layers_above(self):
        self.label(302, LABEL)

        out = self.completed("--push")

        self.assertEqual((self.heads["stack-1"], self.heads["stack-2"]),
                         (self.origin("stack-1"), self.origin("stack-2")))
        self.assertEqual(["  #302 stack-1  labelled no-auto-rebase  commented",
                          "  #303 stack-2  not rebased: #302 below it is skipped"],
                         self.lines(out, "Skipped"))
        self.assertNotIn(303, self.github.posted)

    def test_sync_prs_and_heads_outside_the_fork_are_skipped_without_a_comment(self):
        self.prs += [self.pr(340, "shopify-sync/0123456789ab", labels=["sync", "upstream: skip"]),
                     self.pr(341, "feature-x", owner="priya")]
        self.answer_open()

        out = self.completed("--push")

        self.assertEqual(["  #340 shopify-sync/0123456789ab  labelled sync",
                          "  #341 feature-x  head is in priya/yugabyte-db, not "
                          "Shopify/yugabyte-db"], self.lines(out, "Skipped"))
        self.assertEqual([301, 302, 303], sorted(self.github.posted))

    def shared_head(self):
        """#351 stacks `shared` on stack-1; the opted-out #352 has the same head into
        master-shopify."""
        self.open_branch(351, "shared", "stack-1", {"src/s.cc": "s\n"}, base="stack-1")
        self.prs.append(self.pr(352, "shared", labels=[LABEL]))
        self.answer_open()

    def test_an_opted_out_head_branch_is_not_pushed_through_another_pr(self):
        self.shared_head()

        out = self.completed("--push")

        self.assertEqual(self.heads["shared"], self.origin("shared"))
        self.assertEqual(["  #351 shared  its head branch is opted out by #352",
                          "  #352 shared  labelled no-auto-rebase  commented"],
                         self.lines(out, "Skipped"))
        self.assertNotIn(351, self.github.posted)

    def test_pr_does_not_drop_the_pr_that_opts_the_head_branch_out(self):
        self.shared_head()

        out = self.completed("--pr", "351", "--push")

        self.assertEqual(self.heads["shared"], self.origin("shared"))
        self.assertEqual(["  #351 shared  its head branch is opted out by #352"],
                         self.lines(out, "Skipped"))
        self.assertEqual(["#302", "#303"], [row.split()[0] for row in self.lines(out, "Rebased")])

    def test_a_pr_whose_head_is_master_shopify_is_skipped_and_not_traversed(self):
        self.prs.append(self.pr(360, "master-shopify", base="feature-a"))
        self.answer_open()

        out = self.completed("--push")

        self.assertEqual(["#301", "#302", "#303"],
                         [row.split()[0] for row in self.lines(out, "Rebased")])
        self.assertEqual(["  #360 master-shopify  head is master-shopify"],
                         self.lines(out, "Skipped"))
        self.assertEqual(self.tip, self.origin("master-shopify"))
        self.assertEqual(self.origin("stack-1"), git(self.dev, "rev-parse",
                                                     f"{self.origin('stack-2')}^"))

    def test_pr_with_a_master_shopify_headed_pr_open_does_not_crash(self):
        self.prs.append(self.pr(360, "master-shopify", base="feature-a"))
        self.answer_open()

        out = self.completed("--pr", "303", "--push")

        self.assertEqual(["#302", "#303"], [row.split()[0] for row in self.lines(out, "Rebased")])
        self.assertEqual([], self.lines(out, "Skipped"))


class PreconditionTest(RebaseTestCase):
    def test_uncommitted_changes_in_the_checkout_are_refused(self):
        Path(self.dev, "src/a.cc").write_text("dirty\n")

        code, out, err = self.rebase("--push")

        self.assertEqual((1, f"{os.path.realpath(self.dev)} has uncommitted changes to tracked "
                          "files; commit or stash them, then rerun\n"),
                         (code, err.replace(self.dev, os.path.realpath(self.dev))), out)
        self.assertEqual([], self.writes())

    def test_a_pr_number_that_is_not_one_is_a_usage_error(self):
        code, out, err = self.rebase("--pr", "abc")

        self.assertEqual(2, code, out)
        self.assertIn("abc is not a fork PR number such as #70", err)


class CliTest(testfixtures.RepoTestCase):
    def test_rebase_open_prs_help_documents_its_options(self):
        out = io.StringIO()

        self.assertEqual(0, Cli.run(["rebase-open-prs", "--help"], out=out))
        for option in ("--push", "--pr", "--repo-path", "--no-fetch", "--json"):
            self.assertIn(option, out.getvalue())

    def test_importing_the_module_has_no_side_effects(self):
        home, cwd = os.path.join(self.tmpdir, "home"), os.path.join(self.tmpdir, "cwd")
        os.makedirs(home)
        os.makedirs(cwd)
        directory = os.path.dirname(os.path.abspath(__file__))
        code = f"import sys; sys.path.insert(0, {directory!r}); import upstream_rebase"

        result = subprocess.run(["python3", "-I", "-B", "-c", code], cwd=cwd, capture_output=True,
                                text=True, env={**os.environ, "HOME": home})

        self.assertEqual((0, "", ""), (result.returncode, result.stdout, result.stderr))
        self.assertEqual(([], []), (os.listdir(home), os.listdir(cwd)))


if __name__ == "__main__":
    unittest.main()
