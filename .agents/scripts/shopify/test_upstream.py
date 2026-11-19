"""Exercise `upstream.py status` against throwaway git repositories and an in-memory GitHub.

Tests never read or write a real checkout and never call GitHub or the network.
"""

from datetime import datetime, timezone
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import unittest

from upstream import Cli, Error, GitHub, parser, upstream_state
import testfixtures


SCRIPT = str(Path(__file__).resolve().with_name("upstream.py"))
T0, DAY = 1793534400, 86400  # 2026-11-01T12:00:00Z
NOW = datetime(2026, 11, 19, 12, tzinfo=timezone.utc)
UPSTREAM_PR = "https://github.com/yugabyte/yugabyte-db/pull/"
MERGED_UPSTREAM = "a" * 40


class FakeGitHub(GitHub):
    """In-memory stand-in for the four gh queries `status` makes; anything else fails."""

    def __init__(self, pull_requests=(), comments=None, upstream=None, ahead=None):
        super().__init__(executable="/nonexistent/gh")
        self.pull_requests = list(pull_requests)
        self.comments_by_pr = comments or {}
        self.upstream = upstream or {}
        self.ahead = ahead or {}
        self.comment_requests = []

    def merged_pull_requests(self):
        return self.pull_requests

    def comments(self, number):
        self.comment_requests.append(number)
        return self.comments_by_pr.get(number, [])

    def upstream_pull_request(self, number):
        if number not in self.upstream:
            raise Error(f"Failed to run `gh pr view {number} --repo yugabyte/yugabyte-db`\n"
                        "no pull requests found\n")
        return self.upstream[number]

    def ahead_by(self, sha):
        return self.ahead[sha]


def fork_pr(number, title, merge=None, labels=(), merged_at="2026-11-01T12:00:00Z"):
    return {"number": number, "title": title, "mergeCommit": {"oid": merge} if merge else None,
            "mergedAt": merged_at, "labels": [{"name": label} for label in labels],
            "author": {"login": "priya"},
            "url": f"https://github.com/Shopify/yugabyte-db/pull/{number}"}


def comment(body, created_at="2026-11-02T00:00:00Z"):
    return {"body": body, "created_at": created_at}


def upstream_pr(number, state, title="Expose the refresh batch metric"):
    merged = state == "MERGED"
    return {"number": number, "state": state, "title": title, "url": UPSTREAM_PR + str(number),
            "mergedAt": "2026-11-12T08:00:00Z" if merged else None,
            "mergeCommit": {"oid": MERGED_UPSTREAM} if merged else None}


def submitted(number, upstream_number, state, ahead=None):
    """A GitHub where fork PR `number` has an Upstream: comment naming `upstream_number`."""
    return FakeGitHub(
        [], {number: [comment(f"Upstream: {UPSTREAM_PR}{upstream_number}")]},
        {upstream_number: upstream_pr(upstream_number, state)},
        {} if ahead is None else {MERGED_UPSTREAM: ahead})


def dates(timestamp):
    return {"GIT_AUTHOR_DATE": f"{timestamp} +0000", "GIT_COMMITTER_DATE": f"{timestamp} +0000"}


class StatusTestCase(testfixtures.RepoTestCase):
    """main is the fork's history and `up` upstream's; both start from one base commit."""

    def setUp(self):
        super().setUp()
        self.base = self.fixture.commit("upstream base", T0)
        self.fixture.branch("up")

    def upstream_commit(self, message, at):
        self.fixture.git("checkout", "--quiet", "up")
        sha = self.fixture.commit(message, at)
        self.fixture.git("checkout", "--quiet", "-")
        return sha

    def merge(self, branch, message, at, *options):
        self.fixture.git("merge", "--quiet", "--no-ff", *options, "-m", message, branch,
                         env=dates(at))
        return self.fixture.git("rev-parse", "HEAD")

    def branch_commits(self, name, *messages):
        """Commits `messages` on a new branch from main and returns to main."""
        self.fixture.git("checkout", "--quiet", "-b", name, "main")
        shas = [self.fixture.commit(message, T0 + DAY) for message in messages]
        self.fixture.git("checkout", "--quiet", "main")
        return shas

    def publish(self, fork="main", upstream="up"):
        self.fixture.ref("refs/remotes/origin/master-shopify", self.fixture.git("rev-parse", fork))
        if upstream is not None:
            self.fixture.ref("refs/remotes/upstream/master",
                             self.fixture.git("rev-parse", upstream))

    def cli(self, *argv, github=None):
        out, err = io.StringIO(), io.StringIO()
        code = Cli.run(list(argv), github=github or FakeGitHub(), out=out, err=err, now=NOW)
        return code, out.getvalue(), err.getvalue()

    def report(self, github, *options, fork="main"):
        self.publish(fork)
        code, out, err = self.cli("status", "--repo-path", self.fixture.path, "--no-fetch",
                                  *options, github=github)
        self.assertEqual((0, ""), (code, err), out)
        return out

    def lines(self, github, fork="main"):
        return self.report(github, fork=fork).splitlines()

    def json(self, github, fork="main"):
        return json.loads(self.report(github, "--json", fork=fork))

    @staticmethod
    def section(lines, heading):
        start = next(i for i, line in enumerate(lines) if line.startswith(heading))
        end = next((i for i in range(start + 1, len(lines)) if not lines[i].startswith("  ")),
                   len(lines))
        return lines[start:end]

    def squash(self, number, title, at=T0 + DAY):
        return self.fixture.commit(f"{title} (#{number})", at)


class LandingStateTest(StatusTestCase):
    def test_squash_landing_with_an_open_upstream_pr_is_reported_in_full(self):
        squash = self.squash(71, "Expose the refresh batch metric")
        github = submitted(71, 38131, "OPEN")
        github.pull_requests = [fork_pr(71, "[#38130] DocDB: Expose the batch metric", squash)]

        self.assertEqual([
            f"master-shopify {squash[:9]}: 1 carried commit since upstream {self.base[:9]}; "
            "no sync point",
            "",
            "Not submitted (0)",
            "Open upstream (1)",
            "  #71  yugabyte#38131  Expose the refresh batch metric",
            "Landed upstream, dropped at the next sync (0)",
            "Restructured upstream, awaiting a replacement Upstream: comment (0)",
            "Closed upstream without merging, needs a disposition (0)",
            "Fork-only (0)",
            "Adopted by earlier syncs (0)",
            "Unidentified commits (0)",
        ], self.lines(github))

    def merged_upstream(self, state, ahead=None):
        squash = self.squash(69, "Refresh the catalog cache")
        github = submitted(69, 38110, state, ahead)
        github.pull_requests = [fork_pr(69, "[#38100] DocDB: Refresh the catalog cache", squash)]
        return self.lines(github)

    def test_a_merged_upstream_pr_on_upstream_master_has_landed(self):
        self.assertEqual(["Landed upstream, dropped at the next sync (1)",
                          "  #69  yugabyte#38110  merged 2026-11-12"],
                         self.section(self.merged_upstream("MERGED", ahead=0), "Landed"))

    def test_a_merged_upstream_pr_not_on_upstream_master_was_restructured(self):
        self.assertEqual(["Restructured upstream, awaiting a replacement Upstream: comment (1)",
                          "  #69  yugabyte#38110  merged 2026-11-12"],
                         self.section(self.merged_upstream("MERGED", ahead=3), "Restructured"))

    def test_an_upstream_pr_closed_without_merging_needs_a_disposition(self):
        self.assertEqual(["Closed upstream without merging, needs a disposition (1)",
                          "  #69  yugabyte#38110  Expose the refresh batch metric"],
                         self.section(self.merged_upstream("CLOSED"), "Closed"))

    def test_a_pr_without_an_upstream_comment_is_not_submitted_with_its_age(self):
        squash = self.squash(70, "Cap the catalog cache refresh batch")
        github = FakeGitHub([fork_pr(70, "Cap the catalog cache refresh batch", squash,
                                     ["Available: 2025.1"])])

        self.assertEqual(["Not submitted (1)",
                          f"  #70  18d   {'Cap the catalog cache refresh batch':<60} priya"],
                         self.section(self.lines(github), "Not submitted"))

    def test_the_newest_upstream_comment_wins(self):
        squash = self.squash(71, "Expose the refresh batch metric")
        github = FakeGitHub(
            [fork_pr(71, "Expose the refresh batch metric", squash)],
            {71: [comment(f"Upstream: {UPSTREAM_PR}38120\nReplaced.\nUpstream: {UPSTREAM_PR}38131",
                          "2026-11-05T00:00:00Z"),
                  comment(f"Upstream: {UPSTREAM_PR}38120", "2026-11-02T00:00:00Z"),
                  comment("Thanks!", "2026-11-06T00:00:00Z")]},
            {38120: upstream_pr(38120, "MERGED"), 38131: upstream_pr(38131, "OPEN")}, {})

        self.assertEqual(["Open upstream (1)",
                          "  #71  yugabyte#38131  Expose the refresh batch metric"],
                         self.section(self.lines(github), "Open upstream"))

    def test_an_upstream_comment_that_is_not_an_upstream_pr_url_is_a_warning(self):
        squash = self.squash(70, "Cap the batch")
        github = FakeGitHub(
            [fork_pr(70, "Cap the batch", squash)],
            {70: [comment("Upstream: https://github.com/Shopify/yugabyte-db/pull/12")]})

        lines = self.lines(github)

        self.assertEqual("Not submitted (1)", lines[2])
        self.assertEqual(["Warnings", "  #70 has an Upstream: comment that is not a "
                          "yugabyte/yugabyte-db pull request URL: "
                          "https://github.com/Shopify/yugabyte-db/pull/12"],
                         self.section(lines, "Warnings"))

    def test_an_upstream_pr_that_cannot_be_read_is_a_warning(self):
        squash = self.squash(70, "Cap the batch")
        github = FakeGitHub([fork_pr(70, "Cap the batch", squash)],
                            {70: [comment(f"Upstream: {UPSTREAM_PR}38999")]})

        lines = self.lines(github)

        self.assertEqual("Not submitted (1)", lines[2])
        self.assertEqual(["Warnings", "  #70: could not read upstream PR yugabyte#38999: "
                          "no pull requests found"],
                         self.section(lines, "Warnings"))


class ForkOnlyTest(StatusTestCase):
    def fork_only(self, title, *labels):
        github = FakeGitHub([fork_pr(58, title, self.squash(58, title), labels)])
        return github, self.lines(github)

    def test_the_label_and_title_together_mark_fork_only_work_without_reading_comments(self):
        github, lines = self.fork_only("[#45][shopify-only] Build: Share it", "upstream: skip")

        self.assertEqual(["Fork-only (1)", "  #58"], self.section(lines, "Fork-only"))
        self.assertEqual([], github.comment_requests)
        self.assertNotIn("Warnings", lines)

    def test_a_shopify_only_title_without_the_label_is_fork_only_with_a_warning(self):
        _, lines = self.fork_only("[shopify-only] Build: Share helpers")

        self.assertEqual(["Fork-only (1)", "  #58"], self.section(lines, "Fork-only"))
        self.assertEqual(["Warnings",
                          "  #58 is titled [shopify-only] but has no upstream: skip label"],
                         self.section(lines, "Warnings"))

    def test_the_label_without_a_shopify_only_title_is_fork_only_with_a_warning(self):
        _, lines = self.fork_only("[#45] Build: Share helpers", "upstream: skip")

        self.assertEqual(["Fork-only (1)", "  #58"], self.section(lines, "Fork-only"))
        self.assertEqual(["Warnings",
                          "  #58 is labelled upstream: skip but its title has no [shopify-only]"],
                         self.section(lines, "Warnings"))


class AttributionTest(StatusTestCase):
    def contributions(self, report):
        return [(c["number"], c["landings"]) for c in report["contributions"]]

    def test_a_stack_merge_is_attributed_to_every_pr_reporting_it_as_merge_commit(self):
        earlier = self.squash(40, "[shopify-only] Earlier")
        self.branch_commits("stack", "layer 1", "layer 2", "layer 3")
        merge = self.merge("stack", "Merge pull request #43 from Shopify/stack", T0 + 2 * DAY)
        github = FakeGitHub([fork_pr(n, f"[shopify-only] Layer {n}", merge, ["upstream: skip"])
                             for n in (41, 42, 43)]
                            + [fork_pr(40, "[shopify-only] Earlier", None, ["upstream: skip"])])

        report = self.json(github)

        self.assertEqual([(40, [earlier]), (41, [merge]), (42, [merge]), (43, [merge])],
                         self.contributions(report))
        self.assertEqual([], report["unidentified"])
        self.assertIn(f"{merge[:9]}: 2 carried commits", self.lines(github)[0])

    def test_a_merge_landing_with_several_commits_is_one_pr(self):
        earlier = self.squash(49, "Earlier change")
        self.branch_commits("feature", "first", "second")
        merge = self.merge("feature", "Merge pull request #50 from Shopify/feature", T0 + 2 * DAY)
        github = FakeGitHub([fork_pr(50, "[shopify-only] Feature", merge, ["upstream: skip"]),
                             fork_pr(49, "[shopify-only] Earlier", earlier, ["upstream: skip"])])

        report = self.json(github)

        self.assertEqual([(49, [earlier]), (50, [merge])], self.contributions(report))
        self.assertEqual([], report["unidentified"])

    def test_a_merge_landing_whose_first_parent_is_upstream_is_a_landing_not_a_sync(self):
        self.branch_commits("feature", "first", "second")
        merge = self.merge("feature", "Merge pull request #43 from Shopify/feature", T0 + 2 * DAY)
        github = FakeGitHub([fork_pr(43, "[shopify-only] Feature", merge, ["upstream: skip"])])

        report = self.json(github)

        self.assertEqual([], report["sync_points"])
        self.assertEqual([(43, [merge])], self.contributions(report))

    def test_only_the_trailing_pr_number_of_a_subject_attributes_the_landing(self):
        fixed = self.squash(12, "Fix thing", T0 + DAY)
        revert = self.fixture.commit('Revert "Fix thing (#12)" (#15)', T0 + 2 * DAY)
        github = FakeGitHub([fork_pr(12, "Fix thing", fixed),
                             fork_pr(15, 'Revert "Fix thing (#12)"', revert)])

        status = self.json(github)

        self.assertEqual({12: [fixed], 15: [revert]},
                         {c["number"]: c["landings"] for c in status["contributions"]})

    def test_direct_pushes_and_upstream_pr_numbers_are_unidentified(self):
        typo = self.fixture.commit("Fix a typo in the runbook", T0 + DAY)
        picked = self.fixture.commit("[#36913] CDC: Skip a scenario (#38388)", T0 + 2 * DAY)

        lines = self.lines(FakeGitHub())

        self.assertTrue(lines[0].startswith(f"master-shopify {picked[:9]}: 2 carried commits"),
                        lines[0])
        self.assertEqual(["Unidentified commits (2)",
                          f"  {typo[:9]}  Fix a typo in the runbook",
                          f"  {picked[:9]}  [#36913] CDC: Skip a scenario (#38388)"],
                         self.section(lines, "Unidentified"))


class SyncPointTest(StatusTestCase):
    def test_a_plain_upstream_merge_is_a_sync_point_and_not_a_landing(self):
        squash = self.squash(70, "Cap the batch")
        merged = self.upstream_commit("upstream change", T0 + 2 * DAY)
        sync = self.merge("up", "Merge branch 'yugabyte:master' into master-shopify", T0 + 3 * DAY)
        self.upstream_commit("later upstream change", T0 + 4 * DAY)
        github = FakeGitHub([fork_pr(70, "Cap the batch", squash)])

        lines = self.lines(github)

        self.assertEqual(f"master-shopify {sync[:9]}: 1 carried commit since upstream "
                         f"{merged[:9]}; last sync 2026-11-04 (upstream merge)", lines[0])
        self.assertEqual("Not submitted (1)", lines[2])
        self.assertEqual("Unidentified commits (0)", lines[-1])

    def test_consecutive_upstream_merges_are_each_a_sync_point(self):
        self.squash(70, "Cap the batch")
        self.upstream_commit("upstream change", T0 + 2 * DAY)
        first = self.merge("up", "Merge upstream", T0 + 3 * DAY)
        self.upstream_commit("another upstream change", T0 + 4 * DAY)
        second = self.merge("up", "Merge upstream again", T0 + 5 * DAY)

        report = self.json(FakeGitHub())

        self.assertEqual([{"sha": first, "date": "2026-11-04T12:00:00+00:00",
                           "kind": "upstream-merge"},
                          {"sha": second, "date": "2026-11-06T12:00:00+00:00",
                           "kind": "upstream-merge"}], report["sync_points"])
        self.assertEqual([], report["contributions"])
        self.assertEqual(1, len(report["unidentified"]))

    def test_dropped_trailers_on_a_sync_merge_are_adopted_prs(self):
        self.squash(69, "Refresh the cache")
        carried = self.squash(70, "Cap the batch", T0 + 2 * DAY)
        target = self.upstream_commit("upstream lands 38110", T0 + 3 * DAY)
        self.fixture.git("checkout", "--quiet", "-b", "sync", target)
        sync = self.merge("main", "Sync master-shopify to upstream\n\n"
                          "Dropped: #69 (upstream #38110)\nDropped: #68 (upstream #38100)",
                          T0 + 4 * DAY, "-s", "ours")
        self.fixture.git("cherry-pick", carried, env=dates(T0 + 4 * DAY))
        replayed = self.fixture.git("rev-parse", "HEAD")
        github = FakeGitHub([fork_pr(70, "Cap the batch", carried)])

        report = self.json(github, fork="sync")
        lines = self.lines(github, fork="sync")

        self.assertEqual(target, report["base"])
        self.assertEqual([{"sha": sync, "date": "2026-11-05T12:00:00+00:00", "kind": "sync"}],
                         report["sync_points"])
        self.assertEqual([(70, [replayed])],
                         [(c["number"], c["landings"]) for c in report["contributions"]])
        self.assertEqual([{"number": 68, "upstream_number": 38100, "sync": sync},
                          {"number": 69, "upstream_number": 38110, "sync": sync}],
                         report["adopted"])
        self.assertEqual(["Adopted by earlier syncs (2)",
                          "  #68  yugabyte#38100  dropped 2026-11-05",
                          "  #69  yugabyte#38110  dropped 2026-11-05"],
                         self.section(lines, "Adopted"))
        self.assertTrue(lines[0].endswith("; last sync 2026-11-05 (sync)"), lines[0])


class JsonTest(StatusTestCase):
    def test_json_lists_every_derived_record(self):
        squash = self.squash(71, "Expose the refresh batch metric")
        typo = self.fixture.commit("Fix a typo", T0 + 2 * DAY)
        merged = self.upstream_commit("upstream change", T0 + 3 * DAY)
        sync = self.merge("up", "Merge upstream", T0 + 4 * DAY)
        github = submitted(71, 38131, "OPEN")
        github.pull_requests = [fork_pr(71, "[#38130] DocDB: Expose the metric", squash)]

        self.assertEqual({
            "fork": sync, "upstream": merged, "base": merged,
            "sync_points": [{"sha": sync, "date": "2026-11-05T12:00:00+00:00",
                             "kind": "upstream-merge"}],
            "contributions": [{
                "number": 71, "title": "[#38130] DocDB: Expose the metric", "author": "priya",
                "url": "https://github.com/Shopify/yugabyte-db/pull/71",
                "merged_at": "2026-11-01T12:00:00Z", "landings": [squash], "state": "open",
                "upstream_number": 38131, "upstream_url": UPSTREAM_PR + "38131",
                "upstream_state": "OPEN", "upstream_title": "Expose the refresh batch metric",
                "upstream_merged_at": None, "age_days": 18}],
            "adopted": [],
            "unidentified": [{"sha": typo, "subject": "Fix a typo"}],
            "warnings": [],
        }, self.json(github))


class UpstreamGitHubTest(testfixtures.RepoTestCase):
    def gh(self, output):
        executable = os.path.join(self.tmpdir, "gh")
        with open(executable, "w", encoding="utf-8") as script:
            script.write(f"#!/bin/sh\nprintf '%s\\n' '{output}'\n")
        os.chmod(executable, 0o755)
        return GitHub(executable=executable)

    def test_ahead_by_parses_the_number_gh_prints(self):
        self.assertEqual(0, self.gh("0").ahead_by(MERGED_UPSTREAM))

    def test_ahead_by_rejects_output_that_is_not_a_number(self):
        with self.assertRaises(Error) as caught:
            self.gh("null").ahead_by(MERGED_UPSTREAM)

        self.assertEqual("Failed to read ahead_by comparing upstream master with "
                         f"{MERGED_UPSTREAM}: 'null'", str(caught.exception))

    def test_a_merged_upstream_pr_without_a_merge_commit_is_restructured(self):
        view = {"state": "MERGED", "mergeCommit": None}

        self.assertEqual("restructured", upstream_state(view, lambda sha: 0))


class CliTest(StatusTestCase):
    def run_script(self, *args, python_args=("-B",)):
        home, cwd = os.path.join(self.tmpdir, "home"), os.path.join(self.tmpdir, "cwd")
        os.makedirs(home, exist_ok=True)
        os.makedirs(cwd, exist_ok=True)
        env = {**os.environ, "HOME": home,
               "GIT_CEILING_DIRECTORIES": os.path.realpath(self.tmpdir)}
        result = subprocess.run([sys.executable, *python_args, *args], cwd=cwd, env=env,
                                capture_output=True, text=True)
        return result, home, cwd

    def test_a_command_is_required(self):
        self.assertEqual((2, "", "A command is required: status\n"), self.cli())

    def test_a_missing_upstream_ref_without_fetching_is_a_git_error(self):
        self.publish(upstream=None)

        code, out, err = self.cli("status", "--repo-path", self.fixture.path, "--no-fetch")

        self.assertEqual((1, ""), (code, out))
        self.assertEqual(f"upstream/master does not resolve to a commit in {self.fixture.path}\n",
                         err)

    def test_importing_the_module_has_no_side_effects(self):
        code = f"import sys; sys.path.insert(0, {os.path.dirname(SCRIPT)!r}); import upstream"

        result, home, cwd = self.run_script("-c", code, python_args=("-I", "-B"))

        self.assertEqual((0, "", ""), (result.returncode, result.stdout, result.stderr))
        self.assertEqual(([], []), (os.listdir(home), os.listdir(cwd)))

    def test_status_help_documents_the_options_without_side_effects(self):
        result, home, cwd = self.run_script(SCRIPT, "status", "--help")

        self.assertEqual(0, result.returncode, result.stderr)
        for option in ("--repo-path", "--no-fetch", "--upstream-ref", "--fork-ref", "--json"):
            self.assertIn(option, result.stdout)
        self.assertEqual(([], []), (os.listdir(home), os.listdir(cwd)))

    def test_repo_path_defaults_to_the_scripts_checkout(self):
        self.addCleanup(os.chdir, os.getcwd())
        os.chdir(self.fixture.path)

        self.assertEqual(str(Path(SCRIPT).parents[3]), parser().parse_args(["status"]).repo_path)


if __name__ == "__main__":
    unittest.main()
