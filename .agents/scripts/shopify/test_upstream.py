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

from upstream import Cli, Error, GitHub, attribute, parser, upstream_state
import testfixtures


SCRIPT = str(Path(__file__).resolve().with_name("upstream.py"))
T0, DAY = 1793534400, 86400  # 2026-11-01T12:00:00Z
NOW = datetime(2026, 11, 19, 12, tzinfo=timezone.utc)
UPSTREAM_PR = "https://github.com/yugabyte/yugabyte-db/pull/"
MISSING = "a" * 40  # a commit the fixture repository does not have
TOOL_REF = "refs/shopify/upstream-master"
SKIP = ["upstream: skip"]


class FakeGitHub(GitHub):
    """In-memory stand-in for the three gh queries `status` makes; anything else fails."""

    def __init__(self, pull_requests=(), comments=None, upstream=None):
        super().__init__(executable="/nonexistent/gh")
        self.pull_requests = list(pull_requests)
        self.comments_by_pr = comments or {}
        self.upstream = upstream or {}
        self.comment_requests = []
        self.upstream_requests = []

    def merged_pull_requests(self):
        return self.pull_requests

    def comments(self, number):
        self.comment_requests.append(number)
        return self.comments_by_pr.get(number, [])

    def upstream_pull_request(self, number):
        self.upstream_requests.append(number)
        if number not in self.upstream:
            raise Error(f"Failed to run `gh pr view {number} --repo yugabyte/yugabyte-db`\n"
                        "no pull requests found\n")
        return self.upstream[number]


def fork_pr(number, title, merge=None, labels=(), merged_at="2026-11-01T12:00:00Z",
            base="master-shopify"):
    return {"number": number, "title": title, "mergeCommit": {"oid": merge} if merge else None,
            "mergedAt": merged_at, "labels": [{"name": label} for label in labels],
            "author": {"login": "priya"}, "baseRefName": base,
            "url": f"https://github.com/Shopify/yugabyte-db/pull/{number}"}


def comment(body, created_at="2026-11-02T00:00:00Z"):
    return {"body": body, "created_at": created_at}


def upstream_pr(number, state, title="Expose the refresh batch metric", merge=MISSING):
    merged = state == "MERGED"
    return {"number": number, "state": state, "title": title, "url": UPSTREAM_PR + str(number),
            "mergedAt": "2026-11-12T08:00:00Z" if merged else None,
            "mergeCommit": {"oid": merge} if merged else None}


def submitted(number, upstream_number, state, merge=MISSING):
    """A GitHub where fork PR `number` has an Upstream: comment naming `upstream_number`."""
    return FakeGitHub(
        [], {number: [comment(f"Upstream: {UPSTREAM_PR}{upstream_number}")]},
        {upstream_number: upstream_pr(upstream_number, state, merge=merge)})


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

    def rev(self, name):
        return self.fixture.git("rev-parse", name)

    def publish(self, fork="main", upstream="up"):
        self.fixture.ref("refs/remotes/origin/master-shopify", self.rev(fork))
        if upstream is not None:
            self.fixture.ref(TOOL_REF, self.rev(upstream))

    def cli(self, *argv, github=None):
        out, err = io.StringIO(), io.StringIO()
        code = Cli.run(list(argv), github=github or FakeGitHub(), out=out, err=err, now=NOW)
        return code, out.getvalue(), err.getvalue()

    def report(self, github, *options, fork="main", upstream="up"):
        self.publish(fork, upstream)
        code, out, err = self.cli("status", "--repo-path", self.fixture.path, "--no-fetch",
                                  *options, github=github)
        self.assertEqual((0, ""), (code, err), out)
        return out

    def lines(self, github, fork="main", upstream="up"):
        return self.report(github, fork=fork, upstream=upstream).splitlines()

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
            f"master-shopify {squash[:9]}: 1 carried commit since upstream {self.base[:9]} "
            "(2026-11-01); no sync point",
            "",
            "Not submitted (0)",
            "Open upstream (1)",
            "  #71  yugabyte#38131  Expose the refresh batch metric",
            "Landed upstream, dropped at the next sync (0)",
            "Restructured upstream, awaiting a replacement Upstream: comment (0)",
            "Closed upstream without merging, needs a disposition (0)",
            "Fork-only (0)",
            "Reverted in the fork (0)",
            "Adopted by earlier syncs (0)",
            "Unidentified commits (0)",
        ], self.lines(github))

    def merged_upstream(self, state, merge=MISSING):
        squash = self.squash(69, "Refresh the catalog cache")
        github = submitted(69, 38110, state, merge)
        github.pull_requests = [fork_pr(69, "[#38100] DocDB: Refresh the catalog cache", squash)]
        return self.lines(github)

    def test_a_merged_upstream_pr_whose_merge_commit_is_on_upstream_master_has_landed(self):
        merge = self.upstream_commit("Refresh the catalog cache (#38110)", T0 + 2 * DAY)

        self.assertEqual(["Landed upstream, dropped at the next sync (1)",
                          "  #69  yugabyte#38110  merged 2026-11-12"],
                         self.section(self.merged_upstream("MERGED", merge), "Landed"))

    def test_a_merged_upstream_pr_whose_merge_commit_is_off_upstream_master_was_restructured(self):
        merge = self.branch_commits("restack", "Refresh the catalog cache (#38110)")[0]

        self.assertEqual(["Restructured upstream, awaiting a replacement Upstream: comment (1)",
                          "  #69  yugabyte#38110  merged 2026-11-12"],
                         self.section(self.merged_upstream("MERGED", merge), "Restructured"))

    def test_a_merged_upstream_pr_whose_merge_commit_is_not_local_was_restructured(self):
        self.assertEqual(["Restructured upstream, awaiting a replacement Upstream: comment (1)",
                          "  #69  yugabyte#38110  merged 2026-11-12"],
                         self.section(self.merged_upstream("MERGED", MISSING), "Restructured"))

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
            {38120: upstream_pr(38120, "MERGED"), 38131: upstream_pr(38131, "OPEN")})

        self.assertEqual(["Open upstream (1)",
                          "  #71  yugabyte#38131  Expose the refresh batch metric"],
                         self.section(self.lines(github), "Open upstream"))

    def test_browser_copied_upstream_lines_are_read(self):
        squash = self.squash(71, "Expose the refresh batch metric")
        url = UPSTREAM_PR + "38131"
        bodies = (f"Upstream: <{url}>", f"Upstream: {url}.", f"Upstream: {url},",
                  f"Upstream: {url}/files", f"Upstream: {url}#issuecomment-1",
                  f"Upstream: {url}?x=y", f"**Upstream:** {url}", f"Upstream: <{url}>.")
        for body in bodies:
            with self.subTest(body=body):
                github = FakeGitHub([fork_pr(71, "Expose the refresh batch metric", squash)],
                                    {71: [comment(body)]}, {38131: upstream_pr(38131, "OPEN")})

                lines = self.lines(github)

                self.assertEqual(["Open upstream (1)",
                                  "  #71  yugabyte#38131  Expose the refresh batch metric"],
                                 self.section(lines, "Open upstream"))
                self.assertNotIn("Warnings", lines)

    def test_an_upstream_label_without_a_url_after_it_is_not_an_upstream_line(self):
        squash = self.squash(71, "Expose the refresh batch metric")
        github = FakeGitHub(
            [fork_pr(71, "Expose the refresh batch metric", squash)],
            {71: [comment(f"Upstream: {UPSTREAM_PR}38131", "2026-11-02T00:00:00Z"),
                  comment(f"Upstream:\nUpstream: \n{UPSTREAM_PR}38999", "2026-11-05T00:00:00Z")]},
            {38131: upstream_pr(38131, "OPEN")})

        lines = self.lines(github)

        self.assertEqual(["Open upstream (1)",
                          "  #71  yugabyte#38131  Expose the refresh batch metric"],
                         self.section(lines, "Open upstream"))
        self.assertNotIn("Warnings", lines)

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

    def test_a_shopify_only_title_without_the_label_is_upstream_bound_with_a_warning(self):
        github, lines = self.fork_only("[shopify-only] Build: Share helpers")

        self.assertEqual(["Not submitted (1)",
                          f"  #58  18d   {'[shopify-only] Build: Share helpers':<60} priya"],
                         self.section(lines, "Not submitted"))
        self.assertEqual(["Fork-only (0)"], self.section(lines, "Fork-only"))
        self.assertEqual([58], github.comment_requests)
        self.assertEqual(["Warnings",
                          "  #58 is titled [shopify-only] but has no upstream: skip label: the "
                          "label decides, so it counts as upstream-bound"],
                         self.section(lines, "Warnings"))

    def test_the_label_without_a_shopify_only_title_is_fork_only_with_a_warning(self):
        github, lines = self.fork_only("[#45] Build: Share helpers", "upstream: skip")

        self.assertEqual(["Not submitted (0)"], self.section(lines, "Not submitted"))
        self.assertEqual(["Fork-only (1)", "  #58"], self.section(lines, "Fork-only"))
        self.assertEqual([], github.comment_requests)
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
        self.assertEqual([], report["warnings"])
        self.assertIn(f"{merge[:9]}: 2 carried commits", self.lines(github)[0])

    def test_attribution_prefers_the_subject_then_the_trailer_then_the_merge_commit(self):
        prs = [fork_pr(n, f"PR {n}", merge="c" * 40) for n in (1, 2, 3)]
        landings = [
            {"sha": "a" * 40, "subject": "Fix thing (#1)",
             "message": "Fix thing (#1)\n\nShopify-PR: #2"},
            {"sha": "c" * 40, "subject": "Merge pull request #3",
             "message": "Shopify-PR: #2 #2 #9"},
            {"sha": "d" * 40, "subject": "Merge pull request #3", "message": "nothing here"},
        ]

        landed, unidentified = attribute(landings, prs)

        self.assertEqual({1: ["a" * 40], 2: ["c" * 40]}, landed)
        self.assertEqual([{"sha": "d" * 40, "subject": "Merge pull request #3"}], unidentified)

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
        self.assertNotIn("Warnings", lines)

    def test_a_replayed_landing_belongs_to_every_pr_its_shopify_pr_trailer_names(self):
        replayed = self.fixture.commit("Merge pull request #64 from Shopify/stack\n\n"
                                       "Shopify-PR: #55 #58 #59 #63 #64", T0 + DAY)
        numbers = (55, 58, 59, 63, 64)
        github = FakeGitHub([fork_pr(n, f"[shopify-only] Layer {n}", MISSING, SKIP)
                             for n in numbers])

        report = self.json(github)

        self.assertEqual([(n, [replayed]) for n in numbers], self.contributions(report))
        self.assertEqual(([], []), (report["unidentified"], report["warnings"]))

    def test_shopify_pr_trailer_numbers_that_are_not_merged_fork_prs_are_ignored(self):
        replayed = self.fixture.commit("Merge pull request #64 from Shopify/stack\n\n"
                                       "Shopify-PR: #99 #64", T0 + DAY)
        github = FakeGitHub([fork_pr(64, "[shopify-only] Layer", MISSING, SKIP)])

        report = self.json(github)

        self.assertEqual([(64, [replayed])], self.contributions(report))
        self.assertEqual(([], []), (report["unidentified"], report["warnings"]))


class SyncPointTest(StatusTestCase):
    def test_a_plain_upstream_merge_is_a_sync_point_and_not_a_landing(self):
        squash = self.squash(70, "Cap the batch")
        merged = self.upstream_commit("upstream change", T0 + 2 * DAY)
        sync = self.merge("up", "Merge branch 'yugabyte:master' into master-shopify", T0 + 3 * DAY)
        self.upstream_commit("later upstream change", T0 + 4 * DAY)
        github = FakeGitHub([fork_pr(70, "Cap the batch", squash)])

        lines = self.lines(github)

        self.assertEqual(f"master-shopify {sync[:9]}: 1 carried commit since upstream "
                         f"{merged[:9]} (2026-11-03); last sync 2026-11-04 (upstream merge)",
                         lines[0])
        self.assertEqual("Not submitted (1)", lines[2])
        self.assertEqual("Unidentified commits (0)", lines[-1])

    def test_an_upstream_merge_beyond_a_stale_upstream_ref_is_an_unattributed_merge(self):
        squash = self.squash(70, "Cap the batch")
        stale = self.upstream_commit("upstream change", T0 + 2 * DAY)
        self.upstream_commit("newer upstream change", T0 + 3 * DAY)
        merge = self.merge("up", "Merge branch 'yugabyte:master' into master-shopify", T0 + 4 * DAY)

        lines = self.lines(FakeGitHub([fork_pr(70, "Cap the batch", squash)]), upstream=stale)

        self.assertEqual(f"master-shopify {merge[:9]}: 2 carried commits since upstream "
                         f"{stale[:9]} (2026-11-03); no sync point", lines[0])
        self.assertEqual(["Unidentified commits (1)",
                          f"  {merge[:9]}  Merge branch 'yugabyte:master' into master-shopify"],
                         self.section(lines, "Unidentified"))
        self.assertEqual(["Warnings", "  1 unattributed merge commit(s) on master-shopify; if the "
                          "upstream ref is stale, run without --no-fetch"],
                         self.section(lines, "Warnings"))

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
        github = FakeGitHub([fork_pr(70, "Cap the batch", carried),
                             fork_pr(69, "Refresh the cache", MISSING)])

        report = self.json(github, fork="sync")
        lines = self.lines(github, fork="sync")

        self.assertEqual(target, report["base"])
        self.assertEqual([{"sha": sync, "date": "2026-11-05T12:00:00+00:00", "kind": "sync"}],
                         report["sync_points"])
        self.assertEqual([(70, [replayed])],
                         [(c["number"], c["landings"]) for c in report["contributions"]])
        self.assertEqual([{"number": 68, "upstream_number": 38100, "sync": sync,
                           "date": "2026-11-05T12:00:00+00:00"},
                          {"number": 69, "upstream_number": 38110, "sync": sync,
                           "date": "2026-11-05T12:00:00+00:00"}],
                         report["adopted"])
        self.assertEqual(["Adopted by earlier syncs (2)",
                          "  #68  yugabyte#38100  dropped 2026-11-05",
                          "  #69  yugabyte#38110  dropped 2026-11-05"],
                         self.section(lines, "Adopted"))
        self.assertTrue(lines[0].endswith("; last sync 2026-11-05 (sync)"), lines[0])
        self.assertEqual([], report["warnings"])

    def test_prs_dropped_by_a_sync_behind_the_merge_base_are_still_adopted(self):
        first_pr = self.squash(70, "Cap the batch")
        second_pr = self.squash(71, "Expose the metric", T0 + 2 * DAY)
        first_target = self.upstream_commit("upstream lands 38110", T0 + 3 * DAY)
        self.fixture.git("checkout", "--quiet", "-b", "first-sync", first_target)
        first_sync = self.merge("main", "Sync master-shopify to upstream\n\n"
                                "Dropped: #70 (upstream #38110)", T0 + 4 * DAY, "-s", "ours")
        self.fixture.git("cherry-pick", second_pr, env=dates(T0 + 4 * DAY))
        second_target = self.upstream_commit("upstream lands 38131", T0 + 5 * DAY)
        self.fixture.git("checkout", "--quiet", "-b", "second-sync", second_target)
        second_sync = self.merge("first-sync", "Sync master-shopify to upstream\n\n"
                                 "Dropped: #71 (upstream #38131)", T0 + 6 * DAY, "-s", "ours")
        github = FakeGitHub([fork_pr(70, "Cap the batch", first_pr),
                             fork_pr(71, "Expose the metric", second_pr)])

        report = self.json(github, fork="second-sync")
        lines = self.lines(github, fork="second-sync")

        self.assertEqual(second_target, report["base"])
        self.assertEqual([second_sync], [point["sha"] for point in report["sync_points"]])
        self.assertEqual([{"number": 70, "upstream_number": 38110, "sync": first_sync,
                           "date": "2026-11-05T12:00:00+00:00"},
                          {"number": 71, "upstream_number": 38131, "sync": second_sync,
                           "date": "2026-11-07T12:00:00+00:00"}],
                         report["adopted"])
        self.assertEqual(["Adopted by earlier syncs (2)",
                          "  #70  yugabyte#38110  dropped 2026-11-05",
                          "  #71  yugabyte#38131  dropped 2026-11-07"],
                         self.section(lines, "Adopted"))
        self.assertEqual([], report["warnings"])
        self.assertNotIn("Warnings", lines)

    def test_a_dropped_pr_that_is_carried_again_is_a_warning(self):
        carried = self.squash(70, "Cap the batch")
        target = self.upstream_commit("upstream change", T0 + 3 * DAY)
        self.fixture.git("checkout", "--quiet", "-b", "sync", target)
        sync = self.merge("main", "Sync master-shopify to upstream\n\n"
                          "Dropped: #70 (upstream #38110)", T0 + 4 * DAY, "-s", "ours")
        self.fixture.git("cherry-pick", carried, env=dates(T0 + 4 * DAY))

        lines = self.lines(FakeGitHub([fork_pr(70, "Cap the batch", carried)]), fork="sync")

        self.assertEqual(["Not submitted (1)", f"  #70  18d   {'Cap the batch':<60} priya"],
                         self.section(lines, "Not submitted"))
        self.assertEqual(["Adopted by earlier syncs (1)",
                          "  #70  yugabyte#38110  dropped 2026-11-05"],
                         self.section(lines, "Adopted"))
        self.assertEqual(["Warnings",
                          f"  PR #70 was dropped by sync {sync[:9]} but is carried again"],
                         self.section(lines, "Warnings"))


class RevertTest(StatusTestCase):
    def revert(self, number, at, *targets, subject="Revert the fix"):
        """`git revert` of `targets` landed as fork PR `number`, with one `This reverts commit`
        line per target; a target may be given as (sha, the text the line names it by)."""
        targets = [target if isinstance(target, tuple) else (target, target)
                   for target in targets]
        for sha, _ in targets:
            self.fixture.git("revert", "--no-commit", sha)
        lines = "\n".join(f"This reverts commit {text}." for _, text in targets)
        self.fixture.git("commit", "--quiet", "-m", f"{subject} (#{number})\n\n{lines}",
                         env=dates(at))
        return self.rev("HEAD")

    def pair(self, length=40, comments=None):
        """#12 lands a fix and #15 reverts it, naming the fix by its first `length` hex digits."""
        fixed = self.squash(12, "Fix thing")
        reverting = self.revert(15, T0 + 2 * DAY, (fixed, fixed[:length]))
        github = FakeGitHub([fork_pr(12, "Fix thing", fixed),
                             fork_pr(15, 'Revert "Fix thing"', reverting)], comments)
        return github, fixed, reverting

    @staticmethod
    def states(report):
        return {c["number"]: (c["state"], c["reverted_by"], c["reverts"])
                for c in report["contributions"]}

    def test_a_revert_of_a_carried_landing_marks_both_without_asking_upstream(self):
        github, _, _ = self.pair()

        lines = self.lines(github)

        self.assertEqual(["Not submitted (0)"], self.section(lines, "Not submitted"))
        self.assertEqual(["Fork-only (0)"], self.section(lines, "Fork-only"))
        self.assertEqual(["Reverted in the fork (1)", "  #12  reverted by #15"],
                         self.section(lines, "Reverted in the fork"))
        self.assertNotIn("Warnings", lines)
        self.assertEqual(([12], []), (github.comment_requests, github.upstream_requests))

    def test_a_revert_naming_the_original_squash_pairs_after_a_sync_replayed_it(self):
        # A sync replays #12 under a new sha; the revert text still names the squash commit,
        # which GitHub records as #12's mergeCommit.
        original = "b" * 40
        replayed = self.squash(12, "Fix thing")
        reverting = self.revert(15, T0 + 2 * DAY, (replayed, original[:12]))
        github = FakeGitHub([fork_pr(12, "Fix thing", original),
                             fork_pr(15, 'Revert "Fix thing"', reverting)])

        report = self.json(github)

        self.assertEqual({12: ("reverted", 15, None), 15: ("revert", None, 12)},
                         self.states(report))

    def test_json_carries_the_revert_pair(self):
        github, fixed, reverting = self.pair()

        report = self.json(github)

        self.assertEqual({12: ("reverted", 15, None), 15: ("revert", None, 12)},
                         self.states(report))
        self.assertEqual({12: [fixed], 15: [reverting]},
                         {c["number"]: c["landings"] for c in report["contributions"]})
        self.assertEqual([], report["warnings"])

    def test_a_seven_character_abbreviated_sha_names_the_reverted_landing(self):
        github, _, _ = self.pair(7)

        self.assertEqual({12: ("reverted", 15, None), 15: ("revert", None, 12)},
                         self.states(self.json(github)))

    def test_a_sha_shorter_than_seven_characters_names_nothing(self):
        github, _, _ = self.pair(6)

        self.assertEqual({12: ("not-submitted", None, None), 15: ("not-submitted", None, None)},
                         self.states(self.json(github)))

    def test_every_this_reverts_commit_line_is_read(self):
        fixed = self.squash(12, "Fix thing")
        other = self.squash(13, "Fix another thing", T0 + 2 * DAY)
        reverting = self.revert(15, T0 + 3 * DAY, self.base, (fixed, fixed[:9]), other)
        github = FakeGitHub([fork_pr(12, "Fix thing", fixed),
                             fork_pr(13, "Fix another thing", other),
                             fork_pr(15, "Revert both fixes", reverting)])

        report = self.json(github)
        lines = self.lines(github)

        self.assertEqual({12: ("reverted", 15, None), 13: ("reverted", 15, None),
                          15: ("revert", None, 12)}, self.states(report))
        self.assertEqual(["Reverted in the fork (2)", "  #12  reverted by #15",
                          "  #13  reverted by #15"], self.section(lines, "Reverted in the fork"))
        self.assertEqual(["Not submitted (0)"], self.section(lines, "Not submitted"))

    def test_a_revert_of_a_commit_the_fork_does_not_carry_is_an_ordinary_contribution(self):
        reverting = self.revert(15, T0 + DAY, self.base, subject="Revert an upstream change")
        github = FakeGitHub([fork_pr(15, "Revert an upstream change", reverting)])

        lines = self.lines(github)

        self.assertEqual(["Not submitted (1)",
                          f"  #15  18d   {'Revert an upstream change':<60} priya"],
                         self.section(lines, "Not submitted"))
        self.assertEqual(["Reverted in the fork (0)"],
                         self.section(lines, "Reverted in the fork"))
        self.assertEqual([15], github.comment_requests)

    def test_a_revert_of_a_revert_reapplies_the_change(self):
        github, _, reverting = self.pair()
        reapplied = self.revert(18, T0 + 3 * DAY, reverting, subject="Reapply the fix")
        github.pull_requests.append(fork_pr(18, "Reapply the fix", reapplied))

        report = self.json(github)

        self.assertEqual({12: ("reverted", 15, None), 15: ("revert", None, 12),
                          18: ("not-submitted", None, None)}, self.states(report))

    def test_a_pr_reverted_after_it_was_submitted_upstream_is_a_warning(self):
        url = f"{UPSTREAM_PR}38131"
        github, _, _ = self.pair(comments={12: [comment(f"Upstream: {url}")]})

        lines = self.lines(github)

        self.assertEqual(["Reverted in the fork (1)", "  #12  reverted by #15"],
                         self.section(lines, "Reverted in the fork"))
        self.assertEqual(["Open upstream (0)"], self.section(lines, "Open upstream"))
        self.assertEqual(["Warnings", f"  #12 was reverted by #15 after it was submitted upstream "
                          f"({url}); close or revert the upstream PR"],
                         self.section(lines, "Warnings"))
        self.assertEqual([], github.upstream_requests)

    def test_a_revert_pair_is_accounted_for_in_coverage(self):
        github, _, _ = self.pair()
        github.pull_requests.append(fork_pr(72, "Lost on the way", MISSING))

        self.assertEqual(["merged PR #72 is neither carried nor adopted"],
                         self.json(github)["warnings"])


class CoverageTest(StatusTestCase):
    def test_a_merged_master_shopify_pr_that_is_neither_carried_nor_adopted_is_a_warning(self):
        squash = self.squash(70, "Cap the batch")
        github = FakeGitHub([fork_pr(70, "Cap the batch", squash),
                             fork_pr(72, "Lost on the way", MISSING),
                             fork_pr(73, "Backport", MISSING, base="2025.1-shopify")])

        self.assertEqual(["merged PR #72 is neither carried nor adopted"],
                         self.json(github)["warnings"])

    def test_a_pr_list_as_long_as_the_limit_may_be_truncated(self):
        github = FakeGitHub([fork_pr(n, "Backport", MISSING, base="2025.1-shopify")
                             for n in range(1, 5001)])

        self.assertEqual(["gh pr list returned 5000 PRs; the list may be truncated"],
                         self.json(github)["warnings"])


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
            "base_date": "2026-11-04T12:00:00+00:00",
            "sync_points": [{"sha": sync, "date": "2026-11-05T12:00:00+00:00",
                             "kind": "upstream-merge"}],
            "contributions": [{
                "number": 71, "title": "[#38130] DocDB: Expose the metric", "author": "priya",
                "url": "https://github.com/Shopify/yugabyte-db/pull/71",
                "merged_at": "2026-11-01T12:00:00Z", "landings": [squash], "state": "open",
                "upstream_number": 38131, "upstream_url": UPSTREAM_PR + "38131",
                "upstream_state": "OPEN", "upstream_title": "Expose the refresh batch metric",
                "upstream_merged_at": None, "age_days": 18, "reverted_by": None,
                "reverts": None}],
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

    def test_comments_flatten_the_pages_gh_slurps(self):
        pages = '[[{"body": "first"}], [{"body": "second"}]]'

        self.assertEqual([{"body": "first"}, {"body": "second"}], self.gh(pages).comments(70))

    def test_a_merged_upstream_pr_without_a_merge_commit_is_restructured(self):
        view = {"state": "MERGED", "mergeCommit": None}

        self.assertEqual("restructured", upstream_state(view, lambda sha: True))

    def test_a_merge_commit_that_is_not_a_full_sha_never_reaches_git(self):
        asked = []
        for oid in ("--help", "HEAD", "abc123", "A" * 40):
            with self.subTest(oid=oid):
                view = {"state": "MERGED", "mergeCommit": {"oid": oid}}

                self.assertEqual("restructured", upstream_state(view, asked.append))
        self.assertEqual([], asked)


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
        self.assertEqual((2, "", "A command is required: status, submit or sync\n"), self.cli())

    def test_a_missing_upstream_ref_without_fetching_says_to_fetch_once(self):
        self.publish(upstream=None)

        code, out, err = self.cli("status", "--repo-path", self.fixture.path, "--no-fetch")

        self.assertEqual((1, ""), (code, out))
        self.assertEqual(f"{TOOL_REF} does not resolve to a commit in {self.fixture.path}; run "
                         "once without --no-fetch to fetch upstream master\n", err)

    def test_a_missing_explicit_upstream_ref_is_a_git_error_without_the_fetch_hint(self):
        self.publish(upstream=None)

        code, out, err = self.cli("status", "--repo-path", self.fixture.path, "--no-fetch",
                                  "--upstream-ref", "refs/heads/absent")

        self.assertEqual((1, "", f"refs/heads/absent does not resolve to a commit in "
                                 f"{self.fixture.path}\n"), (code, out, err))

    def test_status_fetches_master_shopify_and_upstream_master_into_their_refs(self):
        upstream_repo = os.path.join(self.tmpdir, "upstream.git")
        origin_repo = os.path.join(self.tmpdir, "origin.git")
        self.fixture.init_bare(upstream_repo)
        self.fixture.init_bare(origin_repo)
        squash = self.squash(70, "Cap the batch")
        merged = self.upstream_commit("upstream change", T0 + 2 * DAY)
        sync = self.merge("up", "Merge branch 'yugabyte:master' into master-shopify", T0 + 3 * DAY)
        latest = self.upstream_commit("later upstream change", T0 + 4 * DAY)
        self.fixture.git("push", "--quiet", upstream_repo, "up:refs/heads/master")
        self.fixture.git("push", "--quiet", origin_repo, "main:refs/heads/master-shopify")
        self.fixture.git("remote", "add", "origin", origin_repo)

        code, out, err = self.cli("status", "--repo-path", self.fixture.path,
                                  "--upstream-url", upstream_repo,
                                  github=FakeGitHub([fork_pr(70, "Cap the batch", squash)]))

        self.assertEqual((0, ""), (code, err), out)
        self.assertEqual((latest, sync),
                         (self.rev(TOOL_REF), self.rev("refs/remotes/origin/master-shopify")))
        self.assertEqual([f"master-shopify {sync[:9]}: 1 carried commit since upstream "
                          f"{merged[:9]} (2026-11-03); last sync 2026-11-04 (upstream merge)",
                          "", "Not submitted (1)"], out.splitlines()[:3])

    def test_importing_the_module_has_no_side_effects(self):
        code = f"import sys; sys.path.insert(0, {os.path.dirname(SCRIPT)!r}); import upstream"

        result, home, cwd = self.run_script("-c", code, python_args=("-I", "-B"))

        self.assertEqual((0, "", ""), (result.returncode, result.stdout, result.stderr))
        self.assertEqual(([], []), (os.listdir(home), os.listdir(cwd)))

    def test_status_help_documents_the_options_without_side_effects(self):
        result, home, cwd = self.run_script(SCRIPT, "status", "--help")

        self.assertEqual(0, result.returncode, result.stderr)
        for option in ("--repo-path", "--no-fetch", "--upstream-ref", "--upstream-url",
                       "--fork-ref", "--json"):
            self.assertIn(option, result.stdout)
        self.assertEqual(([], []), (os.listdir(home), os.listdir(cwd)))

    def test_repo_path_defaults_to_the_scripts_checkout(self):
        self.addCleanup(os.chdir, os.getcwd())
        os.chdir(self.fixture.path)

        self.assertEqual(str(Path(SCRIPT).parents[3]), parser().parse_args(["status"]).repo_path)


if __name__ == "__main__":
    unittest.main()
