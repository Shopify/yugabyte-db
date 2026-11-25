"""Exercise `upstream.py sync --land` against throwaway repositories and an in-memory GitHub.

Each test builds a sync candidate with `sync --yes` (the sync tests' fixture), then lands its PR:
the fork is a bare repository, `gh` answers from canned responses keyed by its exact arguments,
and `devx` from a fake Tool. Nothing reads or writes a real checkout or the network.
"""

import io
import json
import os
import subprocess
import unittest

from scm import Error, Repository, Tool
from test_upstream_sync import (FORK, FORK_PULL, NOW, SyncTestCase, fork_pr, git)
from upstream import Cli, attribute


LAND_FIELDS = ("number,state,isDraft,baseRefName,headRefName,headRefOid,headRepository,"
               "headRepositoryOwner,labels,reviews,reviewDecision,statusCheckRollup,url")
MEMBERSHIP = "orgs/Shopify/teams/yugabyte-code-reviewers/memberships/"
RULES = f"repos/{FORK}/rules/branches/master-shopify"
PR_URL = FORK_PULL + "200"
MERGED_AT = "2026-11-20T12:00:00Z"


class FakeBinks(Tool):
    """Answers `devx binks --json review list|comments` from canned JSON and records each call:
    `reviews` is the list output, `comments` maps the arguments after `comments` (joined by
    spaces) to its output, and `missing` makes devx not found, as Tool reports it."""

    def __init__(self):
        self.reviews = []
        self.comments = {}
        self.missing = False
        self.calls = []

    def run(self, argv, cwd=None, echo=None, check=True):
        argv = [str(arg) for arg in argv]
        self.calls.append(argv)
        if self.missing:
            raise Error("devx was not found")
        if argv[:6] == ["devx", "binks", "--json", "review", "list", "--pr"]:
            return subprocess.CompletedProcess(argv, 0, json.dumps(self.reviews), "")
        if argv[:5] == ["devx", "binks", "--json", "review", "comments"]:
            output = json.dumps(self.comments[" ".join(argv[5:])])
            return subprocess.CompletedProcess(argv, 0, output, "")
        raise AssertionError(f"unexpected command: {argv}")


class LandTestCase(SyncTestCase):
    """A candidate built and pushed by `sync`, its PR #200 open, approved, checked and reviewed
    by Binks, and master-shopify with no pull_request rule."""

    def setUp(self):
        super().setUp()
        self.published()
        self.head = self.candidate()[-1]["sha"]
        self.sleeps = []
        self.binks = FakeBinks()
        self.binks.reviews = [{"id": "rev2", "status": "ready", "head_sha": self.head,
                               "comment_count": 0},
                              {"id": "rev1", "status": "ready", "head_sha": self.old,
                               "comment_count": 3}]
        self.pr = {
            "number": 200, "state": "OPEN", "isDraft": False, "baseRefName": "master-shopify",
            "headRefName": self.branch, "headRefOid": self.head,
            "headRepository": {"name": "yugabyte-db"},
            "headRepositoryOwner": {"login": "Shopify"},
            "labels": [{"name": "upstream: skip"}, {"name": "sync"}],
            "reviews": [self.review("dana", self.head)],
            "reviewDecision": "APPROVED",
            "statusCheckRollup": [
                {"__typename": "CheckRun", "name": "pr-title", "status": "COMPLETED",
                 "conclusion": "SUCCESS"},
                {"__typename": "CheckRun", "name": "pr-lint", "status": "COMPLETED",
                 "conclusion": "SUCCESS"},
                {"__typename": "CheckRun", "name": "bld-db", "status": "QUEUED",
                 "conclusion": ""}],
            "url": PR_URL}
        self.github.answer("pr", "view", "200", "--repo", FORK, "--json", LAND_FIELDS,
                           value=self.pr)
        self.github.answer("api", MEMBERSHIP + "dana", value={"state": "active"})
        self.github.answer("api", RULES, value=[
            {"type": "deletion", "ruleset_source_type": "Repository", "ruleset_id": 7}])
        self.github.answer("pr", "view", "200", "--repo", FORK, "--json", "state,mergedAt",
                           value={"state": "MERGED", "mergedAt": MERGED_AT})

    @staticmethod
    def review(login, commit, state="APPROVED", at="2026-11-19T12:00:00Z"):
        return {"author": {"login": login}, "state": state, "commit": {"oid": commit},
                "submittedAt": at}

    def land(self, *argv, answers=None, confirm=True):
        out, err = io.StringIO(), io.StringIO()
        interactive = answers is not None
        stdin = io.StringIO("".join(f"{answer}\n" for answer in answers or []))
        confirming = ["--confirm-land", self.head[:12]] if confirm else []
        code = Cli.run(["sync", "--land", "200", "--repo-path", self.dev, "--upstream-url",
                        self.upstream_url, *confirming, *argv], github=self.github, out=out,
                       err=err, now=NOW, tool=self.binks, interactive=interactive, stdin=stdin,
                       cwd=os.path.join(self.tmpdir, "work"),
                       workdir=os.path.join(self.tmpdir, "scratch"), program="upstream.py",
                       sleep=self.sleeps.append)
        return code, out.getvalue(), err.getvalue()

    def fork_tip(self):
        return git(self.fork_url, "rev-parse", "refs/heads/master-shopify")

    def break_glass(self, head=None):
        return (f"Break-glass (skips these checks): `git push origin {head or self.head}:"
                "master-shopify`")

    def refused(self, message, *argv, break_glass=True, head=None):
        """Lands, expecting exit 1 with `message` (and the break-glass line) and no push."""
        code, out, err = self.land(*argv)
        expected = message + (f"\n{self.break_glass(head)}" if break_glass else "") + "\n"
        self.assertEqual((1, expected), (code, err), out)
        self.assertEqual(self.old, self.fork_tip())
        return out

    def commit_tree(self, tree, *parents, message):
        """A commit made without a worktree, as `git commit-tree` does."""
        argv = ["commit-tree", tree]
        for parent in parents:
            argv += ["-p", parent]
        return git(self.dev, *argv, "-m", message)

    def push_candidate(self, sha):
        """Replaces the candidate on origin with `sha` and makes it the PR's head."""
        git(self.dev, "push", "--quiet", "--force", "origin", f"{sha}:refs/heads/{self.branch}")
        self.pr["headRefOid"] = sha
        self.pr["reviews"] = [self.review("dana", sha)]
        self.binks.reviews[0]["head_sha"] = sha
        self.head = sha

    @property
    def sync_subject(self):
        return f"Sync master-shopify to upstream {self.short}"


class HappyPathTest(LandTestCase):
    def test_land_fast_forwards_master_shopify_and_keeps_the_remote_branch(self):
        code, out, err = self.land()

        self.assertEqual((0, ""), (code, err), out)
        self.assertEqual(self.head, self.fork_tip())
        self.assertEqual(self.head, self.remote_head())
        self.assertFalse(os.path.exists(self.worktree))
        self.assertNotIn(self.branch, self.local_branches())
        self.assertEqual([], self.sleeps)
        self.assertEqual([
            f"Land PR #200 ({PR_URL}) by fast-forwarding master-shopify:",
            f"  master-shopify: {self.old[:12]} -> {self.head[:12]}",
            f"  Upstream: {self.short}",
            "  Carried: 6 replays",
            "  Dropped (1): #69 (upstream #38110)",
            f"Pushed {self.head[:12]} to master-shopify.",
            f"GitHub marked PR #200 merged at {MERGED_AT}.",
            f"Removed the worktree {self.worktree}.",
            f"Deleted the branch {self.branch}.",
            f"Kept {self.branch} on origin: it is the record of this sync.",
            "Next, rebase the open PRs onto it: "
            "python3 .agents/scripts/shopify/upstream.py rebase-open-prs --push",
        ], out.splitlines()[-11:])
        self.assertEqual([["devx", "binks", "--json", "review", "list", "--pr", PR_URL]],
                         self.binks.calls)

    def test_an_interactive_land_asks_for_the_typed_phrase(self):
        code, out, err = self.land(answers=[f"land {self.head[:12]}"], confirm=False)

        self.assertEqual((0, ""), (code, err), out)
        self.assertIn(f"Type `land {self.head[:12]}` to push {self.head[:12]} to master-shopify: ",
                      out)
        self.assertEqual(self.head, self.fork_tip())

    def test_a_pr_github_has_not_marked_merged_yet_is_reported_after_polling(self):
        self.github.answer("pr", "view", "200", "--repo", FORK, "--json", "state,mergedAt",
                           value={"state": "OPEN", "mergedAt": None})

        code, out, err = self.land()

        self.assertEqual((0, ""), (code, err), out)
        self.assertEqual([3] * 9, self.sleeps)
        self.assertIn("GitHub has not marked PR #200 merged yet; it does once it sees "
                      "master-shopify contains its head.", out.splitlines())
        self.assertEqual(self.head, self.fork_tip())

    def test_binks_findings_are_printed_and_answered_ones_land(self):
        self.binks.reviews[0]["comment_count"] = 2
        self.binks.comments["rev2"] = [
            {"severity": "medium", "file_path": "src/batch.cc", "line": 1,
             "title": "Cap unchecked", "body": "The cap is never checked against the limit."},
            {"severity": "low", "path": "src/metric.cc", "body": "Name the unit"}]

        code, out, err = self.land("--findings-answered")

        self.assertEqual((0, ""), (code, err), out)
        self.assertIn("Binks review rev2 has 2 findings:\n"
                      "  medium src/batch.cc:1 Cap unchecked\n"
                      "  low src/metric.cc Name the unit\n", out)

    def test_a_control_plane_binks_review_is_read_from_its_own_list(self):
        self.binks.reviews = {
            "review_owner": "control_plane", "reviews": [],
            "control_plane_reviews": [{"id": "cp9", "status": "ready", "head_sha": self.head,
                                       "comment_count": 1}]}
        self.binks.comments["--control-plane cp9"] = [
            {"severity": "low", "file_path": "a.cc", "title": "Nit"}]

        code, out, err = self.land("--findings-answered")

        self.assertEqual((0, ""), (code, err), out)
        self.assertIn("Binks review cp9 has 1 finding:\n  low a.cc Nit\n", out)
        self.assertEqual(["devx", "binks", "--json", "review", "comments", "--control-plane",
                          "cp9"], self.binks.calls[-1])
        self.assertEqual(self.head, self.fork_tip())

    def test_an_interactive_land_asks_whether_binks_findings_are_answered(self):
        self.binks.reviews[0]["comment_count"] = 1
        self.binks.comments["rev2"] = [{"severity": "low", "path": "a.cc", "body": "Nit"}]

        code, out, err = self.land(answers=["y", f"land {self.head[:12]}"], confirm=False)

        self.assertEqual((0, ""), (code, err), out)
        self.assertIn("Is every finding fixed or answered? [y/N] ", out)
        self.assertEqual(self.head, self.fork_tip())

    def test_a_candidate_that_skipped_a_landing_lands_and_reports_it(self):
        self.push_candidate(self.candidate()[-2]["sha"])

        code, out, err = self.land()

        self.assertEqual((0, ""), (code, err), out)
        self.assertEqual(self.head, self.fork_tip())
        lines = out.splitlines()
        start = lines.index(f"Land PR #200 ({PR_URL}) by fast-forwarding master-shopify:")
        self.assertEqual(["  Carried: 5 replays", "  Skipped (1): #74 Tune the retry backoff (#74)",
                          "  Dropped (1): #69 (upstream #38110)"], lines[start + 3:start + 6])

    def test_a_sparse_checkout_can_land(self):
        git(self.dev, "config", "core.sparseCheckout", "true")

        code, out, err = self.land()

        self.assertEqual((0, ""), (code, err), out)
        self.assertEqual(self.head, self.fork_tip())

    def test_a_candidate_branch_checked_out_elsewhere_is_left_alone(self):
        git(self.worktree, "checkout", "--quiet", "--detach")
        git(self.dev, "branch", "--quiet", "-f", self.branch, self.old)

        code, out, err = self.land()

        self.assertEqual((0, ""), (code, err), out)
        self.assertIn(f"Kept the local branch {self.branch}: it is at {self.old[:12]}, not the "
                      "landed head.", out.splitlines())
        self.assertIn(self.branch, self.local_branches())


class RefusalTest(LandTestCase):
    def test_a_draft_pr_is_refused(self):
        self.pr["isDraft"] = True

        self.refused("PR #200 is a draft; mark it ready with `gh pr ready 200 --repo "
                     "Shopify/yugabyte-db` once Binks and a reviewer have seen it",
                     break_glass=False)

    def test_a_closed_pr_is_refused(self):
        self.pr["state"] = "CLOSED"

        self.refused("PR #200 is CLOSED, not open; reopen it, or build a new sync",
                     break_glass=False)

    def test_a_merged_pr_is_refused_without_the_break_glass_command(self):
        self.pr["state"] = "MERGED"

        self.refused("PR #200 is already merged; there is nothing to land", break_glass=False)

    def test_a_pr_against_another_base_is_refused(self):
        self.pr["baseRefName"] = "main"

        self.refused("PR #200 is against main, not master-shopify; only a sync PR lands this way",
                     break_glass=False)

    def test_a_pr_from_another_branch_is_refused(self):
        self.pr["headRefName"] = "feature"

        self.refused("PR #200's head is Shopify/yugabyte-db:feature, not a shopify-sync/<sha> "
                     "branch in Shopify/yugabyte-db; only a sync PR lands this way",
                     break_glass=False)

    def test_a_pr_from_another_fork_is_refused(self):
        self.pr["headRepositoryOwner"] = {"login": "someone"}

        self.refused(f"PR #200's head is someone/yugabyte-db:{self.branch}, not a "
                     "shopify-sync/<sha> branch in Shopify/yugabyte-db; only a sync PR lands "
                     "this way", break_glass=False)

    def test_a_pr_without_the_sync_label_is_refused(self):
        self.pr["labels"] = [{"name": "upstream: skip"}]

        self.refused("PR #200 is not labelled sync; only a sync PR lands this way",
                     break_glass=False)

    def test_a_candidate_that_moved_after_github_saw_it_is_refused(self):
        stale = self.candidate()[-2]["sha"]
        self.pr["headRefOid"] = stale

        self.refused(f"origin's {self.branch} is at {self.head[:12]}, but PR #200's head is "
                     f"{stale[:12]}; rerun once GitHub has caught up, and have the current head "
                     "reviewed", head=stale)

    def test_a_candidate_without_a_sync_merge_is_refused(self):
        self.push_candidate(self.commit_tree(git(self.dev, "rev-parse", f"{self.to}^{{tree}}"),
                                             self.to, message="Not a sync"))

        self.refused(f"{self.branch} has no `Sync master-shopify to upstream <sha>` merge on its "
                     f"first-parent line above upstream {self.short}; rebuild the sync (or, with "
                     "--no-fetch, fetch upstream first)")

    def test_a_merge_that_is_not_sync_shaped_is_refused(self):
        tree = git(self.dev, "rev-parse", f"{self.old}^{{tree}}")
        merge = self.commit_tree(tree, self.to, self.old, message=(
            f"{self.sync_subject}\n\nOld-master-shopify: {self.old}"))
        self.push_candidate(merge)

        self.refused(f"{merge[:12]} on {self.branch} is not a sync merge of upstream {self.short}: "
                     f"it needs upstream {self.short} as its first parent and that parent's tree "
                     "(git merge -s ours), and the commit its Old-master-shopify: line names as "
                     "its second parent; rebuild the sync (or, with --no-fetch, fetch upstream "
                     "first)")

    def test_a_merge_commit_after_the_sync_merge_is_refused(self):
        head = self.head
        merge = self.commit_tree(git(self.dev, "rev-parse", f"{head}^{{tree}}"), head, self.l70,
                                 message="Merge something")
        self.push_candidate(merge)

        self.refused(f"{merge[:12]} Merge something on {self.branch} is a merge commit after the "
                     "sync merge; a candidate carries only replays. Rebuild the sync")

    def test_an_appended_commit_that_belongs_to_no_pr_is_refused(self):
        tree = git(self.dev, "rev-parse", f"{self.to}^{{tree}}")
        reset = self.commit_tree(tree, self.head, message="Reset to upstream")
        self.push_candidate(reset)

        self.refused(f"{reset[:12]} Reset to upstream on {self.branch} belongs to no merged fork "
                     "PR; a candidate carries only the replays of its sync plan. Rebuild the sync")

    def test_a_replay_of_a_pr_outside_the_plan_is_refused(self):
        tree = git(self.dev, "rev-parse", f"{self.head}^{{tree}}")
        extra = self.commit_tree(tree, self.head, message="Refresh the catalog cache (#69)")
        self.push_candidate(extra)

        self.refused(f"{extra[:12]} Refresh the catalog cache (#69) on {self.branch} replays #69, "
                     "which the sync plan does not carry. Rebuild the sync")

    def test_a_landing_replayed_twice_is_refused(self):
        tree = git(self.dev, "rev-parse", f"{self.head}^{{tree}}")
        twice = self.commit_tree(tree, self.head, message="Tune the retry backoff (#74)")
        self.push_candidate(twice)

        self.refused(f"{twice[:12]} Tune the retry backoff (#74) on {self.branch} replays #74 "
                     "again; the sync plan carries it once. Rebuild the sync")

    def test_master_shopify_moved_since_the_candidate_was_built_is_refused_without_break_glass(
            self):
        moved = self.add_landing({"src/late.cc": "late\n"}, "Land late", 77)

        code, out, err = self.land()

        self.assertEqual((1, f"master-shopify moved since the candidate was built (from "
                             f"{self.old[:12]} to {moved[:12]}); rebuild the sync\n"),
                         (code, err), out)
        self.assertEqual(moved, self.fork_tip())

    def test_a_candidate_already_on_master_shopify_is_reported_as_landed(self):
        git(self.dev, "push", "--quiet", "origin", f"{self.head}:refs/heads/master-shopify")

        code, out, err = self.land()

        self.assertEqual((0, ""), (code, err), out)
        self.assertEqual(f"PR #200 is already landed: master-shopify is at its head "
                         f"{self.head[:12]}; GitHub marks the PR merged shortly.",
                         out.splitlines()[-1])

    def test_a_pr_without_an_approval_is_refused(self):
        self.pr["reviews"] = [self.review("dana", self.head, "COMMENTED")]

        self.refused(f"PR #200 has no approving review of its head {self.head[:12]} from a "
                     "Shopify/yugabyte-code-reviewers member; ask one to approve the current "
                     "head (approvals of earlier heads do not count)")

    def test_an_approval_of_an_earlier_head_is_refused(self):
        self.pr["reviews"] = [self.review("dana", self.candidate()[-2]["sha"])]

        self.refused(f"PR #200 has no approving review of its head {self.head[:12]} from a "
                     "Shopify/yugabyte-code-reviewers member; ask one to approve the current "
                     "head (approvals of earlier heads do not count)")

    def test_an_approval_by_someone_outside_the_team_is_refused(self):
        self.pr["reviews"] = [self.review("sam", self.head)]
        self.github.answer("api", MEMBERSHIP + "sam",
                           value=Error("Failed to run `gh api`\ngh: Not Found (HTTP 404)\n"))

        self.refused(f"PR #200 has no approving review of its head {self.head[:12]} from a "
                     "Shopify/yugabyte-code-reviewers member; ask one to approve the current "
                     "head (approvals of earlier heads do not count)")

    def test_changes_requested_after_an_approval_by_the_same_member_is_refused(self):
        self.pr["reviews"] = [
            self.review("dana", self.head, at="2026-11-19T12:00:00Z"),
            self.review("dana", self.head, "CHANGES_REQUESTED", at="2026-11-19T13:00:00Z"),
            self.review("dana", self.head, "COMMENTED", at="2026-11-19T14:00:00Z")]
        self.pr["reviewDecision"] = ""

        self.refused(f"PR #200 has changes requested by dana on {self.head[:12]}; resolve them "
                     "and ask for a new review")

    def test_changes_requested_on_an_earlier_head_still_block_after_another_approval(self):
        earlier = self.candidate()[-2]["sha"]
        self.github.answer("api", MEMBERSHIP + "sam", value={"state": "active"})
        self.pr["reviews"] = [
            self.review("sam", earlier, "CHANGES_REQUESTED", at="2026-11-19T12:00:00Z"),
            self.review("dana", self.head, at="2026-11-19T13:00:00Z")]
        self.pr["reviewDecision"] = ""

        self.refused(f"PR #200 has changes requested by sam on {earlier[:12]}; resolve them and "
                     "ask for a new review")

        self.pr["reviews"].append(self.review("sam", self.head, at="2026-11-19T14:00:00Z"))

        code, out, err = self.land()

        self.assertEqual(0, code, err)
        self.assertEqual(self.head, self.fork_tip())

    def test_an_approval_after_changes_requested_by_the_same_member_lands(self):
        self.pr["reviews"] = [
            self.review("dana", self.head, "CHANGES_REQUESTED", at="2026-11-19T12:00:00Z"),
            self.review("dana", self.head, at="2026-11-19T13:00:00Z")]

        code, out, err = self.land()

        self.assertEqual((0, ""), (code, err), out)
        self.assertEqual(self.head, self.fork_tip())

    def test_a_review_decision_of_changes_requested_is_refused(self):
        self.pr["reviewDecision"] = "CHANGES_REQUESTED"

        self.refused("PR #200's review decision is CHANGES_REQUESTED; resolve the outstanding "
                     "reviews first")

    def test_a_review_decision_of_review_required_is_refused(self):
        self.pr["reviewDecision"] = "REVIEW_REQUIRED"

        self.refused("PR #200's review decision is REVIEW_REQUIRED; resolve the outstanding "
                     "reviews first")

    def test_a_missing_review_decision_is_a_warning(self):
        self.pr["reviewDecision"] = None

        code, out, err = self.land()

        self.assertEqual((0, "PR #200 has no review decision on GitHub; the team member's "
                             f"approval of {self.head[:12]} is the gate\n"), (code, err), out)
        self.assertEqual(self.head, self.fork_tip())

    def test_a_failed_pr_lint_is_refused(self):
        self.pr["statusCheckRollup"][1]["conclusion"] = "FAILURE"

        self.refused("PR #200's pr-lint check is FAILURE, not SUCCESS; fix it and wait for the "
                     "check to pass")

    def test_absent_pr_title_and_pr_lint_checks_are_fine(self):
        self.pr["statusCheckRollup"] = []

        code, out, err = self.land()

        self.assertEqual((0, ""), (code, err), out)

    def test_a_pr_without_a_binks_review_is_refused_with_an_unpinned_request(self):
        self.binks.reviews = []

        self.refused(f"PR #200 has no Binks review; request one with `devx binks review run "
                     f"{PR_URL}`")

    def test_a_binks_review_of_an_earlier_head_is_refused(self):
        self.binks.reviews[0]["head_sha"] = self.old

        self.refused(f"PR #200's newest Binks review (rev2) is of {self.old[:12]}, not its head "
                     f"{self.head[:12]}; request one with `devx binks review run {PR_URL} "
                     f"--expect-head {self.head}`")

    def test_an_unfinished_binks_review_is_refused(self):
        self.binks.reviews[0]["status"] = "running"

        self.refused("PR #200's newest Binks review (rev2) is running, not ready; wait for it, "
                     "then rerun")

    def test_a_failed_binks_review_asks_for_a_new_one(self):
        self.binks.reviews[0]["status"] = "failed"

        self.refused("PR #200's newest Binks review (rev2) is failed, not ready; request a new "
                     f"one with `devx binks review run {PR_URL} --expect-head {self.head}`")

    def test_a_missing_devx_is_refused(self):
        self.binks.missing = True

        self.refused("Binks review required; install devx")

    def test_binks_findings_without_findings_answered_are_refused(self):
        self.binks.reviews[0]["comment_count"] = 1
        self.binks.comments["rev2"] = [{"severity": "high", "path": "a.cc", "line": 3,
                                        "body": "Leak"}]

        out = self.refused("Binks review rev2 has 1 finding; fix or answer it, then rerun with "
                           "--findings-answered")

        self.assertIn("  high a.cc:3 Leak\n", out)

    def test_declining_the_binks_findings_question_is_refused(self):
        self.binks.reviews[0]["comment_count"] = 1
        self.binks.comments["rev2"] = [{"severity": "high", "path": "a.cc", "body": "Leak"}]

        code, out, err = self.land(answers=["n"], confirm=False)

        self.assertEqual((1, "Not landed: fix or answer every Binks finding first\n"
                             f"{self.break_glass()}\n"), (code, err), out)
        self.assertEqual(self.old, self.fork_tip())

    def test_an_active_pull_request_rule_is_a_warning_and_the_push_is_attempted(self):
        self.github.answer("api", RULES, value=[
            {"type": "pull_request", "ruleset_source_type": "Repository", "ruleset_id": 42,
             "parameters": {"required_approving_review_count": 1}}])

        code, out, err = self.land()

        self.assertEqual((0, "An active pull_request rule (ruleset 42) applies to master-shopify; "
                             "the fast-forward may be rejected; options if it is: a scoped bypass "
                             "identity for syncs, or disabling that ruleset for the push\n"),
                         (code, err), out)
        self.assertEqual(self.head, self.fork_tip())

    def test_a_rejected_push_reports_the_rejection_and_changes_nothing(self):
        hook = os.path.join(self.fork_url, "hooks", "pre-receive")
        with open(hook, "w", encoding="utf-8") as script:
            script.write("#!/bin/sh\necho 'GH013: Repository rule violations found' >&2\n"
                         "exit 1\n")
        os.chmod(hook, 0o755)

        code, out, err = self.land()

        self.assertEqual(1, code, out)
        self.assertTrue(err.startswith("The push to master-shopify was rejected; nothing "
                                       "changed:\n"), err)
        self.assertIn("remote: GH013: Repository rule violations found\n", err)
        self.assertTrue(err.endswith("\nIf a pull_request rule rejected it, the options are a "
                                     "scoped bypass identity for syncs, or disabling that "
                                     "ruleset for the push\n"), err)
        self.assertNotIn("Break-glass", err)
        self.assertNotIn("Pushed", out)
        self.assertEqual(self.old, self.fork_tip())
        self.assertTrue(os.path.exists(self.worktree))

    def test_a_mistyped_phrase_is_a_usage_error(self):
        code, out, err = self.land(answers=["land it"], confirm=False)

        self.assertEqual((2, f"The answer was not `land {self.head[:12]}`; nothing was pushed\n"),
                         (code, err), out)
        self.assertEqual(self.old, self.fork_tip())

    def test_a_mismatched_confirm_land_is_a_usage_error(self):
        code, out, err = self.land("--confirm-land", self.old[:12], confirm=False)

        self.assertEqual((2, f"--confirm-land {self.old[:12]} is not the head "
                             f"{self.head[:12]}; nothing was pushed\n"), (code, err), out)
        self.assertEqual(self.old, self.fork_tip())

    def test_without_a_terminal_or_confirm_land_it_stops_with_the_rerun_command(self):
        code, out, err = self.land(confirm=False)

        self.assertEqual((4, ""), (code, err), out)
        self.assertEqual(["To land PR #200, rerun:",
                          f"  upstream.py sync --land 200 --repo-path {self.dev} --upstream-url "
                          f"{self.upstream_url} --confirm-land {self.head[:12]}"],
                         out.splitlines()[-2:])
        self.assertEqual(self.old, self.fork_tip())

    def test_land_with_another_sync_mode_is_a_usage_error(self):
        code, out, err = self.land("--yes", "--continue")

        self.assertEqual((2, "--land cannot be combined with --continue, --yes\n"), (code, err))

    def test_land_options_without_land_are_a_usage_error(self):
        code, out, err = self.sync("--findings-answered")

        self.assertEqual((2, "--findings-answered and --confirm-land need --land\n"), (code, err))


class StatusAfterLandingTest(LandTestCase):
    def test_the_landed_sync_pr_is_neither_a_contribution_nor_a_warning(self):
        self.assertEqual(0, self.land()[0])
        sync_pr = fork_pr(200, self.title, self.head, ["upstream: skip", "sync"])
        self.answer_merged(sync_pr)
        git(self.dev, "fetch", "--quiet", "origin",
            "+refs/heads/master-shopify:refs/remotes/origin/master-shopify")
        out, err = io.StringIO(), io.StringIO()

        code = Cli.run(["status", "--repo-path", self.dev, "--no-fetch", "--upstream-ref",
                        self.to, "--json"], github=self.github, out=out,
                       err=err, now=NOW)

        self.assertEqual((0, ""), (code, err.getvalue()), out.getvalue())
        status = json.loads(out.getvalue())
        landings = {c["number"]: c["landings"] for c in status["contributions"]}
        self.assertEqual([41, 42, 70, 71, 72, 73, 74], sorted(landings))
        self.assertEqual([self.head], landings[74])
        self.assertEqual([], status["unidentified"])
        self.assertEqual(["#74: could not read upstream PR yugabyte#38160: no pull requests found"],
                         status["warnings"])

    def test_a_squash_merged_sync_pr_is_a_warning(self):
        tree = git(self.dev, "rev-parse", f"{self.head}^{{tree}}")
        squash = self.commit_tree(tree, self.old, message=f"{self.title} (#200)")
        git(self.dev, "push", "--quiet", "origin", f"{squash}:refs/heads/master-shopify")
        self.answer_merged(fork_pr(200, self.title, squash, ["upstream: skip", "sync"]))
        git(self.dev, "fetch", "--quiet", "origin",
            "+refs/heads/master-shopify:refs/remotes/origin/master-shopify")
        out, err = io.StringIO(), io.StringIO()

        code = Cli.run(["status", "--repo-path", self.dev, "--no-fetch", "--upstream-ref",
                        self.to, "--json"], github=self.github, out=out, err=err, now=NOW)

        self.assertEqual((0, ""), (code, err.getvalue()), out.getvalue())
        status = json.loads(out.getvalue())
        self.assertNotIn(200, [c["number"] for c in status["contributions"]])
        self.assertIn("sync PR #200 was merged with a GitHub button, undoing the sync; see the "
                      "Landing section of its body", status["warnings"])

    def test_attribution_never_gives_the_last_replay_to_the_sync_pr(self):
        commits = Repository(self.dev).first_parent_commits(self.to, self.head)
        sync_pr = fork_pr(200, self.title, self.head, ["upstream: skip", "sync"])

        landed, unidentified = attribute(commits[1:], self.merged + [sync_pr])

        self.assertEqual(([self.head], [], None), (landed[74], unidentified, landed.get(200)))


class CliTest(unittest.TestCase):
    def test_sync_help_documents_the_land_options(self):
        out = io.StringIO()

        self.assertEqual(0, Cli.run(["sync", "--help"], out=out))
        for option in ("--land", "--findings-answered", "--confirm-land"):
            self.assertIn(option, out.getvalue())


if __name__ == "__main__":
    unittest.main()
