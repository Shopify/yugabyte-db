"""Exercise the release planner against throwaway git repositories and an in-memory GitHub.

Tests never read or write a real checkout and never call GitHub or the network.
"""

import io
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from release import (
    AvailabilityLabels, CherryPickList, Cli, Commit, Error, GitError, GitHub, Planner, Repository,
    UpstreamVersion,
)


SCRIPT = str(Path(__file__).resolve().with_name("release.py"))
GIT_ENV = {
    "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_SYSTEM": os.devnull,
    "GIT_AUTHOR_NAME": "Test", "GIT_AUTHOR_EMAIL": "test@example.invalid",
    "GIT_COMMITTER_NAME": "Test", "GIT_COMMITTER_EMAIL": "test@example.invalid",
}
INVALID_VERSION = ("Invalid upstream version {!r}: expected three or four numeric parts without "
                   "leading zeros, such as 2025.1.2 or v2025.2.8.0")
T0, DAY = 1735689600, 86400


class GitFixture:
    """A throwaway git repository under a temporary directory."""

    def __init__(self, path):
        self.path = path
        self.counter = 0
        os.makedirs(path, exist_ok=True)
        self.git("init", "--quiet", "--initial-branch=main")

    def git(self, *args, env=None):
        result = subprocess.run(["git", "-C", self.path, *args], capture_output=True, text=True,
                                env={**os.environ, **GIT_ENV, **(env or {})})
        if result.returncode:
            raise AssertionError(f"git {' '.join(args)} failed: {result.stderr}")
        return result.stdout.strip()

    def commit(self, message=None, timestamp=None):
        self.counter += 1
        Path(self.path, f"file{self.counter}.txt").write_text(f"content {self.counter}\n")
        self.git("add", ".")
        dates = None
        if timestamp is not None:
            dates = {"GIT_AUTHOR_DATE": f"{timestamp} +0000",
                     "GIT_COMMITTER_DATE": f"{timestamp} +0000"}
        self.git("commit", "--quiet", "-m", message or f"commit {self.counter}", env=dates)
        return self.git("rev-parse", "HEAD")

    def tag(self, name, ref="HEAD"):
        self.git("tag", name, ref)

    def branch(self, name, ref="HEAD"):
        self.git("branch", name, ref)

    def remote_branch(self, name, ref="HEAD"):
        self.git("update-ref", f"refs/remotes/origin/{name}", self.git("rev-parse", ref))

    def init_bare(self, bare_path):
        self.git("init", "--quiet", "--bare", bare_path)

    def tags_list(self):
        tags = self.git("tag")
        return tags.split("\n") if tags else []

    def refs(self):
        return self.git("for-each-ref", "--format=%(refname) %(objectname)")


class FakeGitHub(GitHub):
    """In-memory stand-in for the gh client that records which PRs were requested."""

    def __init__(self, pull_requests, commits_by_pr=None):
        super().__init__()
        self._pull_requests = pull_requests
        self.commits_by_pr = commits_by_pr or {}
        self.requested_prs = []

    def pull_requests(self, repo):
        return self._pull_requests

    def commits_in_pr(self, repo, number):
        self.requested_prs.append(number)
        return self.commits_by_pr[number]


def pr(number, *labels):
    return {"number": number, "labels": [{"name": label} for label in labels]}


def pr_commits(*oids, merge=None):
    return {"commits": [{"oid": oid} for oid in oids],
            "mergeCommit": {"oid": merge} if merge else None}


def unlabelled_github(oids_by_pr):
    """A GitHub whose PRs all carry `Available: nil`, so every one is selected."""
    return FakeGitHub([pr(number, "Available: nil") for number in oids_by_pr],
                      {number: pr_commits(*oids) for number, oids in oids_by_pr.items()})


class RepoTestCase(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="yb-builds-test")
        self.addCleanup(shutil.rmtree, self.tmpdir)
        environment = patch.dict(os.environ, GIT_ENV)
        environment.start()
        self.addCleanup(environment.stop)
        self.fixture = GitFixture(os.path.join(self.tmpdir, "yugabyte-db"))

    def cli(self, *argv, github):
        out, err = io.StringIO(), io.StringIO()
        code = Cli.run(list(argv), github=github, out=out, err=err)
        return code, out.getvalue(), err.getvalue()

    def plan_cli(self, github, path=None, version="2025.2.8.0"):
        return self.cli("--version", version, "--repo-path", path or self.fixture.path,
                        "--no-fetch", github=github)

    def tagged_patch(self, fixture=None):
        """Tags a base commit v2025.2.8.0 and returns a Shopify patch committed on top of it."""
        fixture = fixture or self.fixture
        fixture.tag("v2025.2.8.0", fixture.commit("base"))
        return fixture.commit("shopify patch")


class UpstreamVersionTest(unittest.TestCase):
    def test_parses_three_part_versions(self):
        version = UpstreamVersion.parse("2025.1.2")

        self.assertEqual("2025.1.2", version.full)
        self.assertEqual("2025.1", version.series)
        self.assertEqual("v2025.1.2", version.tag)

    def test_parses_four_part_versions_and_strips_the_v_prefix(self):
        version = UpstreamVersion.parse("v2025.2.8.0")

        self.assertEqual("2025.2.8.0", version.full)
        self.assertEqual("2025.2", version.series)
        self.assertEqual("v2025.2.8.0", version.tag)

    def test_rejects_unsupported_shapes(self):
        for text in ["2025", "2025.1", "2025.1.2.3.4", "2025.1.x", "2025.1.2-shopify-1", ""]:
            with self.subTest(text=text):
                with self.assertRaises(Error) as caught:
                    UpstreamVersion.parse(text)
                self.assertEqual(INVALID_VERSION.format(text), str(caught.exception))

    def test_rejects_leading_zeros_in_any_part(self):
        for text in ["2025.01.2", "02025.1.2", "2025.1.02", "2025.1.2.00", "v2025.2.8.01"]:
            with self.subTest(text=text):
                with self.assertRaises(Error) as caught:
                    UpstreamVersion.parse(text)
                self.assertEqual(INVALID_VERSION.format(text), str(caught.exception))

    def test_accepts_zero_as_a_part(self):
        self.assertEqual("2025.2.0.0", UpstreamVersion.parse("v2025.2.0.0").full)

    def test_series_compares_the_first_two_numeric_components(self):
        a = UpstreamVersion.parse("2025.1.9")
        b = UpstreamVersion.parse("2025.2.0.0")
        c = UpstreamVersion.parse("2025.1.0.0")

        self.assertFalse(a.same_series(b))
        self.assertTrue(a.same_series(c))

    def test_orders_three_and_four_part_versions_numerically(self):
        versions = [UpstreamVersion.parse(v)
                    for v in ["2025.2.8.0", "2025.1.10", "2025.1.9", "2025.2.0.0"]]

        self.assertEqual(["2025.1.9", "2025.1.10", "2025.2.0.0", "2025.2.8.0"],
                         [version.full for version in sorted(versions)])

    def test_three_part_and_zero_padded_four_part_versions_are_equal_in_order(self):
        a, b = UpstreamVersion.parse("2025.1.2"), UpstreamVersion.parse("2025.1.2.0")

        self.assertEqual(a, b)
        self.assertFalse(a < b or b < a)

    def test_three_part_and_zero_padded_four_part_versions_hash_alike(self):
        self.assertEqual(hash(UpstreamVersion.parse("2025.1.2")),
                         hash(UpstreamVersion.parse("2025.1.2.0")))


class AvailabilityLabelsTest(unittest.TestCase):
    def inspect_pr(self, labels, target):
        return AvailabilityLabels.inspect_pull_request(pr(1, *labels),
                                                       UpstreamVersion.parse(target))

    def test_pull_requests_without_availability_labels_are_not_inspected(self):
        self.assertIsNone(self.inspect_pr(["bug"], "2025.1.5"))

    def test_version_label_at_or_below_target_in_same_series_is_covered_by_label(self):
        entry = self.inspect_pr(["Available: 2025.1.3"], "2025.1.5")

        self.assertEqual("covered_by_label", entry.status)
        self.assertFalse(entry.verified)

    def test_label_equal_to_the_target_is_covered(self):
        self.assertEqual("covered_by_label",
                         self.inspect_pr(["Available: 2025.2.8.0"], "2025.2.8.0").status)

    def test_version_label_above_target_is_needed(self):
        self.assertEqual("needed_by_label",
                         self.inspect_pr(["Available: 2025.1.3"], "2025.1.2").status)

    def test_patch_label_does_not_cover_a_later_series(self):
        self.assertEqual("needed_by_label",
                         self.inspect_pr(["Available: 2025.1.3"], "2025.2.1").status)

    def test_series_initial_label_covers_later_series(self):
        self.assertEqual("covered_by_label",
                         self.inspect_pr(["Available: 2025.1.0"], "2025.2.1").status)
        self.assertEqual("covered_by_label",
                         self.inspect_pr(["Available: 2025.1.0.0"], "2025.2.1").status)

    def test_next_series_label_does_not_cover_the_previous_series(self):
        self.assertEqual("needed_by_label",
                         self.inspect_pr(["Available: 2025.2.0"], "2025.1.9").status)
        self.assertEqual("covered_by_label",
                         self.inspect_pr(["Available: 2025.2.0"], "2025.2.1").status)

    def test_nil_label_alone_is_unknown_availability(self):
        entry = self.inspect_pr(["Available: nil"], "2025.1.5")

        self.assertEqual("unknown_availability", entry.status)
        self.assertEqual([], entry.versions)
        self.assertTrue(entry.unknown)

    def test_mixed_nil_and_version_labels_do_not_raise_and_use_the_version(self):
        self.assertEqual("covered_by_label", self.inspect_pr(
            ["Available: nil", "Available: 2025.1.3"], "2025.1.5").status)
        self.assertEqual("needed_by_label", self.inspect_pr(
            ["Available: 2025.1.3", "Available: nil"], "2025.1.2").status)

    def test_unrecognised_label_values_are_unknown_and_reported(self):
        entry = self.inspect_pr(["Available: soon"], "2025.1.5")

        self.assertEqual("unknown_availability", entry.status)
        self.assertEqual(["soon"], entry.unrecognized)

    def test_inspect_all_selects_needed_and_unknown_pull_requests(self):
        prs = [pr(1, "Available: 2025.1.3"), pr(2, "Available: nil"),
               pr(3, "Available: 2025.9.0"), pr(4, "bug")]

        result = AvailabilityLabels.inspect_all(prs, UpstreamVersion.parse("2025.1.5"))

        self.assertEqual([1, 2, 3], [entry.number for entry in result])
        self.assertEqual([2, 3], AvailabilityLabels.selected_numbers(result))


class PlannerTest(RepoTestCase):
    def setUp(self):
        super().setUp()
        self.base = self.fixture.commit("base")
        self.fixture.tag("v2025.1.2")
        self.upstream = self.fixture.commit("upstream 2025.2.8.0")
        self.fixture.tag("v2025.2.8.0")

    def plan(self, version, **options):
        return Planner(Repository(self.fixture.path)).plan(version, [], **options)

    def assert_plan_error(self, message, version, **options):
        with self.assertRaises(Error) as caught:
            self.plan(version, **options)
        self.assertEqual(message, str(caught.exception))

    def test_plans_a_three_part_version_against_its_v_prefixed_tag(self):
        plan = self.plan("2025.1.2")

        self.assertEqual("2025.1.2", plan.upstream_version)
        self.assertEqual("2025.1", plan.series)
        self.assertEqual("v2025.1.2", plan.upstream_tag)
        self.assertEqual(self.base, plan.upstream_commit)
        self.assertEqual(1, plan.sequence)
        self.assertEqual("2025.1.2-shopify-1-candidate", plan.candidate_branch)
        self.assertEqual("2025.1.2-shopify-1", plan.release_name)
        self.assertFalse(plan.candidate_exists)

    def test_plans_a_four_part_version_and_omits_v_from_the_candidate_name(self):
        plan = self.plan("v2025.2.8.0")

        self.assertEqual("2025.2.8.0", plan.upstream_version)
        self.assertEqual("2025.2", plan.series)
        self.assertEqual("v2025.2.8.0", plan.upstream_tag)
        self.assertEqual(self.upstream, plan.upstream_commit)
        self.assertEqual("2025.2.8.0-shopify-1-candidate", plan.candidate_branch)

    def test_missing_upstream_tag_is_an_error(self):
        self.assert_plan_error(
            f"Upstream tag v2025.9.9.9 (or 2025.9.9.9) not found in {self.fixture.path}",
            "2025.9.9.9")

    def test_falls_back_to_an_unprefixed_upstream_tag_only_when_no_v_tag_exists(self):
        self.fixture.tag("2025.3.0.0", self.base)

        self.assertEqual("2025.3.0.0", self.plan("2025.3.0.0").upstream_tag)

    def test_prefers_the_v_prefixed_tag_when_both_exist(self):
        self.fixture.tag("2025.2.8.0", self.base)

        self.assertEqual("v2025.2.8.0", self.plan("2025.2.8.0").upstream_tag)

    def test_existing_release_tag_advances_the_sequence(self):
        self.fixture.tag("2025.2.8.0-shopify-1")

        plan = self.plan("2025.2.8.0")

        self.assertEqual(2, plan.sequence)
        self.assertEqual("2025.2.8.0-shopify-2-candidate", plan.candidate_branch)

    def test_release_branch_on_origin_advances_the_sequence(self):
        self.fixture.remote_branch("2025.2.8.0-shopify-3")

        self.assertEqual(4, self.plan("2025.2.8.0").sequence)

    def test_a_local_release_branch_advances_the_sequence(self):
        self.fixture.branch("2025.2.8.0-shopify-1")

        self.assertEqual(2, self.plan("2025.2.8.0").sequence)

    def test_releases_of_other_versions_do_not_affect_the_sequence(self):
        self.fixture.tag("2025.2.8-shopify-5")
        self.fixture.tag("2025.2.8.0.1-shopify-5")
        self.fixture.tag("2025.2.8.1-shopify-5")

        self.assertEqual(1, self.plan("2025.2.8.0").sequence)

    def test_resumes_an_unreleased_candidate_that_descends_from_the_upstream_tag(self):
        self.fixture.tag("2025.2.8.0-shopify-1")
        self.fixture.branch("2025.2.8.0-shopify-2-candidate", "v2025.2.8.0")

        plan = self.plan("2025.2.8.0")

        self.assertEqual(2, plan.sequence)
        self.assertTrue(plan.candidate_exists)

    def test_resumes_an_unreleased_candidate_that_only_exists_on_origin(self):
        self.fixture.remote_branch("2025.2.8.0-shopify-1-candidate", "v2025.2.8.0")

        plan = self.plan("2025.2.8.0")

        self.assertEqual(1, plan.sequence)
        self.assertTrue(plan.candidate_exists)

    def test_rejects_a_candidate_that_does_not_descend_from_the_upstream_tag(self):
        self.fixture.branch("2025.2.8.0-shopify-1-candidate", "v2025.1.2")

        self.assert_plan_error(
            "Candidate branch 2025.2.8.0-shopify-1-candidate conflicts with upstream tag "
            "v2025.2.8.0: it is not based on that tag", "2025.2.8.0")

    def test_rejects_an_explicit_candidate_whose_sequence_is_already_released(self):
        self.fixture.tag("2025.2.8.0-shopify-1")

        self.assert_plan_error(
            "Release 2025.2.8.0-shopify-1 already exists and is immutable; choose a new sequence",
            "2025.2.8.0", target_branch="2025.2.8.0-shopify-1-candidate")

    def test_rejects_an_explicit_candidate_name_that_is_not_canonical(self):
        self.assert_plan_error(
            "Target branch 'candidate-2025.2.8.0-shopify-2' must be named "
            "2025.2.8.0-shopify-<sequence>-candidate",
            "2025.2.8.0", target_branch="candidate-2025.2.8.0-shopify-2")

    def test_rejects_an_explicit_candidate_for_another_version(self):
        self.assert_plan_error(
            "Target branch '2025.1.2-shopify-1-candidate' must be named "
            "2025.2.8.0-shopify-<sequence>-candidate",
            "2025.2.8.0", target_branch="2025.1.2-shopify-1-candidate")

    def test_accepts_an_explicit_canonical_candidate(self):
        plan = self.plan("2025.2.8.0", target_branch="2025.2.8.0-shopify-7-candidate")

        self.assertEqual(7, plan.sequence)
        self.assertEqual("2025.2.8.0-shopify-7-candidate", plan.candidate_branch)

    def test_planning_never_moves_or_creates_refs(self):
        self.fixture.tag("2025.2.8.0-shopify-1")
        self.fixture.branch("2025.2.8.0-shopify-2-candidate", "v2025.2.8.0")
        before = self.fixture.refs()
        head = self.fixture.git("rev-parse", "HEAD")

        self.plan("2025.2.8.0")
        self.plan("2025.1.2")

        self.assertEqual(before, self.fixture.refs())
        self.assertEqual(head, self.fixture.git("rev-parse", "HEAD"))
        self.assertEqual("", self.fixture.git("status", "--porcelain"))

    def test_previous_release_is_the_highest_lower_release_across_three_and_four_part_versions(
            self):
        self.fixture.tag("2025.1.2-shopify-1")
        self.fixture.tag("2025.1.2-shopify-4")
        self.fixture.remote_branch("2025.2.7.0-shopify-3")
        self.fixture.remote_branch("2025.2.7.0-shopify-10")
        self.fixture.tag("2025.2.8.0-shopify-1")
        self.fixture.tag("2025.2.9.0-shopify-1")

        self.assertEqual("2025.2.7.0-shopify-10", self.plan("2025.2.8.0").previous_release)

    def test_previous_release_ranks_sequence_numerically_regardless_of_ref_order(self):
        self.fixture.tag("2025.2.7.0-shopify-9")
        self.fixture.tag("2025.2.7.0-shopify-3")
        self.fixture.remote_branch("2025.2.7.0-shopify-10")

        self.assertEqual("2025.2.7.0-shopify-10", self.plan("2025.2.8.0").previous_release)

    def test_previous_release_ranks_sequence_numerically_when_the_highest_is_listed_first(self):
        self.fixture.tag("2025.2.7.0-shopify-10")
        self.fixture.remote_branch("2025.2.7.0-shopify-9")
        self.fixture.remote_branch("2025.2.7.0-shopify-3")

        self.assertEqual("2025.2.7.0-shopify-10", self.plan("2025.2.8.0").previous_release)

    def test_previous_release_is_nil_when_there_is_none(self):
        self.assertIsNone(self.plan("2025.1.2").previous_release)

    def test_explicit_previous_release_must_look_like_a_release(self):
        self.assert_plan_error(
            "Invalid previous release 'main': expected <version>-shopify-<sequence>",
            "2025.2.8.0", previous_release="main")

    def test_plan_reports_the_label_inspection(self):
        prs = [pr(5, "Available: nil", "Available: 2025.2.9.0"), pr(6, "Available: 2025.1.2")]

        plan = Planner(Repository(self.fixture.path)).plan("2025.2.8.0", prs)

        self.assertEqual([5, 6], [entry.number for entry in plan.label_inspection])
        self.assertEqual([5, 6], plan.cherry_pick_prs)


class GitRepositoryTest(RepoTestCase):
    def assert_git_error(self, message, path):
        with self.assertRaises(GitError) as caught:
            Repository(path)
        self.assertEqual(message, str(caught.exception))

    def test_requires_an_explicit_path(self):
        self.assert_git_error("A repository path is required", None)

    def test_rejects_a_missing_directory_without_falling_back_to_a_default(self):
        missing = os.path.join(self.tmpdir, "does-not-exist")

        self.assert_git_error(f"Repository path {missing} is not a directory", missing)

    def test_rejects_a_directory_that_is_not_a_git_repository(self):
        plain = os.path.join(self.tmpdir, "plain")
        os.mkdir(plain)

        self.assert_git_error(f"Repository path {plain} is not a git repository", plain)

    def test_commands_run_against_the_given_path_not_the_working_directory(self):
        self.fixture.commit()
        self.fixture.tag("v1.0.0")
        other = tempfile.mkdtemp(prefix="yb-builds-cwd")
        self.addCleanup(shutil.rmtree, other)
        self.addCleanup(os.chdir, os.getcwd())

        os.chdir(other)
        tags = Repository(self.fixture.path).tags()

        self.assertEqual(["v1.0.0"], tags)

    def test_passes_arguments_without_shell_interpretation(self):
        self.fixture.commit()
        marker = os.path.join(self.tmpdir, "injected")

        with self.assertRaises(GitError):
            Repository(self.fixture.path).commit_timestamp(f"HEAD; touch {marker}")

        self.assertFalse(os.path.exists(marker))

    def test_lists_local_and_origin_branches(self):
        self.fixture.commit()
        self.fixture.remote_branch("release-a")

        repo = Repository(self.fixture.path)

        self.assertEqual(["main"], repo.local_branches())
        self.assertEqual(["release-a"], repo.origin_branches())

    def test_ancestor_check(self):
        first = self.fixture.commit()
        self.fixture.tag("first")
        self.fixture.commit()
        repo = Repository(self.fixture.path)

        self.assertTrue(repo.ancestor(first, "main"))
        self.assertFalse(repo.ancestor("main", first))


class CliTest(RepoTestCase):
    def run_script(self, *args, python_args=("-B",)):
        home = os.path.join(self.tmpdir, "home")
        cwd = os.path.join(self.tmpdir, "cwd")
        os.makedirs(home, exist_ok=True)
        os.makedirs(cwd, exist_ok=True)
        env = {**os.environ, "HOME": home, "GIT_CONFIG_GLOBAL": os.devnull,
               "GIT_CONFIG_SYSTEM": os.devnull,
               "GIT_CEILING_DIRECTORIES": os.path.realpath(self.tmpdir)}
        result = subprocess.run([sys.executable, *python_args, *args], cwd=cwd, env=env,
                                capture_output=True, text=True)
        return result, home, cwd

    def import_module(self):
        code = (f"import sys; sys.path.insert(0, {os.path.dirname(SCRIPT)!r}); import release; "
                "print(hasattr(release, 'YUGABYTE_DIR'))")
        return self.run_script("-c", code, python_args=("-I", "-B"))

    def test_importing_the_module_has_no_side_effects(self):
        result, home, cwd = self.import_module()

        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual([], os.listdir(home))
        self.assertEqual([], os.listdir(cwd))
        self.assertEqual("", result.stderr)

    def test_importing_the_module_defines_no_default_checkout_path(self):
        result, _, _ = self.import_module()

        self.assertEqual("False\n", result.stdout)

    def test_help_has_no_side_effects_and_documents_repo_path(self):
        result, home, cwd = self.run_script(SCRIPT, "--help")

        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("--repo-path=path", result.stdout)
        self.assertIn("--no-fetch", result.stdout)
        self.assertEqual([], os.listdir(home))
        self.assertEqual([], os.listdir(cwd))

    def test_missing_repo_path_outside_a_repository_is_a_usage_error_without_touching_any_checkout(
            self):
        result, home, cwd = self.run_script(SCRIPT, "--version", "2025.1.2")

        self.assertEqual(2, result.returncode)
        self.assertEqual(
            "--repo-path is required when the current directory is not a git repository\n",
            result.stderr)
        self.assertEqual([], os.listdir(home))
        self.assertEqual([], os.listdir(cwd))

    def test_nonexistent_repo_path_is_reported(self):
        missing = os.path.join(self.tmpdir, "nope")

        result, _, _ = self.run_script(SCRIPT, "--version", "2025.1.2", "--repo-path", missing)

        self.assertEqual(1, result.returncode)
        self.assertEqual(f"Repository path {missing} is not a directory\n", result.stderr)

    def test_repo_underscore_spelling_is_accepted_for_dev_flag_compatibility(self):
        missing = os.path.join(self.tmpdir, "nope")

        result, _, _ = self.run_script(SCRIPT, "--version", "2025.1.2", "--repo_path", missing)

        self.assertEqual(1, result.returncode)
        self.assertEqual(f"Repository path {missing} is not a directory\n", result.stderr)

    def test_no_fetch_underscore_spelling_is_accepted_for_dev_flag_compatibility(self):
        self.fixture.tag("v2025.2.8.0", self.fixture.commit("base"))

        code, out, _ = self.cli("--version", "2025.2.8.0", "--repo_path", self.fixture.path,
                                "--no_fetch", github=FakeGitHub([]))

        self.assertEqual(0, code)
        self.assertIn("All commits have been applied.", out)

    def test_version_is_required(self):
        result, _, _ = self.run_script(SCRIPT, "--repo-path", self.fixture.path)

        self.assertEqual(2, result.returncode)
        self.assertEqual("--version is required\n", result.stderr)

    def test_repo_path_defaults_to_the_repository_containing_the_working_directory(self):
        github = unlabelled_github({42: [self.tagged_patch()]})
        self.addCleanup(os.chdir, os.getcwd())
        os.chdir(self.fixture.path)

        code, out, err = self.cli("--version", "2025.2.8.0", "--no-fetch", github=github)

        self.assertEqual(0, code, err)
        self.assertIn("cd " + shlex.quote(os.path.realpath(self.fixture.path)), out.splitlines())

    def test_prints_checkout_instructions_with_the_real_v_prefixed_tag(self):
        patch_commit = self.tagged_patch()
        github = unlabelled_github({42: [patch_commit]})

        code, out, err = self.plan_cli(github)

        self.assertEqual(0, code, err)
        lines = out.splitlines()
        self.assertIn(f"cd {self.fixture.path}", lines)
        self.assertIn("git checkout v2025.2.8.0", lines)
        self.assertIn("git checkout -b 2025.2.8.0-shopify-1-candidate", lines)
        self.assertIn(f"git cherry-pick -x {patch_commit}", lines)
        self.assertEqual([42], github.requested_prs)

    def test_cd_path_is_shell_escaped(self):
        spaced = GitFixture(os.path.join(self.tmpdir, "my repo"))
        github = unlabelled_github({1: [self.tagged_patch(spaced)]})

        code, out, _ = self.plan_cli(github, path=spaced.path)

        self.assertEqual(0, code)
        self.assertIn("cd " + shlex.quote(spaced.path), out.splitlines())
        self.assertIn(f"cd '{self.tmpdir}/my repo'\n", out)

    def test_default_run_fetches_from_origin_and_no_fetch_skips_it(self):
        remote = os.path.join(self.tmpdir, "origin.git")
        upstream = GitFixture(os.path.join(self.tmpdir, "upstream"))
        upstream.init_bare(remote)
        upstream.tag("v2025.2.8.0", upstream.commit("upstream release"))
        upstream.git("push", "--quiet", remote, "main", "--tags")
        self.fixture.commit("local")
        self.fixture.git("remote", "add", "origin", remote)
        argv = ["--version", "2025.2.8.0", "--repo-path", self.fixture.path]
        github = FakeGitHub([])

        skipped, _, skipped_err = self.cli(*argv, "--no-fetch", github=github)

        self.assertEqual(1, skipped)
        self.assertEqual(
            f"Upstream tag v2025.2.8.0 (or 2025.2.8.0) not found in {self.fixture.path}\n",
            skipped_err)
        self.assertEqual([], self.fixture.tags_list())

        fetched, _, fetched_err = self.cli(*argv, github=github)

        self.assertEqual(0, fetched, fetched_err)
        self.assertEqual(["v2025.2.8.0"], self.fixture.tags_list())

    def test_missing_gh_binary_is_a_clear_error(self):
        executable = os.path.join(self.tmpdir, "no-such-gh")

        with self.assertRaises(Error) as caught:
            GitHub(executable=executable).pull_requests("Shopify/yugabyte-db")

        self.assertEqual(
            f"The gh CLI was not found ({executable}); install gh and run `gh auth login`",
            str(caught.exception))

    def test_cli_reports_a_missing_gh_binary_with_exit_code_1(self):
        self.fixture.tag("v2025.2.8.0", self.fixture.commit("base"))

        code, _, err = self.plan_cli(GitHub(executable=os.path.join(self.tmpdir, "no-such-gh")))

        self.assertEqual(1, code)
        self.assertIn("The gh CLI was not found", err)

    def test_an_error_that_already_ends_in_a_newline_is_written_with_exactly_one(self):
        self.fixture.tag("v2025.2.8.0", self.fixture.commit("base"))
        missing = "f" * 40
        git_stderr = subprocess.run(
            ["git", "-C", self.fixture.path, "show", "--no-patch", "--format=%ct", missing],
            capture_output=True, text=True).stderr
        expected = (f"Failed to run `git show --no-patch --format=%ct {missing}` in "
                    f"{self.fixture.path}\n{git_stderr}")

        code, _, err = self.plan_cli(unlabelled_github({1: [missing]}))

        self.assertEqual(1, code)
        self.assertTrue(git_stderr.endswith("\n") and not git_stderr.endswith("\n\n"), git_stderr)
        self.assertEqual(expected, err)

    def test_resumed_candidate_skips_commits_already_cherry_picked(self):
        self.fixture.tag("v2025.2.8.0", self.fixture.commit("base"))
        self.fixture.git("checkout", "--quiet", "-b", "source")
        patch_commit = self.fixture.commit("patch")
        self.fixture.git("checkout", "--quiet", "main")
        self.fixture.git("checkout", "--quiet", "-b", "2025.2.8.0-shopify-1-candidate")
        self.fixture.git("cherry-pick", "-x", patch_commit)

        code, out, _ = self.plan_cli(unlabelled_github({42: [patch_commit]}),
                                     version="v2025.2.8.0")

        self.assertEqual(0, code)
        self.assertIn("All commits have been applied.", out)
        self.assertNotIn("git cherry-pick", out)

    def test_resumed_candidate_with_remaining_commits_prints_no_checkout_commands(self):
        self.fixture.tag("v2025.2.8.0", self.fixture.commit("base", T0))
        self.fixture.git("checkout", "--quiet", "-b", "source")
        a, b = self.fixture.commit("a", T0 + DAY), self.fixture.commit("b", T0 + 2 * DAY)
        self.fixture.git("checkout", "--quiet", "-b", "2025.2.8.0-shopify-1-candidate", "main")
        self.fixture.git("cherry-pick", "-x", a)

        code, out, err = self.plan_cli(unlabelled_github({1: [a], 2: [b]}))

        self.assertEqual(0, code, err)
        self.assertEqual([f"git cherry-pick -x {b}"],
                         [line for line in out.splitlines() if line.startswith("git ")])


class CherryPickListTest(RepoTestCase):
    def test_orders_commits_by_parent_committer_timestamp_within_one_day(self):
        self.fixture.tag("v2025.2.8.0", self.fixture.commit("base", timestamp=1735689600))
        first = self.fixture.commit("first", timestamp=1735693200)
        second = self.fixture.commit("second", timestamp=1735696800)

        code, out, err = self.plan_cli(unlabelled_github({1: [second], 2: [first]}))

        self.assertEqual(0, code, err)
        lines = out.splitlines()
        self.assertLess(lines.index(f"{first}, PR: 2"), lines.index(f"{second}, PR: 1"))
        self.assertEqual([f"git cherry-pick -x {first}", f"git cherry-pick -x {second}"],
                         [line for line in lines if line.startswith("git cherry-pick")])

    def test_merge_commits_are_cherry_picked_against_their_first_parent(self):
        self.fixture.tag("v2025.2.8.0", self.fixture.commit("base"))
        self.fixture.git("checkout", "--quiet", "-b", "feature")
        feature = self.fixture.commit("feature")
        self.fixture.git("checkout", "--quiet", "main")
        self.fixture.commit("mainline")
        self.fixture.git("merge", "--quiet", "--no-ff", "-m", "merge feature", "feature")
        merge = self.fixture.git("rev-parse", "HEAD")
        github = FakeGitHub([pr(7, "Available: nil")], {7: pr_commits(feature, merge=merge)})

        code, out, err = self.plan_cli(github)

        self.assertEqual(0, code, err)
        self.assertIn(f"git cherry-pick -x -m 1 {merge}", out.splitlines())
        self.assertNotIn(feature, out)

    def test_squash_merge_commits_are_cherry_picked_without_a_mainline(self):
        self.fixture.tag("v2025.2.8.0", self.fixture.commit("base"))
        self.fixture.git("checkout", "--quiet", "-b", "feature")
        feature = self.fixture.commit("feature")
        self.fixture.git("checkout", "--quiet", "main")
        squash = self.fixture.commit("squashed feature")
        github = FakeGitHub([pr(7, "Available: nil")], {7: pr_commits(feature, merge=squash)})

        code, out, err = self.plan_cli(github)

        self.assertEqual(0, code, err)
        self.assertEqual([f"git cherry-pick -x {squash}"],
                         [line for line in out.splitlines() if line.startswith("git cherry-pick")])

    def test_merge_commit_is_ordered_by_its_own_timestamp_not_its_parents(self):
        self.fixture.tag("v2025.2.8.0", self.fixture.commit("base", T0))
        self.fixture.commit("first parent", T0 + DAY)
        self.fixture.git("checkout", "--quiet", "-b", "feature")
        feature = self.fixture.commit("feature", T0 + 2 * DAY)
        self.fixture.git("checkout", "--quiet", "main")
        merged = f"{T0 + 5 * DAY} +0000"
        self.fixture.git("merge", "--quiet", "--no-ff", "-m", "merge feature", "feature",
                         env={"GIT_AUTHOR_DATE": merged, "GIT_COMMITTER_DATE": merged})
        merge = self.fixture.git("rev-parse", "HEAD")
        self.fixture.git("checkout", "--quiet", "-b", "other", "v2025.2.8.0")
        self.fixture.commit("parent", T0 + 3 * DAY)
        other = self.fixture.commit("other", T0 + 4 * DAY)
        github = FakeGitHub([pr(1, "Available: nil"), pr(2, "Available: nil")],
                            {1: pr_commits(feature, merge=merge), 2: pr_commits(other)})

        code, out, err = self.plan_cli(github)

        self.assertEqual(0, code, err)
        self.assertEqual([f"git cherry-pick -x {other}", f"git cherry-pick -x -m 1 {merge}"],
                         [line for line in out.splitlines() if line.startswith("git cherry-pick")])

    def test_squash_merge_after_a_true_merge_is_ordered_after_it_whatever_gh_lists_first(self):
        self.fixture.tag("v2025.2.8.0", self.fixture.commit("base", T0))
        self.fixture.git("checkout", "--quiet", "-b", "feature")
        feature = self.fixture.commit("feature", T0 + DAY)
        self.fixture.git("checkout", "--quiet", "main")
        merged = f"{T0 + 5 * DAY} +0000"
        self.fixture.git("merge", "--quiet", "--no-ff", "-m", "merge feature", "feature",
                         env={"GIT_AUTHOR_DATE": merged, "GIT_COMMITTER_DATE": merged})
        merge = self.fixture.git("rev-parse", "HEAD")
        squash = self.fixture.commit("squashed PR 2", T0 + 10 * DAY)
        github = FakeGitHub([pr(2, "Available: nil"), pr(1, "Available: nil")],
                            {1: pr_commits(feature, merge=merge),
                             2: pr_commits("f" * 40, merge=squash)})

        code, out, err = self.plan_cli(github)

        self.assertEqual(0, code, err)
        self.assertEqual([f"git cherry-pick -x -m 1 {merge}", f"git cherry-pick -x {squash}"],
                         [line for line in out.splitlines() if line.startswith("git cherry-pick")])

    def test_open_pr_commits_are_ordered_by_their_parents_timestamp_not_their_own(self):
        self.fixture.tag("v2025.2.8.0", self.fixture.commit("base", T0))
        self.fixture.git("checkout", "--quiet", "-b", "a")
        self.fixture.commit("parent of a", T0 + DAY)
        a = self.fixture.commit("a", T0 + 4 * DAY)
        self.fixture.git("checkout", "--quiet", "-b", "b", "v2025.2.8.0")
        self.fixture.commit("parent of b", T0 + 2 * DAY)
        b = self.fixture.commit("b", T0 + 3 * DAY)

        code, out, err = self.plan_cli(unlabelled_github({2: [b], 1: [a]}))

        self.assertEqual(0, code, err)
        self.assertEqual([f"git cherry-pick -x {a}", f"git cherry-pick -x {b}"],
                         [line for line in out.splitlines() if line.startswith("git cherry-pick")])

    def test_open_pr_commits_with_the_same_parent_keep_the_listing_order(self):
        self.fixture.tag("v2025.2.8.0", self.fixture.commit("base", T0))
        self.fixture.git("checkout", "--quiet", "-b", "a", "v2025.2.8.0")
        a = self.fixture.commit("a", T0 + 3 * DAY)
        self.fixture.git("checkout", "--quiet", "-b", "b", "v2025.2.8.0")
        b = self.fixture.commit("b", T0 + DAY)

        code, out, err = self.plan_cli(unlabelled_github({2: [a], 1: [b]}))

        self.assertEqual(0, code, err)
        self.assertEqual([f"git cherry-pick -x {a}", f"git cherry-pick -x {b}"],
                         [line for line in out.splitlines() if line.startswith("git cherry-pick")])

    def test_done_detects_a_pr_commit_that_sits_directly_on_the_candidate(self):
        self.fixture.tag("v2025.2.8.0", self.fixture.commit("base"))
        self.fixture.git("checkout", "--quiet", "-b", "2025.2.8.0-shopify-1-candidate")
        direct = self.fixture.commit("direct")
        self.fixture.git("checkout", "--quiet", "main")

        code, out, err = self.plan_cli(unlabelled_github({1: [direct]}))

        self.assertEqual(0, code, err)
        self.assertIn("All commits have been applied.", out)

    def test_last_cherry_pick_trailer_wins_when_a_reused_commit_is_picked_again(self):
        self.fixture.tag("v2025.2.8.0", self.fixture.commit("base"))
        self.fixture.git("checkout", "--quiet", "-b", "source")
        patch_commit = self.fixture.commit("patch")
        self.fixture.git("checkout", "--quiet", "-b", "release", "main")
        self.fixture.git("cherry-pick", "-x", patch_commit)
        reused = self.fixture.git("rev-parse", "HEAD")
        self.fixture.tag("2025.2.7.0-shopify-1")
        self.fixture.git("checkout", "--quiet", "-b", "2025.2.8.0-shopify-1-candidate", "main")
        self.fixture.git("cherry-pick", "-x", reused)
        self.fixture.git("checkout", "--quiet", "main")

        code, out, err = self.plan_cli(unlabelled_github({1: [patch_commit]}))

        self.assertEqual(0, code, err)
        self.assertIn(f"{reused}, PR: 1", out.splitlines())
        self.assertIn("All commits have been applied.", out)

    def test_reuses_the_cherry_pick_recorded_in_the_previous_release(self):
        self.fixture.tag("v2025.2.8.0", self.fixture.commit("base"))
        self.fixture.git("checkout", "--quiet", "-b", "source")
        patch_commit = self.fixture.commit("patch")
        self.fixture.git("checkout", "--quiet", "-b", "release", "main")
        self.fixture.git("cherry-pick", "-x", patch_commit)
        reused = self.fixture.git("rev-parse", "HEAD")
        self.fixture.tag("2025.2.7.0-shopify-1")
        self.fixture.git("checkout", "--quiet", "main")

        code, out, err = self.plan_cli(unlabelled_github({3: [patch_commit]}))

        self.assertEqual(0, code, err)
        lines = out.splitlines()
        self.assertEqual("Using previous Shopify release: 2025.2.7.0-shopify-1", lines[0])
        self.assertIn(f"git cherry-pick -x {reused}", lines)

    def test_applied_commits_out_of_order_are_an_error(self):
        a, b = Commit("aaa", 1, 0, 0, False, False), Commit("bbb", 2, 0, 0, False, False)

        with self.assertRaises(Error) as caught:
            CherryPickList.remaining([a, b], [b])

        self.assertEqual("bbb unexpected, aaa for PR 1 expected", str(caught.exception))
        self.assertEqual([b], CherryPickList.remaining([a, b], [a]))


if __name__ == "__main__":
    unittest.main()
