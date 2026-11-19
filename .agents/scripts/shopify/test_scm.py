"""Exercise the shared git and gh plumbing against throwaway repositories and executables.

Tests never read or write a real checkout and never call GitHub or the network.
"""

import os
import shutil
import tempfile
import unittest
from unittest.mock import patch

from scm import ArgumentParser, Error, GitError, GitHub, Repository, UsageError
from testfixtures import RepoTestCase


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

    def test_a_missing_git_binary_is_a_clear_error(self):
        empty = os.path.join(self.tmpdir, "empty-path")
        os.mkdir(empty)

        with patch.dict(os.environ, {"PATH": empty}):
            self.assert_git_error("git was not found; install git", self.fixture.path)

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

    def test_first_parent_commits_list_the_first_parent_line_oldest_first(self):
        base = self.fixture.commit("base")
        self.fixture.git("checkout", "--quiet", "-b", "side")
        side = self.fixture.commit("side")
        self.fixture.git("checkout", "--quiet", "main")
        mainline = self.fixture.commit("mainline", 1793534400)
        self.fixture.git("merge", "--quiet", "--no-ff", "-m", "merge side", "side")
        merge = self.fixture.git("rev-parse", "HEAD")
        self.fixture.ref("refs/remotes/upstream/master", side)
        repo = Repository(self.fixture.path)

        commits = repo.first_parent_commits(base, "main")

        self.assertEqual(base, repo.merge_base("refs/remotes/upstream/master", mainline))
        self.assertEqual([(mainline, [base], "mainline"), (merge, [mainline, side], "merge side")],
                         [(c["sha"], c["parents"], c["subject"]) for c in commits])
        # Older git prints +00:00 for UTC and newer git prints Z.
        self.assertIn(commits[0]["date"], ("2026-11-01T12:00:00+00:00", "2026-11-01T12:00:00Z"))
        self.assertEqual(repo.tree(merge), commits[1]["tree"])

    def test_fetch_updates_the_remote_tracking_ref_from_a_local_remote(self):
        remote = os.path.join(self.tmpdir, "origin.git")
        self.fixture.init_bare(remote)
        self.fixture.commit("base")
        self.fixture.git("remote", "add", "origin", remote)
        self.fixture.git("push", "--quiet", "origin", "main")
        pushed = self.fixture.commit("later")
        self.fixture.git("push", "--quiet", "origin", "main")
        self.fixture.git("update-ref", "-d", "refs/remotes/origin/main")
        repo = Repository(self.fixture.path)

        repo.fetch("origin", "main")

        self.assertEqual(pushed, self.fixture.git("rev-parse", "refs/remotes/origin/main"))


class GitHubTest(RepoTestCase):
    def test_missing_gh_binary_is_a_clear_error(self):
        executable = os.path.join(self.tmpdir, "no-such-gh")

        with self.assertRaises(Error) as caught:
            GitHub(executable=executable).run("pr", "list", "--repo", "Shopify/yugabyte-db")

        self.assertEqual(
            f"The gh CLI was not found ({executable}); install gh and run `gh auth login`",
            str(caught.exception))

    def test_json_parses_the_output_and_run_returns_it_raw(self):
        executable = os.path.join(self.tmpdir, "echo-gh")
        with open(executable, "w", encoding="utf-8") as script:
            script.write("#!/bin/sh\nprintf '%s\\n' \"$@\"\n")
        os.chmod(executable, 0o755)
        github = GitHub(executable=executable)

        self.assertEqual("one\ntwo words; true\n", github.run("one", "two words; true"))
        self.assertEqual({"number": 7}, github.json('{"number": 7}'))

    def test_a_failing_command_reports_its_stderr(self):
        executable = os.path.join(self.tmpdir, "failing-gh")
        with open(executable, "w", encoding="utf-8") as script:
            script.write("#!/bin/sh\necho 'HTTP 404: Not Found' >&2\nexit 1\n")
        os.chmod(executable, 0o755)

        with self.assertRaises(Error) as caught:
            GitHub(executable=executable).json("pr", "view", "999")

        self.assertEqual(f"Failed to run `{executable} pr view 999`\nHTTP 404: Not Found\n",
                         str(caught.exception))

    def test_output_that_is_not_json_is_a_clear_error(self):
        executable = os.path.join(self.tmpdir, "echo-gh")
        with open(executable, "w", encoding="utf-8") as script:
            script.write("#!/bin/sh\nprintf '%s\\n' \"$@\"\n")
        os.chmod(executable, 0o755)

        with self.assertRaises(Error) as caught:
            GitHub(executable=executable).json("not json")

        self.assertEqual(f"Failed to parse the output of `{executable} not json`",
                         str(caught.exception))


class ArgumentParserTest(unittest.TestCase):
    def test_argument_errors_raise_usage_errors_instead_of_exiting(self):
        parser = ArgumentParser(prog="x", add_help=False)
        parser.add_argument("--version")

        with self.assertRaises(UsageError) as caught:
            parser.parse_args(["--bogus"])

        self.assertEqual("unrecognized arguments: --bogus", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
