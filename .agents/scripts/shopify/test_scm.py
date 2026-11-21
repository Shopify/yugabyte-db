"""Exercise the shared git and gh plumbing against throwaway repositories and executables.

Tests never read or write a real checkout and never call GitHub or the network.
"""

import io
import os
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

from scm import ArgumentParser, Error, GitError, GitHub, Repository, Tool, UsageError
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
        mainline = self.fixture.commit("mainline\n\nShopify-PR: #1 #2", 1793534400)
        self.fixture.git("merge", "--quiet", "--no-ff", "-m", "merge side", "side")
        merge = self.fixture.git("rev-parse", "HEAD")
        self.fixture.ref("refs/remotes/upstream/master", side)
        repo = Repository(self.fixture.path)

        commits = repo.first_parent_commits(base, "main")

        self.assertEqual(base, repo.merge_base("refs/remotes/upstream/master", mainline))
        self.assertEqual([(mainline, [base], "mainline", "mainline\n\nShopify-PR: #1 #2\n"),
                          (merge, [mainline, side], "merge side", "merge side\n")],
                         [(c["sha"], c["parents"], c["subject"], c["message"]) for c in commits])
        # Older git prints +00:00 for UTC and newer git prints Z.
        self.assertIn(commits[0]["date"], ("2026-11-01T12:00:00+00:00", "2026-11-01T12:00:00Z"))
        self.assertEqual(repo.tree(merge), commits[1]["tree"])

    def test_matching_merges_lists_merges_off_the_excluded_ref_whose_message_matches(self):
        base = self.fixture.commit("base", 1793534400)
        self.fixture.git("checkout", "--quiet", "-b", "side")
        self.fixture.commit("side change\n\nDropped: #1 (upstream #10)", 1793534400)
        self.fixture.git("checkout", "--quiet", "main")
        self.fixture.git("merge", "--quiet", "--no-ff", "-m", "plain merge", "side")
        self.fixture.git("checkout", "--quiet", "-b", "older", base)
        self.fixture.commit("older change", 1793534400)
        self.fixture.git("checkout", "--quiet", "main")
        dated = {"GIT_COMMITTER_DATE": "1793620800 +0000"}
        self.fixture.git("merge", "--quiet", "--no-ff", "-s", "ours", "-m",
                         "sync\n\nDropped: #2 (upstream #20)", "older", env=dated)
        sync = self.fixture.git("rev-parse", "HEAD")
        repo = Repository(self.fixture.path)

        merges = repo.matching_merges("main", base, "^Dropped: #")

        self.assertEqual([(sync, "sync\n\nDropped: #2 (upstream #20)\n")],
                         [(m["sha"], m["message"]) for m in merges])
        # Older git prints +00:00 for UTC and newer git prints Z.
        self.assertIn(merges[0]["date"], ("2026-11-02T12:00:00+00:00", "2026-11-02T12:00:00Z"))
        self.assertEqual([], repo.matching_merges("main", sync, "^Dropped: #"))
        self.assertEqual([], repo.matching_merges("main", base, "^Dropped: #3"))

    def test_fetch_updates_the_remote_tracking_ref_from_a_local_remote(self):
        remote = os.path.join(self.tmpdir, "origin.git")
        self.fixture.init_bare(remote)
        self.fixture.commit("base")
        self.fixture.git("remote", "add", "origin", remote)
        self.fixture.git("push", "--quiet", "origin", "main")
        pushed = self.fixture.commit("later")
        self.fixture.git("push", "--quiet", "origin", "main")
        self.fixture.git("update-ref", "-d", "refs/remotes/origin/main")
        # A single-branch clone's mapping excludes main; an explicit refspec still updates it.
        self.fixture.git("config", "remote.origin.fetch",
                         "+refs/heads/other:refs/remotes/origin/other")
        repo = Repository(self.fixture.path)

        repo.fetch("origin", "+refs/heads/main:refs/remotes/origin/main")

        self.assertEqual(pushed, self.fixture.git("rev-parse", "refs/remotes/origin/main"))


class RepositoryVerbTest(RepoTestCase):
    def write(self, files, message):
        for name, content in files.items():
            path = Path(self.fixture.path, name)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content)
        self.fixture.git("add", "-A")
        self.fixture.git("commit", "--quiet", "-m", message)
        return self.fixture.git("rev-parse", "HEAD")

    def test_clean_ignores_untracked_files_but_not_tracked_changes(self):
        self.write({"a.txt": "a\n"}, "add a")
        repo = Repository(self.fixture.path)
        Path(self.fixture.path, "untracked.txt").write_text("new\n")

        self.assertTrue(repo.clean())
        Path(self.fixture.path, "a.txt").write_text("changed\n")
        self.assertFalse(repo.clean())

    def test_cherry_pick_records_its_origin_and_amend_message_replaces_the_message(self):
        base = self.write({"a.txt": "a\n"}, "base")
        self.fixture.git("checkout", "--quiet", "-b", "side")
        picked = self.write({"b.txt": "b\n"}, "Add b (#7)")
        self.fixture.git("checkout", "--quiet", "main")
        repo = Repository(self.fixture.path)
        message = Path(self.tmpdir, "message.txt")
        message.write_text("New subject\n\nNew body\n")

        repo.cherry_pick(picked)
        self.assertEqual(f"Add b (#7)\n\n(cherry picked from commit {picked})",
                         repo.message("HEAD").rstrip("\n"))
        repo.amend_message(str(message))

        self.assertEqual("New subject\n\nNew body", repo.message("HEAD").rstrip("\n"))
        self.assertEqual(base, repo.parent_commit("HEAD"))

    def test_amend_message_keeps_the_message_verbatim_whatever_commit_cleanup_says(self):
        self.write({"a.txt": "a\n"}, "base")
        self.fixture.git("config", "commit.cleanup", "strip")
        message = Path(self.tmpdir, "message.txt")
        message.write_text("Subject\n\n#12 starts this line\n")

        Repository(self.fixture.path).amend_message(str(message))

        self.assertEqual("Subject\n\n#12 starts this line\n",
                         self.fixture.git("show", "--no-patch", "--format=%B", "HEAD") + "\n")

    def test_amend_message_changes_only_the_message_and_leaves_staged_edits_staged(self):
        self.write({"a.txt": "a\n", "b.txt": "b\n"}, "base")
        tree = self.fixture.git("rev-parse", "HEAD^{tree}")
        Path(self.fixture.path, "b.txt").write_text("staged\n")
        self.fixture.git("add", "b.txt")
        message = Path(self.tmpdir, "message.txt")
        message.write_text("Reworded\n")

        Repository(self.fixture.path).amend_message(str(message))

        self.assertEqual(tree, self.fixture.git("rev-parse", "HEAD^{tree}"))
        self.assertEqual("M  b.txt", self.fixture.git("status", "--porcelain"))
        self.assertEqual("Reworded", self.fixture.git("log", "-1", "--format=%B"))

    def test_commits_excluding_lists_what_none_of_the_refs_reach(self):
        base = self.write({"a.txt": "a\n"}, "base")
        self.fixture.git("checkout", "--quiet", "-b", "one")
        one = self.write({"b.txt": "b\n"}, "one")
        self.fixture.git("checkout", "--quiet", "-b", "top")
        top = self.write({"c.txt": "c\n"}, "top")
        repo = Repository(self.fixture.path)

        self.assertEqual([top], repo.commits_excluding("top", base, one))
        self.assertEqual([top, one], repo.commits_excluding("top", base))

    def test_a_conflicting_cherry_pick_raises_and_abort_restores_the_branch(self):
        self.write({"a.txt": "a\n"}, "base")
        self.fixture.git("checkout", "--quiet", "-b", "side")
        picked = self.write({"a.txt": "side\n"}, "side")
        self.fixture.git("checkout", "--quiet", "main")
        head = self.write({"a.txt": "main\n"}, "main")
        repo = Repository(self.fixture.path)

        with self.assertRaises(GitError):
            repo.cherry_pick(picked)
        repo.cherry_pick_abort()

        self.assertEqual((head, True), (repo.commit_for("HEAD"), repo.clean()))

    def test_changed_paths_and_patch_id_compare_commits(self):
        base = self.write({"a.txt": "a\n", "b.txt": "b\n"}, "base")
        change = self.write({"a.txt": "a2\n", "src/c.proto": "c\n"}, "change")
        self.fixture.git("checkout", "--quiet", "-b", "copy", base)
        copy = self.write({"a.txt": "a2\n", "src/c.proto": "c\n"}, "copy")
        other = self.write({"b.txt": "b2\n"}, "other")
        repo = Repository(self.fixture.path)

        self.assertEqual(["a.txt", "src/c.proto"], repo.changed_paths(base, change))
        self.assertEqual(repo.patch_id(change), repo.patch_id(copy))
        self.assertNotEqual(repo.patch_id(change), repo.patch_id(other))
        self.assertRegex(repo.patch_id(change), r"^[0-9a-f]{40}$")

    def test_remotes_their_urls_and_config(self):
        self.fixture.commit()
        self.fixture.git("remote", "add", "upstream", "https://example.invalid/up.git")
        repo = Repository(self.fixture.path)

        repo.set_config("gh-stack.remote", "upstream")

        self.assertEqual(["upstream"], repo.remotes())
        self.assertEqual("https://example.invalid/up.git", repo.remote_url("upstream"))
        self.assertEqual("upstream", repo.config("gh-stack.remote"))
        self.assertIsNone(repo.config("gh-stack.missing"))

    def test_worktree_root_is_the_top_level_directory_of_a_subdirectory(self):
        self.write({"src/a.txt": "a\n"}, "base")

        root = Repository(os.path.join(self.fixture.path, "src")).worktree_root()

        self.assertEqual(os.path.realpath(self.fixture.path), os.path.realpath(root))

    def test_create_branch_does_not_track_its_start_point(self):
        first = self.fixture.commit()
        self.fixture.remote_branch("feature")
        repo = Repository(self.fixture.path)

        repo.create_branch("feature", "refs/remotes/origin/feature")

        self.assertEqual(first, repo.commit_for("feature"))
        self.assertIsNone(repo.config("branch.feature.remote"))


class ToolTest(RepoTestCase):
    def script(self, name, body):
        executable = os.path.join(self.tmpdir, name)
        with open(executable, "w", encoding="utf-8") as script:
            script.write("#!/bin/sh\n" + body)
        os.chmod(executable, 0o755)
        return executable

    def test_run_returns_output_and_passes_arguments_without_a_shell(self):
        echo = self.script("echo-args", "pwd\nprintf '%s\\n' \"$@\"\n")
        marker = os.path.join(self.tmpdir, "injected")

        result = Tool().run([echo, "two words", f"; touch {marker}"], cwd=self.fixture.path)

        self.assertEqual(0, result.returncode)
        self.assertEqual([os.path.realpath(self.fixture.path), "two words", f"; touch {marker}"],
                         result.stdout.splitlines())
        self.assertFalse(os.path.exists(marker))

    def test_a_failing_command_raises_with_its_stderr_unless_check_is_false(self):
        failing = self.script("failing", "echo out\necho 'it broke' >&2\nexit 3\n")

        with self.assertRaises(Error) as caught:
            Tool().run([failing, "x"])
        result = Tool().run([failing], check=False)

        self.assertEqual(f"Failed to run `{failing} x`\nit broke\n", str(caught.exception))
        self.assertEqual((3, "out\n", "it broke\n"),
                         (result.returncode, result.stdout, result.stderr))

    def test_echo_copies_standard_output_while_collecting_it(self):
        lines = self.script("lines", "echo one\necho two\nexit 5\n")
        echoed = io.StringIO()

        result = Tool().run([lines], echo=echoed, check=False)

        self.assertEqual((5, "one\ntwo\n", "one\ntwo\n"),
                         (result.returncode, result.stdout, echoed.getvalue()))

    def test_a_missing_executable_is_a_clear_error(self):
        missing = os.path.join(self.tmpdir, "no-such-tool")

        for call in (lambda: Tool().run([missing]), lambda: Tool().interactive([missing])):
            with self.subTest(call=call), self.assertRaises(Error) as caught:
                call()
            self.assertEqual(f"{missing} was not found", str(caught.exception))

    def test_interactive_returns_the_exit_code(self):
        self.assertEqual(4, Tool().interactive([self.script("four", "exit 4\n")]))


class GitHubTest(RepoTestCase):
    def test_run_can_run_in_a_given_directory(self):
        executable = os.path.join(self.tmpdir, "pwd-gh")
        with open(executable, "w", encoding="utf-8") as script:
            script.write("#!/bin/sh\npwd\n")
        os.chmod(executable, 0o755)

        output = GitHub(executable=executable).run("repo", "set-default", "--view",
                                                   cwd=self.fixture.path)

        self.assertEqual(os.path.realpath(self.fixture.path) + "\n", output)

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
