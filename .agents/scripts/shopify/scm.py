"""Shared git and gh plumbing for the Shopify contribution tools.

Every command runs as an argv list, never via a shell. Requires Python 3.10+; importing has no
side effects.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess


OID = re.compile(r"[0-9a-f]{40}")


class Error(Exception):
    pass


class GitError(Error):
    pass


class UsageError(Error):
    pass


class Repository:
    """Runs git against one explicit path as `git -C <path>` argv lists, never via a shell."""

    def __init__(self, path):
        if path is None or str(path) == "":
            raise GitError("A repository path is required")
        self.path = os.path.abspath(os.path.expanduser(str(path)))
        if not os.path.isdir(self.path):
            raise GitError(f"Repository path {self.path} is not a directory")
        if self._git("rev-parse", "--is-inside-work-tree").stdout.strip() != "true":
            raise GitError(f"Repository path {self.path} is not a git repository")

    def _git(self, *args):
        try:
            return subprocess.run(["git", "-C", self.path, *args], capture_output=True,
                                  encoding="utf-8", errors="replace")
        except FileNotFoundError as error:
            raise GitError("git was not found; install git") from error

    def _run(self, *args):
        result = self._git(*args)
        if result.returncode:
            raise GitError(f"Failed to run `git {' '.join(args)}` in {self.path}\n{result.stderr}")
        return result.stdout

    def _succeeds(self, *args):
        return self._git(*args).returncode == 0

    def _lines(self, *args):
        return [line for line in self._run(*args).split("\n") if line.strip()]

    def _log_shas(self, *args):
        return [line.split()[0] for line in self._lines(
            "log", "--oneline", "--no-abbrev-commit", *args)]

    def tags(self):
        return self._lines("tag")

    def local_branches(self):
        return self._lines("for-each-ref", "--format=%(refname:short)", "refs/heads")

    def origin_branches(self):
        prefix = "refs/remotes/origin/"
        names = (ref.removeprefix(prefix)
                 for ref in self._lines("for-each-ref", "--format=%(refname)", prefix))
        return [name for name in names if name != "HEAD"]

    def tag_exists(self, name):
        return self._succeeds("show-ref", "--verify", "--quiet", f"refs/tags/{name}")

    def branch_exists(self, name):
        return self._succeeds("show-ref", "--verify", "--quiet", f"refs/heads/{name}")

    def origin_branch_exists(self, name):
        return self._succeeds("show-ref", "--verify", "--quiet", f"refs/remotes/origin/{name}")

    def commit_for(self, ref):
        """Resolves a ref to the commit it ultimately points at (peels annotated tags)."""
        return self._run("rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}").strip()

    def ancestor(self, ancestor, descendant):
        return self._succeeds("merge-base", "--is-ancestor", ancestor, descendant)

    def parent_commit(self, commit):
        return self._run("rev-parse", f"{commit}^").strip()

    def parent_count(self, commit):
        return len(self._run("rev-list", "--parents", "-n", "1", commit).split()) - 1

    def commit_timestamp(self, commit):
        return int(self._run("show", "--no-patch", "--format=%ct", commit).strip())

    def commits_between(self, from_rev, to_rev):
        return self._log_shas(f"{from_rev}..{to_rev}")

    def grep_commits_on_branch(self, branch, pattern):
        return self._log_shas("--grep", pattern, branch)

    def cherry_picked_commits_between(self, from_rev, to_rev):
        return self._log_shas("--grep", "cherry picked from commit", f"{from_rev}..{to_rev}")

    def message(self, commit):
        return self._run("show", "--no-patch", "--format=%B", commit)

    def merge_base(self, one, other):
        return self._run("merge-base", one, other).strip()

    def tree(self, commit):
        return self._run("rev-parse", f"{commit}^{{tree}}").strip()

    def first_parent_commits(self, from_rev, to_rev):
        """Oldest first, the first-parent line of from_rev..to_rev as dicts with sha, parents,
        tree, date (committer, ISO 8601), subject and message."""
        fields = ("sha", "parents", "tree", "date", "subject", "message")
        output = self._run("log", "--first-parent", "--reverse", "-z",
                           "--format=%H%x1f%P%x1f%T%x1f%cI%x1f%s%x1f%B", f"{from_rev}..{to_rev}")
        commits = [dict(zip(fields, record.split("\x1f", 5))) for record in output.split("\0")
                   if record]
        for commit in commits:
            commit["parents"] = commit["parents"].split()
        return commits

    def matching_merges(self, to_rev, exclude, pattern):
        """Oldest first, the merge commits reachable from to_rev but not exclude with a message
        line matching the extended regex `pattern`, as dicts with sha, date (committer, ISO 8601)
        and message."""
        fields = ("sha", "date", "message")
        output = self._run("log", "--merges", "--extended-regexp", f"--grep={pattern}",
                           "--reverse", "-z", "--format=%H%x1f%cI%x1f%B", to_rev, f"^{exclude}")
        return [dict(zip(fields, record.split("\x1f", 2))) for record in output.split("\0")
                if record]

    def fetch(self, *args):
        return self._run("fetch", *args)

    def fetch_all_with_tags(self):
        return self._run("fetch", "--all", "--tags")


class GitHub:
    """Runs the gh CLI as argv lists, never via a shell."""

    def __init__(self, executable="gh"):
        self.executable = executable

    def run(self, *args):
        """Returns the command's stdout."""
        command = [self.executable, *args]
        try:
            result = subprocess.run(command, capture_output=True, encoding="utf-8")
        except FileNotFoundError as error:
            raise Error(f"The gh CLI was not found ({self.executable}); "
                        "install gh and run `gh auth login`") from error
        if result.returncode:
            raise Error(f"Failed to run `{' '.join(command)}`\n{result.stderr}")
        return result.stdout

    def json(self, *args):
        """Returns the command's stdout parsed as JSON."""
        output = self.run(*args)
        try:
            return json.loads(output)
        except json.JSONDecodeError as error:
            command = [self.executable, *args]
            raise Error(f"Failed to parse the output of `{' '.join(command)}`") from error


class ArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        raise UsageError(message)
