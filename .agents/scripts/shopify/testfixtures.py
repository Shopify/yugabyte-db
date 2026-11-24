"""Shared test fixtures: throwaway git repositories under a temporary directory.

Tests never read or write a real checkout and never call GitHub or the network.
"""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch


GIT_ENV = {
    "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_SYSTEM": os.devnull,
    "GIT_AUTHOR_NAME": "Test", "GIT_AUTHOR_EMAIL": "test@example.invalid",
    "GIT_COMMITTER_NAME": "Test", "GIT_COMMITTER_EMAIL": "test@example.invalid",
}


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

    def commit(self, message=None, timestamp=None, content=None):
        self.counter += 1
        Path(self.path, f"file{self.counter}.txt").write_text(
            content or f"content {self.counter}\n")
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
        self.ref(f"refs/remotes/origin/{name}", self.git("rev-parse", ref))

    def ref(self, name, sha):
        """Points the full ref `name` (such as refs/remotes/upstream/master) at `sha`."""
        self.git("update-ref", name, sha)

    def init_bare(self, bare_path):
        self.git("init", "--quiet", "--bare", bare_path)

    def tags_list(self):
        tags = self.git("tag")
        return tags.split("\n") if tags else []

    def refs(self):
        return self.git("for-each-ref", "--format=%(refname) %(objectname)")


class RepoTestCase(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="yb-builds-test")
        self.addCleanup(shutil.rmtree, self.tmpdir)
        environment = patch.dict(os.environ, GIT_ENV)
        environment.start()
        self.addCleanup(environment.stop)
        self.fixture = GitFixture(os.path.join(self.tmpdir, "yugabyte-db"))
