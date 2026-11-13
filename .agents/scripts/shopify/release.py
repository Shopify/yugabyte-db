#!/usr/bin/env python3
"""List the Shopify patches to cherry-pick onto an upstream YugabyteDB release tag.

Prints the git commands that create or resume `<version>-shopify-<sequence>-candidate`.
Uses git and gh; requires Python 3.10+. Importing has no side effects, and planning never
writes refs: it only reads through gh and, unless --no-fetch, runs `git fetch --all --tags`.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import functools
import json
import os
import re
import shlex
import subprocess
import sys


GITHUB_REPO = "Shopify/yugabyte-db"
RELEASE_NAME = re.compile(
    r"(?P<version>[0-9]+\.[0-9]+\.[0-9]+(?:\.[0-9]+)?)-shopify-(?P<sequence>[0-9]+)")
CHERRY_PICKED = re.compile(r"\(cherry picked from commit ([a-f0-9]+)\)")


class Error(Exception):
    pass


class GitError(Error):
    pass


class UsageError(Error):
    pass


@functools.total_ordering
class UpstreamVersion:
    """An upstream release such as 2025.1.2 or 2025.2.8.0; the series is its first two parts."""

    PATTERN = re.compile(
        r"v?(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:\.(0|[1-9][0-9]*))?")

    def __init__(self, parts):
        self.parts = tuple(parts)
        # Without the `v` prefix, as used in Shopify candidate and release names.
        self.full = ".".join(map(str, self.parts))
        self.series = ".".join(map(str, self.parts[:2]))
        self.tag = "v" + self.full
        self.padded = self.parts + (0,) * (4 - len(self.parts))
        # True for the first release of a series (2025.1.0 or 2025.1.0.0).
        self.series_initial = not any(self.parts[2:])

    @classmethod
    def parse(cls, text):
        text = "" if text is None else str(text)
        match = cls.PATTERN.fullmatch(text)
        if not match:
            raise Error(f"Invalid upstream version {text!r}: expected three or four numeric parts "
                        "without leading zeros, such as 2025.1.2 or v2025.2.8.0")
        return cls(int(part) for part in match.groups() if part is not None)

    def same_series(self, other):
        return self.parts[:2] == other.parts[:2]

    def __eq__(self, other):
        if not isinstance(other, UpstreamVersion):
            return NotImplemented
        return self.padded == other.padded

    def __hash__(self):
        return hash(self.padded)

    def __lt__(self, other):
        if not isinstance(other, UpstreamVersion):
            return NotImplemented
        return self.padded < other.padded


class AvailabilityLabels:
    """Legacy `Available: <version>` PR labels: a hint, never verified coverage."""

    LABEL = re.compile(r"Available:\s*(.*?)\s*", re.ASCII)

    @dataclass
    class Entry:
        number: int
        versions: list
        unknown: bool
        unrecognized: list
        status: str
        verified: bool = False

    @classmethod
    def inspect_pull_request(cls, pull_request, target):
        """Returns None when the pull request has no `Available:` label."""
        values = []
        for label in pull_request.get("labels") or []:
            match = cls.LABEL.fullmatch(str(label.get("name") or ""))
            if match:
                values.append(match.group(1))
        if not values:
            return None
        versions, unrecognized, unknown = [], [], False
        for value in values:
            if not value or value.casefold() == "nil":
                unknown = True
                continue
            try:
                versions.append(UpstreamVersion.parse(value))
            except Error:
                unrecognized.append(value)
                unknown = True
        return cls.Entry(pull_request.get("number"), sorted(versions), unknown, unrecognized,
                         cls._status(versions, unknown, target))

    @classmethod
    def inspect_all(cls, pull_requests, target):
        entries = (cls.inspect_pull_request(pr, target) for pr in pull_requests)
        return [entry for entry in entries if entry is not None]

    @staticmethod
    def selected_numbers(entries):
        """Pull requests to list: everything the labels do not say is already covered."""
        return [entry.number for entry in entries if entry.status != "covered_by_label"]

    @staticmethod
    def _status(versions, unknown, target):
        # A label covers the target when the target is at or after the labelled release and
        # either shares its series or the labelled release is the first of its series (later
        # series branch from, and so contain, that work).
        if any(target >= version and (version.same_series(target) or version.series_initial)
               for version in versions):
            return "covered_by_label"
        if not versions and unknown:
            return "unknown_availability"
        return "needed_by_label"


@dataclass
class Plan:
    upstream_version: str
    series: str
    upstream_tag: str
    upstream_commit: str
    sequence: int
    candidate_branch: str
    candidate_ref: str | None
    release_name: str
    previous_release: str | None
    label_inspection: list
    cherry_pick_prs: list
    repo_path: str

    @property
    def candidate_exists(self):
        return self.candidate_ref is not None


class Planner:
    """Builds a Plan from read-only repository inspection; never fetches or touches refs."""

    def __init__(self, repo):
        self.repo = repo

    def plan(self, version, pull_requests, target_branch=None, previous_release=None):
        upstream = UpstreamVersion.parse(version)
        upstream_tag = self._resolve_upstream_tag(upstream)
        upstream_commit = self.repo.commit_for(upstream_tag)
        if target_branch is None:
            sequence = self._next_sequence(upstream)
        else:
            sequence = self._sequence_from_target(upstream, target_branch)
        release_name = f"{upstream.full}-shopify-{sequence}"
        if sequence in self._release_refs(upstream):
            raise Error(f"Release {release_name} already exists and is immutable; "
                        "choose a new sequence")
        candidate_branch = release_name + "-candidate"
        candidate_ref = None
        if self.repo.branch_exists(candidate_branch):
            candidate_ref = candidate_branch
        elif self.repo.origin_branch_exists(candidate_branch):
            candidate_ref = "origin/" + candidate_branch
        if candidate_ref is not None and not self.repo.ancestor(upstream_commit, candidate_ref):
            raise Error(f"Candidate branch {candidate_branch} conflicts with upstream tag "
                        f"{upstream_tag}: it is not based on that tag")
        if previous_release is None:
            previous_release = self._find_previous_release(upstream)
        elif not RELEASE_NAME.fullmatch(previous_release):
            raise Error(f"Invalid previous release {previous_release!r}: "
                        "expected <version>-shopify-<sequence>")
        entries = AvailabilityLabels.inspect_all(pull_requests, upstream)
        return Plan(upstream.full, upstream.series, upstream_tag, upstream_commit, sequence,
                    candidate_branch, candidate_ref, release_name, previous_release, entries,
                    AvailabilityLabels.selected_numbers(entries), self.repo.path)

    def _resolve_upstream_tag(self, upstream):
        for name in (upstream.tag, upstream.full):
            if self.repo.tag_exists(name):
                return name
        raise Error(f"Upstream tag {upstream.tag} (or {upstream.full}) not found in "
                    f"{self.repo.path}")

    def _release_refs(self, upstream):
        """sequence -> ref, for release tags and branches of exactly this upstream version."""
        found = {}
        for ref in self.repo.tags() + self.repo.origin_branches() + self.repo.local_branches():
            match = RELEASE_NAME.fullmatch(ref)
            if match and match["version"] == upstream.full:
                found[int(match["sequence"])] = ref
        return found

    @staticmethod
    def _candidate_pattern(upstream):
        return re.compile(re.escape(upstream.full) + r"-shopify-([0-9]+)-candidate")

    def _next_sequence(self, upstream):
        released = list(self._release_refs(upstream))
        pattern = self._candidate_pattern(upstream)
        candidates = [int(match[1]) for match in map(
            pattern.fullmatch, self.repo.local_branches() + self.repo.origin_branches()) if match]
        if not released and not candidates:
            return 1
        highest = max(released + candidates)
        return highest + 1 if highest in released else highest

    def _sequence_from_target(self, upstream, target_branch):
        match = self._candidate_pattern(upstream).fullmatch(target_branch)
        if not match:
            raise Error(f"Target branch {target_branch!r} must be named "
                        f"{upstream.full}-shopify-<sequence>-candidate")
        return int(match[1])

    def _find_previous_release(self, upstream):
        """Highest Shopify release (tag or origin branch) of a strictly lower upstream version."""
        ceiling = upstream.padded + (0,)
        releases = []
        for ref in dict.fromkeys(self.repo.tags() + self.repo.origin_branches()):
            match = RELEASE_NAME.fullmatch(ref)
            if match:
                key = (UpstreamVersion.parse(match["version"]).padded
                       + (int(match["sequence"]),))
                if key < ceiling:
                    releases.append((key, ref))
        return max(releases, key=lambda release: release[0])[1] if releases else None


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
        return subprocess.run(["git", "-C", self.path, *args], capture_output=True,
                              encoding="utf-8", errors="replace")

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

    def show(self, commit):
        return self._run("show", commit)

    def fetch_all_with_tags(self):
        return self._run("fetch", "--all", "--tags")


class GitHub:
    """gh CLI access; only the command line entry point creates one."""

    def __init__(self, executable="gh"):
        self.executable = executable

    def pull_requests(self, repo):
        return self._run("pr", "list", "--repo", repo, "-L", "5000", "--state", "all",
                         "--json", "number,labels")

    def commits_in_pr(self, repo, number):
        return self._run("pr", "view", "--repo", repo, "--json", "commits,mergeCommit",
                         str(number))

    def _run(self, *args):
        command = [self.executable, *args]
        try:
            result = subprocess.run(command, capture_output=True, encoding="utf-8")
        except FileNotFoundError as error:
            raise Error(f"The gh CLI was not found ({self.executable}); "
                        "install gh and run `gh auth login`") from error
        if result.returncode:
            raise Error(f"Failed to run `{' '.join(command)}`\n{result.stderr}")
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError as error:
            raise Error(f"Failed to parse the output of `{' '.join(command)}`") from error


@dataclass
class Commit:
    commit: str
    pr: int
    timestamp: int
    parent_timestamp: int
    merge_commit: bool  # more than one parent: cherry-pick with -m 1
    merged: bool  # the PR's landed mergeCommit: ordered by its own timestamp


class CherryPickList:
    """Turns a plan's selected PRs into ordered commits and the cherry-picks still to run."""

    def __init__(self, repo, github, github_repo):
        self.repo = repo
        self.github = github
        self.github_repo = github_repo

    def commits(self, plan):
        found = [c for number in plan.cherry_pick_prs for c in self._commits_for_pr(number)]
        # sorted() is stable, so input order breaks ties.
        ordered = sorted(found, key=lambda c: c.timestamp if c.merged else c.parent_timestamp)
        if plan.previous_release is None:
            return ordered
        previous_ref = self._previous_release_ref(plan.previous_release)
        for c in ordered:
            # Reuse a cherry-pick from the previous release to keep any conflict resolution
            # that was done there.
            reused = self.repo.grep_commits_on_branch(
                previous_ref, f"cherry picked from commit {c.commit}")
            if reused:
                c.commit = reused[0]
        return ordered

    def done(self, plan, commits):
        if not plan.candidate_exists:
            return []
        picked = set(self._already_cherry_picked(plan))
        committed = set(self.repo.commits_between(plan.upstream_tag, plan.candidate_ref))
        return [c for c in commits if c.commit in picked or c.commit in committed]

    @staticmethod
    def remaining(commits, completed):
        for index, c in enumerate(completed):
            expected = commits[index]
            if expected.commit != c.commit:
                raise Error(f"{c.commit} unexpected, {expected.commit} for PR {expected.pr} "
                            "expected")
        return commits[len(completed):]

    def _commits_for_pr(self, number):
        info = self.github.commits_in_pr(self.github_repo, number)
        merge = info.get("mergeCommit")
        # A landed PR is its mergeCommit alone; a squash merge's is single-parent and needs no
        # -m 1. For a rebase merge gh reports only the last rebased commit, so the PR's other
        # commits are not listed (as in the Ruby).
        return [Commit(oid, number, self.repo.commit_timestamp(oid),
                       self.repo.commit_timestamp(self.repo.parent_commit(oid)),
                       merge_commit=self.repo.parent_count(oid) > 1, merged=merge is not None)
                for oid in (entry["oid"] for entry in
                            ([merge] if merge is not None else info["commits"]))]

    def _previous_release_ref(self, previous_release):
        if self.repo.origin_branch_exists(previous_release):
            return "origin/" + previous_release
        if self.repo.tag_exists(previous_release):
            return "refs/tags/" + previous_release
        raise Error(f"Previous release {previous_release} not found as an origin branch or tag "
                    f"in {self.repo.path}")

    def _already_cherry_picked(self, plan):
        found = []
        for sha in self.repo.cherry_picked_commits_between(plan.upstream_tag, plan.candidate_ref):
            matches = [m for m in map(CHERRY_PICKED.search, self.repo.show(sha).split("\n")) if m]
            if matches:
                found.append(matches[-1][1])
        return found


class _Parser(argparse.ArgumentParser):
    def error(self, message):
        raise UsageError(message)


def parser():
    result = _Parser(
        prog="release.py", add_help=False, formatter_class=argparse.RawDescriptionHelpFormatter,
        usage="release.py --version=version [--repo-path=path] [options]",
        description="This script determines the list of commits that need to be\n"
                    "cherry-picked into the target branch for a specific yugabyte\nversion.")
    result.add_argument("--version", metavar="version",
                        help='upstream version (e.g. "v2025.1.1.2" or "2025.1.1.2")')
    result.add_argument("--repo-path", "--repo_path", dest="repo_path", metavar="path",
                        help="path to the yugabyte-db checkout "
                             "(default: the git repository containing the current directory)")
    result.add_argument("--target-branch", "--target_branch", dest="target_branch",
                        metavar="branch", help="target candidate branch "
                        '(default: "${version}-shopify-${sequence}-candidate")')
    result.add_argument("--previous-release", "--previous_release", dest="previous_release",
                        metavar="previous",
                        help='previous Shopify release (e.g. "2024.2.5-shopify-1")')
    result.add_argument("--no-fetch", "--no_fetch", dest="fetch", action="store_false",
                        help="do not run `git fetch --all --tags` before planning")
    result.add_argument("-h", "--help", action="store_true", help="Prints this help")
    return result


def default_repo_path():
    result = subprocess.run(["git", "rev-parse", "--show-toplevel"], capture_output=True,
                            text=True)
    if result.returncode or not result.stdout.strip():
        raise UsageError(
            "--repo-path is required when the current directory is not a git repository")
    return result.stdout.rstrip("\n")


class Cli:
    def __init__(self, github, out, err):
        self.github = github
        self.out = out
        self.err = err

    @classmethod
    def run(cls, argv, github=None, out=None, err=None):
        return cls(GitHub() if github is None else github,
                   sys.stdout if out is None else out,
                   sys.stderr if err is None else err).main(argv)

    def main(self, argv):
        try:
            options = parser()
            args = options.parse_args(argv)
            if args.help:
                self.out.write(options.format_help())
                return 0
            repo_path = default_repo_path() if args.repo_path is None else args.repo_path
            if args.version is None:
                raise UsageError("--version is required")
            self.execute(args, repo_path)
            return 0
        except UsageError as error:
            self.say(str(error), stream=self.err)
            return 2
        except Error as error:
            self.say(str(error), stream=self.err)
            return 1

    def execute(self, args, repo_path):
        repo = Repository(repo_path)
        if args.fetch:
            repo.fetch_all_with_tags()
        plan = Planner(repo).plan(args.version, self.github.pull_requests(GITHUB_REPO),
                                  args.target_branch, args.previous_release)
        self.report(repo, plan)

    def say(self, *lines, stream=None):
        """Write lines the way Ruby's puts does: no extra newline when one is already present."""
        stream = self.out if stream is None else stream
        for line in lines:
            stream.write(line if line.endswith("\n") else line + "\n")

    def report(self, repo, plan):
        if plan.previous_release:
            self.say(f"Using previous Shopify release: {plan.previous_release}")
        else:
            self.say("Warning: Failed to find previous Shopify release, starting from scratch")
        listing = CherryPickList(repo, self.github, GITHUB_REPO)
        commits = listing.commits(plan)
        self.say(f"The following commits are necessary for upstream version "
                 f"{plan.upstream_version}:", *(f"{c.commit}, PR: {c.pr}" for c in commits))
        done = listing.done(plan, commits)
        if done:
            self.say("", f"The following commits have already been applied to "
                     f"{plan.candidate_branch}:", "", *(f"{c.commit}, PR: {c.pr}" for c in done))
        self.say("")
        remaining = listing.remaining(commits, done)
        if not remaining:
            self.say("All commits have been applied.")
            return
        self.print_commands(plan, remaining)

    def print_commands(self, plan, remaining):
        if not plan.candidate_exists:
            self.say(f"cd {shlex.quote(plan.repo_path)}", "git fetch --all --tags",
                     f"git checkout {plan.upstream_tag}",
                     f"git checkout -b {plan.candidate_branch}")
        for c in remaining:
            flags = "-x -m 1" if c.merge_commit else "-x"
            self.say(f"git cherry-pick {flags} {c.commit}")


if __name__ == "__main__":
    sys.exit(Cli.run(sys.argv[1:]))
