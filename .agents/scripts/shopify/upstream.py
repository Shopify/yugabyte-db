#!/usr/bin/env python3
"""Report where each change master-shopify carries stands upstream.

`status` derives everything from git and GitHub and stores nothing: the landings on
master-shopify's first-parent line since its merge base with upstream master, the fork PR each
belongs to, and that PR's `Upstream:` record. Uses git and gh; requires Python 3.10+. Importing
has no side effects, and the only writes are the fetches `status` runs unless --no-fetch.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sys

from scm import ArgumentParser, Error, GitError, Repository, UsageError
from scm import GitHub as _GitHub


FORK_REPO = "Shopify/yugabyte-db"
UPSTREAM_REPO = "yugabyte/yugabyte-db"
UPSTREAM_URL = "https://github.com/yugabyte/yugabyte-db.git"
SKIP_LABEL = "upstream: skip"
SHOPIFY_ONLY = "[shopify-only]"
PR_SUFFIX = re.compile(r"\(#(\d+)\)$")
UPSTREAM_LINE = re.compile(r"^Upstream:[ \t]*(\S*)", re.MULTILINE)
UPSTREAM_PR_URL = re.compile(r"https://github\.com/yugabyte/yugabyte-db/pull/(\d+)/?")
DROPPED = re.compile(r"^Dropped:[ \t]*#(\d+)[ \t]+\(upstream #(\d+)\)[ \t]*$", re.MULTILINE)


class GitHub(_GitHub):
    """The gh queries `status` makes; only the command line entry point creates one."""

    def merged_pull_requests(self):
        return self.json("pr", "list", "--repo", FORK_REPO, "--state", "merged", "-L", "1000",
                         "--json", "number,title,mergeCommit,mergedAt,labels,author,url")

    def comments(self, number):
        return self.json("api", f"repos/{FORK_REPO}/issues/{number}/comments?per_page=100")

    def upstream_pull_request(self, number):
        return self.json("pr", "view", str(number), "--repo", UPSTREAM_REPO,
                         "--json", "number,state,mergedAt,mergeCommit,url,title")

    def ahead_by(self, sha):
        """How many commits `sha` has that upstream master lacks: 0 means it has landed."""
        output = self.run("api", f"repos/{UPSTREAM_REPO}/compare/master...{sha}",
                          "--jq", ".ahead_by").strip()
        if not output.isdigit():
            raise Error(f"Failed to read ahead_by comparing upstream master with {sha}: {output!r}")
        return int(output)


@dataclass
class SyncPoint:
    sha: str
    date: str
    kind: str  # upstream-merge: upstream master merged in; sync: an `-s ours` rebuild


@dataclass
class Contribution:
    number: int
    title: str
    author: str | None
    url: str | None
    merged_at: str | None
    landings: list
    state: str = "not-submitted"
    upstream_number: int | None = None
    upstream_url: str | None = None
    upstream_state: str | None = None
    upstream_title: str | None = None
    upstream_merged_at: str | None = None
    age_days: int | None = None


@dataclass
class Status:
    fork: str
    upstream: str
    base: str
    sync_points: list = field(default_factory=list)
    contributions: list = field(default_factory=list)
    adopted: list = field(default_factory=list)
    unidentified: list = field(default_factory=list)
    warnings: list = field(default_factory=list)


def parse_time(text):
    # Python 3.10's fromisoformat rejects a trailing Z.
    return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)


def age_days(merged_at, now):
    return (now - parse_time(merged_at)).days if merged_at else None


def sync_kind(repo, commit, upstream):
    """The kind of sync point a first-parent commit is, or None for a landing.

    A merge that brings in upstream history on a later parent merges upstream master; one whose
    first parent is upstream and whose tree is that parent's is a sync's `-s ours` merge. A
    landing's first parent may also be upstream (the fork equalled upstream), so that alone does
    not make a sync point.
    """
    parents = commit["parents"]
    if len(parents) < 2:
        return None
    if any(repo.ancestor(parent, upstream) for parent in parents[1:]):
        return "upstream-merge"
    if repo.ancestor(parents[0], upstream) and repo.tree(parents[0]) == commit["tree"]:
        return "sync"
    return None


def dropped(message):
    """(fork PR, upstream PR) pairs from a sync merge's `Dropped: #N (upstream #M)` trailers."""
    return [(int(fork), int(upstream)) for fork, upstream in DROPPED.findall(message)]


def attribute(landings, pull_requests):
    """Maps PR number -> landing shas, and lists the landings no merged fork PR accounts for.

    A `(#N)` subject naming a merged fork PR is that PR; otherwise every PR whose mergeCommit is
    the landing (several for a stack merge). A `(#N)` naming no fork PR is upstream's number.
    """
    numbers = {pr["number"] for pr in pull_requests}
    by_merge = {}
    for pr in pull_requests:
        by_merge.setdefault((pr.get("mergeCommit") or {}).get("oid"), []).append(pr["number"])
    landed, unidentified = {}, []
    for commit in landings:
        match = PR_SUFFIX.search(commit["subject"])
        if match and int(match[1]) in numbers:
            owners = [int(match[1])]
        else:
            owners = by_merge.get(commit["sha"], [])
        if not owners:
            unidentified.append({"sha": commit["sha"], "subject": commit["subject"]})
        for number in owners:
            landed.setdefault(number, []).append(commit["sha"])
    return landed, unidentified


def fork_only(pr):
    """(whether the PR is fork-only work, a warning when its label and title disagree)."""
    labelled = any(label.get("name") == SKIP_LABEL for label in pr.get("labels") or [])
    titled = SHOPIFY_ONLY in (pr.get("title") or "")
    warning = None
    if titled and not labelled:
        warning = f"#{pr['number']} is titled {SHOPIFY_ONLY} but has no {SKIP_LABEL} label"
    elif labelled and not titled:
        warning = f"#{pr['number']} is labelled {SKIP_LABEL} but its title has no {SHOPIFY_ONLY}"
    return labelled or titled, warning


def upstream_link(comments):
    """The URL on the last `Upstream:` line of the newest comment that has one, else None."""
    link = None
    for entry in sorted(comments, key=lambda entry: entry.get("created_at") or ""):
        found = UPSTREAM_LINE.findall(entry.get("body") or "")
        if found:
            link = found[-1]
    return link


def upstream_number(url):
    match = UPSTREAM_PR_URL.fullmatch(url)
    return int(match[1]) if match else None


def upstream_state(view, ahead_by):
    """open, closed, landed or restructured, from `gh pr view` of the upstream PR."""
    state = view.get("state")
    if state == "OPEN":
        return "open"
    if state != "MERGED":
        return "closed"
    sha = (view.get("mergeCommit") or {}).get("oid")
    return "landed" if sha and ahead_by(sha) == 0 else "restructured"


def contribution(pr, landings, github, now, warnings):
    author = (pr.get("author") or {}).get("login")
    result = Contribution(pr["number"], pr.get("title") or "", author, pr.get("url"),
                          pr.get("mergedAt"), landings, age_days=age_days(pr.get("mergedAt"), now))
    only, warning = fork_only(pr)
    if warning:
        warnings.append(warning)
    if only:
        result.state = "fork-only"
        return result
    link = upstream_link(github.comments(result.number))
    if link is None:
        return result
    number = upstream_number(link)
    if number is None:
        warnings.append(f"#{result.number} has an Upstream: comment that is not a "
                        f"{UPSTREAM_REPO} pull request URL: {link}")
        return result
    try:
        view = github.upstream_pull_request(number)
    except Error as error:
        reason = str(error).strip().splitlines()[-1]
        warnings.append(f"#{result.number}: could not read upstream PR yugabyte#{number}: {reason}")
        return result
    result.state = upstream_state(view, github.ahead_by)
    result.upstream_number, result.upstream_url = number, view.get("url")
    result.upstream_state, result.upstream_title = view.get("state"), view.get("title")
    result.upstream_merged_at = view.get("mergedAt")
    return result


def resolve(repo, ref):
    try:
        return repo.commit_for(ref)
    except GitError:
        raise GitError(f"{ref} does not resolve to a commit in {repo.path}") from None


def derive(repo, github, fork_ref, upstream_ref, now):
    fork, upstream = resolve(repo, fork_ref), resolve(repo, upstream_ref)
    status = Status(fork, upstream, repo.merge_base(upstream, fork))
    landings = []
    for commit in repo.first_parent_commits(status.base, fork):
        kind = sync_kind(repo, commit, upstream)
        if kind is None:
            landings.append(commit)
            continue
        point = SyncPoint(commit["sha"], parse_time(commit["date"]).isoformat(), kind)
        status.sync_points.append(point)
        status.adopted += [{"number": fork_pr, "upstream_number": upstream_pr, "sync": point.sha}
                           for fork_pr, upstream_pr in dropped(repo.message(point.sha))]
    status.adopted.sort(key=lambda entry: entry["number"])
    pull_requests = {pr["number"]: pr for pr in github.merged_pull_requests()}
    landed, status.unidentified = attribute(landings, pull_requests.values())
    status.contributions = [contribution(pull_requests[number], landed[number], github, now,
                                         status.warnings) for number in sorted(landed)]
    return status


SECTIONS = (
    ("not-submitted", "Not submitted"),
    ("open", "Open upstream"),
    ("landed", "Landed upstream, dropped at the next sync"),
    ("restructured", "Restructured upstream, awaiting a replacement Upstream: comment"),
    ("closed", "Closed upstream without merging, needs a disposition"),
)


def entry_line(c):
    number = f"#{c.number}"
    if c.state == "not-submitted":
        age = f"{'?' if c.age_days is None else c.age_days}d"
        return f"  {number:<4} {age:<5} {c.title:<60} {c.author or ''}".rstrip()
    upstream = f"yugabyte#{c.upstream_number}"
    if c.state in ("landed", "restructured"):
        return f"  {number:<4} {upstream}  merged {(c.upstream_merged_at or '?')[:10]}"
    return f"  {number:<4} {upstream}  {c.upstream_title or c.title}"


def render(status, fork_name):
    """The human report: a header, then each state's PRs, then any warnings."""
    carried = len({sha for c in status.contributions for sha in c.landings})
    carried += len(status.unidentified)
    header = (f"{fork_name} {status.fork[:9]}: {carried} carried commit{'s' * (carried != 1)} "
              f"since upstream {status.base[:9]}; ")
    if status.sync_points:
        last = status.sync_points[-1]
        header += f"last sync {last.date[:10]} ({last.kind.replace('-', ' ')})"
    else:
        header += "no sync point"
    lines = [header, ""]
    for state, heading in SECTIONS:
        entries = [c for c in status.contributions if c.state == state]
        lines += [f"{heading} ({len(entries)})", *map(entry_line, entries)]
    fork_only_prs = [f"#{c.number}" for c in status.contributions if c.state == "fork-only"]
    lines.append(f"Fork-only ({len(fork_only_prs)})")
    if fork_only_prs:
        lines.append("  " + " ".join(fork_only_prs))
    dates = {point.sha: point.date[:10] for point in status.sync_points}
    lines.append(f"Adopted by earlier syncs ({len(status.adopted)})")
    lines += [f"  {'#' + str(a['number']):<4} yugabyte#{a['upstream_number']}  "
              f"dropped {dates[a['sync']]}" for a in status.adopted]
    lines.append(f"Unidentified commits ({len(status.unidentified)})")
    lines += [f"  {u['sha'][:9]}  {u['subject']}" for u in status.unidentified]
    if status.warnings:
        lines += ["", "Warnings", *(f"  {warning}" for warning in status.warnings)]
    return lines


def parser():
    result = ArgumentParser(prog="upstream.py", add_help=False,
                            description="Track the changes master-shopify carries upstream.")
    result.add_argument("-h", "--help", action="store_true", default=argparse.SUPPRESS,
                        help="print this help")
    commands = result.add_subparsers(dest="command", metavar="command")
    status = commands.add_parser(
        "status", add_help=False, help="show where each carried change stands upstream",
        description="Show where each change master-shopify carries stands upstream, derived "
                    "from git and GitHub. Read-only apart from the fetches.")
    status.add_argument("--repo-path", metavar="path",
                        default=str(Path(__file__).resolve().parents[3]),
                        help="path to the yugabyte-db checkout "
                             "(default: the checkout containing this script)")
    status.add_argument("--no-fetch", dest="fetch", action="store_false",
                        help="do not fetch origin master-shopify and upstream master first")
    status.add_argument("--upstream-ref", metavar="ref",
                        help="upstream master (default: FETCH_HEAD, or upstream/master with "
                             "--no-fetch)")
    status.add_argument("--fork-ref", metavar="ref", default="origin/master-shopify",
                        help="the fork's master-shopify (default: origin/master-shopify)")
    status.add_argument("--json", action="store_true", help="print one JSON object")
    status.add_argument("-h", "--help", action="store_true", default=argparse.SUPPRESS,
                        help="print this help")
    result.commands = {"status": status}
    return result


class Cli:
    def __init__(self, github, out, err, now):
        self.github = github
        self.out = out
        self.err = err
        self.now = now

    @classmethod
    def run(cls, argv, github=None, out=None, err=None, now=None):
        return cls(GitHub() if github is None else github,
                   sys.stdout if out is None else out,
                   sys.stderr if err is None else err,
                   datetime.now(timezone.utc) if now is None else now).main(argv)

    def main(self, argv):
        try:
            options = parser()
            args = options.parse_args(argv)
            if getattr(args, "help", False):
                self.out.write(options.commands.get(args.command, options).format_help())
                return 0
            if args.command is None:
                raise UsageError("A command is required: status")
            self.status(args)
            return 0
        except UsageError as error:
            self.say(str(error), stream=self.err)
            return 2
        except Error as error:
            self.say(str(error), stream=self.err)
            return 1

    def status(self, args):
        repo = Repository(args.repo_path)
        if args.fetch:
            repo.fetch("origin", "master-shopify")
            repo.fetch(UPSTREAM_URL, "master")
        upstream_ref = args.upstream_ref or ("FETCH_HEAD" if args.fetch else "upstream/master")
        status = derive(repo, self.github, args.fork_ref, upstream_ref, self.now)
        if args.json:
            self.say(json.dumps(asdict(status), indent=2))
        else:
            self.say(*render(status, args.fork_ref.removeprefix("origin/")))

    def say(self, *lines, stream=None):
        stream = self.out if stream is None else stream
        for line in lines:
            stream.write(line if line.endswith("\n") else line + "\n")


if __name__ == "__main__":
    sys.exit(Cli.run(sys.argv[1:]))
