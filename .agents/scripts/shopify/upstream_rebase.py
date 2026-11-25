"""`upstream.py rebase-open-prs`: rebase the open PRs onto master-shopify after a sync lands.

`sync --land` moves master-shopify onto a newer upstream commit, which leaves every open PR into
it behind by that upstream delta. This rebases each open PR into master-shopify, and the stack
layers above it, bottom first, in a scratch worktree beside the developer's checkout, which is
never touched. A dry run unless `--push`: then each rebased branch whose every commit keeps its
`git patch-id --stable` is pushed with a lease on the head it was rebased from, and every PR
touched gets one comment, marked with the master-shopify tip so a rerun does not repeat it.
Uses git and gh; requires Python 3.10+. Importing has no side effects.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
import json
import os
import shutil
import tempfile

from scm import Error, GitError
from upstream import FORK_BRANCH, FORK_REPO, PR_LIMIT, SYNC_LABEL, resolve
from upstream_land import SYNC_SUBJECT
from upstream_submit import head_repository
import upstream_sync
from upstream_sync import OLD_REF, SHORT, Checkout, Sync


OPT_OUT_LABEL = "no-auto-rebase"
OPT_OUT_DESCRIPTION = "Not rebased by upstream.py rebase-open-prs after a sync"
OPT_OUT_COLOR = "FBCA04"
OPEN_FIELDS = ("number,baseRefName,headRefName,headRefOid,headRepository,headRepositoryOwner,"
               "labels,url")
MARKER = "<!-- upstream.py rebase-open-prs {tip} -->"
GROUPS = (  # (JSON key, heading, statuses)
    ("rebased", "Rebased", ("rebased",)),
    ("conflicted", "Conflicted", ("conflicted",)),
    ("not_pushed", "Not pushed", ("patch-changed", "merge-commit", "lease-failed", "push-failed",
                                  "unstacked", "blocked")),
    ("up_to_date", "Up to date", ("up-to-date",)),
    ("skipped", "Skipped", ("fork-branch", "opted-out", "sync", "cross-repo", "excluded-head",
                            "unfetched", "duplicate", "below-skipped")),
)
SKIPPED = GROUPS[-1][2]
COMMENTED = ("rebased", "conflicted", "patch-changed", "merge-commit", "lease-failed",
             "unstacked", "blocked", "opted-out")
BELOW = {"conflicted": "conflicted", "patch-changed": "changed its patch",
         "merge-commit": "holds a merge commit"}  # else: was not rebased
REBASED = ("Rebased onto master-shopify {tip}{after}; the patch is unchanged. Before working on "
           "this branch: `git fetch origin && git reset --hard origin/{branch}` when you have no "
           "local changes, or `gh stack sync --remote origin` when you do (it re-pushes the "
           "branch under a new sha).")
CONFLICTED = ("Not rebased onto master-shopify {tip}{after}: `{branch}` conflicts with it in:\n\n"
              "{paths}\n\nRebase it yourself:\n\n"
              "1. `gh stack rebase --remote origin`\n"
              "2. Resolve the conflicts and `git add` the resolved files.\n"
              "3. `gh stack rebase --continue`\n"
              "4. `build-support/lint.sh --rev origin/master-shopify`\n"
              "5. `gh stack push --remote origin`\n\n"
              "A resolution changes the diff, so request a new Binks review.")
PATCH_CHANGED = ("Not pushed: `{branch}` rebases onto master-shopify {tip} without conflicts, but "
                 "its patch changed: upstream touched its context, or a commit of it is already "
                 "on master-shopify. Rebase it yourself "
                 "(`gh stack rebase --remote origin`), check the diff, lint, `gh stack push "
                 "--remote origin`, and request a new Binks review.")
MERGE_COMMIT = ("Not rebased onto master-shopify {tip}: `{branch}` holds a merge commit, which "
                "rebase-open-prs does not replay; rebase it yourself with `gh stack rebase "
                "--remote origin`, check the diff, lint, `gh stack push --remote origin`, and "
                "request a new Binks review.")
LEASE_FAILED = ("Not pushed: `{branch}` moved on origin while it was being rebased onto "
                "master-shopify {tip}; rebase it yourself with `gh stack rebase --remote origin`, "
                "then `gh stack push --remote origin`.")
UNSTACKED = ("Not rebased onto master-shopify {tip}: `{branch}` is not stacked on the head of "
             "`{lower}`, the layer below it, so which commits are its own is unclear; restack it "
             "with `gh stack rebase --remote origin`, then `gh stack push --remote origin`.")
OPTED_OUT = ("Not rebased onto master-shopify {tip}: this PR is labelled `no-auto-rebase`; rebase "
             "it yourself with `gh stack rebase --remote origin` when you are ready.")
BLOCKED = ("{reason}, so nothing was pushed for master-shopify {tip}; once that is resolved, "
           "`gh stack rebase --remote origin` restacks this layer too.")


class GitHub(upstream_sync.GitHub):
    """The gh queries and writes `rebase-open-prs` makes, beyond `sync`'s; only the command line
    entry point creates one."""

    def every_open_pull_request(self):
        return self.json("pr", "list", "--repo", FORK_REPO, "--state", "open", "-L",
                         str(PR_LIMIT), "--json", OPEN_FIELDS)

    def create_opt_out_label(self):
        self.run("label", "create", OPT_OUT_LABEL, "--repo", FORK_REPO, "--description",
                 OPT_OUT_DESCRIPTION, "--color", OPT_OUT_COLOR)


class Git(Checkout):
    """The git commands `rebase-open-prs` needs beyond `sync`'s."""

    def rebase(self, onto, upstream, head):
        """Rebases the commits upstream..head onto `onto`, detached: None when it applies, else
        the conflicting paths, after aborting it. Other branches are never updated, and merges
        are never recreated. A stop without conflicting paths (a commit hook or signing that
        failed) is aborted and raised: it is not the author's to resolve."""
        # -c rather than --no-update-refs: the same effect, and older git ignores an unknown
        # config key where it would refuse an unknown option.
        result = self._git("-c", "rebase.updateRefs=false", "-c", "rebase.rebaseMerges=false",
                           "rebase", "--quiet", "--no-autosquash", "--no-verify", "--onto", onto,
                           upstream, head)
        if result.returncode == 0:
            return None
        failure = (f"Failed to run `git rebase --onto {onto} {upstream} {head}` in {self.path}\n"
                   f"{result.stderr}")
        if not self.rebasing():
            raise GitError(failure)
        paths = self.unmerged_paths()
        self.abort_rebase()
        if not paths:
            raise GitError(failure)
        return paths

    def merges(self, upstream, head):
        """The merge commits in upstream..head."""
        return self._lines("rev-list", "--merges", f"{upstream}..{head}")

    def rebasing(self):
        return any(os.path.isdir(self.git_path(name)) for name in ("rebase-merge", "rebase-apply"))

    def abort_rebase(self):
        self._run("rebase", "--abort")

    def last_change(self, since, tip, path):
        """(sha, subject) of the newest non-merge commit in since..tip that touched `path`."""
        line = self._run("log", "-1", "--no-merges", "--format=%H%x1f%s", f"{since}..{tip}",
                         "--", path).strip()
        sha, _, subject = line.partition("\x1f")
        return (sha, subject) if sha else (None, None)

    def last_sync(self, tip):
        """The upstream commit, 12 characters, the newest sync merge on tip's first-parent line
        names in its subject, or None."""
        subject = self._run("log", "--first-parent", "-1", "--extended-regexp",
                            f"--grep=^Sync {FORK_BRANCH} to upstream [0-9a-f]{{{SHORT}}}$",
                            "--format=%s", tip).strip()
        match = SYNC_SUBJECT.fullmatch(subject)
        return match[1] if match else None

    def push_with_lease(self, branch, sha, expected):
        """Pushes `sha` to `branch` on origin only while origin has it at `expected`: None when
        it did, else git's error output."""
        result = self._git("push", "--quiet", f"--force-with-lease=refs/heads/{branch}:{expected}",
                           "origin", f"{sha}:refs/heads/{branch}")
        return None if result.returncode == 0 else result.stderr


@dataclass
class Entry:
    number: int
    branch: str
    base: str
    url: str | None
    below: int | None = None  # the PR whose head branch is this one's base
    status: str = ""
    old: str | None = None
    new: str | None = None
    reason: str = ""
    conflicts: list = field(default_factory=list)  # dicts: path, commit, subject
    pushed: bool = False
    commented: bool = False


def labelled(pr, name):
    return any(label.get("name") == name for label in pr.get("labels") or [])


def in_fork(pr):
    return head_repository(pr) == FORK_REPO.lower()


def chains(prs):
    """(pr, the PR below it or None) for every open PR into master-shopify and every PR stacked
    on one, bottom first, each chain after the one before it. Each PR appears once, under one
    parent; a PR whose head is master-shopify, or in another repository, has no layers."""
    above = {}
    for pr in prs:
        above.setdefault(pr.get("baseRefName"), []).append(pr)
    order, seen = [], set()

    def visit(pr, below):
        seen.add(pr["number"])
        order.append((pr, below))
        if not in_fork(pr) or pr.get("headRefName") == FORK_BRANCH:
            return
        for layer in sorted(above.get(pr.get("headRefName"), []), key=lambda p: p["number"]):
            if layer["number"] not in seen:
                visit(layer, pr)

    for pr in sorted(above.get(FORK_BRANCH, []), key=lambda p: p["number"]):
        if pr["number"] not in seen:
            visit(pr, None)
    return order


def excluded_heads(prs):
    """{head branch: the lowest-numbered PR excluding it} over every open PR whose head is in
    the fork and that is labelled no-auto-rebase or sync: a push rewrites the branch for every
    PR with that head. A head in another repository is another branch, so excludes nothing
    here."""
    excluded = {}
    for pr in sorted(prs, key=lambda p: p["number"]):
        if in_fork(pr) and (labelled(pr, OPT_OUT_LABEL) or labelled(pr, SYNC_LABEL)):
            excluded.setdefault(pr.get("headRefName") or "", pr["number"])
    return excluded


def restrict(order, numbers):
    """The chains, from their bottom PR up, that hold one of `numbers`."""
    bottom, root = {}, None
    for pr, below in order:
        root = pr["number"] if below is None else bottom[below["number"]]
        bottom[pr["number"]] = root
    unknown = [number for number in numbers if number not in bottom]
    if unknown:
        raise Error(f"#{unknown[0]} is not an open PR into {FORK_BRANCH} or stacked on one")
    wanted = {bottom[number] for number in numbers}
    return [(pr, below) for pr, below in order if bottom[pr["number"]] in wanted]


def hold(pr, lower, heads, fetched, excluded):
    """(status, reason) when this tool must leave the PR's branch alone, else ("", ""): below a
    layer that was not rebased, a head that is master-shopify, a sync PR, a head in another
    repository, opted out, a head branch another PR excludes (`excluded_heads`) or already
    rebased, or a head this checkout has not fetched."""
    branch = pr.get("headRefName") or ""
    if lower and lower.status not in ("rebased", "up-to-date"):
        if lower.status in SKIPPED:
            return "below-skipped", f"not rebased: #{lower.number} below it is skipped"
        return "blocked", (f"not rebased: #{lower.number} below it "
                           f"{BELOW.get(lower.status, 'was not rebased')}")
    if in_fork(pr) and branch == FORK_BRANCH:
        return "fork-branch", f"head is {FORK_BRANCH}"
    if labelled(pr, SYNC_LABEL):
        return "sync", f"labelled {SYNC_LABEL}"
    if not in_fork(pr):
        return "cross-repo", f"head is in {head_repository(pr)}, not {FORK_REPO}"
    if labelled(pr, OPT_OUT_LABEL):
        return "opted-out", f"labelled {OPT_OUT_LABEL}"
    if branch in excluded:
        return "excluded-head", f"its head branch is opted out by #{excluded[branch]}"
    if branch in heads:
        return "duplicate", f"its head branch is also #{heads[branch]}'s"
    if not fetched:
        return "unfetched", f"origin/{branch} is not fetched; run without --no-fetch"
    return "", ""


class Rebase(Sync):
    workdir_prefix = "upstream-rebase-"
    full_checkout = True  # rebasing checks every path out
    name = "rebase-open-prs"

    def __init__(self, args, github, out, err, say, now, **options):
        super().__init__(args, github, out, err, say, now, **options)
        self.scratch = None  # the scratch worktree's path, once added
        self.work = None
        self.excluded = {}  # head branch: the PR excluding it, from every open PR

    def main(self):
        repo = Git(self.checkout().path)
        if not repo.clean():
            raise Error(f"{repo.path} has uncommitted changes to tracked files; commit or stash "
                        "them, then rerun")
        self.github.auth_status()
        prs = self.github.every_open_pull_request()
        self.excluded = excluded_heads(prs)  # before --pr, which may drop the excluding PR
        order = chains(prs)
        if self.args.pr:
            order = restrict(order, self.args.pr)
        hint = self.fetch_heads(repo, order)
        tip = resolve(repo, OLD_REF, hint)
        if self.args.push and OPT_OUT_LABEL not in self.github.labels(FORK_REPO):
            self.github.create_opt_out_label()
        try:
            entries = self.rebase_all(repo, order, tip)
        finally:
            self.remove_scratch(repo)
        failed = self.push_all(repo, entries) if self.args.push else False
        upstream = repo.last_sync(tip)
        if self.args.push:
            failed = self.comment_all(entries, tip, upstream) or failed
        self.report(entries, tip, upstream)
        return 1 if failed else 0

    # Inputs.

    def fetch_heads(self, repo, order):
        if not self.args.fetch:
            return "; run once without --no-fetch"
        # Explicit refspecs update the refs whatever the clone's fetch mapping, as sync does.
        specs = [f"+refs/heads/{FORK_BRANCH}:{OLD_REF}"]
        specs += [f"+refs/heads/{pr['headRefName']}:refs/remotes/origin/{pr['headRefName']}"
                  for pr, _ in order if self.eligible(pr)]
        repo.fetch("origin", *dict.fromkeys(specs))
        return ""

    def eligible(self, pr):
        return hold(pr, None, {}, True, self.excluded) == ("", "")

    # Rebasing.

    def rebase_all(self, repo, order, tip):
        entries, by_number, heads, since = [], {}, {}, {}
        for pr, below in order:
            lower = by_number[below["number"]] if below else None
            entry = Entry(pr["number"], pr.get("headRefName") or "", pr.get("baseRefName") or "",
                          pr.get("url"), lower.number if lower else None)
            entries.append(entry)
            by_number[entry.number] = entry
            entry.status, entry.reason = hold(pr, lower, heads,
                                              repo.origin_branch_exists(entry.branch),
                                              self.excluded)
            if not entry.status:
                heads[entry.branch] = entry.number
                since[entry.number] = self.rebase_one(repo, entry, lower, tip,
                                                      since.get(lower.number) if lower else None)
        return entries

    def rebase_one(self, repo, entry, lower, tip, since):
        """Rebases one PR's branch; returns the master-shopify commit its chain was based on."""
        entry.old = repo.commit_for(f"refs/remotes/origin/{entry.branch}")
        if lower is None:
            upstream = since = repo.merge_base(entry.old, tip)
            onto = tip
        elif not repo.ancestor(lower.old, entry.old):  # the lower layer moved without it
            entry.status = "unstacked"
            entry.reason = f"not rebased: not stacked on #{lower.number}'s head"
            return since
        else:
            upstream, onto = lower.old, lower.new
        if upstream == onto:  # nothing below it moved
            entry.status, entry.new = "up-to-date", entry.old
            return since
        if upstream == entry.old:  # no commits of its own: nothing to rebase or push
            entry.status, entry.new = "up-to-date", entry.old
            entry.reason = "no commits of its own"
            return since
        if repo.merges(upstream, entry.old):  # rebase would flatten or recreate it
            entry.status, entry.reason = "merge-commit", "holds a merge commit, not rebased"
            return since
        work = self.worktree(repo, tip)
        paths = work.rebase(onto, upstream, entry.old)
        if paths is not None:
            entry.status = "conflicted"
            for path in paths:
                sha, subject = work.last_change(since, tip, path)
                entry.conflicts.append({"path": path, "commit": sha, "subject": subject})
            return since
        entry.new = work.commit_for("HEAD")
        if entry.new == entry.old:
            entry.status = "up-to-date"
            return since
        original = [work.patch_id(sha) for sha in work.commits_between(upstream, entry.old)]
        rebased = [work.patch_id(sha) for sha in work.commits_between(onto, entry.new)]
        # A commit with no patch (empty, or a merge's empty combined diff) proves nothing.
        same = original == rebased and None not in original
        entry.status = "rebased" if same else "patch-changed"
        if entry.status == "patch-changed":
            entry.reason = "patch changed, not pushed"
        return since

    def worktree(self, repo, tip):
        """The scratch worktree, detached, added beside the checkout on first use."""
        if self.work is None:
            parent = os.path.dirname(repo.worktree_root())
            self.scratch = tempfile.mkdtemp(prefix="yugabyte-db-rebase-", dir=parent)
            repo.add_worktree(self.scratch, tip)
            self.work = Git(self.scratch)
        return self.work

    def remove_scratch(self, repo):
        if self.scratch is None:
            return
        try:
            if self.work is not None and self.work.rebasing():
                self.work.abort_rebase()
            repo.remove_worktree(self.scratch)
        except GitError as error:
            self.warn(f"Could not remove the worktree {self.scratch}: "
                      f"{str(error).strip().splitlines()[-1]}")
        shutil.rmtree(self.scratch, ignore_errors=True)
        repo.prune_worktrees()
        self.scratch = self.work = None

    # Publishing.

    def push_all(self, repo, entries):
        """Pushes every rebased branch bottom first; a layer whose lower layer was not pushed is
        not pushed either. Returns whether a push failed for another reason than its lease."""
        failed, blocked = False, set()
        for entry in entries:
            if entry.status != "rebased":
                if entry.status != "up-to-date":
                    blocked.add(entry.number)
                continue
            if entry.below in blocked:
                entry.status = "blocked"
                entry.reason = f"not pushed: #{entry.below} below it was not pushed"
                blocked.add(entry.number)
                continue
            error = repo.push_with_lease(entry.branch, entry.new, entry.old)
            if error is None:
                entry.pushed = True
                continue
            blocked.add(entry.number)
            if "stale info" in error:
                entry.status = "lease-failed"
                entry.reason = f"{entry.branch} moved on origin, not pushed"
            else:
                failed = True
                entry.status = "push-failed"
                entry.reason = f"push failed: {(error.strip().splitlines() or ['?'])[-1]}"
        return failed

    def comment_all(self, entries, tip, upstream):
        """Comments on every PR touched; returns whether a comment failed, which is a warning
        so the report of what was pushed still prints."""
        marker, failed = MARKER.format(tip=tip), False
        for entry in entries:
            if entry.status not in COMMENTED or (entry.status == "rebased" and not entry.pushed):
                continue
            # A push rewrote the author's branch, so it is always told; the branch is up to date
            # on any later run for this tip, so it cannot repeat. Other outcomes do repeat.
            try:
                if entry.status != "rebased" and any(
                        marker in (comment.get("body") or "")
                        for comment in self.github.comments(entry.number)):
                    continue
                body = self.comment(entry, tip, upstream)
                self.github.comment_on_pull_request(entry.number, f"{marker}\n{body}")
                entry.commented = True
            except Error as error:
                failed = True
                self.warn(f"Could not comment on #{entry.number}: "
                          f"{str(error).strip().splitlines()[-1]}")
        return failed

    @staticmethod
    def comment(entry, tip, upstream):
        short = tip[:SHORT]
        after = f" after the sync to upstream {upstream}" if upstream else ""
        if entry.status == "rebased":
            return REBASED.format(tip=short, after=after, branch=entry.branch)
        if entry.status == "conflicted":
            paths = "\n".join(
                f"- `{conflict['path']}`" + (f", last changed there by {conflict['commit'][:SHORT]}"
                                             f" {conflict['subject']}"
                                             if conflict["commit"] else "")
                for conflict in entry.conflicts) or "- (no conflicting paths recorded)"
            return CONFLICTED.format(tip=short, after=after, branch=entry.branch, paths=paths)
        if entry.status == "patch-changed":
            return PATCH_CHANGED.format(tip=short, branch=entry.branch)
        if entry.status == "merge-commit":
            return MERGE_COMMIT.format(tip=short, branch=entry.branch)
        if entry.status == "lease-failed":
            return LEASE_FAILED.format(tip=short, branch=entry.branch)
        if entry.status == "unstacked":
            return UNSTACKED.format(tip=short, branch=entry.branch, lower=entry.base)
        if entry.status == "opted-out":
            return OPTED_OUT.format(tip=short)
        return BLOCKED.format(reason=entry.reason[0].upper() + entry.reason[1:], tip=short)

    # Output.

    def report(self, entries, tip, upstream):
        if self.args.json:
            report = {"master_shopify": tip, "upstream": upstream, "push": self.args.push}
            for key, _, statuses in GROUPS:
                report[key] = [asdict(entry) for entry in entries if entry.status in statuses]
            self.say(json.dumps(report, indent=2))
            return
        after = f", the sync to upstream {upstream}" if upstream else ""
        lines = [f"Rebasing the open PRs onto {FORK_BRANCH} {tip[:SHORT]}{after}."]
        for _, heading, statuses in GROUPS:
            group = [entry for entry in entries if entry.status in statuses]
            lines.append(f"{heading} ({len(group)}):")
            lines += [self.line(entry) for entry in group]
        if not self.args.push:
            lines.append("Dry run: nothing was pushed or commented; rerun with --push to push the "
                         "rebased branches and comment on their PRs.")
        self.say(*lines)

    def line(self, entry):
        words = [f"  #{entry.number} {entry.branch}"]
        if entry.status == "up-to-date":
            words.append(entry.old[:SHORT])
        elif entry.status == "conflicted":
            words.append("conflicts in " + (", ".join(
                conflict["path"] + (f" (last changed by {conflict['commit'][:SHORT]} "
                                    f"{conflict['subject']})" if conflict["commit"] else "")
                for conflict in entry.conflicts) or "no recorded path"))
        elif entry.new and entry.status != "blocked":
            words.append(f"{entry.old[:SHORT]} -> {entry.new[:SHORT]}")
        if entry.status == "rebased":
            words.append("pushed" if entry.pushed else "would push")
        elif entry.reason:
            words.append(entry.reason)
        line = "  ".join(words)
        if entry.commented:
            line += ", commented" if entry.status == "rebased" else "  commented"
        return line
