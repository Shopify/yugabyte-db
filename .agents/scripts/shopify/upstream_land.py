"""`upstream.py sync --land <pr>`: land a reviewed sync PR by fast-forwarding master-shopify.

GitHub's merge buttons undo a sync's shape; a fast-forward of master-shopify to the candidate's
head keeps it. Every check runs before the one write, the push, and each refusal says what to do
and, unless master-shopify cannot fast-forward, the break-glass push that skips the checks. Uses
git, gh and devx; requires Python 3.10+. Importing has no side effects.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import time

from interaction import Stop
from scm import OID, Error, GitError, UsageError
from upstream import FORK_BRANCH, FORK_REPO, SYNC_LABEL, UPSTREAM_REF, UPSTREAM_URL, attribute, \
    derive, dropped, resolve, sync_kind, sync_pull_request
from upstream_submit import REVIEWERS_ORG, REVIEWERS_TEAM, head_repository
from upstream_sync import BRANCH_PREFIX, OLD_REF, SHORT, Sync


HEAD_BRANCH = re.compile(rf"{re.escape(BRANCH_PREFIX)}[0-9a-f]{{{SHORT}}}")
SYNC_SUBJECT = re.compile(rf"Sync {FORK_BRANCH} to upstream ([0-9a-f]{{{SHORT}}})")
OLD_LINE = re.compile(rf"^Old-{FORK_BRANCH}:[ \t]*([0-9a-f]{{40}})[ \t]*$", re.MULTILINE)
REQUIRED_CHECKS = ("pr-title", "pr-lint")  # absent when they skip a docs-only change
POLLS, POLL_SECONDS = 10, 3  # how long to wait for GitHub to mark the PR merged
BLOCKING_DECISIONS = ("CHANGES_REQUESTED", "REVIEW_REQUIRED")  # GitHub often reports none
WAITING = ("running", "queued", "pending")  # Binks review statuses that finish by themselves
RULE_OPTIONS = "a scoped bypass identity for syncs, or disabling that ruleset for the push"
REBASE_COMMAND = "python3 .agents/scripts/shopify/upstream.py rebase-open-prs --push"


def entries(data, key):
    """A JSON list, or the list under `key` when the list comes wrapped in an object."""
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return data.get(key) or []
    return []


def effective_reviews(reviews):
    """{login: that reviewer's latest APPROVED or CHANGES_REQUESTED review}, from `gh pr view
    --json reviews`, which is the history: a comment does not change an opinion, a newer
    opinion replaces an older one. Ties keep gh's order."""
    latest = {}
    ordered = sorted(enumerate(reviews), key=lambda item: (item[1].get("submittedAt") or "",
                                                           item[0]))
    for _, review in ordered:
        login = (review.get("author") or {}).get("login")
        if login and review.get("state") in ("APPROVED", "CHANGES_REQUESTED"):
            latest[login] = review
    return latest


def finding_line(comment):
    """One Binks finding: severity, path[:line] and text."""
    path = comment.get("file_path") or comment.get("path") or comment.get("file") or ""
    if path and comment.get("line"):
        path += f":{comment['line']}"
    text = comment.get("title") or comment.get("body") or comment.get("message") or ""
    words = [str(comment.get("severity") or ""), path, " ".join(str(text).split())]
    return "  " + " ".join(word for word in words if word)


class Land(Sync):
    workdir_prefix = "upstream-land-"
    full_checkout = False  # landing reads history and pushes; it checks nothing out

    def __init__(self, args, github, out, err, say, now, sleep=None, **options):
        super().__init__(args, github, out, err, say, now, **options)
        self.sleep = time.sleep if sleep is None else sleep
        self.number = args.land
        self.head = None  # the PR's head commit, once gh reported a well-formed one

    def main(self):
        args = self.args
        combined = [option for option, given in (
            ("--to", args.to), ("--continue", args.resume), ("--skip", args.skip),
            ("--abort", args.abort), ("--yes", args.yes)) if given]
        if combined:
            raise UsageError(f"--land cannot be combined with {', '.join(combined)}")
        repo = self.checkout()
        self.github.auth_status()
        pr = self.github.landing_pull_request(self.number)
        branch = self.check_pull_request(pr)
        hint = self.fetch_candidate(repo, branch)
        old, upstream = resolve(repo, OLD_REF, hint), resolve(repo, UPSTREAM_REF, hint)
        candidate = self.check_candidate(repo, branch, old, upstream, hint)
        if candidate is None:
            self.say(f"PR #{self.number} is already landed: {FORK_BRANCH} is at its head "
                     f"{self.head[:SHORT]}; GitHub marks the PR merged shortly.")
            return 0
        merge, replays = candidate
        skipped = self.check_replays(repo, branch, merge, replays)
        self.check_approval(pr)
        self.check_ci(pr)
        self.check_binks(pr)
        self.check_rules()
        if not repo.ancestor(old, self.head):
            raise Error(f"{FORK_BRANCH} ({old[:SHORT]}) is not an ancestor of {self.head[:SHORT]}, "
                        "so it cannot fast-forward; rebuild the sync")
        self.summary(pr, old, merge, replays, skipped)
        self.confirm_land()
        try:
            repo.fast_forward("origin", self.head, FORK_BRANCH, old)
        except GitError as error:  # no break-glass: whatever rejected this rejects that too
            output = str(error).partition("\n")[2].strip() or str(error)
            rejection = "\n".join(line.rstrip() for line in output.splitlines())
            raise Error(f"The push to {FORK_BRANCH} was rejected; nothing changed:\n{rejection}\n"
                        f"If a pull_request rule rejected it, the options are {RULE_OPTIONS}") \
                from None
        self.say(f"Pushed {self.head[:SHORT]} to {FORK_BRANCH}.")
        self.report_merged()
        self.clean_up(repo, branch)
        self.say(f"Next, rebase the open PRs onto it: {REBASE_COMMAND}")
        return 0

    def refuse(self, message, break_glass=True):
        lines = [message]
        if break_glass and self.head:
            lines.append(f"Break-glass (skips these checks): `git push origin {self.head}:"
                         f"{FORK_BRANCH}`")
        raise Error("\n".join(lines))

    # The checks, in order.

    def check_pull_request(self, pr):
        """The PR's head branch, refusing a PR that is not an open sync PR.

        A PR that is not a sync PR gets no break-glass command: that push would land an
        unreviewed branch on master-shopify.
        """
        number, head = self.number, pr.get("headRefOid")
        self.head = head if isinstance(head, str) and OID.fullmatch(head) else None
        state = pr.get("state")
        if state == "MERGED":
            raise Error(f"PR #{number} is already merged; there is nothing to land")
        if state != "OPEN":
            self.refuse(f"PR #{number} is {state}, not open; reopen it, or build a new sync",
                        break_glass=False)
        if pr.get("isDraft"):
            self.refuse(f"PR #{number} is a draft; mark it ready with `gh pr ready {number} "
                        f"--repo {FORK_REPO}` once Binks and a reviewer have seen it",
                        break_glass=False)
        base = pr.get("baseRefName")
        if base != FORK_BRANCH:
            self.refuse(f"PR #{number} is against {base}, not {FORK_BRANCH}; only a sync PR lands "
                        "this way", break_glass=False)
        branch, owner = pr.get("headRefName") or "", (pr.get("headRepositoryOwner") or {})
        if not HEAD_BRANCH.fullmatch(branch) or head_repository(pr) != FORK_REPO.lower():
            source = f"{owner.get('login')}/{(pr.get('headRepository') or {}).get('name')}"
            self.refuse(f"PR #{number}'s head is {source}:{branch}, not a {BRANCH_PREFIX}<sha> "
                        f"branch in {FORK_REPO}; only a sync PR lands this way", break_glass=False)
        if all(label.get("name") != SYNC_LABEL for label in pr.get("labels") or []):
            self.refuse(f"PR #{number} is not labelled {SYNC_LABEL}; only a sync PR lands this "
                        "way", break_glass=False)
        if self.head is None:
            raise Error(f"gh reported no head commit for PR #{number}: {head!r}")
        return branch

    def fetch_candidate(self, repo, branch):
        if not self.args.fetch:
            return "; run once without --no-fetch"
        # Explicit refspecs update the refs whatever the clone's fetch mapping, as sync does.
        repo.fetch("origin", f"+refs/heads/{FORK_BRANCH}:{OLD_REF}",
                   f"+refs/heads/{branch}:refs/remotes/origin/{branch}")
        repo.fetch(self.args.upstream_url, f"+refs/heads/master:{UPSTREAM_REF}")
        return ""

    def check_candidate(self, repo, branch, old, upstream, hint):
        """(the sync merge, the replays after it oldest first), refusing any other shape; None
        when master-shopify is already at the head."""
        remote = resolve(repo, f"refs/remotes/origin/{branch}", hint)
        if remote != self.head:
            self.refuse(f"origin's {branch} is at {remote[:SHORT]}, but PR #{self.number}'s head "
                        f"is {self.head[:SHORT]}; rerun once GitHub has caught up, and have the "
                        "current head reviewed")
        stale = " (or, with --no-fetch, fetch upstream first)"
        replays, merge = [], None
        for commit in reversed(repo.first_parent_commits(upstream, self.head)):
            if SYNC_SUBJECT.fullmatch(commit["subject"]):
                merge = commit
                break
            if len(commit["parents"]) != 1:
                self.refuse(f"{commit['sha'][:SHORT]} {commit['subject']} on {branch} is a merge "
                            "commit after the sync merge; a candidate carries only replays. "
                            "Rebuild the sync")
            replays.append(commit)
        if merge is None:
            self.refuse(f"{branch} has no `Sync {FORK_BRANCH} to upstream <sha>` merge on its "
                        f"first-parent line above upstream {upstream[:SHORT]}; rebuild the "
                        f"sync{stale}")
        short = branch.removeprefix(BRANCH_PREFIX)
        named = OLD_LINE.findall(merge["message"])
        if not named or merge["parents"][1:] != named[-1:] or \
                merge["parents"][0][:SHORT] != short or \
                SYNC_SUBJECT.fullmatch(merge["subject"])[1] != short or \
                sync_kind(repo, merge, upstream) != "sync":
            self.refuse(f"{merge['sha'][:SHORT]} on {branch} is not a sync merge of upstream "
                        f"{short}: it needs upstream {short} as its first parent and that "
                        f"parent's tree (git merge -s ours), and the commit its Old-{FORK_BRANCH}: "
                        f"line names as its second parent; rebuild the sync{stale}")
        if named[-1] != old and old == self.head:
            return None
        if named[-1] != old:
            self.refuse(f"{FORK_BRANCH} moved since the candidate was built (from "
                        f"{named[-1][:SHORT]} to {old[:SHORT]}); rebuild the sync",
                        break_glass=False)
        return merge, replays[::-1]

    def check_replays(self, repo, branch, merge, replays):
        """The planned landings the candidate skipped, as (label, subject), refusing a commit
        above the sync merge that is not one replay of a landing the sync plan carries: the
        landings on the old master-shopify since its merge base with the upstream commit,
        attributed as `status` does, less those whose PRs the merge's `Dropped:` trailers name."""
        to, old = merge["parents"]
        status = derive(repo, self.github, old, to, self.now)
        owners = {}
        for contribution in status.contributions:
            for sha in contribution.landings:
                owners.setdefault(sha, set()).add(contribution.number)
        drops = {fork for fork, _ in dropped(merge["message"])}
        planned = []
        for commit in repo.first_parent_commits(status.base, old):
            prs = frozenset(owners.get(commit["sha"], ()))
            if sync_kind(repo, commit, to) is None and not (prs and prs <= drops):
                planned.append((prs, commit["subject"]))
        merged = [pr for pr in self.github.merged_pull_requests() if not sync_pull_request(pr)]
        landed, unidentified = attribute(replays, merged)
        unowned = {entry["sha"] for entry in unidentified}
        by_replay = {}
        for number, shas in landed.items():
            for sha in shas:
                by_replay.setdefault(sha, set()).add(number)
        remaining = list(planned)
        for commit in replays:
            name = f"{commit['sha'][:SHORT]} {commit['subject']} on {branch}"
            if commit["sha"] in unowned:
                self.refuse(f"{name} belongs to no merged fork PR; a candidate carries only the "
                            "replays of its sync plan. Rebuild the sync")
            prs = frozenset(by_replay[commit["sha"]])
            label = " ".join(f"#{number}" for number in sorted(prs))
            match = next((entry for entry in remaining if entry[0] == prs), None)
            if match is None and any(entry[0] == prs for entry in planned):
                self.refuse(f"{name} replays {label} again; the sync plan carries it once. "
                            "Rebuild the sync")
            if match is None:
                self.refuse(f"{name} replays {label}, which the sync plan does not carry. "
                            "Rebuild the sync")
            remaining.remove(match)
        return [(" ".join(f"#{number}" for number in sorted(prs)), subject)
                for prs, subject in remaining]

    def check_approval(self, pr):
        effective = effective_reviews(pr.get("reviews") or [])
        blocking = [f"{login} on {(review.get('commit') or {}).get('oid', '?')[:SHORT]}"
                    for login, review in sorted(effective.items())
                    if review["state"] == "CHANGES_REQUESTED"]
        if blocking:  # whatever head it was on: a newer push does not answer it
            self.refuse(f"PR #{self.number} has changes requested by {', '.join(blocking)}; "
                        "resolve them and ask for a new review")
        on_head = [login for login, review in effective.items()
                   if (review.get("commit") or {}).get("oid") == self.head]
        if not any(self.github.reviewer(login) for login in on_head):
            self.refuse(f"PR #{self.number} has no approving review of its head "
                        f"{self.head[:SHORT]} from a {REVIEWERS_ORG}/{REVIEWERS_TEAM} member; ask "
                        "one to approve the current head (approvals of earlier heads do not "
                        "count)")
        decision = pr.get("reviewDecision")
        if decision in BLOCKING_DECISIONS:
            self.refuse(f"PR #{self.number}'s review decision is {decision}; resolve the "
                        "outstanding reviews first")
        if decision != "APPROVED":
            self.warn(f"PR #{self.number} has no review decision on GitHub; the team member's "
                      f"approval of {self.head[:SHORT]} is the gate")

    def check_ci(self, pr):
        for entry in pr.get("statusCheckRollup") or []:
            name = entry.get("name") or entry.get("context")
            result = entry.get("conclusion") or entry.get("state") or entry.get("status")
            if name in REQUIRED_CHECKS and result != "SUCCESS":
                self.refuse(f"PR #{self.number}'s {name} check is {result or 'pending'}, not "
                            "SUCCESS; fix it and wait for the check to pass")

    def binks(self, *args):
        argv = ["devx", "binks", "--json", *args]
        try:
            result = self.tool.run(argv, check=False)
        except Error:  # Tool raises only when devx itself is not found
            self.refuse("Binks review required; install devx")
        if result.returncode:
            self.refuse(f"Failed to run `{shlex.join(argv)}`\n{result.stderr.rstrip()}")
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError:
            self.refuse(f"`{shlex.join(argv)}` printed no JSON")

    def check_binks(self, pr):
        url = pr.get("url") or f"https://github.com/{FORK_REPO}/pull/{self.number}"
        request = f"`devx binks review run {url} --expect-head {self.head}`"
        listed = self.binks("review", "list", "--pr", url)
        control_plane = isinstance(listed, dict) and listed.get("review_owner") == "control_plane"
        reviews = entries(listed, "control_plane_reviews" if control_plane else "reviews")
        if not reviews:  # a first request pinned with --expect-head fails as head_not_current
            self.refuse(f"PR #{self.number} has no Binks review; request one with `devx binks "
                        f"review run {url}`")
        newest = reviews[0]  # Binks lists the newest review first
        review, reviewed = str(newest.get("id")), newest.get("head_sha") or "none"
        if reviewed != self.head:
            self.refuse(f"PR #{self.number}'s newest Binks review ({review}) is of "
                        f"{reviewed[:SHORT]}, not its head {self.head[:SHORT]}; request one with "
                        f"{request}")
        status = newest.get("status")
        if status != "ready":
            then = "wait for it, then rerun" if status in WAITING else \
                f"request a new one with {request}"
            self.refuse(f"PR #{self.number}'s newest Binks review ({review}) is {status}, not "
                        f"ready; {then}")
        count = newest.get("comment_count") or 0
        if not count:
            return
        if not re.fullmatch(r"\w[\w-]*", review):
            raise Error(f"Binks reported the review id {review!r}, which this tool cannot use")
        which = ["--control-plane", review] if control_plane else [review]
        findings = entries(self.binks("review", "comments", *which), "comments")
        self.say(f"Binks review {review} has {count} finding{'s' * (count != 1)}:",
                 *map(finding_line, findings))
        if self.args.findings_answered:
            return
        if not self.interactive:
            self.refuse(f"Binks review {review} has {count} finding{'s' * (count != 1)}; fix or "
                        f"answer {'each' if count != 1 else 'it'}, then rerun with "
                        "--findings-answered")
        if not self.agree("Is every finding fixed or answered? [y/N] "):
            self.refuse("Not landed: fix or answer every Binks finding first")

    def check_rules(self):
        blocking = [rule for rule in self.github.branch_rules(FORK_BRANCH)
                    if rule.get("type") == "pull_request"]
        if blocking:
            rulesets = ", ".join(sorted({str(rule.get("ruleset_id")) for rule in blocking}))
            self.warn(f"An active pull_request rule (ruleset {rulesets}) applies to "
                      f"{FORK_BRANCH}; the fast-forward may be rejected; options if it is: "
                      f"{RULE_OPTIONS}")

    # The landing.

    def summary(self, pr, old, merge, replays, skipped):
        drops = [f"#{fork} (upstream #{upstream})" for fork, upstream in dropped(merge["message"])]
        count = len(replays)  # each one matched a planned landing
        lines = [f"Land PR #{self.number} ({pr.get('url')}) by fast-forwarding {FORK_BRANCH}:",
                 f"  {FORK_BRANCH}: {old[:SHORT]} -> {self.head[:SHORT]}",
                 f"  Upstream: {merge['parents'][0][:SHORT]}",
                 f"  Carried: {count} replay{'s' * (count != 1)}"]
        if skipped:
            lines.append(f"  Skipped ({len(skipped)}): " +
                         ", ".join(f"{label} {subject}" for label, subject in skipped))
        self.say(*lines, f"  Dropped ({len(drops)}): {', '.join(drops) or 'none'}")

    def confirm_land(self):
        short = self.head[:SHORT]
        if self.args.confirm_land is not None:
            if self.args.confirm_land != short:
                raise UsageError(f"--confirm-land {self.args.confirm_land} is not the head "
                                 f"{short}; nothing was pushed")
            return
        if not self.interactive:
            raise Stop(f"To land PR #{self.number}, rerun:", "  " + self.command())
        phrase = f"land {short}"
        if self.ask(f"Type `{phrase}` to push {short} to {FORK_BRANCH}: ") != phrase:
            raise UsageError(f"The answer was not `{phrase}`; nothing was pushed")

    def report_merged(self):
        """GitHub marks the PR merged once it sees its head on master-shopify; waits a little."""
        for attempt in range(POLLS):
            try:
                view = self.github.pull_request_state(self.number)
            except Error as error:
                reason = str(error).strip().splitlines()[-1]
                self.warn(f"Could not read PR #{self.number}'s state: {reason}")
                return
            if view.get("state") == "MERGED":
                self.say(f"GitHub marked PR #{self.number} merged at {view.get('mergedAt')}.")
                return
            if attempt < POLLS - 1:
                self.sleep(POLL_SECONDS)
        self.say(f"GitHub has not marked PR #{self.number} merged yet; it does once it sees "
                 f"{FORK_BRANCH} contains its head.")

    def clean_up(self, repo, branch):
        """Removes the candidate's worktree and local branch; keeps the branch on origin."""
        repo.prune_worktrees()
        worktree = repo.worktrees().get(branch)
        own = {os.path.realpath(path) for path in (repo.worktree_root(), repo.main_worktree())}
        keep = False
        if worktree and os.path.realpath(worktree) in own:
            self.say(f"Kept {branch}: it is checked out in your checkout {worktree}.")
            keep = True
        elif worktree:
            try:
                repo.remove_worktree(worktree, force=False)
                self.say(f"Removed the worktree {worktree}.")
            except GitError:
                self.say(f"Kept the worktree {worktree}: it has changes; remove it with `git "
                         f"worktree remove {worktree}`.")
                keep = True
        if not keep and repo.branch_exists(branch):
            local = repo.commit_for(f"refs/heads/{branch}")
            if local == self.head:
                repo.delete_branch(branch)
                self.say(f"Deleted the branch {branch}.")
            else:
                self.say(f"Kept the local branch {branch}: it is at {local[:SHORT]}, not the "
                         "landed head.")
        self.say(f"Kept {branch} on origin: it is the record of this sync.")

    def command(self, yes=False, skip=None):
        """The rerun command that confirms this landing without a terminal."""
        args = self.args
        argv = [*shlex.split(self.program), "sync", "--land", str(self.number), "--repo-path",
                self.repo_path]
        if args.upstream_url != UPSTREAM_URL:
            argv += ["--upstream-url", args.upstream_url]
        if not args.fetch:
            argv.append("--no-fetch")
        if args.findings_answered:
            argv.append("--findings-answered")
        argv += ["--confirm-land", self.head[:SHORT]]
        return shlex.join(argv)
