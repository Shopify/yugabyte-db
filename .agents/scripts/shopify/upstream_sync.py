"""`upstream.py sync`: rebuild master-shopify on a newer upstream commit as a candidate branch.

The candidate `shopify-sync/<short sha>` starts at the upstream commit, merges the old
master-shopify with `-s ours` (its `Dropped:` trailers record the PRs upstream has landed), and
replays every other landing on top, in a worktree beside the developer's checkout, which is never
touched. After one confirmation it pushes the branch and opens or updates its draft PR against
master-shopify. Landing that PR, a fast-forward of master-shopify, is `sync --land`
(upstream_land.py). Uses git and gh; requires Python 3.10+. Importing has no side effects.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import re
import shlex

from interaction import UPSTREAM_SCRIPT, Interaction, Stop
from scm import Error, GitError, Repository, UsageError, last_url, remote_pattern
from upstream import (FORK_BRANCH, FORK_REPO, PR_SUFFIX, SKIP_LABEL, SYNC_LABEL, UPSTREAM_REF,
                      UPSTREAM_REPO, UPSTREAM_URL, derive, resolve, shopify_prs, sync_kind)
import upstream_submit


BRANCH_PREFIX = "shopify-sync/"
SHORT = 12
OLD_REF = f"refs/remotes/origin/{FORK_BRANCH}"
SYNC_LABEL_DESCRIPTION = "Rebuild of master-shopify on a newer upstream commit"
SYNC_LABEL_COLOR = "0E8A16"
STATE_FILE = "shopify-sync.json"  # per-worktree: what this tool saw while replaying
FORK_REMOTE_URL = remote_pattern(FORK_REPO)
FORK_PULL_URL = f"https://github.com/{FORK_REPO}/pull/"
FORK_PULL = re.compile(rf"{re.escape(FORK_PULL_URL)}\d+")
UPSTREAM_PULL_URL = f"https://github.com/{UPSTREAM_REPO}/pull/"
UPSTREAM_COMMIT_URL = f"https://github.com/{UPSTREAM_REPO}/commit/"
SKIPPED = {"empty": "applies empty on upstream", "conflict": "conflicted on upstream",
           "missing": "is not on the candidate built on upstream"}
CONFLICT = {True: "yes", False: "no", None: "?"}  # None: replayed before this worktree's state
RANGE_DIFF_LINES = 200
BODY_LIMIT = 60000
SUMMARY = ("This draft PR rebuilds `master-shopify` on upstream commit `{short}`: the branch "
           "starts at that commit, merges the old `master-shopify` with `-s ours` so its history "
           "stays reachable, and replays on top the landings the fork still carries, with their "
           "messages unchanged. GitHub's diff against `master-shopify` is therefore upstream's "
           "changes since the last sync, not the fork's: review the lists below instead, the PRs "
           "dropped because upstream landed them, the carried landings with the range-diff of "
           "each replay against its original, and the warnings.")
LANDING = ("Do not use the merge button: squash, merge-commit and rebase all undo the sync. It "
           "lands by fast-forwarding master-shopify to this head once Binks and a "
           "yugabyte-code-reviewers member have approved it. Land it with "
           "`python3 .agents/scripts/shopify/upstream.py sync --land <pr>`, which checks approval, "
           "Binks, CI and the fast-forward first. Break-glass, skipping those checks: "
           "`git push origin {head}:master-shopify`. After landing, run "
           "`python3 .agents/scripts/shopify/upstream.py rebase-open-prs --push` to rebase the "
           "open PRs onto the new master-shopify.")
# upstream's pr-upgrade check wants this section on any PR whose diff touches a .proto file.
# A sync PR's diff against master-shopify is mostly upstream's, but a dropped landing or a
# replay adapted in a conflict changes the diff too, so the inventory comes from the candidate.
UPGRADE_NONE = "No `.proto` file changes in this diff."
UPGRADE_UPSTREAM = ("{count} in upstream's commits `{base}..{to}`, reviewed and merged in "
                    "yugabyte/yugabyte-db, which this sync takes as they are{paths}")
UPGRADE_BOTH = ("{count} changed both in upstream's commits `{base}..{to}` and by a replayed "
                "landing, accounted for below{paths}")
UPGRADE_FORK = ("{count} from this sync's own changes to the fork's carried landings, "
                "accounted for below{paths}")
UPGRADE_CLEAN = ("{label} replays its original landing without conflict (range-diff below), so "
                 "that fork PR's upgrade and rollback notes still apply")
UPGRADE_ADAPTED = ("{label} was replayed {how}{resolved}; its original notes may not describe "
                   "the resolved schema: review its range-diff and add notes here before landing")
UPGRADE_RESOLVED_AWAY = " and its own `.proto` changes were resolved away in favour of upstream's"
UPGRADE_DROPPED = ("{label}, which changed `.proto` files, is dropped from the fork by this "
                   "sync ({reason}), so the diff removes its schema changes")
UPGRADE_EMPTY = ("{label}, which changed `.proto` files, applied empty on upstream: upstream "
                 "already contains its schema changes, and this sync carries nothing of it")
DROP_REASONS = {"conflict": "it conflicted and was skipped",
                "missing": "it was removed from the candidate"}
UPGRADE_TAIL = ("The previous `master-shopify` tip `{old}` stays reachable as the sync merge's "
                "second parent, named by its `Old-master-shopify:` trailer.")
LAND_FIELDS = ("number,state,isDraft,baseRefName,headRefName,headRefOid,headRepository,"
               "headRepositoryOwner,labels,reviews,reviewDecision,statusCheckRollup,url")


class GitHub(upstream_submit.GitHub):
    """The gh queries and writes `sync` makes; only the command line entry point creates one."""

    def create_sync_label(self):
        self.run("label", "create", SYNC_LABEL, "--repo", FORK_REPO, "--description",
                 SYNC_LABEL_DESCRIPTION, "--color", SYNC_LABEL_COLOR)

    def open_pull_requests(self, branch):
        """The open PRs from `branch` against master-shopify whose head is in the fork itself:
        --head also matches same-named branches in other forks, and PRs against other bases."""
        prs = self.json("pr", "list", "--repo", FORK_REPO, "--head", branch, "--state", "open",
                        "--json", "number,url,baseRefName,headRepository,headRepositoryOwner")
        return [pr for pr in prs if pr.get("baseRefName") == FORK_BRANCH and
                upstream_submit.head_repository(pr) == FORK_REPO.lower()]

    def create_pull_request(self, branch, title, body_file):
        output = self.run("pr", "create", "--repo", FORK_REPO, "--base", FORK_BRANCH, "--head",
                          branch, "--draft", "--title", title, "--body-file", body_file,
                          "--label", SKIP_LABEL, "--label", SYNC_LABEL)
        url = last_url(output, FORK_PULL)
        if url is None:
            raise Error(f"gh pr create printed no {FORK_REPO} pull request URL:\n{output}")
        return url

    def edit_pull_request(self, number, body_file):
        self.run("pr", "edit", str(number), "--repo", FORK_REPO, "--body-file", body_file)

    # `sync --land` reads these; it writes nothing through gh.

    def landing_pull_request(self, number):
        return self.json("pr", "view", str(number), "--repo", FORK_REPO, "--json", LAND_FIELDS)

    def pull_request_state(self, number):
        return self.json("pr", "view", str(number), "--repo", FORK_REPO, "--json",
                         "state,mergedAt")

    def branch_rules(self, branch):
        """The active ruleset rules that apply to `branch` in the fork."""
        return self.json("api", f"repos/{FORK_REPO}/rules/branches/{branch}")


class Checkout(Repository):
    """The git commands `sync` needs beyond the shared ones."""

    def worktrees(self):
        """{branch name: path} of the worktrees that have a branch checked out."""
        found, path = {}, None
        for line in self._run("worktree", "list", "--porcelain").split("\n"):
            if line.startswith("worktree "):
                path = line.removeprefix("worktree ")
            elif line.startswith("branch refs/heads/"):
                found[line.removeprefix("branch refs/heads/")] = path
        return found

    def main_worktree(self):
        """The path of the repository's main worktree, which `git worktree list` names first."""
        first = self._run("worktree", "list", "--porcelain").split("\n", 1)[0]
        return first.removeprefix("worktree ")

    def prune_worktrees(self):
        self._run("worktree", "prune")

    def add_worktree(self, path, start, new_branch=None):
        args = ["--no-track", "-b", new_branch] if new_branch else []
        self._run("worktree", "add", "--quiet", *args, path, start)

    def remove_worktree(self, path, force=True):
        self._run("worktree", "remove", *(["--force"] if force else []), path)

    def delete_branch(self, name):
        self._run("branch", "--quiet", "-D", name)

    def remote_head(self, remote, branch):
        """The sha of `branch` on `remote`, asked over the network, or None."""
        for line in self._lines("ls-remote", "--heads", remote, f"refs/heads/{branch}"):
            sha, ref = line.split("\t", 1)
            if ref == f"refs/heads/{branch}":
                return sha
        return None

    def push(self, remote, sha, branch):
        """Pushes the commit `sha`, not whatever `branch` points at by then."""
        self._run("push", "--quiet", remote, f"{sha}:refs/heads/{branch}")

    def fast_forward(self, remote, sha, branch, expected):
        """Pushes the commit `sha` to `branch` only while `remote` still has it at `expected`."""
        self._run("push", "--quiet", f"--force-with-lease=refs/heads/{branch}:{expected}", remote,
                  f"{sha}:refs/heads/{branch}")

    def delete_remote_branch(self, remote, branch):
        self._run("push", "--quiet", remote, "--delete", f"refs/heads/{branch}")

    def has_commit(self, sha):
        return self._succeeds("cat-file", "-e", f"{sha}^{{commit}}")

    def picking(self):
        """The commit a stopped cherry-pick is applying, or None."""
        result = self._git("rev-parse", "--quiet", "--verify", "CHERRY_PICK_HEAD")
        return result.stdout.strip() if result.returncode == 0 else None

    def unmerged_paths(self):
        output = self._run("diff", "--name-only", "--diff-filter=U", "-z")
        return sorted(path for path in output.split("\0") if path)

    def pick(self, commit, merge):
        """Cherry-picks `commit` (a merge against its first parent): ok, conflict or empty."""
        result = self._git("cherry-pick", *(["-m", "1"] if merge else []), commit)
        if result.returncode == 0:
            return "ok"
        if self.picking() is None:
            raise GitError(f"Failed to run `git cherry-pick {commit}` in {self.path}\n"
                           f"{result.stderr}")
        return "empty" if self.clean() and not self.unmerged_paths() else "conflict"

    def merge_ours(self, commit, message_file):
        self._run("merge", "--quiet", "--no-ff", "--no-verify", "-s", "ours", "--cleanup=verbatim",
                  "-F", message_file, commit)

    def range_diff(self, args):
        return self._run("range-diff", "--no-color", *args)

    def author_date(self, commit):
        return self._run("show", "--no-patch", "--format=%aI", commit).strip()

    def describe(self, commit):
        """(committer date as ISO 8601, subject) of a commit."""
        date, _, subject = self._run("show", "--no-patch", "--format=%cI%x1f%s",
                                     commit).strip().partition("\x1f")
        return date, subject

    def git_path(self, name):
        path = self._run("rev-parse", "--git-path", name).strip()
        return path if os.path.isabs(path) else os.path.join(self.path, path)


@dataclass
class Landing:
    sha: str
    subject: str
    merge: bool
    numbers: list  # the fork PRs `attribute` gave it
    message: str = ""  # the landing's commit message
    new: str | None = None  # its replay on the candidate
    conflict: bool | None = None  # None when this tool did not see it replayed

    @property
    def label(self):
        return " ".join(f"#{number}" for number in self.numbers)

    @property
    def needs_trailer(self):
        """Whether a replay needs a `Shopify-PR:` trailer for `status` to attribute it: GitHub's
        mergeCommit names the original landing only, so a replay is attributed by a `(#N)`
        subject naming its PR or by a trailer it already carries, else by the trailer."""
        match = PR_SUFFIX.search(self.subject)
        if match and int(match[1]) in self.numbers:
            return False
        return not set(self.numbers) <= set(shopify_prs(self.message))

    @property
    def range_diff_args(self):
        """A merge landing against its first parent (git 2.48 or later), its replay as is."""
        if self.merge:
            return ["--diff-merges=first-parent", "--right-only", f"{self.sha}^1..{self.sha}",
                    f"{self.new}^..{self.new}"]
        return [f"{self.sha}^..{self.sha}", f"{self.new}^..{self.new}"]


@dataclass
class Plan:
    to: str
    old: str
    base: str  # the upstream commit the old master-shopify was built on
    carried: list
    drops: list  # dicts: number, url, upstream_number, upstream_url
    warnings: list
    skipped: list = field(default_factory=list)  # (landing, reason)
    review_warnings: list = field(default_factory=list)  # range-diffs that could not run

    @property
    def short(self):
        return self.to[:SHORT]

    @property
    def message(self):
        lines = [f"Sync {FORK_BRANCH} to upstream {self.short}", "",
                 f"Old-{FORK_BRANCH}: {self.old}"]
        lines += [f"Dropped: #{drop['number']} (upstream #{drop['upstream_number']})"
                  for drop in self.drops]
        return "\n".join(lines) + "\n"


def new_state():
    """The replays in candidate order ({"landing": sha, "commit": sha, "seen": whether this
    tool saw the pick, so knows if it conflicted}), the landings that conflicted (by sha), those
    skipped ({"landing": sha, "reason": empty, conflict or missing}), and the stop."""
    return {"replayed": [], "conflicts": [], "skipped": [], "stopped": None}


def table_cell(text):
    return text.replace("|", "\\|")


def fenced(text):
    """`text` in a code fence longer than any backtick run inside it."""
    longest = max((len(run) for run in re.findall(r"`+", text)), default=0)
    fence = "`" * max(3, longest + 1)
    return f"{fence}\n{text.rstrip(chr(10))}\n{fence}"


def omitted_block(labels):
    """One block naming every range-diff left out of the body, and how to see each."""
    count = len(labels)
    return (f"### Omitted\n\nThe range-diffs of {count} replay{'s' * (count != 1)} "
            f"({', '.join(labels)}) are omitted to keep this body under {BODY_LIMIT:,} "
            "characters. See each with `git range-diff <old>^..<old> <new>^..<new>`, the old and "
            "new SHAs from the Carried table (for a merge landing, `--diff-merges=first-parent "
            "--right-only <old>^1..<old>`).")


def protos(work, old, new):
    return [path for path in work.changed_paths(old, new) if path.endswith(".proto")]


def first_parent(landing):
    return f"{landing.sha}^1" if landing.merge else f"{landing.sha}^"


def name_paths(paths, limit=8):
    named = ", ".join(f"`{path}`" for path in paths[:limit])
    if len(paths) > limit:
        named += f" and {len(paths) - limit} more"
    return f" ({named})" if paths else ""


def upgrade_section(work, plan, carried, head):
    """The `## Upgrade/Rollback safety` paragraph of a sync PR, from the candidate's own diff
    against the old master-shopify: which `.proto` files change, whose each change is, which
    replays or dropped landings touch a `.proto` file, and how the old tip stays reachable."""
    changed = protos(work, plan.old, head)
    upstream = set(protos(work, plan.base, plan.to))
    replayed = {path for landing in carried
                for path in protos(work, f"{landing.new}^", landing.new)}
    theirs = [path for path in changed if path in upstream and path not in replayed]
    both = [path for path in changed if path in upstream and path in replayed]
    ours = [path for path in changed if path not in upstream]
    sentences = []
    if not changed:
        sentences.append(UPGRADE_NONE)
    else:
        parts = []
        if theirs:
            parts.append(UPGRADE_UPSTREAM.format(count=len(theirs), base=plan.base[:SHORT],
                                                 to=plan.short, paths=name_paths(theirs)))
        if both:
            parts.append(UPGRADE_BOTH.format(count=len(both), base=plan.base[:SHORT],
                                             to=plan.short, paths=name_paths(both)))
        if ours:
            parts.append(UPGRADE_FORK.format(count=len(ours), paths=name_paths(ours)))
        count = f"{len(changed)} `.proto` file{'s' * (len(changed) != 1)}"
        sentences.append(f"{count} change{'s' * (len(changed) == 1)} in this diff: "
                         + "; ".join(parts) + ".")
    for landing in carried:
        # The original's .proto edits count too: a conflict resolved in upstream's favour leaves
        # the replay without a .proto delta while the diff drops the fork's schema edits.
        original = protos(work, first_parent(landing), landing.sha)
        replay = protos(work, f"{landing.new}^", landing.new)
        if not original and not replay:
            continue
        if landing.conflict is False:
            sentences.append(UPGRADE_CLEAN.format(label=landing.label) + ".")
        else:
            how = ("with a conflict resolution" if landing.conflict
                   else "before this worktree recorded whether it conflicted")
            resolved = UPGRADE_RESOLVED_AWAY if original and not replay else ""
            sentences.append(UPGRADE_ADAPTED.format(label=landing.label, how=how,
                                                    resolved=resolved) + ".")
    for landing, reason in plan.skipped:
        if not protos(work, first_parent(landing), landing.sha):
            continue
        if reason == "empty":
            sentences.append(UPGRADE_EMPTY.format(label=landing.label) + ".")
        else:
            sentences.append(UPGRADE_DROPPED.format(
                label=landing.label, reason=DROP_REASONS.get(reason, reason)) + ".")
    sentences.append(UPGRADE_TAIL.format(old=plan.old[:SHORT]))
    return " ".join(sentences)


def range_diff_section(entries, budget):
    """The ## Range-diff text, within `budget` characters, for (label, subject, command, output)
    entries; an output of None means the command failed.

    Every entry first gets its placeholder, the command that shows it ("Not available" for a
    failed one), and outputs (capped at RANGE_DIFF_LINES lines) replace placeholders in order
    while the budget allows. When even the placeholders do not fit, outputs are admitted in order
    while room is left for one block naming everything omitted, failed entries included.
    """
    if not entries:
        return "None."
    placeholders, outputs = [], []
    for label, subject, command, output in entries:
        heading, shown = f"### {label} {subject}\n\n", f"`{command}`"
        if output is None:
            placeholders.append(f"{heading}Not available: git range-diff failed here; see it with "
                                f"{shown}.")
            outputs.append(None)
            continue
        placeholders.append(f"{heading}Omitted to keep this body under {BODY_LIMIT:,} "
                            f"characters; see it with {shown}.")
        lines = output.rstrip("\n").split("\n")
        block = heading + fenced("\n".join(lines[:RANGE_DIFF_LINES]))
        if len(lines) > RANGE_DIFF_LINES:
            block += (f"\n\nTruncated at {RANGE_DIFF_LINES} of {len(lines)} lines; see the rest "
                      f"with {shown}.")
        outputs.append(block)
    separators = 2 * (len(entries) - 1)
    spare = budget - sum(map(len, placeholders)) - separators
    if spare >= 0:
        parts = []
        for placeholder, output in zip(placeholders, outputs):
            if output is not None and len(output) - len(placeholder) <= spare:
                parts.append(output)
                spare -= len(output) - len(placeholder)
            else:
                parts.append(placeholder)
        return "\n\n".join(parts)
    spare = budget - len(omitted_block([entry[0] for entry in entries]))
    parts, omitted = [], []
    for entry, output in zip(entries, outputs):
        if output is not None and len(output) + 2 <= spare:
            parts.append(output)
            spare -= len(output) + 2
        else:
            omitted.append(entry[0])
    return "\n\n".join(parts + ([omitted_block(omitted)] if omitted else []))


class Sync(Interaction):
    workdir_prefix = "upstream-sync-"
    full_checkout = True  # replaying needs every path checked out
    name = "sync"  # the command the checkout's refusals name

    def __init__(self, args, github, out, err, say, now, **options):
        super().__init__(args, out, err, say, **options)
        self.github = github
        self.now = now
        self.state = {}
        self.pinned = None  # the upstream commit, which every rerun command names
        self.repo_path = None  # the checkout, absolute, which every rerun command names

    def default_program(self):
        """`python3 <absolute upstream.py>`: recovery commands run from the candidate worktree."""
        return f"python3 {shlex.quote(str(UPSTREAM_SCRIPT))}"

    def main(self):
        args = self.args
        if args.resume and args.abort:
            raise UsageError("Pass --continue or --abort, not both")
        if args.skip and not args.resume:
            raise UsageError("--skip needs --continue")
        if args.findings_answered or args.confirm_land is not None:
            raise UsageError("--findings-answered and --confirm-land need --land")
        repo = self.checkout()
        if args.abort:
            return self.abort(repo)
        self.github.auth_status()
        hint = self.fetch(repo)
        to, old = self.target(repo, hint), resolve(repo, OLD_REF, hint)
        self.pinned = to
        if repo.ancestor(to, old):
            self.say(f"{FORK_BRANCH} already contains upstream {to[:SHORT]}; nothing to sync")
            return 0
        branch = BRANCH_PREFIX + to[:SHORT]
        repo.prune_worktrees()
        worktree = repo.worktrees().get(branch)
        exists = bool(worktree or repo.branch_exists(branch) or self.remote_head(repo, branch))
        if exists and not args.resume:
            raise Error(f"{branch} already exists; use --continue to resume or --abort to discard")
        if not exists and args.resume:
            raise Error(f"There is no {branch} to continue; run sync without --continue")
        if args.resume:
            work = self.reopen(repo, branch, worktree)
            merge = self.check_candidate(work, branch, to, old)
            plan = self.plan(repo, to, old)
            remaining = self.resume(work, plan, branch, merge)
        else:
            plan = self.plan(repo, to, old)
            work = self.create(repo, branch, to)
            self.sync_merge(work, plan)
            remaining = plan.carried
        self.replay(work, plan, remaining)
        head = work.commit_for("HEAD")
        body = self.body(work, plan, head)
        body_file = self.write("sync-body.md", body)
        if len(body) >= BODY_LIMIT:
            raise Error(f"The PR body would be {len(body):,} characters, over the "
                        f"{BODY_LIMIT:,}-character cap even with every range-diff omitted; "
                        f"nothing was pushed. See {body_file} and rerun with --continue once it "
                        "is shorter")
        self.preview(work, plan, branch, head, body_file)
        self.confirm(branch)
        self.publish(repo, plan, branch, head, body_file, work)
        return 0

    # Inputs.

    def checkout(self):
        path = self.args.repo_path or str(Path(__file__).resolve().parents[3])
        repo = Checkout(path)
        self.repo_path = repo.path
        if "origin" not in repo.remotes():
            raise Error(f"{repo.path} has no origin remote; {self.name} runs in a fork checkout")
        url = repo.remote_url("origin")
        if not FORK_REMOTE_URL.search(url):
            raise Error(f"origin of {repo.path} is {url}, not {FORK_REPO}; {self.name} runs in a "
                        "fork checkout")
        sparse = (repo.config("core.sparseCheckout") or "").lower()
        if self.full_checkout and sparse in ("true", "yes", "on", "1"):
            raise Error(f"{self.name} needs a full checkout; {repo.path} is sparse "
                        f"(core.sparseCheckout is {sparse})")
        return repo

    def fetch(self, repo):
        if not self.args.fetch:
            return "; run once without --no-fetch to fetch upstream master"
        # Explicit refspecs update both refs whatever the clone's fetch mapping, as status does.
        repo.fetch("origin", f"+refs/heads/{FORK_BRANCH}:{OLD_REF}")
        repo.fetch(self.args.upstream_url, f"+refs/heads/master:{UPSTREAM_REF}")
        return ""

    def target(self, repo, hint):
        tip = resolve(repo, UPSTREAM_REF, hint)
        if self.args.to is None:
            return tip
        to = resolve(repo, self.args.to)
        if not repo.ancestor(to, tip):
            raise Error(f"{self.args.to} ({to[:SHORT]}) is not on upstream master "
                        f"({UPSTREAM_REF} at {tip[:SHORT]})")
        return to

    def remote_head(self, repo, branch):
        if self.args.fetch:
            return repo.remote_head("origin", branch)
        if repo.origin_branch_exists(branch):
            return repo.commit_for(f"refs/remotes/origin/{branch}")
        return None

    def plan(self, repo, to, old):
        """What the sync drops and carries, from `status`'s derivation against `to`."""
        status = derive(repo, self.github, OLD_REF, to, self.now)
        if status.unidentified:
            count = len(status.unidentified)
            lines = [f"{count} commit{'s' * (count != 1)} on {FORK_BRANCH} since upstream "
                     f"{status.base[:SHORT]} belong{'s' * (count == 1)} to no merged fork PR; "
                     "attribute these landings first:"]
            for entry in status.unidentified:
                note = (" (a merge commit that is neither a sync point nor a landing)"
                        if repo.parent_count(entry["sha"]) > 1 else "")
                lines.append(f"  {entry['sha'][:SHORT]}  {entry['subject']}{note}")
            raise Error("\n".join(lines))
        owners = {}
        for contribution in status.contributions:
            for sha in contribution.landings:
                owners.setdefault(sha, []).append(contribution)
        carried, drops, warnings = [], {}, list(status.warnings)
        for commit in repo.first_parent_commits(status.base, status.fork):
            if sync_kind(repo, commit, to) is not None:
                continue
            prs = owners[commit["sha"]]
            if all(pr.state == "landed" for pr in prs):
                for pr in prs:
                    drops[pr.number] = {"number": pr.number,
                                        "url": pr.url or f"{FORK_PULL_URL}{pr.number}",
                                        "upstream_number": pr.upstream_number,
                                        "upstream_url": pr.upstream_url or
                                        f"{UPSTREAM_PULL_URL}{pr.upstream_number}"}
                continue
            warnings += [f"#{pr.number} landed upstream as yugabyte#{pr.upstream_number} but "
                         f"shares landing {commit['sha'][:SHORT]} with PRs that have not; the "
                         "landing is carried" for pr in prs if pr.state == "landed"]
            carried.append(Landing(commit["sha"], commit["subject"], len(commit["parents"]) > 1,
                                   [pr.number for pr in prs], commit["message"]))
        return Plan(to, old, status.base, carried, list(drops.values()), warnings)

    # The candidate.

    def directory(self, repo, short):
        return os.path.join(os.path.dirname(repo.worktree_root()), f"yugabyte-db-sync-{short}")

    def create(self, repo, branch, to):
        path = self.directory(repo, to[:SHORT])
        if os.path.exists(path):
            raise Error(f"{path} already exists; move it away, then rerun")
        repo.add_worktree(path, to, new_branch=branch)
        self.say(f"Created {branch} at upstream {to[:SHORT]} in the worktree {path}.")
        work = Checkout(path)
        self.state = new_state()
        self.save_state(work)
        return work

    @staticmethod
    def require_own_worktree(repo, branch, worktree):
        """Refuses a candidate checked out in the developer's checkout, which sync never uses."""
        own = {os.path.realpath(path) for path in (repo.worktree_root(), repo.main_worktree())}
        if worktree and os.path.realpath(worktree) in own:
            raise Error(f"{branch} is checked out in your checkout {worktree}; check out another "
                        "branch in your checkout, then rerun")

    def reopen(self, repo, branch, worktree):
        """The candidate's worktree, added again when only its branch is left."""
        self.require_own_worktree(repo, branch, worktree)
        if worktree:
            work = Checkout(worktree)
        else:
            path = self.directory(repo, branch.removeprefix(BRANCH_PREFIX))
            if os.path.exists(path):
                raise Error(f"{path} already exists but is not {branch}'s worktree; move it "
                            "away, then rerun")
            if repo.branch_exists(branch):
                repo.add_worktree(path, branch)
            else:
                tracking = f"refs/remotes/origin/{branch}"
                if self.args.fetch:
                    repo.fetch("origin", f"+refs/heads/{branch}:{tracking}")
                repo.add_worktree(path, resolve(repo, tracking), new_branch=branch)
            self.say(f"Added the worktree {path} for {branch}.")
            work = Checkout(path)
        self.state = self.load_state(work)
        return work

    def state_path(self, work):
        return work.git_path(STATE_FILE)

    def load_state(self, work):
        try:
            text = Path(self.state_path(work)).read_text(encoding="utf-8")
        except FileNotFoundError:
            return new_state()
        try:
            return {**new_state(), **json.loads(text)}
        except json.JSONDecodeError as error:
            raise Error(f"{self.state_path(work)} is not valid JSON; run --abort and sync "
                        "again") from error

    def save_state(self, work):
        Path(self.state_path(work)).write_text(json.dumps(self.state, indent=2) + "\n",
                                               encoding="utf-8")

    def sync_merge(self, work, plan):
        work.merge_ours(plan.old, self.write("sync-message.txt", plan.message))
        merge = work.first_parent_commits(plan.to, "HEAD")
        if len(merge) != 1 or sync_kind(work, merge[0], plan.to) != "sync":
            raise Error(f"The merge on {work.path} is not one status classifies as a sync; "
                        "run --abort and report it")

    def check_candidate(self, work, branch, to, old):
        """The candidate's sync merge, refusing a candidate this run cannot continue."""
        if work.current_branch() != branch:
            raise Error(f"{work.path} has {work.current_branch() or 'a detached HEAD'} checked "
                        f"out, not {branch}; check {branch} out there, then rerun")
        commits = work.first_parent_commits(to, "HEAD")
        merge = commits[0] if commits else None
        if merge is None or merge["parents"][:1] != [to] or sync_kind(work, merge, to) != "sync":
            raise Error(f"{branch} does not start with a sync merge on upstream {to[:SHORT]}; "
                        "run --abort and sync again")
        if merge["parents"][1] != old:
            raise Error(f"{FORK_BRANCH} moved from {merge['parents'][1][:SHORT]} to "
                        f"{old[:SHORT]} since {branch} was built; run --abort and sync again")
        return merge

    def resume(self, work, plan, branch, merge):
        """Matches the candidate's replays to the plan and returns the landings still to do."""
        if merge["message"].rstrip("\n") != plan.message.rstrip("\n"):
            raise Error(f"The PRs this sync drops changed since {branch} was built; run --abort "
                        "and sync again")
        if work.picking():
            if self.args.skip:
                raise Error(f"{work.path} is still cherry-picking; run `git -C {work.path} "
                            "cherry-pick --abort` first, then rerun with --skip")
            raise Stop(*self.stop_lines(work, plan, self.in_progress(work, plan)))
        if not work.clean():
            raise Error(f"{work.path} has uncommitted changes to tracked files; commit or discard "
                        "them, then rerun")
        commits = work.first_parent_commits(plan.to, "HEAD")
        self.match(work, plan, commits[1:])
        stopped = self.state.get("stopped") or {}
        landing = next((landing for landing in plan.carried
                        if landing.sha == stopped.get("sha")), None)
        conflicting = landing if landing and landing.new is None and \
            stopped["reason"] == "conflict" else None
        for number in self.args.skip or []:
            if conflicting is None or number not in conflicting.numbers:
                raise UsageError(f"--skip #{number}: fork PR #{number} is not the conflicting "
                                 "pick this sync stopped on")
        self.state["stopped"] = None
        if landing and landing.new is not None:  # the developer committed the stopped pick
            self.record(landing, seen=True)
            if stopped["reason"] == "conflict":
                self.state["conflicts"].append(landing.sha)
        elif landing and stopped["reason"] == "empty":  # skipped as the stop said
            self.skip(landing, "empty")
        elif conflicting and self.args.skip:  # dropped by the developer; else picked again
            self.skip(conflicting, "conflict")
        done = [index for index, landing in enumerate(plan.carried) if landing.new]
        after = done[-1] + 1 if done else 0
        for landing in plan.carried[:after]:
            if landing.new is None:
                self.skip(landing, "missing")
        order = {landing.sha: k for k, landing in enumerate(plan.carried)}
        self.state["skipped"] = sorted(
            (entry for entry in self.state["skipped"] if entry["landing"] in order),
            key=lambda entry: order[entry["landing"]])
        skipped = {entry["landing"] for entry in self.state["skipped"]}
        self.mark(plan)
        self.save_state(work)
        return [landing for landing in plan.carried[after:] if landing.sha not in skipped]

    def skip(self, landing, reason):
        """Records `landing` as skipped, unless it already is, with why: empty, conflict, or
        missing (absent from the candidate between replays)."""
        if all(entry["landing"] != landing.sha for entry in self.state["skipped"]):
            self.state["skipped"].append({"landing": landing.sha, "reason": reason})

    def match(self, work, plan, commits):
        """Pairs each commit after the sync merge with the planned landing it replays, in order:
        by the replay this tool recorded, else (a pick the developer committed) by subject,
        author date and, for a landing that needs one, its Shopify-PR trailer. Planned landings in
        between were skipped."""
        recorded = {entry["commit"]: entry["landing"] for entry in self.state["replayed"]}
        index = {landing.sha: k for k, landing in enumerate(plan.carried)}
        position = 0
        for number, commit in enumerate(commits):
            found = index.get(recorded.get(commit["sha"]))
            if found is None or found < position:
                found = self.match_by_content(work, plan, commit, position)
            landing = plan.carried[found]
            landing.new = commit["sha"]
            if landing.needs_trailer and not shopify_prs(commit["message"]):
                if number != len(commits) - 1:
                    raise Error(f"{commit['sha'][:SHORT]} replays landing "
                                f"{landing.sha[:SHORT]} without its `Shopify-PR: "
                                f"{landing.label}` trailer; add it by hand, then rerun")
                self.add_trailer(work, landing)  # a conflicted pick committed by the developer
            self.record(landing)
            position = found + 1

    @staticmethod
    def match_by_content(work, plan, commit, position):
        """The next landing a commit the developer made replays: same subject and author date,
        and, when the commit has a `Shopify-PR:` trailer, exactly the landing's PRs, whether the
        trailer was appended or came with the original."""
        date = work.author_date(commit["sha"])
        prs = set(shopify_prs(commit["message"]))
        for k in range(position, len(plan.carried)):
            landing = plan.carried[k]
            if landing.subject == commit["subject"] and \
                    (not prs or prs == set(landing.numbers)) and \
                    work.author_date(landing.sha) == date:
                return k
        raise Error(f"{commit['sha'][:SHORT]} {commit['subject']} on the candidate replays no "
                    "planned landing; sync carries only landings. Remove it (land a fix on "
                    "master-shopify first), then rerun with --continue")

    def record(self, landing, seen=None):
        """Records `landing`'s replay, replacing an earlier one (an amended replay), whose
        `seen` it keeps unless given."""
        earlier = [entry for entry in self.state["replayed"] if entry["landing"] == landing.sha]
        if seen is None:
            seen = bool(earlier) and earlier[0].get("seen", False)
        self.state["replayed"] = [entry for entry in self.state["replayed"]
                                  if entry["landing"] != landing.sha]
        self.state["replayed"].append({"landing": landing.sha, "commit": landing.new,
                                       "seen": seen})

    def in_progress(self, work, plan):
        sha = work.picking()
        landing = next((landing for landing in plan.carried if landing.sha == sha), None)
        if landing is None:
            raise Error(f"{work.path} is cherry-picking {sha[:SHORT]}, which this sync does not "
                        "carry; finish or abort it there, then rerun")
        return landing

    def replay(self, work, plan, remaining):
        for landing in remaining:
            result = work.pick(landing.sha, landing.merge)
            if result != "ok":
                self.state["stopped"] = {"sha": landing.sha, "reason": result}
                self.save_state(work)
                raise Stop(*self.stop_lines(work, plan, landing))
            landing.new = work.commit_for("HEAD")
            if landing.needs_trailer:
                self.add_trailer(work, landing)
            self.record(landing, seen=True)
        self.mark(plan)
        self.save_state(work)

    def add_trailer(self, work, landing):
        message = work.message("HEAD").rstrip("\n")
        work.amend_message(self.write("replay-message.txt",
                                      f"{message}\n\nShopify-PR: {landing.label}\n"))
        landing.new = work.commit_for("HEAD")

    def mark(self, plan):
        """Each landing's conflict flag and the skipped landings, from the saved state."""
        seen = {entry["landing"] for entry in self.state["replayed"] if entry.get("seen")}
        for landing in plan.carried:
            if landing.sha in self.state["conflicts"]:
                landing.conflict = True
            elif landing.sha in seen:
                landing.conflict = False
        reasons = {entry["landing"]: entry["reason"] for entry in self.state["skipped"]}
        plan.skipped = [(landing, reasons[landing.sha]) for landing in plan.carried
                        if landing.sha in reasons]

    def stop_lines(self, work, plan, landing):
        prs = f"fork PR{'s' * (len(landing.numbers) > 1)} {landing.label}"
        rerun = "  " + self.command()
        paths = work.unmerged_paths()
        if not paths and work.clean():
            return (f"{prs} applies empty on upstream {plan.short}; if upstream took an equivalent "
                    f"fix, run `git cherry-pick --skip` in {work.path} and rerun with --continue; "
                    f"status will then report {landing.label} as neither carried nor adopted until "
                    "a disposition exists", rerun)
        if not paths:
            return (f"Cherry-picking {prs} ({landing.sha[:SHORT]} {landing.subject}) is resolved "
                    "but not committed:", f"  cd {work.path}", "  git cherry-pick --continue",
                    rerun)
        return (f"Cherry-picking {prs} ({landing.sha[:SHORT]} {landing.subject}) conflicts in:",
                *(f"  {path}" for path in paths), "Resolve it, then continue:",
                f"  cd {work.path}", "  (resolve the conflicts)", "  git add <the resolved paths>",
                "  git cherry-pick --continue", rerun,
                "To drop it instead, keeping upstream's version:",
                f"  git -C {work.path} cherry-pick --abort",
                "  " + self.command(skip=landing.numbers[0]))

    # Review.

    def range_diffs(self, work, plan):
        """(label, subject, command, output) per replay; a failed range-diff has no output and is a
        warning, so the run still completes."""
        entries = []
        for landing in plan.carried:
            if not landing.new:
                continue
            command = " ".join(["git", "range-diff", *landing.range_diff_args])
            try:
                output = work.range_diff(landing.range_diff_args)
            except GitError as error:
                output = None
                reason = str(error).strip().splitlines()[-1].replace("`", "'")
                if landing.merge:
                    reason += "; merge-landing range-diffs need git 2.48 or later"
                plan.review_warnings.append(f"the range-diff of {landing.label} failed "
                                            f"({reason}); see it with `{command}`")
            entries.append((landing.label, landing.subject, command, output))
        return entries

    def all_warnings(self, plan):
        return plan.warnings + plan.review_warnings + [
            f"fork PR{'s' * (len(landing.numbers) > 1)} {landing.label} {SKIPPED[reason]} "
            f"{plan.short} and was skipped; status reports it as neither carried nor adopted "
            "until a disposition exists" for landing, reason in plan.skipped]

    def body(self, work, plan, head):
        date, subject = work.describe(plan.to)
        drops = "None."
        if plan.drops:
            drops = "\n".join(["| Fork PR | Upstream PR |", "| --- | --- |"] + [
                f"| [#{drop['number']}]({drop['url']}) | "
                f"[yugabyte#{drop['upstream_number']}]({drop['upstream_url']}) |"
                for drop in plan.drops])
        carried = [landing for landing in plan.carried if landing.new]
        rows = "None."
        if carried:
            rows = "\n".join(["| Fork PR | Subject | Old → new | Conflict |",
                              "| --- | --- | --- | --- |"] + [
                f"| {' '.join(f'[#{n}]({FORK_PULL_URL}{n})' for n in landing.numbers)} | "
                f"{table_cell(landing.subject)} | `{landing.sha[:SHORT]}` → "
                f"`{landing.new[:SHORT]}` | {CONFLICT[landing.conflict]} |"
                for landing in carried])
        diffs = self.range_diffs(work, plan)
        warnings = self.all_warnings(plan)
        sections = [
            ("Summary", SUMMARY.format(short=plan.short)),
            ("Upgrade/Rollback safety", upgrade_section(work, plan, carried, head)),
            ("Upstream", f"[`{plan.to}`]({UPSTREAM_COMMIT_URL}{plan.to}) ({date[:10]}): "
                         f"{subject}\n\nOld `{FORK_BRANCH}`: `{plan.old}`"),
            ("Dropped", drops),
            ("Carried", rows),
            ("Range-diff", None),
            ("Warnings", "\n".join(f"- {warning}" for warning in warnings) or "None."),
            ("Landing", LANDING.format(head=head)),
        ]
        fixed = sum(len(name) + len(text or "") + 6 for name, text in sections)
        budget = BODY_LIMIT - fixed - 1000
        text = range_diff_section(diffs, budget)
        return "\n\n".join(f"## {name}\n\n{text if content is None else content}"
                           for name, content in sections) + "\n"

    def preview(self, work, plan, branch, head, body_file):
        date, subject = work.describe(plan.to)
        carried = [landing for landing in plan.carried if landing.new]
        conflicts = [landing.label for landing in carried if landing.conflict]
        lines = [f"Candidate {branch} at {head[:SHORT]} in {work.path}:",
                 f"  Upstream: {plan.short} {date[:10]} {subject}",
                 f"  Old {FORK_BRANCH}: {plan.old[:SHORT]}",
                 f"  Carried ({len(carried)}):"]
        lines += [f"    {landing.label}  {landing.subject}  {landing.sha[:SHORT]} -> "
                  f"{landing.new[:SHORT]}" for landing in carried]
        lines.append(f"  Dropped ({len(plan.drops)}):")
        lines += [f"    #{drop['number']} -> yugabyte#{drop['upstream_number']}"
                  for drop in plan.drops]
        lines.append(f"  Conflicts resolved: {', '.join(conflicts) or 'none'}")
        if plan.skipped:
            lines.append(f"  Skipped: {', '.join(landing.label for landing, _ in plan.skipped)}")
        warnings = self.all_warnings(plan)
        lines.append(f"  Warnings ({len(warnings)}):")
        lines += [f"    {warning}" for warning in warnings]
        lines.append(f"  PR body: {body_file}")
        self.say(*lines)

    def confirm(self, branch):
        if self.args.yes:
            return
        rerun = "  " + self.command(yes=True)
        if not self.interactive:
            raise Stop(f"To push {branch} and open its PR, rerun:", rerun)
        if not self.agree(f"Push {branch} and open its PR against {FORK_BRANCH}? [y/N] "):
            raise Stop("Not pushed. To push it and open its PR later:", rerun)

    # Publishing.

    def publish(self, repo, plan, branch, head, body_file, work):
        now = repo.commit_for(f"refs/heads/{branch}")
        if now != head or work.commit_for("HEAD") != head:
            raise Error(f"{branch} moved from {head[:SHORT]} to {now[:SHORT]}: the candidate "
                        "changed while you were deciding; rerun with --continue")
        if work.picking() or not work.clean():
            raise Error(f"{work.path} changed during the confirmation; rerun with --continue")
        remote = self.remote_head(repo, branch)
        if remote and remote != head and not (repo.has_commit(remote) and
                                              repo.ancestor(remote, head)):
            raise Error(f"origin's {branch} ({remote[:SHORT]}) is not an ancestor of the "
                        f"candidate ({head[:SHORT]}): the candidate was rewritten after it was "
                        "pushed, and sync never force-pushes. Run --abort and sync again, or "
                        "restore the pushed history in the worktree.")
        repo.push("origin", head, branch)
        self.say(f"Pushed {branch} to origin.")
        if SYNC_LABEL not in self.github.labels(FORK_REPO):
            self.github.create_sync_label()
        existing = self.github.open_pull_requests(branch)
        if existing:
            url = existing[0]["url"]
            self.github.edit_pull_request(existing[0]["number"], body_file)
            self.say(f"Updated the body of {url}.")
        else:
            title = f"[shopify-only] Build: Sync {FORK_BRANCH} to upstream {plan.short}"
            url = self.github.create_pull_request(branch, title, body_file)
            self.say(f"Opened {url} as a draft.")
        self.say(f"Review it with Binks before requesting review: devx binks review run {url}",
                 f"Worktree: {work.path}")

    # Abort.

    def abort(self, repo):
        branch = self.abort_target(repo)
        if branch is None:
            self.say(f"There is no {BRANCH_PREFIX} branch; nothing to abort.")
            return 0
        repo.prune_worktrees()
        worktree = repo.worktrees().get(branch)
        local = repo.branch_exists(branch)
        remote = self.remote_head(repo, branch)
        if not (worktree or local or remote):
            self.say(f"There is no {branch}; nothing to abort.")
            return 0
        self.require_own_worktree(repo, branch, worktree)
        if worktree:
            repo.remove_worktree(worktree)
            self.say(f"Removed the worktree {worktree}.")
        if local:
            repo.delete_branch(branch)
            self.say(f"Deleted the branch {branch}.")
        if remote:
            if self.args.yes or (self.interactive and self.agree(
                    f"Delete {branch} on origin? Its PR closes. [y/N] ")):
                repo.delete_remote_branch("origin", branch)
                self.say(f"Deleted {branch} on origin.")
            else:
                self.say(f"Kept {branch} on origin; delete it with `git push origin --delete "
                         f"{branch}`.")
        return 0

    def abort_target(self, repo):
        """--to's candidate; else the only local candidate; else upstream master's, or None."""
        if self.args.to:
            return BRANCH_PREFIX + resolve(repo, self.args.to)[:SHORT]
        local = [name for name in repo.local_branches() if name.startswith(BRANCH_PREFIX)]
        if len(local) > 1:
            raise Error(f"Several candidates exist ({', '.join(local)}); pass --to <upstream sha>")
        if local:
            return local[0]
        try:
            return BRANCH_PREFIX + repo.commit_for(UPSTREAM_REF)[:SHORT]
        except GitError:
            return None

    # Plumbing.

    def command(self, yes=False, skip=None):
        """The `--continue` rerun command, pinned to this run's upstream commit. `skip` adds
        `--skip #<skip>`; earlier `--skip` options are saved in the state, so are not repeated."""
        args = self.args
        argv = [*shlex.split(self.program), "sync", "--continue"]
        if skip:
            argv += ["--skip", f"#{skip}"]
        argv += ["--to", self.pinned, "--repo-path", self.repo_path]
        if args.upstream_url != UPSTREAM_URL:
            argv += ["--upstream-url", args.upstream_url]
        if not args.fetch:
            argv.append("--no-fetch")
        if yes:
            argv.append("--yes")
        return shlex.join(argv)
