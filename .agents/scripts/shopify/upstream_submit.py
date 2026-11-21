"""`upstream.py submit`: submit a fork PR merged into master-shopify to yugabyte/yugabyte-db.

Performs the shopify-upstream-pr skill's Steps 1-6 under the developer's own gh login, in two
phases. Phase A is local: it checks the fork PR, decides the upstream issue and the base, and
cherry-picks the PR's landing onto feature-stack/Shopify/<change> in the developer's upstream
checkout, then stops for the test plan. Phase B writes in public after one confirmation: the
upstream issue when one is needed, the PR through create-pr.sh, and the records on the fork.
Uses git, gh and gh stack; requires Python 3.10+. Importing has no side effects.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import re
import shlex
import sys
import tempfile

from scm import Error, GitError, Repository, Tool, UsageError
import upstream
from upstream import (FORK_BRANCH, FORK_REPO, FORK_URL, PR_SUFFIX, SHOPIFY_ONLY, SKIP_LABEL,
                      UPSTREAM_REF, UPSTREAM_REPO, UPSTREAM_URL, attribute, fork_only, resolve,
                      sync_kind, upstream_link, upstream_number, upstream_state)


FORK_REF = "refs/shopify/fork-master-shopify"  # owned by this tool; no other fetch moves it
REVIEWERS_ORG, REVIEWERS_TEAM = "Shopify", "yugabyte-code-reviewers"
STACK_PREFIX = "feature-stack/Shopify/"
FORK_ISSUE_URL = f"https://github.com/{FORK_REPO}/issues/"
FORK_PULL_URL = f"https://github.com/{FORK_REPO}/pull/"
UPSTREAM_ISSUE_URL = f"https://github.com/{UPSTREAM_REPO}/issues/"
PR_FIELDS = ("number,title,body,state,baseRefName,mergeCommit,labels,reviews,reviewDecision,url,"
             "author")
ISSUES_SCRIPT = Path(__file__).resolve().with_name("issues.py")
CREATE_PR = (".agents", "scripts", "create-pr.sh")
# The pr-title grammar of .github/workflows/pr-title.yml for one GitHub issue.
TITLE = re.compile(r"\[#(\d+)\] ([a-zA-Z0-9_-]+(?:,[a-zA-Z0-9_-]+)*): (\S.{8,98}\S)")
CREATE_PR_COMPONENT = re.compile(r"[A-Za-z][A-Za-z0-9]+")  # create-pr.sh's -t check
REFS = re.compile(r"Refs[ \t]+<?https://github\.com/(shopify|yugabyte)/yugabyte-db/issues/(\d+)>?"
                  r"[.,]?", re.IGNORECASE)
UPSTREAM_ISSUE = re.compile(r"https://github\.com/yugabyte/yugabyte-db/issues/(\d+)",
                            re.IGNORECASE)
UPSTREAM_ISSUE_LINE = re.compile(r"^(?:\*\*)?Upstream issue:(?:\*\*)?[ \t]*<?(https://github\.com/"
                                 r"yugabyte/yugabyte-db/issues/\d+)", re.MULTILINE | re.IGNORECASE)
UPSTREAM_PULL = re.compile(r"https://github\.com/yugabyte/yugabyte-db/pull/(\d+)")
UPSTREAM_REMOTE_URL = re.compile(r"[:/]yugabyte/yugabyte-db(?:\.git)?/?$", re.IGNORECASE)
FORK_REFERENCE = re.compile(r"(?:\b(?:fix(?:es|ed)?|close[sd]?|resolve[sd]?):?[ \t]+)?"
                            r"(?<![\w/#&])#(\d+)\b", re.IGNORECASE)
PUBLICATION = re.compile(r"<!-- shopify-yb-publication:.*-->")
PLAN_PROMPT = "Test plan file path, e to write one in $EDITOR, q to quit: "
CREATE_PR_FAILURES = {
    1: "create-pr.sh refused (exit 1); its message above says why. Fix that and rerun",
    3: "create-pr.sh: lint failed; fix as a new commit, rerun",
    5: "create-pr.sh: `gh stack push` failed; read its message above, then rerun",
    6: "create-pr.sh opened the PR as a draft but `gh stack link` failed; link it as its message "
       "above says, then rerun to record it",
}


def normalize_newlines(text):
    """`text` with CRLF and lone CR line endings as LF: GitHub keeps the CRLF of a body or
    comment edited in the web UI, and line-anchored patterns would miss its lines."""
    return (text or "").replace("\r\n", "\n").replace("\r", "\n")


def with_normal_body(entry):
    return {**entry, "body": normalize_newlines(entry.get("body"))}


def link_fork_references(text):
    """Every bare `#N` becomes the fork issue URL, dropping a closing keyword before it: a fork
    number pushed upstream would link an unrelated upstream issue."""
    return FORK_REFERENCE.sub(lambda match: FORK_ISSUE_URL + match[1], text)


def commit_message(message, subject):
    """`message` with `subject` as its first line and fork references in the rest linked."""
    body = link_fork_references(message.partition("\n")[2].strip("\n"))
    return f"{subject}\n\n{body}\n" if body.strip() else f"{subject}\n"


def strip_shopify_context(body):
    """A fork issue body without its publication marker and `## Shopify context` section, which
    runs up to the next `###` heading or the end."""
    kept, skipping = [], False
    for line in normalize_newlines(body).split("\n"):
        if PUBLICATION.fullmatch(line.strip()):
            continue
        if line.strip() == "## Shopify context":
            skipping = True
            continue
        if skipping and line.startswith("###"):
            skipping = False
        if not skipping:
            kept.append(line)
    return "\n".join(kept).rstrip() + "\n"


def summary_section(body):
    """The text of a `## Summary` section up to the next level-1 or level-2 heading, or None."""
    body = normalize_newlines(body)
    match = re.search(r"^## Summary[ \t]*$", body, re.MULTILINE)
    if match is None:
        return None
    rest = body[match.end():]
    end = re.search(r"^#{1,2} ", rest, re.MULTILINE)
    text = (rest[:end.start()] if end else rest).strip("\n").rstrip()
    return text + "\n" if text else None


def slug(description):
    """A lowercase hyphenated branch segment of at most 48 characters."""
    return re.sub(r"[^a-z0-9]+", "-", description.lower()).strip("-")[:48].rstrip("-")


class GitHub(upstream.GitHub):
    """The gh queries and writes `submit` makes; only the command line entry point creates one."""

    def auth_status(self):
        self.run("auth", "status")

    def default_repository(self, path):
        """gh's default repository for the checkout at `path`, or None when none is set."""
        try:
            return self.run("repo", "set-default", "--view", cwd=path).strip() or None
        except Error:
            return None

    def set_default_repository(self, path):
        self.run("repo", "set-default", UPSTREAM_REPO, cwd=path)

    # Every body and comment enters the tool through these queries, with LF line endings.

    def comments(self, number):
        return [with_normal_body(entry) for entry in super().comments(number)]

    def pull_request(self, number):
        return with_normal_body(self.json("pr", "view", str(number), "--repo", FORK_REPO,
                                          "--json", PR_FIELDS))

    def reviewer(self, login):
        """Whether `login` is an active member of the reviewers team; a 404 means no."""
        try:
            membership = self.json(
                "api", f"orgs/{REVIEWERS_ORG}/teams/{REVIEWERS_TEAM}/memberships/{login}")
        except Error as error:
            if "HTTP 404" in str(error):
                return False
            raise
        return membership.get("state") == "active"

    def merged_with_head(self, branch):
        return self.json("pr", "list", "--repo", FORK_REPO, "--state", "merged", "--head", branch,
                         "--json", "number,url")

    def upstream_stack(self, number):
        """The GitHub stack an upstream PR is in, or None; a stack needs two PRs."""
        output = self.run("api", f"repos/{UPSTREAM_REPO}/pulls/{number}", "--jq", ".stack")
        return None if output.strip() in ("", "null") else output.strip()

    def fork_issue(self, number):
        return with_normal_body(self.json("issue", "view", str(number), "--repo", FORK_REPO,
                                          "--json", "title,body,labels"))

    def upstream_labels(self):
        return {label["name"] for label in self.json("label", "list", "--repo", UPSTREAM_REPO,
                                                     "--limit", "1000", "--json", "name")}

    def create_issue(self, title, body_file, labels):
        args = ["issue", "create", "--repo", UPSTREAM_REPO, "--title", title,
                "--body-file", body_file]
        if labels:
            args += ["--label", ",".join(labels)]
        output = self.run(*args)
        match = UPSTREAM_ISSUE.search(output.strip().splitlines()[-1] if output.strip() else "")
        if match is None:
            raise Error(f"gh issue create printed no {UPSTREAM_REPO} issue URL:\n{output}")
        return match[0]

    def comment_on_issue(self, number, body):
        self.run("issue", "comment", str(number), "--repo", FORK_REPO, "--body", body)

    def branch_pull_requests(self, branch):
        prs = self.json("pr", "list", "--repo", UPSTREAM_REPO, "--head", branch, "--state", "all",
                        "--json", "number,url,state,mergeCommit,headRepository,"
                        "headRepositoryOwner,body")
        return [with_normal_body(entry) for entry in prs]

    def comment_on_pull_request(self, number, body):
        self.run("pr", "comment", str(number), "--repo", FORK_REPO, "--body", body)


class Stop(Exception):
    """Stops for developer input (exit 4); `lines` say how to continue."""

    def __init__(self, *lines):
        super().__init__("\n".join(lines))
        self.lines = lines


@dataclass
class Title:
    issue: int
    component: str
    description: str


@dataclass
class IssuePlan:
    number: int | None  # the upstream issue, or None while it is still to be created
    source: str = ""
    fork_issue: int | None = None
    title: str | None = None
    body: str | None = None
    labels: list = field(default_factory=list)
    link_private: bool = False  # record the reused issue with issues.py link
    associate: bool = False  # comment `Upstream issue: <url>` on the fork issue


@dataclass
class Base:
    branch: str  # master, or the parent's feature-stack branch to stack on
    parent: int | None = None
    parent_view: dict | None = None
    in_github_stack: bool = False

    @property
    def stacked(self):
        return self.branch != "master"


def on_upstream(pr):
    """Whether a `gh pr list` entry's head branch is in yugabyte/yugabyte-db itself."""
    owner = ((pr.get("headRepositoryOwner") or {}).get("login") or "").lower()
    name = ((pr.get("headRepository") or {}).get("name") or "").lower()
    return f"{owner}/{name}" == UPSTREAM_REPO


def associated(comments, number):
    """Whether a fork issue comment already names upstream issue `number`."""
    return any(int(UPSTREAM_ISSUE.fullmatch(url)[1]) == number for entry in comments
               for url in UPSTREAM_ISSUE_LINE.findall(entry.get("body") or ""))


def readable(path, what):
    path = os.path.abspath(os.path.expanduser(path))
    if not os.path.isfile(path) or os.path.getsize(path) == 0:
        raise Error(f"The {what} file {path} is missing or empty")
    return path


class Submit:
    def __init__(self, args, github, out, err, say, tool=None, stdin=None, interactive=None,
                 cwd=None, program=None, workdir=None):
        self.args = args
        self.github = github
        self.out = out
        self.err = err
        self.say = say
        self.tool = Tool() if tool is None else tool
        self.stdin = sys.stdin if stdin is None else stdin
        if interactive is None:
            interactive = sys.stdin.isatty() and sys.stdout.isatty()
        self.interactive = interactive and not args.yes
        self.cwd = os.getcwd() if cwd is None else cwd
        self.program = self.default_program() if program is None else program
        self.workdir_path = workdir
        self.number = args.fork_pr
        self.comments = []
        self.restructured = False  # the fork PR's upstream PR was merged without landing

    def default_program(self):
        script = Path(upstream.__file__).resolve()
        try:
            shown = str(script.relative_to(Path(self.cwd).resolve()))
        except ValueError:
            shown = str(script)
        return f"python3 {shlex.quote(shown)}"

    def run(self):
        try:
            return self.submit()
        except Stop as stop:
            self.say(*stop.lines)
            return 4

    # Phase A: local only.

    def submit(self):
        args = self.args
        if self.number is None:
            raise UsageError("submit needs the fork PR number")
        if args.repo_path and args.clone_to:
            raise UsageError("Pass --repo-path or --clone-to, not both")
        change = self.branch_option()
        test_plan = readable(args.test_plan, "test plan") if args.test_plan else None
        upgrade = readable(args.upgrade, "upgrade") if args.upgrade else None
        pr = self.github.pull_request(self.number)
        self.check_preconditions(pr)
        title = self.parse_title(pr)
        summary = summary_section(pr.get("body") or "")
        if summary is None:
            raise Error(f"Fork PR #{self.number}'s body has no ## Summary section to reuse "
                        "upstream")
        self.comments = self.github.comments(self.number)
        issue = self.plan_issue(pr, title)
        self.require_gh_stack()
        repo, remote = self.checkout()
        self.fetch(repo, remote)
        if self.already_submitted(repo):
            return 0
        change = change or slug(title.description)
        if not change:
            raise Error("The title's description gives no branch name; pass --branch <name>")
        branch = STACK_PREFIX + change
        existing = self.existing_pull_request(repo, branch)
        if existing:  # only its records are left: no build, test plan or upgrade notes
            if issue.number is None:  # the PR cites an issue; creating another duplicates it
                raise Error(f"{existing['url']} exists for {branch}, but no upstream issue is "
                            "known for it; pass --upstream-issue")
            self.preview_records(branch, issue, existing)
            self.confirm_records(issue, existing)
            self.publish_records(issue, existing)
            return 0
        if args.yes and not test_plan:
            raise UsageError("--yes needs --test-plan")
        base = self.plan_base(repo, pr)
        landing = self.landing(repo, pr)
        self.build(repo, remote, base, branch, landing, self.subject(title, issue))
        sha = repo.commit_for("HEAD")
        self.say(f"Built {branch} at {sha} in {repo.path}. Build it and run the change's tests "
                 "there; that run is the test plan.")
        protos = [path for path in repo.changed_paths(f"{landing}^", landing)
                  if path.endswith(".proto")]
        self.require_upgrade(repo, protos, upgrade)
        test_plan = test_plan or self.test_plan(repo, branch)
        summary_file = self.write("summary.md", self.summary(summary, base))
        self.preview(repo, branch, sha, title, issue, base, summary_file, test_plan, upgrade)
        self.confirm(repo, test_plan)
        self.publish(repo, branch, title, issue, landing, summary_file, test_plan, upgrade)
        return 0

    def require_gh_stack(self):
        try:
            installed = self.tool.run(["gh", "stack", "--help"], check=False).returncode == 0
        except Error:  # Tool raises only when gh itself is not found
            raise Error("gh was not found; install gh and run `gh auth login`") from None
        if not installed:
            raise Error("gh stack is not installed; install it as stack-prs § Setup describes: "
                        "gh extension install github/gh-stack")

    def is_landed(self, repo):
        return lambda sha: repo.ancestor(sha, self.upstream_tip)

    def check_preconditions(self, pr):
        only, _ = fork_only(pr)
        if only:
            raise Error(f"Fork PR #{self.number} is fork-only work ({SKIP_LABEL} or "
                        f"{SHOPIFY_ONLY}): it does not go upstream")
        if pr.get("state") != "MERGED":
            raise Error(f"Fork PR #{self.number} is {pr.get('state')}, not merged: a fork PR "
                        f"merges into {FORK_BRANCH} before it is submitted upstream")
        approvers = dict.fromkeys(
            (review.get("author") or {}).get("login") for review in pr.get("reviews") or []
            if review.get("state") == "APPROVED")
        if not any(login and self.github.reviewer(login) for login in approvers):
            raise Error(f"Fork PR #{self.number} has no approving review from a "
                        f"{REVIEWERS_ORG}/{REVIEWERS_TEAM} member, or this login cannot read the "
                        "team's membership")

    def parse_title(self, pr):
        text = PR_SUFFIX.sub("", pr.get("title") or "").rstrip()
        match = TITLE.fullmatch(text)
        if match is None:
            raise Error(f"Fork PR #{self.number}'s title {text} does not follow [#N] Component: "
                        "Short description")
        if not CREATE_PR_COMPONENT.fullmatch(match[2]):
            raise Error(f"Fork PR #{self.number}'s component {match[2]} is not one create-pr.sh "
                        "accepts (a letter, then letters and digits); open the upstream PR by "
                        "hand as shopify-upstream-pr Step 5 describes")
        return Title(int(match[1]), match[2], match[3])

    @staticmethod
    def subject(title, issue):
        """The commit subject; a fork issue number stands in until Phase B creates the issue."""
        return f"[#{issue.number or title.issue}] {title.component}: {title.description}"

    def already_submitted(self, repo):
        """True when the fork PR's Upstream: comment names an open or landed upstream PR; a
        restructured one gets a replacement, and a closed one is refused."""
        link = upstream_link(self.comments)
        if link is None:
            return False
        number = upstream_number(link)
        if number is None:
            self.warn(f"Fork PR #{self.number} has an Upstream: comment that is not a "
                      f"{UPSTREAM_REPO} pull request URL: {link}")
            return False
        view = self.github.upstream_pull_request(number)
        state = upstream_state(view, self.is_landed(repo))
        url = view.get("url") or link
        if state in ("open", "landed"):
            shown = "open" if state == "open" else "landed on upstream master"
            self.say(f"Fork PR #{self.number} is already submitted upstream: {url} ({shown}); "
                     "nothing to do")
            return True
        if state == "restructured":
            self.say(f"Fork PR #{self.number}'s upstream PR {url} was merged without landing on "
                     "upstream master: it was restructured. After the build and the test plan, "
                     "this run opens a replacement PR and records it in a new Upstream: comment.")
            self.restructured = True
            return False
        raise Error(f"Fork PR #{self.number}'s upstream PR {url} was closed without merging; "
                    "note the outcome on the fork PR and raise it with the team")

    def plan_issue(self, pr, title):
        """Step 1, read-only: the upstream issue to reuse, or the one to create."""
        first = (pr.get("body") or "").strip().split("\n", 1)[0].strip()
        match = REFS.fullmatch(first)
        if match is None:
            raise Error(f"Fork PR #{self.number}'s body does not start with `Refs <issue URL>` "
                        f"naming the issue its title cites")
        number = int(match[2])
        is_upstream = match[1].lower() == "yugabyte"
        cited = (UPSTREAM_ISSUE_URL if is_upstream else FORK_ISSUE_URL) + str(number)
        if number != title.issue:
            raise Error(f"Fork PR #{self.number}'s title cites #{title.issue} but its Refs line "
                        f"names {cited}")
        if is_upstream:
            if self.args.upstream_issue not in (None, number):
                raise Error(f"--upstream-issue {self.args.upstream_issue} disagrees with "
                            f"{cited}, which fork PR #{self.number} cites")
            return self.reused(number, "cited by the fork PR", None, [])
        fork_comments = self.github.comments(number)
        if self.args.upstream_issue:
            return self.reused(self.args.upstream_issue, "from --upstream-issue", number,
                               fork_comments)
        recorded = None
        for entry in sorted(fork_comments, key=lambda entry: entry.get("created_at") or ""):
            found = UPSTREAM_ISSUE_LINE.findall(entry.get("body") or "")
            if found:
                recorded = found[-1]
        if recorded:
            return self.reused(int(UPSTREAM_ISSUE.fullmatch(recorded)[1]),
                               f"recorded on fork issue #{number}", number, fork_comments)
        if self.args.private_issue:
            numbers = self.private_upstream_issues()
            if len(numbers) > 1:
                raise Error(f"The private issue records several upstream issues "
                            f"({', '.join(f'#{n}' for n in numbers)}); pass --upstream-issue")
            if numbers:
                return IssuePlan(numbers[0], "recorded in the private tracker", number,
                                 associate=not associated(fork_comments, numbers[0]))
        source = self.github.fork_issue(number)
        upstream_labels = self.github.upstream_labels()
        labels = [label["name"] for label in source.get("labels") or []
                  if label.get("name") in upstream_labels]
        return IssuePlan(None, "create", number, source.get("title") or "",
                         strip_shopify_context(source.get("body") or ""), labels)

    def reused(self, number, source, fork_issue, fork_comments):
        """A reused upstream issue, linked privately when --private-issue lacks it and named on
        the fork issue when no comment there names it yet."""
        link = bool(self.args.private_issue) and number not in self.private_upstream_issues()
        associate = fork_issue is not None and not associated(fork_comments, number)
        return IssuePlan(number, source, fork_issue, link_private=link, associate=associate)

    def private_upstream_issues(self):
        """The upstream issue numbers `issues.py inspect` reports for --private-issue."""
        output = self.tool.run([sys.executable, str(ISSUES_SCRIPT), "inspect", "--issue",
                                self.args.private_issue]).stdout
        try:
            public = json.loads(output).get("public_issues") or []
        except (json.JSONDecodeError, AttributeError) as error:
            raise Error("Failed to parse the output of issues.py inspect") from error
        return sorted({int(found[1]) for url in public
                       if (found := UPSTREAM_ISSUE.fullmatch(url))})

    def checkout(self):
        """The Preconditions' checkout: the upstream clone, its upstream remote, and gh set up
        for it. Only a checkout the developer named (--repo-path, --clone-to or the prompt) is
        configured; a discovered one is refused when it is not set up."""
        args = self.args
        named = True
        if args.repo_path:
            path = os.path.abspath(os.path.expanduser(args.repo_path))
        elif args.clone_to:
            path = self.clone(args.clone_to)
        else:
            path = self.discover()
            named = path is None
            path = path or self.ask_checkout()
        repo = Repository(path)
        remote = self.upstream_remote(repo)
        self.require_clean(repo)
        default = self.github.default_repository(repo.path)
        if default is None and named:
            self.github.set_default_repository(repo.path)
            self.say(f"Set gh's default repository in {repo.path} to {UPSTREAM_REPO}.")
        elif default != UPSTREAM_REPO:
            raise Error(f"gh's default repository in {repo.path} is {default}; run `gh repo "
                        f"set-default {UPSTREAM_REPO}` there first")
        configured = repo.config("gh-stack.remote")
        if configured is None and not named:
            raise Error(f"gh-stack.remote is not set in {repo.path}; run `git config "
                        f"gh-stack.remote {remote}` there first, or name the checkout with "
                        "--repo-path")
        if configured is None:
            repo.set_config("gh-stack.remote", remote)
        elif configured != remote:
            raise Error(f"gh-stack.remote in {repo.path} is {configured}, not {remote}, the "
                        f"remote pointing at {UPSTREAM_REPO}; run `git config gh-stack.remote "
                        f"{remote}` there first")
        self.github.auth_status()
        return repo, remote

    @staticmethod
    def require_clean(repo):
        if not repo.clean():
            raise Error(f"{repo.path} has uncommitted changes to tracked files; commit or stash "
                        "them first")

    def discover(self):
        try:
            repo = Repository(self.cwd)
            root = repo.worktree_root()
        except GitError:
            return None
        path = self.cwd if os.path.realpath(self.cwd) == os.path.realpath(root) else root
        return path if self.github.default_repository(path) == UPSTREAM_REPO else None

    def ask_checkout(self):
        proposed = os.path.join(os.path.dirname(os.path.abspath(self.cwd)),
                                "yugabyte-db-upstream")
        found = f"No {UPSTREAM_REPO} checkout found in {self.cwd}."
        if not self.interactive:
            raise Stop(f"{found} Rerun with an existing checkout or a directory to clone into:",
                       "  " + self.command(repo_path="<checkout>", yes=self.args.yes),
                       "  " + self.command(clone_to=proposed, yes=self.args.yes))
        self.say(found, "  1  use an existing checkout (enter its path)",
                 f"  2  clone {UPSTREAM_REPO} to {proposed}", "  q  quit")
        while True:
            choice = self.ask("Choice [1/2/q]: ")
            if choice in (None, "q"):
                raise Stop("Stopped; nothing was changed.")
            if choice == "1":
                path = self.ask("Path of the checkout: ")
                if path:
                    return os.path.abspath(os.path.expanduser(path))
            elif choice == "2":
                return self.clone(proposed)

    def clone(self, path):
        path = os.path.abspath(os.path.expanduser(path))
        if os.path.isdir(path) and os.listdir(path):
            return path  # an earlier --clone-to made it
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.tool.run(["git", "clone", "-o", "upstream", self.args.upstream_url, path],
                      echo=self.out)
        self.github.set_default_repository(path)
        Repository(path).set_config("gh-stack.remote", "upstream")
        return path

    def upstream_remote(self, repo):
        matches = [name for name in repo.remotes()
                   if UPSTREAM_REMOTE_URL.search(repo.remote_url(name))]
        if not matches:
            raise Error(f"No remote of {repo.path} points at {UPSTREAM_REPO}; add one, as "
                        "stack-prs § Setup describes")
        configured = repo.config("gh-stack.remote")
        if configured in matches:
            return configured
        if len(matches) > 1:
            raise Error(f"Several remotes of {repo.path} point at {UPSTREAM_REPO} "
                        f"({', '.join(matches)}); run `git config gh-stack.remote <name>` there")
        return matches[0]

    def fetch(self, repo, remote):
        hint = ""
        if self.args.fetch:
            # Explicit refspecs, whatever the clone's mapping: the snapshots only this tool
            # writes, and the remote's tracking master that gh stack rebase --remote moves.
            repo.fetch(self.args.fork_url, f"+refs/heads/{FORK_BRANCH}:{FORK_REF}")
            repo.fetch(self.args.upstream_url, f"+refs/heads/master:{UPSTREAM_REF}")
            repo.fetch(remote, f"+refs/heads/master:refs/remotes/{remote}/master")
        else:
            hint = "; run once without --no-fetch"
        self.fork_tip = resolve(repo, FORK_REF, hint)
        self.upstream_tip = resolve(repo, UPSTREAM_REF, hint)

    def plan_base(self, repo, pr):
        """Step 2: master, or the open upstream PR of the parent to stack on."""
        parents = self.args.parent or []
        if len(parents) > 1:
            raise Error(f"Several parents ({', '.join(f'#{p}' for p in parents)}): decide the "
                        "arrangement with the team, one chain in dependency order")
        if parents:
            parent = parents[0]
        elif pr.get("baseRefName") not in (None, FORK_BRANCH):
            found = self.github.merged_with_head(pr["baseRefName"])
            if len(found) != 1:
                raise Error(f"Fork PR #{self.number} is based on {pr['baseRefName']}, which "
                            f"{len(found)} merged fork PRs have as their head; name the parent "
                            "with --parent")
            parent = found[0]["number"]
        else:
            return Base("master")
        only, _ = fork_only(self.github.pull_request(parent))
        if only:
            raise Error(f"Parent fork PR #{parent} is fork-only work: this change cannot go "
                        "upstream as is; stop and raise it with the team")
        link = upstream_link(self.github.comments(parent))
        if link is None:
            raise Error(f"Parent fork PR #{parent} has no upstream PR yet: submit it first")
        number = upstream_number(link)
        if number is None:
            raise Error(f"Parent fork PR #{parent}'s Upstream: comment is not a {UPSTREAM_REPO} "
                        f"pull request URL: {link}")
        view = self.github.upstream_pull_request(number)
        state = upstream_state(view, self.is_landed(repo))
        url = view.get("url") or link
        if state == "landed":
            return Base("master", parent)
        if state == "restructured":
            raise Error(f"Parent fork PR #{parent}'s upstream PR {url} was merged but has not "
                        "landed on upstream master: it was restructured; wait for the "
                        f"replacement Upstream: comment on #{parent}")
        if state == "closed":
            raise Error(f"Parent fork PR #{parent}'s upstream PR {url} was closed without "
                        "merging; raise it with the team")
        head = view.get("headRefName") or ""
        if not head.startswith(STACK_PREFIX):
            raise Error(f"Parent fork PR #{parent}'s upstream PR {url} has the head {head}, not "
                        f"a {STACK_PREFIX} branch; stack on it by hand")
        return Base(head, parent, view, self.github.upstream_stack(number) is not None)

    def landing(self, repo, pr):
        """Step 3's commit: the PR's single squash landing on master-shopify's first parents."""
        base = repo.merge_base(self.upstream_tip, self.fork_tip)
        landings = [commit for commit in repo.first_parent_commits(base, self.fork_tip)
                    if sync_kind(repo, commit, self.upstream_tip) is None]
        shas = attribute(landings, [pr])[0].get(self.number, [])
        if not shas:
            raise Error(f"Fork PR #{self.number} has no commit on {FORK_BRANCH}'s first-parent "
                        f"line since upstream {base[:9]}; nothing to cherry-pick")
        if len(shas) > 1 or repo.parent_count(shas[0]) > 1:
            raise Error(f"Fork PR #{self.number} landed on {FORK_BRANCH} as "
                        f"{', '.join(sha[:9] for sha in shas)}, not a single squash commit; "
                        "cherry-pick it by hand as shopify-upstream-pr Step 3 describes")
        return shas[0]

    def branch_option(self):
        if not self.args.branch:
            return None
        name = self.args.branch.removeprefix(STACK_PREFIX)
        if not re.fullmatch(r"[a-z0-9][a-z0-9-]*", name):
            raise UsageError(f"--branch {self.args.branch} is not a lowercase hyphenated name")
        return name

    def build(self, repo, remote, base, branch, landing, subject):
        """Step 3: the branch in its gh stack with the landing cherry-picked onto it; a rerun
        reuses a branch that already has the pick."""
        if base.stacked:
            parent_ref = f"refs/remotes/{remote}/{base.branch}"
            if self.args.fetch:
                repo.fetch(remote, f"+refs/heads/{base.branch}:{parent_ref}")
            bases = [resolve(repo, parent_ref)]
        pick_line = f"(cherry picked from commit {landing})"
        if repo.branch_exists(branch):
            if not base.stacked:
                # gh stack rebase --remote moves a layer onto the remote's tracking master, which
                # can differ from this tool's snapshot either way; either is the planned base.
                tracking = resolve(repo, f"refs/remotes/{remote}/master",
                                   f"; fetch it with `git fetch {remote} master`")
                bases = [tracking, self.upstream_tip]
            commits = repo.commits_excluding(branch, *bases)
            picks = [commit for commit in commits if pick_line in repo.message(commit)]
            if picks:
                if base.stacked:
                    placed, on = repo.ancestor(bases[0], branch), None
                else:  # upstream master may have moved on since the branch was built
                    on = repo.parent_commit(picks[-1])
                    placed = any(repo.ancestor(on, ref) for ref in bases)
                self.require_placement(repo, remote, base, branch, placed, on)
                self.say(f"{branch} already has the cherry-pick of {landing[:9]}.")
                return
            if commits:
                raise Error(f"{branch} exists without the cherry-pick of {landing[:9]}; pass "
                            "--branch to name another branch")
            # An aborted pick left it empty: pick again only from the planned base.
            tip = repo.commit_for(branch)
            self.require_placement(repo, remote, base, branch, tip in bases,
                                   None if base.stacked else tip)
        else:
            self.create(repo, remote, base, branch)
        try:
            repo.cherry_pick(landing)
        except GitError:
            try:
                repo.cherry_pick_abort()
            except GitError:
                pass
            raise Error(f"Cherry-picking {landing[:9]} onto {base.branch} conflicts; the change "
                        "may need a parent (--parent), or a pick by hand as shopify-upstream-pr "
                        f"Step 3 describes. {branch} is left at its base") from None
        self.rewrite(repo, subject)
        if repo.changed_paths("HEAD^", "HEAD") != repo.changed_paths(f"{landing}^", landing):
            raise Error(f"The cherry-pick on {branch} changes other paths than {landing[:9]}; "
                        "compare them by hand")
        if repo.patch_id("HEAD") != repo.patch_id(landing):
            self.warn(f"Warning: the cherry-pick's patch differs from {landing[:9]}'s; compare "
                      f"`git diff {base.branch}` with `gh pr diff {self.number} --repo "
                      f"{FORK_REPO}`")

    def create(self, repo, remote, base, branch):
        if not base.stacked:
            self.stack(repo, "init", branch)
            self.stack(repo, "rebase", "--remote", remote)
            return
        if base.in_github_stack:
            self.stack(repo, "checkout", base.parent_view["url"])
            self.require_top(repo, base.branch)
            repo.checkout(base.branch)
            self.stack(repo, "add", branch)
            return
        parent_ref = f"refs/remotes/{remote}/{base.branch}"
        if repo.branch_exists(base.branch):
            if repo.commit_for(base.branch) != repo.commit_for(parent_ref):
                raise Error(f"Local {base.branch} differs from {remote}/{base.branch}; sync it "
                            "as stack-prs § Syncing describes, then rerun")
            repo.checkout(base.branch)
            if self.stack_branches(repo) is not None:  # this clone already tracks its stack
                self.require_top(repo, base.branch)
                self.stack(repo, "add", branch)
                return
        else:
            repo.create_branch(base.branch, parent_ref)
        self.stack(repo, "init", base.branch, branch)

    def require_placement(self, repo, remote, base, branch, placed, on=None):
        """Checks out a reused branch, refusing it unless its history (`placed`) and its local
        gh stack both put it on the planned base: this run does not restack. `on` is the
        upstream master commit a master-based branch starts from, to tell a stale snapshot
        apart from a misplaced branch."""
        repo.checkout(branch)
        trunk, names = self.stack_view(repo) or (None, [])
        if placed and branch in names:
            below = names[:names.index(branch)]
            if base.stacked:
                on_base = bool(below) and below[-1] == base.branch
            else:
                on_base = not below and trunk == "master"
            if on_base:
                return
        remove = (f"remove it: on {branch} run `gh stack unstack --local`, switch to another "
                  f"branch, run `git branch -D {branch}`, then rerun")
        if placed and on is not None and not repo.ancestor(on, self.upstream_tip):
            raise Error(f"{branch} is built on {remote}/master, which is ahead of this run's "
                        f"upstream snapshot {UPSTREAM_REF}; rerun without --no-fetch. If it is "
                        f"still refused, {remove}")
        raise Error(f"{branch} exists but is not placed on {base.branch}; this run does not "
                    f"restack it. {remove[0].upper()}{remove[1:]}")

    def stack(self, repo, *args, check=True):
        return self.tool.run(["gh", "stack", *args], cwd=repo.path, check=check)

    def stack_view(self, repo):
        """(trunk, unmerged branches bottom up) of the current branch's local gh stack, or
        None when gh stack does not track it."""
        result = self.stack(repo, "view", "--json", check=False)
        if result.returncode:
            return None
        try:
            view = json.loads(result.stdout)
        except json.JSONDecodeError as error:
            raise Error("Failed to parse the output of `gh stack view --json`") from error
        return view.get("trunk"), [entry["name"] for entry in view.get("branches") or []
                                   if not entry.get("isMerged")]

    def stack_branches(self, repo):
        """The unmerged branches of the local gh stack of the current branch, or None."""
        view = self.stack_view(repo)
        return None if view is None else view[1]

    def require_top(self, repo, parent):
        names = self.stack_branches(repo) or []
        if parent not in names:
            raise Error(f"gh stack does not track {parent} in {repo.path}")
        above = names[names.index(parent) + 1:]
        if above:
            raise Error(f"{parent}'s GitHub stack has {', '.join(above)} above it: decide the "
                        "arrangement with the team, one chain in dependency order")

    def rewrite(self, repo, subject):
        current = repo.message("HEAD")
        wanted = commit_message(current, subject)
        if wanted.rstrip("\n") != current.rstrip("\n"):
            repo.amend_message(self.write("message.txt", wanted))

    def require_upgrade(self, repo, protos, upgrade):
        """Refuses a .proto change without upgrade notes before the test plan is asked for."""
        if protos and upgrade is None:
            if self.interactive:
                rerun = self.command(repo_path=repo.path, upgrade="upgrade.md")
            else:
                rerun = self.command(repo_path=repo.path, upgrade="upgrade.md",
                                     test_plan=self.args.test_plan or "testplan.md", yes=True)
            raise Error(f"The commit changes .proto files ({', '.join(protos)}); pass --upgrade "
                        f"<file> with upgrade and rollback notes. Rerun with them:\n  {rerun}")

    def test_plan(self, repo, branch):
        """Step 4's test plan, asked for; a non-interactive run stops here."""
        rerun = "  " + self.command(repo_path=repo.path, test_plan="testplan.md", yes=True)
        if not self.interactive:
            raise Stop("Write what ran to a file and rerun:", rerun)
        while True:
            answer = self.ask(PLAN_PROMPT)
            if answer in (None, "q"):
                raise Stop("Stopped before publishing. Write what ran to a file and rerun:", rerun)
            if answer == "e":
                path = self.edit(branch)
                if path:
                    return path
            elif answer:
                path = os.path.abspath(os.path.expanduser(answer))
                if os.path.isfile(path) and os.path.getsize(path):
                    return path
                self.say(f"{path} is not a file with a test plan in it.")

    def edit(self, branch):
        template = f"## What ran on {branch}\n\n- \n"
        path = self.write("testplan.md", template)
        editor = shlex.split(os.environ.get("EDITOR") or "vi")
        if self.tool.interactive([*editor, path]):
            self.say("The editor failed; nothing was saved.")
            return None
        if Path(path).read_text().strip() == template.strip():
            self.say("The test plan is unchanged; write what ran.")
            return None
        return path

    def existing_pull_request(self, repo, branch):
        """An open or landed upstream PR for the branch, to record instead of opening one; a
        restructured one is not live and gets a replacement, and a closed one is refused."""
        mine = re.compile(rf"^Shopify fork PR: {re.escape(FORK_PULL_URL)}{self.number}[ \t]*$",
                          re.MULTILINE)
        # --head also matches same-named branches in other repositories, which are not this
        # branch; a PR on this branch is this contribution's only when its body says so.
        prs = [pr for pr in self.github.branch_pull_requests(branch) if on_upstream(pr)]
        for pr in prs:
            if not mine.search(pr.get("body") or ""):
                raise Error(f"{branch} already has PR {pr.get('url')} for another change; pass "
                            "--branch <other name>")
        states = [(pr, upstream_state(pr, self.is_landed(repo))) for pr in prs]
        live = [pr for pr, state in states if state in ("open", "landed")]
        if live:
            return live[0]
        closed = [pr for pr, state in states if state == "closed"]
        if closed:
            raise Error(f"A closed upstream PR {closed[0].get('url')} exists for {branch}; pass "
                        "--branch to open a new one")
        return None

    def summary(self, section, base):
        text = link_fork_references(section) + f"\nShopify fork PR: {FORK_PULL_URL}{self.number}\n"
        if base.stacked:
            text += (f"Stacked on #{base.parent_view['number']}, based on {base.branch}, so the "
                     "diff is exactly this PR's commit.\n")
        return text

    # Phase B: public writes after one confirmation.

    def issue_lines(self, issue):
        if issue.number:
            lines = [f"  Upstream issue: reuse #{issue.number} ({issue.source})"]
            if issue.associate:
                lines.append(f'    and comment "Upstream issue: {UPSTREAM_ISSUE_URL}'
                             f'{issue.number}" on {FORK_REPO}#{issue.fork_issue}')
            if issue.link_private:
                lines.append(f"    and record it with issues.py link --issue "
                             f"{self.args.private_issue}")
            return lines
        labels = f"with labels {', '.join(issue.labels)}" if issue.labels else "with no labels"
        record = f'    then comment "Upstream issue: <url>" on {FORK_REPO}#{issue.fork_issue}'
        if self.args.private_issue:
            record += f" and record it with issues.py link --issue {self.args.private_issue}"
        return [f'  Upstream issue: create "{issue.title}" {labels}', record]

    def preview(self, repo, branch, sha, title, issue, base, summary_file, test_plan, upgrade):
        lines = [f"Ready to publish {branch} ({sha[:9]}) from {repo.path}:",
                 *self.issue_lines(issue)]
        number = f"#{issue.number}" if issue.number else "#?"
        note = "" if issue.number else " (the new upstream issue's number)"
        lines.append(f"  Pull request: [{number}] {title.component}: {title.description}{note}")
        stacked = f" (stacked on {base.parent_view['url']})" if base.stacked else ""
        lines += [f"    base: {base.branch}{stacked}", f"    summary: {summary_file}",
                  f"    test plan: {test_plan}"]
        if upgrade:
            lines.append(f"    upgrade notes: {upgrade}")
        if self.args.draft:
            lines.append("    opened as a draft")
        lines.append(f'  Record: comment "Upstream: <PR url>" on {FORK_REPO}#{self.number}')
        self.say(*lines)

    def preview_records(self, branch, issue, existing):
        self.say(f"{existing['url']} already exists for {branch}; only its records are left:",
                 *self.issue_lines(issue),
                 f'  Record: comment "Upstream: {existing["url"]}" on {FORK_REPO}#{self.number}')

    def confirm(self, repo, test_plan):
        if self.args.yes:
            return
        rerun = "  " + self.command(repo_path=repo.path, test_plan=test_plan, yes=True)
        if not self.interactive:
            raise Stop("To publish, rerun:", rerun)
        if (self.ask("Publish? [y/N] ") or "").lower() not in ("y", "yes"):
            raise Stop("Not published. To publish later:", rerun)

    def confirm_records(self, issue, existing):
        """Like confirm, but a stop prints the record commands themselves."""
        if self.args.yes:
            return
        commands = []
        if issue.associate:
            commands.append(shlex.join(
                ["gh", "issue", "comment", str(issue.fork_issue), "--repo", FORK_REPO, "--body",
                 f"Upstream issue: {UPSTREAM_ISSUE_URL}{issue.number}"]))
        if issue.link_private:
            commands.append(shlex.join(
                ["python3", str(ISSUES_SCRIPT), "link", "--issue", self.args.private_issue,
                 "--public-issue", UPSTREAM_ISSUE_URL + str(issue.number)]))
        commands.append(shlex.join(["gh", "pr", "comment", str(self.number), "--repo", FORK_REPO,
                                    "--body", f"Upstream: {existing['url']}"]))
        lines = [f"  {command}" for command in commands]
        if not self.interactive:
            raise Stop("To record it, run:", *lines)
        if (self.ask("Publish? [y/N] ") or "").lower() not in ("y", "yes"):
            raise Stop("Not recorded. To record it, run:", *lines)

    def publish(self, repo, branch, title, issue, landing, summary_file, test_plan, upgrade):
        # Before any write: the pause for the test plan may have changed the checkout.
        self.require_clean(repo)
        self.check_pick(repo, branch, title, issue, landing)
        number = issue.number
        if number is None:
            number = self.create_issue(issue)
        else:
            self.record_issue(issue)
        url = self.open_pull_request(repo, title, number, landing, summary_file, test_plan,
                                     upgrade)
        # A newest Upstream: comment naming an open or landed PR ended the run early, so this
        # PR is never the one already recorded.
        self.github.comment_on_pull_request(self.number, f"Upstream: {url}")
        if self.args.draft:
            self.say(f"Opened {url} as a draft; mark it ready with:",
                     f"  gh pr ready {upstream_number(url)} -R {UPSTREAM_REPO}")
        else:
            self.say(f"Opened {url}")

    def publish_records(self, issue, existing):
        """The records of a PR that already exists for the branch; its issue is known."""
        self.record_issue(issue)
        self.github.comment_on_pull_request(self.number, f"Upstream: {existing['url']}")
        self.say(f"Recorded {existing['url']}")

    def check_pick(self, repo, branch, title, issue, landing):
        """The pick's subject can be rewritten (it is HEAD) or already cites the issue: an amend
        would rewrite the commits above it."""
        pick = self.pick_commit(repo, landing)
        if pick == repo.commit_for("HEAD"):
            return
        if issue.number is None:
            raise Error(f"The cherry-pick of {landing[:9]} is not the top commit of {branch}, and "
                        "its subject must cite the upstream issue this run creates; remove the "
                        "commits above it and rerun, or create the issue first and pass "
                        "--upstream-issue")
        subject = f"[#{issue.number}] {title.component}: {title.description}"
        if repo.message(pick).split("\n", 1)[0] != subject:
            raise Error(f"The cherry-pick of {landing[:9]} is not the top commit of {branch} and "
                        f"its subject is not {subject}; reword it by hand, then rerun")

    def create_issue(self, issue):
        url = self.github.create_issue(issue.title, self.write("issue.md", issue.body),
                                       issue.labels)
        number = int(UPSTREAM_ISSUE.fullmatch(url)[1])
        self.say(f"Created {url}")
        rerun = f"rerun with `--upstream-issue {number}`"
        body = f"Upstream issue: {url}"
        try:  # the fork issue comment is the record a rerun reads first
            self.github.comment_on_issue(issue.fork_issue, body)
        except Error as error:
            record = shlex.join(["gh", "issue", "comment", str(issue.fork_issue), "--repo",
                                 FORK_REPO, "--body", body])
            raise Error(f"{str(error).rstrip()}\nCreated {url} but did not record it; record "
                        f"it with `{record}`, then {rerun}") from None
        if self.args.private_issue:
            try:
                self.link_private(url)
            except Error as error:
                raise Error(f"{str(error).rstrip()}\nCreated {url} but did not record it in the "
                            f"private tracker; {rerun}") from None
        return number

    def record_issue(self, issue):
        """The records of a reused upstream issue that are still missing."""
        url = UPSTREAM_ISSUE_URL + str(issue.number)
        if issue.associate:
            self.github.comment_on_issue(issue.fork_issue, f"Upstream issue: {url}")
        if issue.link_private:
            self.link_private(url)

    def link_private(self, url):
        self.tool.run([sys.executable, str(ISSUES_SCRIPT), "link", "--issue",
                       self.args.private_issue, "--public-issue", url])

    def open_pull_request(self, repo, title, number, landing, summary_file, test_plan, upgrade):
        if self.pick_commit(repo, landing) == repo.commit_for("HEAD"):
            self.rewrite(repo, f"[#{number}] {title.component}: {title.description}")
        argv = [os.path.join(repo.path, *CREATE_PR), "-i", str(number), "-t",
                f"{title.component}: {title.description}", "-d", summary_file, "-T", test_plan]
        if upgrade:
            argv += ["-U", upgrade]
        if self.args.draft:
            argv.append("-D")
        result = self.tool.run(argv, cwd=repo.path, echo=self.out, check=False)
        if result.returncode == 1 and self.restructured:
            raise Error("create-pr.sh refused (exit 1); its message above says why. A "
                        "replacement for a restructured upstream PR is opened by replaying "
                        "create-pr.sh's calls by hand, as shopify-upstream-pr Step 5 describes "
                        "for a restructured layer")
        if result.returncode:
            raise Error(CREATE_PR_FAILURES.get(
                result.returncode, f"create-pr.sh failed with exit {result.returncode}"))
        lines = [line for line in result.stdout.splitlines() if line.strip()]
        match = UPSTREAM_PULL.search(lines[-1]) if lines else None
        if match is None:
            raise Error("create-pr.sh succeeded but printed no pull request URL last")
        return match[0]

    def pick_commit(self, repo, landing):
        """The commit on HEAD, above upstream master, that cherry-picked `landing`."""
        for commit in repo.commits_between(self.upstream_tip, "HEAD"):
            if f"(cherry picked from commit {landing})" in repo.message(commit):
                return commit
        raise Error(f"HEAD in {repo.path} has no cherry-pick of {landing[:9]}")

    # Plumbing.

    def command(self, repo_path=None, clone_to=None, test_plan=None, upgrade=None, yes=False):
        """The rerun command line, with the developer's options and the given additions."""
        args = self.args
        argv = [*shlex.split(self.program), "submit", str(self.number)]
        if repo_path:
            argv += ["--repo-path", repo_path]
        elif clone_to:
            argv += ["--clone-to", clone_to]
        for parent in args.parent or []:
            argv += ["--parent", str(parent)]
        if args.upstream_issue:
            argv += ["--upstream-issue", str(args.upstream_issue)]
        if args.private_issue:
            argv += ["--private-issue", args.private_issue]
        if args.branch:
            argv += ["--branch", args.branch]
        if args.draft:
            argv.append("--draft")
        if args.fork_url != FORK_URL:
            argv += ["--fork-url", args.fork_url]
        if args.upstream_url != UPSTREAM_URL:
            argv += ["--upstream-url", args.upstream_url]
        if not args.fetch:
            argv.append("--no-fetch")
        upgrade = upgrade or args.upgrade
        if upgrade:
            argv += ["--upgrade", upgrade]
        test_plan = test_plan or args.test_plan
        if test_plan:
            argv += ["--test-plan", test_plan]
        if yes:
            argv.append("--yes")
        return shlex.join(argv)

    def workdir(self):
        if self.workdir_path is None:
            self.workdir_path = tempfile.mkdtemp(prefix="upstream-submit-")
        os.makedirs(self.workdir_path, exist_ok=True)
        return self.workdir_path

    def write(self, name, text):
        path = os.path.join(self.workdir(), name)
        Path(path).write_text(text, encoding="utf-8")
        return path

    def ask(self, prompt):
        self.out.write(prompt)
        self.out.flush()
        line = self.stdin.readline()
        return line.strip() if line else None

    def warn(self, line):
        self.say(line, stream=self.err)
