#!/usr/bin/env python3
"""Shopify issue intake and publication. Uses gh; requires Python 3.10+.

Run --help for the CLI. Semantic sanitization belongs to shopify-create-issue;
this tool accepts only its structured public draft. It never creates upstream
issues. Publication checkpoints live on the private issue so retries can find
a public creation even after a lost response or a failed private backlink.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import tempfile
from urllib.parse import urlsplit
import uuid

from issue_draft import (
    IssueError, PRIVATE_LABEL, PUBLIC_REPO, PUBLIC_REPOS,
    fingerprint, issue_ref, issue_url, render_draft, require_keys, text_field,
)


CHECKPOINT_PREFIX = "<!-- shopify-yb-publication:v1\n"
LINK_PREFIX = "<!-- shopify-yb-public-link:v1 -->\n"
REPO_ROOT = Path(__file__).resolve().parents[3]


class GitHubError(RuntimeError):
    """A GitHub request failed; write outcomes may be unknown."""


class GitHub:
    def request(self, path: str, method: str = "GET", data: dict | None = None,
                paginate: bool = False):
        command = ["gh", "api", "--hostname", "github.com", "--method", method, path]
        if paginate:
            if method != "GET":
                raise IssueError("Only reads can be paginated.")
            command += ["--paginate", "--slurp"]
        if data is not None:
            command += ["--input", "-"]
        try:
            result = subprocess.run(
                command, input=json.dumps(data) if data is not None else None,
                text=True, capture_output=True, timeout=120,
            )
        except (OSError, subprocess.TimeoutExpired) as error:
            raise GitHubError(
                f"GitHub {method} did not complete. Check gh authentication and connectivity."
            ) from error
        if result.returncode:
            # gh error bodies can include submitted private content. Keep them
            # out of diagnostics that an agent might copy into a public issue.
            raise GitHubError(
                f"GitHub {method} failed. Check gh authentication, access, and repository setup."
            )
        try:
            value = json.loads(result.stdout)
        except json.JSONDecodeError as error:
            raise GitHubError(f"GitHub {method} returned an unreadable response.") from error
        if paginate:
            if not isinstance(value, list) or any(not isinstance(page, list) for page in value):
                raise GitHubError("GitHub returned an unexpected paginated response.")
            return [item for page in value for item in page]
        return value


def public_marker(token: str) -> str:
    return f"<!-- shopify-yb-publication:{token} -->"


def checkpoint_body(record: dict) -> str:
    text = CHECKPOINT_PREFIX + json.dumps(record, sort_keys=True) + "\n-->\n\n"
    if record.get("url"):
        text += "Public issue: " + record["url"]
    else:
        text += "Public issue publication is pending. Rerun the issue tool to reconcile it."
    return text


def checkpoint(comments: list[dict]) -> tuple[dict | None, int | None]:
    records = []
    for comment in comments:
        body = comment.get("body") or ""
        if not body.startswith(CHECKPOINT_PREFIX):
            continue
        try:
            encoded, _ = body[len(CHECKPOINT_PREFIX):].split("\n-->", 1)
            record = json.loads(encoded)
            require_keys(record, {"token", "status", "digest", "template", "issue_type"},
                         {"url", "previous_digest"})
            if (not isinstance(record["token"], str)
                    or not re.fullmatch(r"[a-f0-9]{32}", record["token"])
                    or record["status"] not in ("pending", "published", "updating")
                    or not re.fullmatch(r"[a-f0-9]{64}", record["digest"])):
                raise ValueError
            if record["status"] in ("published", "updating"):
                issue_ref(record["url"], (PUBLIC_REPO,))
            if (record["status"] == "updating"
                    and not re.fullmatch(r"[a-f0-9]{64}", record["previous_digest"])):
                raise ValueError
            records.append((record, int(comment["id"])))
        except (ValueError, TypeError, KeyError) as error:
            raise IssueError(
                "The private publication checkpoint is malformed; reconcile it before publishing."
            ) from error
    if len(records) > 1:
        raise IssueError(
            "Multiple publication checkpoints exist. Reconcile them before publishing."
        )
    return records[0] if records else (None, None)


def linked_issues(comments: list[dict]) -> list[str]:
    links = []
    record, _ = checkpoint(comments)
    if record and record.get("url"):
        links.append(record["url"])
    for comment in comments:
        body = comment.get("body") or ""
        if body.startswith(LINK_PREFIX):
            repo, number = issue_ref(body[len(LINK_PREFIX):].strip(), PUBLIC_REPOS)
            links.append(issue_url(repo, number))
    return sorted(set(links))


def content_digest(issue: dict, token: str) -> str:
    suffix = "\n\n" + public_marker(token)
    body = issue.get("body") or ""
    if not body.endswith(suffix):
        raise IssueError(
            "The public issue no longer has its publication marker; reconcile it before updating."
        )
    return fingerprint({"title": issue["title"], "body": body[:-len(suffix)]})


def outside_checkout(path: Path) -> Path:
    path = path.expanduser().resolve()
    if (path.is_relative_to(REPO_ROOT)
            or any((parent / ".git").exists() for parent in (path, *path.parents))):
        raise IssueError("Keep private workspaces and state outside the public checkout.")
    return path


def secure_directory(path: Path) -> Path:
    path = outside_checkout(path)
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    if stat.S_IMODE(path.stat().st_mode) & 0o077:
        raise IssueError(
            "The state directory must be a dedicated private directory with mode 0700."
        )
    return path


def write_state(path: Path, value: dict) -> None:
    with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent, delete=False) as file:
        temporary = Path(file.name)
        try:
            json.dump(value, file)
            file.flush()
            os.fsync(file.fileno())
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)


class IssueWorkflow:
    def __init__(self, github: GitHub, private_repo: str, state_dir: Path,
                 incident_host: str = "vault.shopify.io"):
        if (not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", private_repo)
                or private_repo.casefold() in {repo.casefold() for repo in PUBLIC_REPOS}):
            raise IssueError("Configure a separate private issue repository.")
        self.github = github
        self.private_repo = private_repo
        self.state_dir = state_dir
        self.incident_host = incident_host

    @contextmanager
    def locked(self, key: str):
        directory = secure_directory(self.state_dir)
        path = directory / (fingerprint(key) + ".lock")
        descriptor = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
        with os.fdopen(descriptor, "w") as file:
            try:
                fcntl.flock(file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise IssueError(
                    "Another local operation is running for this issue; retry after it finishes."
                ) from error
            yield

    def repository(self, repo: str, private: bool) -> dict:
        result = self.github.request(f"repos/{repo}")
        visibility = "private" if private else "public"
        if (result.get("private") is not private or result.get("visibility") != visibility
                or result.get("full_name", "").casefold() != repo.casefold()
                or result.get("has_issues") is not True):
            raise IssueError(
                "Repository visibility or issue support does not match the required "
                f"{visibility} target."
            )
        return result

    def get_issue(self, url: str, repos: tuple[str, ...]) -> dict:
        repo, number = issue_ref(url, repos)
        result = self.github.request(f"repos/{repo}/issues/{number}")
        if "pull_request" in result or issue_ref(result.get("html_url"), repos) != (repo, number):
            raise IssueError("The reference is not an issue in the expected repository.")
        return result

    def comments(self, number: int) -> list[dict]:
        return self.github.request(
            f"repos/{self.private_repo}/issues/{number}/comments?per_page=100", paginate=True)

    def all_issues(self, repo: str) -> list[dict]:
        return self.github.request(f"repos/{repo}/issues?state=all&per_page=100", paginate=True)

    def private_source(self, url: str) -> tuple[dict, list[dict], str]:
        self.repository(self.private_repo, True)
        _, number = issue_ref(url, (self.private_repo,))
        issue = self.get_issue(url, (self.private_repo,))
        comments = self.comments(number)
        source = {
            "title": issue["title"], "body": issue.get("body") or "",
            "comments": [{"id": comment["id"], "body": comment.get("body") or ""}
                         for comment in comments
                         if not (comment.get("body") or "").startswith(
                             (CHECKPOINT_PREFIX, LINK_PREFIX))],
        }
        return issue, comments, fingerprint(source)

    def inspect(self, url: str) -> dict:
        issue, comments, digest = self.private_source(url)
        return {
            "issue": issue["html_url"], "source_digest": digest,
            "title": issue["title"], "body": issue.get("body") or "",
            "comments": [{"id": comment["id"], "body": comment.get("body") or ""}
                         for comment in comments
                         if not (comment.get("body") or "").startswith(
                             (CHECKPOINT_PREFIX, LINK_PREFIX))],
            "public_issues": linked_issues(comments),
        }

    def ensure_labels(self, repo: str, labels: list[str]) -> None:
        existing = {item["name"] for item in self.github.request(
            f"repos/{repo}/labels?per_page=100", paginate=True)}
        for label in labels:
            if label not in existing:
                if repo != PUBLIC_REPO:
                    raise IssueError(
                        f"The private tracker must have the {PRIVATE_LABEL} label configured."
                    )
                self.github.request(f"repos/{repo}/labels", "POST", {
                    "name": label, "color": "1d76db",
                    "description": "Yugabyte contribution issue classification.",
                })

    def label_private(self, issue: dict) -> None:
        self.ensure_labels(self.private_repo, [PRIVATE_LABEL])
        if PRIVATE_LABEL not in {label["name"] for label in issue.get("labels", [])}:
            self.github.request(f"repos/{self.private_repo}/issues/{issue['number']}/labels",
                                "POST", {"labels": [PRIVATE_LABEL]})

    def validate_public_reference(self, url: str) -> dict:
        repo, _ = issue_ref(url, PUBLIC_REPOS)
        self.repository(repo, False)
        return self.get_issue(url, PUBLIC_REPOS)

    def intake(self, data: dict, preview: bool = False, retry_create: bool = False) -> dict:
        require_keys(data, {"request_id", "title", "body", "incidents"}, {"public_issues"})
        request_id = data["request_id"]
        if not isinstance(request_id, str) or not re.fullmatch(r"[a-f0-9]{32}", request_id):
            raise IssueError(
                "request_id must be the 32-character ID returned by workspace; reuse it on retries."
            )
        title = text_field(data["title"], "title")
        body = text_field(data["body"], "body")
        if len(title) > 240 or "\n" in title or "\r" in title:
            raise IssueError("Use a single-line private title of at most 240 characters.")
        if not isinstance(data["incidents"], list):
            raise IssueError("incidents must be a list of Vault incident URLs.")
        for url in data["incidents"]:
            try:
                parsed = urlsplit(url)
                valid = (parsed.scheme == "https" and parsed.hostname == self.incident_host
                         and not parsed.username and not parsed.password
                         and bool(parsed.path.strip("/"))
                         and not any(char.isspace() for char in url))
            except (ValueError, TypeError):
                valid = False
            if not valid:
                raise IssueError(
                    "Incident references must be HTTPS links to the configured Vault host."
                )
        links = data.get("public_issues", [])
        if not isinstance(links, list):
            raise IssueError("public_issues must be a list of public issue URLs.")
        self.repository(self.private_repo, True)
        for url in links:
            self.validate_public_reference(url)
        marker = f"<!-- shopify-yb-intake:{request_id} -->"
        body = ("## Problem and impact\n\n" + body
                + "\n\n## Relevant Vault incidents\n\n"
                + ("\n".join("- " + url for url in data["incidents"]) or "None reported.")
                + "\n\n## Public issues\n\n"
                + ("\n".join("- " + url for url in links) or "Publication pending.")
                + "\n\n" + marker)
        if len(body.encode()) > 60000:
            raise IssueError(
                "The private issue is too large; link supporting evidence instead of embedding it."
            )
        payload = {"title": title, "body": body, "labels": [PRIVATE_LABEL]}
        if preview:
            return {"preview": True, "repository": self.private_repo, "issue": payload,
                    "source_digest": fingerprint({"title": title, "body": body, "comments": []})}
        with self.locked("intake:" + self.private_repo + ":" + request_id):
            matches = [issue for issue in self.all_issues(self.private_repo)
                       if "pull_request" not in issue and marker in (issue.get("body") or "")]
            if len(matches) > 1:
                raise IssueError(
                    "Multiple private issues have this intake ID; reconcile them before continuing."
                )
            if matches:
                issue = self.get_issue(matches[0]["html_url"], (self.private_repo,))
                self.label_private(issue)
                for url in links:
                    self.link(issue["html_url"], url)
                return {"private_issue": issue["html_url"], "created": False}
            state_path = self.state_dir.expanduser().resolve() / (
                fingerprint([self.private_repo, request_id]) + ".json")
            if state_path.exists() and not retry_create:
                raise IssueError(
                    "An earlier private create has an unknown outcome. "
                    "Inspect GitHub before explicitly retrying creation."
                )
            self.ensure_labels(self.private_repo, [PRIVATE_LABEL])
            write_state(state_path, {"status": "posting", "request_id": request_id})
            issue = self.github.request(f"repos/{self.private_repo}/issues", "POST", payload)
            _, number = issue_ref(issue.get("html_url"), (self.private_repo,))
            write_state(state_path, {
                "status": "created", "url": issue_url(self.private_repo, number)
            })
            for url in links:
                self.link(issue["html_url"], url)
            return {"private_issue": issue["html_url"], "created": True}

    def link(self, private_url: str, public_url: str, preview: bool = False) -> dict:
        issue, comments, _ = self.private_source(private_url)
        public = self.validate_public_reference(public_url)
        canonical = public["html_url"]
        if preview:
            return {"preview": True, "private_issue": issue["html_url"], "public_issue": canonical}
        self.label_private(issue)
        if canonical not in linked_issues(comments):
            self.github.request(f"repos/{self.private_repo}/issues/{issue['number']}/comments",
                                "POST", {"body": LINK_PREFIX + canonical})
        return {"private_issue": issue["html_url"], "public_issue": canonical}

    def save_checkpoint(self, comment_id: int, record: dict) -> None:
        self.github.request(f"repos/{self.private_repo}/issues/comments/{comment_id}",
                            "PATCH", {"body": checkpoint_body(record)})

    def finish_publication(self, comment_id: int, record: dict, url: str) -> dict:
        record = record | {"url": url, "status": "published"}
        record.pop("previous_digest", None)
        try:
            self.save_checkpoint(comment_id, record)
        except GitHubError as error:
            raise IssueError(
                f"Public issue exists at {url}, but private tracking did not complete. "
                "Rerun with the same draft to recover the backlink."
            ) from error
        return {"public_issue": url, "status": "published"}

    def publish(self, private_url: str, draft: dict, preview: bool = False,
                update: bool = False, retry_create: bool = False) -> dict:
        private_url = issue_url(*issue_ref(private_url, (self.private_repo,)))
        # Preview is read-only, including local journals and lock files.
        if preview:
            return self._publish(private_url, draft, preview=True, update=update)
        with self.locked("publish:" + private_url.casefold()):
            return self._publish(private_url, draft, update=update, retry_create=retry_create)

    def _publish(self, private_url: str, draft: dict, preview: bool = False,
                 update: bool = False, retry_create: bool = False) -> dict:
        issue, comments, source_digest = self.private_source(private_url)
        payload = render_draft(draft, self.private_repo)
        if draft["source_digest"] != source_digest:
            raise IssueError(
                "The private source changed. Inspect it again and regenerate the sanitized draft."
            )
        self.repository(PUBLIC_REPO, False)
        record, comment_id = checkpoint(comments)
        linked_fork = [url for url in linked_issues(comments)
                       if issue_ref(url, PUBLIC_REPOS)[0] == PUBLIC_REPO]
        if len(linked_fork) > 1:
            raise IssueError(
                "Multiple public fork issues are linked. "
                "Choose the intended issue before publishing."
            )
        if preview:
            return {"preview": True, "repository": PUBLIC_REPO, "issue": payload,
                    "existing_public_issue": linked_fork[0] if linked_fork else None}
        if not record and linked_fork:
            public = self.validate_public_reference(linked_fork[0])
            if update:
                raise IssueError(
                    "This public issue was linked, not created by this tool; "
                    "edit it through its owning workflow."
                )
            self.label_private(issue)
            return {"public_issue": public["html_url"], "status": "reused"}
        digest = fingerprint({key: payload[key] for key in ("title", "body")})
        fresh = record is None
        if record:
            if (record["template"] != draft["template"]
                    or record["issue_type"] != draft.get("issue_type")):
                raise IssueError(
                    "Changing the public issue template or type "
                    "requires explicit label reconciliation."
                )
            if record["status"] != "published" and record["digest"] != digest:
                raise IssueError(
                    "An operation is pending with a different public draft; "
                    "recover that operation first."
                )
        self.label_private(issue)
        if fresh:
            self.ensure_labels(PUBLIC_REPO, payload["labels"])
            record = {"token": uuid.uuid4().hex, "status": "pending", "digest": digest,
                      "template": draft["template"], "issue_type": draft.get("issue_type")}
            comment = self.github.request(
                f"repos/{self.private_repo}/issues/{issue['number']}/comments",
                "POST", {"body": checkpoint_body(record)})
            comment_id = comment["id"]
            # A competing publisher must be resolved before either creates more work.
            current, current_id = checkpoint(self.comments(issue["number"]))
            if current != record or current_id != comment_id:
                raise IssueError(
                    "Publication ownership changed; "
                    "reconcile the private checkpoint before retrying."
                )
        suffix = "\n\n" + public_marker(record["token"])
        public_payload = payload | {"body": payload["body"] + suffix}
        if record["status"] == "published":
            public = self.get_issue(record["url"], (PUBLIC_REPO,))
            actual_digest = content_digest(public, record["token"])
            if record["digest"] == digest:
                return {"public_issue": record["url"], "status": "reused",
                        "public_content_changed": actual_digest != digest}
            if not update:
                raise IssueError(
                    "A public issue already exists. "
                    "Use --update for an intentional change to its text."
                )
            if actual_digest != record["digest"]:
                raise IssueError(
                    "The public issue was edited independently; "
                    "reconcile those edits before updating."
                )
            record = record | {
                "status": "updating", "previous_digest": record["digest"], "digest": digest
            }
            self.save_checkpoint(comment_id, record)
        if record["status"] == "updating":
            public = self.get_issue(record["url"], (PUBLIC_REPO,))
            actual_digest = content_digest(public, record["token"])
            if actual_digest not in (record["digest"], record["previous_digest"]):
                raise IssueError(
                    "The public issue was edited independently; "
                    "reconcile those edits before updating."
                )
            if actual_digest != record["digest"]:
                _, number = issue_ref(record["url"], (PUBLIC_REPO,))
                self.github.request(f"repos/{PUBLIC_REPO}/issues/{number}", "PATCH",
                                    {key: public_payload[key] for key in ("title", "body")})
            return self.finish_publication(comment_id, record, record["url"])
        matches = [item for item in self.all_issues(PUBLIC_REPO)
                   if "pull_request" not in item
                   and public_marker(record["token"]) in (item.get("body") or "")]
        if len(matches) > 1:
            raise IssueError(
                "Multiple public issues have the publication marker; "
                "reconcile them before continuing."
            )
        if matches:
            public = self.get_issue(matches[0]["html_url"], (PUBLIC_REPO,))
            if content_digest(public, record["token"]) != digest:
                raise IssueError(
                    "The recovered public issue differs from the pending draft; "
                    "reconcile it before continuing."
                )
            url = public["html_url"]
        else:
            if not fresh and not retry_create:
                raise IssueError(
                    "An earlier public create has an unknown outcome "
                    "and no matching issue is visible. "
                    "Inspect GitHub before explicitly retrying creation; "
                    "do not automatically use --retry-create."
                )
            public = self.github.request(f"repos/{PUBLIC_REPO}/issues", "POST", public_payload)
            _, number = issue_ref(public.get("html_url"), (PUBLIC_REPO,))
            url = issue_url(PUBLIC_REPO, number)
        return self.finish_publication(comment_id, record, url)


def load_input(path: str) -> dict:
    source = outside_checkout(Path(path))
    try:
        return json.loads(source.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise IssueError("Could not read the JSON input file.") from error


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    state_root = Path(os.environ.get("XDG_STATE_HOME", "~/.local/state")).expanduser()
    result.add_argument("--private-repo", default=os.environ.get("SHOPIFY_YB_PRIVATE_REPO"),
                        help="Private tracker owner/repo; defaults to SHOPIFY_YB_PRIVATE_REPO.")
    result.add_argument("--state-dir", type=Path,
                        default=state_root / "shopify-yb-contrib",
                        help="Dedicated private directory outside the checkout (0700).")
    commands = result.add_subparsers(dest="command", required=True)
    commands.add_parser("workspace", help="Create a private workspace for agent inputs and drafts.")
    validate = commands.add_parser(
        "validate", help="Render and validate a draft without publishing or contacting GitHub.")
    validate.add_argument("--draft", required=True)
    intake = commands.add_parser("intake", help="Create or recover a private tracking issue.")
    intake.add_argument("--input", required=True)
    intake.add_argument("--preview", action="store_true")
    intake.add_argument("--retry-create", action="store_true",
                        help="Only after a person verifies an ambiguous create did not succeed.")
    inspect = commands.add_parser("inspect", help="Read private evidence and its source digest.")
    inspect.add_argument("--issue", required=True)
    publish = commands.add_parser(
        "publish", help="Validate and publish a sanitized draft to the Shopify fork.")
    publish.add_argument("--issue", required=True)
    publish.add_argument("--draft", required=True)
    publish.add_argument("--preview", action="store_true")
    publish.add_argument("--update", action="store_true")
    publish.add_argument("--retry-create", action="store_true",
                         help="Only after a person verifies an ambiguous create did not succeed.")
    link = commands.add_parser(
        "link", help="Record an existing public issue in the private tracker.")
    link.add_argument("--issue", required=True)
    link.add_argument("--public-issue", required=True)
    link.add_argument("--preview", action="store_true")
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        if args.command == "workspace":
            state = secure_directory(args.state_dir)
            request_id = uuid.uuid4().hex
            directory = state / request_id
            directory.mkdir(mode=0o700)
            output = {"directory": str(directory), "request_id": request_id}
        else:
            if not args.private_repo:
                raise IssueError(
                    "Set SHOPIFY_YB_PRIVATE_REPO to the team's private issue repository."
                )
            workflow = IssueWorkflow(GitHub(), args.private_repo, args.state_dir)
            if args.command == "validate":
                output = {"preview": True, "repository": PUBLIC_REPO, "source_verified": False,
                          "issue": render_draft(load_input(args.draft), args.private_repo)}
            elif args.command == "intake":
                output = workflow.intake(load_input(args.input), args.preview, args.retry_create)
            elif args.command == "inspect":
                output = workflow.inspect(args.issue)
            elif args.command == "publish":
                output = workflow.publish(args.issue, load_input(args.draft), args.preview,
                                          args.update, args.retry_create)
            else:
                output = workflow.link(args.issue, args.public_issue, args.preview)
        print(json.dumps(output, indent=2, ensure_ascii=False))
        return 0
    except (IssueError, GitHubError, OSError) as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
