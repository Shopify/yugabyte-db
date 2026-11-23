"""Exercise the workflow against an in-memory GitHub issue store.

The store implements the REST operations used by the real gh transport, including
committed writes with lost responses. Tests never call GitHub or use real issues.
"""

from copy import deepcopy
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch
from urllib.parse import urlsplit

from issue_draft import IssueError, PRIVATE_LABEL, PUBLIC_REPO, UPSTREAM_REPO, issue_url
from issues import (
    GitHub, GitHubError, IssueWorkflow, checkpoint,
    load_input, main, secure_directory,
)
from test_issue_draft import PRIVATE_REPO, public_draft


class InMemoryGitHub(GitHub):
    def __init__(self):
        self.repositories = {
            repo: {"full_name": repo, "private": private,
                   "visibility": "private" if private else "public", "has_issues": True}
            for repo, private in (
                (PRIVATE_REPO, True), (PUBLIC_REPO, False), (UPSTREAM_REPO, False))
        }
        self.issues = {repo: {} for repo in self.repositories}
        self.issue_comments = {repo: {} for repo in self.repositories}
        self.labels = {repo: {} for repo in self.repositories}
        self.labels[PRIVATE_REPO][PRIVATE_LABEL] = {"name": PRIVATE_LABEL}
        self.requests = []
        self.failures = []
        self.next_comment = 1

    def seed_issue(self, repo, title="Synthetic private report",
                   body="Evidence for a test case.", labels=()):
        number = len(self.issues[repo]) + 1
        issue = {"number": number, "html_url": issue_url(repo, number), "title": title,
                 "body": body, "state": "open", "labels": [{"name": label} for label in labels]}
        self.issues[repo][number] = issue
        self.issue_comments[repo][number] = []
        return deepcopy(issue)

    def fail_next(self, method, path, after=False, when=lambda data: True):
        self.failures.append((method, path, after, when))

    def request(self, path, method="GET", data=None, paginate=False):
        self.requests.append((method, path, deepcopy(data), paginate))
        failure = next((item for item in self.failures
                        if item[0] == method and item[1] == path and item[3](data)), None)
        if failure:
            self.failures.remove(failure)
            if not failure[2]:
                raise GitHubError("Simulated unavailable GitHub response.")
        result = self.handle(urlsplit(path).path.split("/"), method, data)
        if failure:
            raise GitHubError("Simulated lost GitHub response after committing the write.")
        return deepcopy(result)

    def handle(self, parts, method, data):
        assert parts[0] == "repos", parts
        repo = "/".join(parts[1:3])
        resource = parts[3:]
        if not resource and method == "GET":
            return self.repositories[repo]
        if resource == ["labels"]:
            if method == "GET":
                return list(self.labels[repo].values())
            assert method == "POST"
            self.labels[repo][data["name"]] = data
            return data
        if resource == ["issues"]:
            if method == "GET":
                return list(self.issues[repo].values())
            assert method == "POST"
            assert set(data["labels"]) <= self.labels[repo].keys()
            return self.seed_issue(repo, data["title"], data["body"], data["labels"])
        if resource[:2] == ["issues", "comments"]:
            assert method == "PATCH"
            for comments in self.issue_comments[repo].values():
                for comment in comments:
                    if comment["id"] == int(resource[2]):
                        comment.update(data)
                        return comment
            raise AssertionError("Unknown comment")
        assert resource[0] == "issues", resource
        number = int(resource[1])
        issue = self.issues[repo][number]
        if len(resource) == 2:
            if method == "GET":
                return issue
            assert method == "PATCH"
            issue.update(data)
            return issue
        if resource[2] == "comments":
            comments = self.issue_comments[repo][number]
            if method == "GET":
                return comments
            assert method == "POST"
            comment = {"id": self.next_comment, "body": data["body"]}
            self.next_comment += 1
            comments.append(comment)
            return comment
        if resource[2] == "labels":
            assert method == "POST"
            existing = {label["name"] for label in issue["labels"]}
            issue["labels"] += [{"name": name} for name in data["labels"] if name not in existing]
            return issue["labels"]
        raise AssertionError((method, parts))

    def writes(self, repo=None, method=None, suffix=None):
        return [request for request in self.requests
                if request[0] != "GET"
                and (repo is None or request[1].startswith(f"repos/{repo}/"))
                and (method is None or request[0] == method)
                and (suffix is None or request[1] == f"repos/{repo}/{suffix}")]


class IssueWorkflowTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.state = Path(self.temporary.name) / "state"
        self.github = InMemoryGitHub()
        self.workflow = IssueWorkflow(self.github, PRIVATE_REPO, self.state)
        self.private = self.github.seed_issue(
            PRIVATE_REPO,
            body="Private evidence involving tenant_orders.\n"
            "Incident: https://vault.shopify.io/incidents/synthetic-example")
        self.url = self.private["html_url"]

    def draft(self, **changes):
        return public_draft(
            source_digest=self.workflow.inspect(self.url)["source_digest"], **changes
        )

    def intake_data(self, **changes):
        return {
            "request_id": "b" * 32, "title": "Synthetic intake case",
            "body": "Problem and evidence.",
            "incidents": ["https://vault.shopify.io/incidents/synthetic-example"],
        } | changes

    def record(self):
        return checkpoint(self.github.issue_comments[PRIVATE_REPO][self.private["number"]])

    def public_issue(self):
        return next(iter(self.github.issues[PUBLIC_REPO].values()))

    def test_intake_preserves_private_evidence_and_reuses_its_request_id(self):
        result = self.workflow.intake(self.intake_data())
        self.assertTrue(result["created"])
        private = self.github.issues[PRIVATE_REPO][2]
        self.assertEqual([{"name": PRIVATE_LABEL}], private["labels"])
        self.assertIn("https://vault.shopify.io/incidents/synthetic-example", private["body"])
        self.assertEqual({"private_issue": result["private_issue"], "created": False},
                         self.workflow.intake(self.intake_data()))
        self.assertEqual(1, len(self.github.writes(PRIVATE_REPO, "POST", "issues")))
        self.assertEqual([], self.github.writes(PUBLIC_REPO))

    def test_intake_recovers_a_committed_create_with_a_lost_response(self):
        self.github.fail_next("POST", f"repos/{PRIVATE_REPO}/issues", after=True)
        with self.assertRaises(GitHubError):
            self.workflow.intake(self.intake_data())
        self.assertEqual({"private_issue": issue_url(PRIVATE_REPO, 2), "created": False},
                         self.workflow.intake(self.intake_data()))
        self.assertEqual(1, len(self.github.writes(PRIVATE_REPO, "POST", "issues")))

    def test_intake_does_not_retry_an_ambiguous_create_without_explicit_recovery(self):
        self.github.fail_next("POST", f"repos/{PRIVATE_REPO}/issues")
        with self.assertRaises(GitHubError):
            self.workflow.intake(self.intake_data())
        with self.assertRaises(IssueError) as raised:
            self.workflow.intake(self.intake_data())
        self.assertEqual(
            "An earlier private create has an unknown outcome. "
            "Inspect GitHub before explicitly retrying creation.",
            str(raised.exception),
        )
        self.assertTrue(self.workflow.intake(self.intake_data(), retry_create=True)["created"])
        self.assertEqual(2, len(self.github.issues[PRIVATE_REPO]))

    def test_preview_performs_no_remote_or_local_writes(self):
        self.assertTrue(self.workflow.intake(self.intake_data(), preview=True)["preview"])
        preview = self.workflow.publish(self.url, self.draft(), preview=True)
        self.assertTrue(preview["preview"])
        self.assertEqual(PUBLIC_REPO, preview["repository"])
        self.assertEqual([], self.github.writes())
        self.assertFalse(self.state.exists())

    def test_a_new_intake_preview_can_be_validated_offline_without_publishing(self):
        intake = self.workflow.intake(self.intake_data(), preview=True)
        draft = public_draft(source_digest=intake["source_digest"])
        path = Path(self.temporary.name) / "public.json"
        path.write_text(json.dumps(draft))
        output = io.StringIO()
        with patch.object(GitHub, "request", side_effect=AssertionError("No network expected")):
            with redirect_stdout(output):
                status = main(["--private-repo", PRIVATE_REPO, "validate", "--draft", str(path)])
        self.assertEqual(0, status)
        self.assertFalse(json.loads(output.getvalue())["source_verified"])
        self.assertEqual([], self.github.writes())
        self.assertFalse(self.state.exists())

    def test_intake_retry_repairs_public_links_after_a_partial_failure(self):
        upstream = self.github.seed_issue(UPSTREAM_REPO)
        data = self.intake_data(public_issues=[upstream["html_url"]])
        self.github.fail_next("POST", f"repos/{PRIVATE_REPO}/issues/2/comments")
        with self.assertRaises(GitHubError):
            self.workflow.intake(data)
        result = self.workflow.intake(data)
        self.assertFalse(result["created"])
        self.assertEqual(
            [upstream["html_url"]], self.workflow.inspect(result["private_issue"])["public_issues"]
        )
        self.assertEqual(1, len(self.github.writes(PRIVATE_REPO, "POST", "issues")))

    def test_private_intake_fails_if_the_configured_repository_is_public(self):
        self.github.repositories[PRIVATE_REPO].update(private=False, visibility="public")
        with self.assertRaises(IssueError) as raised:
            self.workflow.intake(self.intake_data())
        self.assertEqual(
            "Repository visibility or issue support does not match the required private target.",
            str(raised.exception),
        )
        self.assertEqual([], self.github.writes())

    def test_publication_fails_if_the_fork_visibility_is_not_public(self):
        draft = self.draft()
        self.github.repositories[PUBLIC_REPO].update(private=True, visibility="private")
        with self.assertRaises(IssueError):
            self.workflow.publish(self.url, draft)
        self.assertEqual([], self.github.writes())

    def test_invalid_drafts_do_not_write_labels_checkpoints_or_issues(self):
        with self.assertRaises(IssueError):
            self.workflow.publish(self.url, self.draft(description="SELECT * FROM tenant_orders;"))
        self.assertEqual([], self.github.writes())

    def test_source_changes_require_a_new_sanitized_draft(self):
        draft = self.draft()
        self.github.issues[PRIVATE_REPO][1]["body"] += "\nNew incident evidence."
        with self.assertRaises(IssueError) as raised:
            self.workflow.publish(self.url, draft)
        self.assertEqual(
            "The private source changed. Inspect it again and regenerate the sanitized draft.",
            str(raised.exception),
        )
        self.assertEqual([], self.github.writes())

    def test_publication_creates_only_a_sanitized_fork_issue_and_a_private_backlink(self):
        result = self.workflow.publish(self.url, self.draft())
        self.assertEqual({"public_issue": issue_url(PUBLIC_REPO, 1), "status": "published"}, result)
        public = self.public_issue()
        self.assertIn("CREATE TABLE t1", public["body"])
        self.assertIn("Proposed synthetic reproduction (not yet executed)", public["body"])
        self.assertNotIn("tenant_orders", public["body"])
        self.assertNotIn(PRIVATE_REPO, public["body"])
        self.assertNotIn("vault.shopify.io", public["body"])
        self.assertNotIn(self.url, public["body"])
        self.assertEqual([], self.github.writes(UPSTREAM_REPO))
        record, _ = self.record()
        self.assertEqual(result["public_issue"], record["url"])
        self.assertEqual("published", record["status"])
        self.assertEqual([{"name": PRIVATE_LABEL}], self.github.issues[PRIVATE_REPO][1]["labels"])

    def test_repeating_publication_keeps_one_public_issue_and_a_stable_source_digest(self):
        draft = self.draft()
        self.workflow.publish(self.url, draft)
        self.assertEqual(draft["source_digest"], self.workflow.inspect(self.url)["source_digest"])
        result = self.workflow.publish(self.url, draft)
        self.assertEqual("reused", result["status"])
        self.assertFalse(result["public_content_changed"])
        self.assertEqual(1, len(self.github.writes(PUBLIC_REPO, "POST", "issues")))

    def test_failed_private_backlink_recovers_without_creating_another_public_issue(self):
        self.github.fail_next("PATCH", f"repos/{PRIVATE_REPO}/issues/comments/1")
        draft = self.draft()
        with self.assertRaises(IssueError) as raised:
            self.workflow.publish(self.url, draft)
        self.assertEqual(
            f"Public issue exists at {issue_url(PUBLIC_REPO, 1)}, "
            "but private tracking did not complete. "
            "Rerun with the same draft to recover the backlink.", str(raised.exception))
        # Recovery also works from a different local state directory.
        resumed = IssueWorkflow(
            self.github, PRIVATE_REPO, Path(self.temporary.name) / "other-state"
        )
        self.assertEqual("published", resumed.publish(self.url, draft)["status"])
        self.assertEqual(1, len(self.github.writes(PUBLIC_REPO, "POST", "issues")))

    def test_public_create_with_a_lost_response_is_recovered_by_opaque_marker(self):
        self.github.fail_next("POST", f"repos/{PUBLIC_REPO}/issues", after=True)
        draft = self.draft()
        with self.assertRaises(GitHubError):
            self.workflow.publish(self.url, draft)
        self.assertEqual("published", self.workflow.publish(self.url, draft)["status"])
        self.assertEqual(1, len(self.github.writes(PUBLIC_REPO, "POST", "issues")))

    def test_an_unobserved_create_is_not_automatically_reissued(self):
        self.github.fail_next("POST", f"repos/{PUBLIC_REPO}/issues")
        draft = self.draft()
        with self.assertRaises(GitHubError):
            self.workflow.publish(self.url, draft)
        with self.assertRaises(IssueError) as raised:
            self.workflow.publish(self.url, draft)
        self.assertEqual(
            "An earlier public create has an unknown outcome and no matching issue is visible. "
            "Inspect GitHub before explicitly retrying creation; "
            "do not automatically use --retry-create.",
            str(raised.exception),
        )
        self.assertEqual(1, len(self.github.writes(PUBLIC_REPO, "POST", "issues")))
        self.assertEqual(
            "published", self.workflow.publish(self.url, draft, retry_create=True)["status"]
        )
        self.assertEqual(1, len(self.github.issues[PUBLIC_REPO]))

    def test_recovery_rejects_an_altered_pending_draft(self):
        self.github.fail_next("POST", f"repos/{PUBLIC_REPO}/issues", after=True)
        with self.assertRaises(GitHubError):
            self.workflow.publish(self.url, self.draft())
        with self.assertRaises(IssueError) as raised:
            self.workflow.publish(
                self.url, self.draft(description="A different public description.")
            )
        self.assertEqual(
            "An operation is pending with a different public draft; recover that operation first.",
            str(raised.exception),
        )
        self.assertEqual(1, len(self.github.issues[PUBLIC_REPO]))

    def test_existing_upstream_issues_are_linked_privately_without_creating_a_fork_issue(self):
        upstream = self.github.seed_issue(UPSTREAM_REPO)
        digest = self.workflow.inspect(self.url)["source_digest"]
        self.workflow.link(self.url, upstream["html_url"])
        self.workflow.link(self.url, upstream["html_url"])
        self.assertEqual([upstream["html_url"]], self.workflow.inspect(self.url)["public_issues"])
        self.assertEqual(digest, self.workflow.inspect(self.url)["source_digest"])
        self.assertEqual(1, len(self.github.writes(PRIVATE_REPO, "POST", "issues/1/comments")))
        self.assertEqual([], self.github.writes(UPSTREAM_REPO))
        self.assertEqual([], self.github.writes(PUBLIC_REPO))

    def test_a_linked_existing_fork_issue_is_reused_without_claiming_edit_ownership(self):
        public = self.github.seed_issue(PUBLIC_REPO)
        self.workflow.link(self.url, public["html_url"])
        self.assertEqual({"public_issue": public["html_url"], "status": "reused"},
                         self.workflow.publish(self.url, self.draft()))
        with self.assertRaises(IssueError):
            self.workflow.publish(self.url, self.draft(), update=True)
        self.assertEqual([], self.github.writes(PUBLIC_REPO))

    def test_public_references_cannot_be_pull_requests_or_private_issues(self):
        public = self.github.seed_issue(PUBLIC_REPO)
        self.github.issues[PUBLIC_REPO][1]["pull_request"] = {"url": "example"}
        for url in (public["html_url"], self.url):
            with self.subTest(url=url), self.assertRaises(IssueError):
                self.workflow.link(self.url, url)
        self.assertEqual([], self.github.writes())

    def test_updates_are_validated_and_preserve_independently_assigned_labels(self):
        self.workflow.publish(self.url, self.draft())
        self.public_issue()["labels"].append({"name": "maintainer-priority"})
        description = "An expanded public description with a proposed synthetic reproducer."
        updated = self.draft(description=description)
        with self.assertRaises(IssueError):
            self.workflow.publish(self.url, updated)
        self.workflow.publish(self.url, updated, update=True)
        self.assertIn(description, self.public_issue()["body"])
        self.assertIn({"name": "maintainer-priority"}, self.public_issue()["labels"])
        with self.assertRaises(IssueError):
            self.workflow.publish(self.url, self.draft(description="tenant_orders"), update=True)
        self.assertIn(description, self.public_issue()["body"])
        self.assertEqual(1, len(self.github.writes(PUBLIC_REPO, "POST", "issues")))

    def test_failed_private_update_tracking_recovers_after_the_public_update(self):
        self.workflow.publish(self.url, self.draft())
        self.github.fail_next(
            "PATCH", f"repos/{PRIVATE_REPO}/issues/comments/1",
            when=lambda data: '"status": "published"' in data["body"],
        )
        updated = self.draft(description="Additional public reproduction details.")
        with self.assertRaises(IssueError):
            self.workflow.publish(self.url, updated, update=True)
        self.assertEqual(
            "published", self.workflow.publish(self.url, updated, update=True)["status"]
        )
        self.assertEqual(1, len(self.github.writes(PUBLIC_REPO, "PATCH", "issues/1")))

    def test_updates_do_not_overwrite_independent_public_edits(self):
        self.workflow.publish(self.url, self.draft())
        public = self.public_issue()
        public["title"] = "A maintainer edited this title"
        with self.assertRaises(IssueError) as raised:
            self.workflow.publish(
                self.url, self.draft(description="Updated public description."), update=True
            )
        self.assertEqual(
            "The public issue was edited independently; reconcile those edits before updating.",
            str(raised.exception),
        )
        self.assertEqual("A maintainer edited this title", public["title"])
        self.assertEqual([], self.github.writes(PUBLIC_REPO, "PATCH"))

    def test_changed_public_text_is_reported_on_a_nonmutating_repeat(self):
        draft = self.draft()
        self.workflow.publish(self.url, draft)
        self.public_issue()["title"] = "A maintainer edited this title"
        self.assertTrue(self.workflow.publish(self.url, draft)["public_content_changed"])
        self.assertEqual([], self.github.writes(PUBLIC_REPO, "PATCH"))

    def test_conflicting_checkpoints_stop_publication(self):
        self.github.fail_next("POST", f"repos/{PUBLIC_REPO}/issues")
        draft = self.draft()
        with self.assertRaises(GitHubError):
            self.workflow.publish(self.url, draft)
        comments = self.github.issue_comments[PRIVATE_REPO][1]
        comments.append(comments[0] | {"id": 999})
        with self.assertRaises(IssueError) as raised:
            self.workflow.publish(self.url, draft)
        self.assertEqual(
            "Multiple publication checkpoints exist. Reconcile them before publishing.",
            str(raised.exception),
        )
        self.assertEqual({}, self.github.issues[PUBLIC_REPO])

    def test_a_local_lock_serializes_publication_for_the_same_issue(self):
        draft = self.draft()
        with self.workflow.locked("publish:" + self.url.casefold()):
            with self.assertRaises(IssueError) as raised:
                self.workflow.publish(self.url, draft)
        self.assertEqual(
            "Another local operation is running for this issue; retry after it finishes.",
            str(raised.exception),
        )
        self.assertEqual([], self.github.writes())

    def test_private_state_is_not_written_to_the_public_checkout_or_a_shared_directory(self):
        with self.assertRaises(IssueError):
            load_input(str(Path(__file__).parent / "private-draft.json"))
        self.state.mkdir(mode=0o755)
        with self.assertRaises(IssueError):
            secure_directory(self.state)
        self.assertEqual(0o755, self.state.stat().st_mode & 0o777)

    def test_private_inputs_and_state_are_rejected_in_another_git_worktree(self):
        other_checkout = Path(self.temporary.name) / "another-checkout"
        other_checkout.mkdir()
        (other_checkout / ".git").write_text("gitdir: /synthetic/worktree/metadata\n")
        with self.assertRaises(IssueError):
            load_input(str(other_checkout / "private-input.json"))
        with self.assertRaises(IssueError):
            secure_directory(other_checkout / "state")
        self.assertFalse((other_checkout / "state").exists())


class GitHubTransportTest(unittest.TestCase):
    def test_pins_github_host_and_sends_json_over_standard_input(self):
        result = subprocess.CompletedProcess([], 0, '{"number": 1}', "")
        with patch.object(subprocess, "run", return_value=result) as run:
            self.assertEqual({"number": 1}, GitHub().request(
                "repos/example/private-tracker/issues", "POST",
                {"body": "Private text", "labels": [PRIVATE_LABEL]}))
        arguments, options = run.call_args
        self.assertEqual([
            "gh", "api", "--hostname", "github.com", "--method", "POST",
            "repos/example/private-tracker/issues", "--input", "-",
        ], arguments[0])
        self.assertEqual(
            {"body": "Private text", "labels": [PRIVATE_LABEL]}, json.loads(options["input"])
        )
        self.assertTrue(options["capture_output"])

    def test_flattens_all_pages_without_using_search_index_results(self):
        result = subprocess.CompletedProcess([], 0, '[[{"id": 1}], [{"id": 2}]]', "")
        with patch.object(subprocess, "run", return_value=result) as run:
            self.assertEqual([{"id": 1}, {"id": 2}], GitHub().request(
                "repos/example/private-tracker/issues?state=all&per_page=100", paginate=True))
        self.assertEqual(["--paginate", "--slurp"], run.call_args.args[0][-2:])

    def test_failure_messages_do_not_echo_submitted_private_data(self):
        result = subprocess.CompletedProcess(
            [], 1, "private response body", "private error details"
        )
        with patch.object(subprocess, "run", return_value=result):
            with self.assertRaises(GitHubError) as raised:
                GitHub().request(
                    "repos/example/private-tracker/issues", "POST", {"body": "private evidence"}
                )
        self.assertEqual(
            "GitHub POST failed. Check gh authentication, access, and repository setup.",
            str(raised.exception),
        )


if __name__ == "__main__":
    unittest.main()
