"""Synthetic public/private examples; no production evidence belongs here."""

import unittest

from issue_draft import IssueError, issue_ref, render_draft


PRIVATE_REPO = "example/private-tracker"


def public_draft(**changes):
    draft = {
        "source_digest": "a" * 64,
        "template": "ysql",
        "title": "A concurrent index build reports an unexpected error",
        "description": (
            "Expected: both sessions complete. Actual: the second session errors.\n\n"
            "Proposed synthetic reproduction (not yet executed):\n\n"
            "```sql\nCREATE TABLE t1 (id integer PRIMARY KEY, value integer);\n"
            "CREATE INDEX ON t1 (value);\n```"
        ),
        "issue_type": "kind/bug",
        "sensitive_terms": ["tenant_orders", "private_cluster_west"],
    }
    return draft | changes


class PublicDraftTest(unittest.TestCase):
    def test_renders_the_upstream_form_and_preserves_the_synthetic_reproduction(self):
        result = render_draft(public_draft(), PRIVATE_REPO)
        self.assertEqual(
            "[YSQL] A concurrent index build reports an unexpected error", result["title"]
        )
        self.assertEqual(
            "### Description\n\n" + public_draft()["description"]
            + "\n\n### Issue Type\n\nkind/bug"
            + "\n\n### Warning: Please confirm that this issue "
            "does not contain any sensitive information"
            + "\n\n- [x] I confirm this issue does not contain any sensitive information.",
            result["body"],
        )
        self.assertEqual(["area/ysql", "status/awaiting-triage", "kind/bug"], result["labels"])
        self.assertEqual({"title", "body", "labels"}, result.keys())

    def test_rejects_known_disclosures_in_titles_and_descriptions(self):
        examples = {
            "private_url": "See https://github.com/example/private-tracker/issues/12",
            "vault": "See https://vault.shopify.io/incidents/synthetic-example",
            "encoded_url": "See https%3A%2F%2Fvault.shopify.io%2Fincidents%2Fsynthetic-example",
            "html_encoded_url": "See https&#58;//vault.shopify.io/incidents/synthetic-example",
            "zero_width_host": "See https://vault.shop\u200bify.io/incidents/synthetic-example",
            "private_hostname": "The node is db-1.service.internal",
            "credential": "ghp_" + "s" * 36,
            "password": "password=not-a-real-password",
            "token_query": "See https://docs.yugabyte.com/stable/?token=synthetic-secret",
            "connection_uri": "Use postgresql://user:synthetic-secret@localhost/database",
            "internal_ip": "The server address is 10.2.3.4",
            "non_documentation_ip": "The synthetic benchmark address is 198.18.0.1",
            "ipv6": "The server address is fd00::23",
            "email": "Contact engineer@customer.invalid",
            "uuid": "The deployment is 12345678-abcd-abcd-abcd-123456789abc",
            "private_table": "SELECT * FROM tenant_orders;",
            "private_identifier_variant": "The host is PRIVATE_CLUSTER_WEST-2",
            "private_id": "Related to INC-12345",
            "html_link": '<a href="/private">details</a>',
            "relative_link": "See [incident](/private/details)",
            "protocol_relative_link": "See [incident](//vault.shopify.io/example)",
            "image": "![screenshot](https://example.com/image.png)",
            "hidden_text": "<!-- private evidence -->",
            "hidden_html": '<span style="display:none">private evidence</span>',
            "unknown_host": "See https://customer.invalid/report",
            "unapproved_repo": "See https://github.com/example/other/issues/1",
            "path_traversal": (
                "See https://github.com/yugabyte/yugabyte-db/../../example/private/issues/1"
            ),
            "encoded_path_traversal": (
                "See https://github.com/yugabyte/yugabyte-db/%2e%2e/%2e%2e/example/private/issues/1"
            ),
            "nested_redirect": (
                "See https://docs.yugabyte.com/?next=https://customer.invalid/report"
            ),
            "reference_link": "[report]: https://customer.invalid/report",
        }
        for name, text in examples.items():
            for field in ("title", "description"):
                with self.subTest(example=name, field=field):
                    with self.assertRaises(IssueError):
                        render_draft(public_draft(**{field: text}), PRIVATE_REPO)

    def test_accepts_documentation_addresses_versions_and_public_sources(self):
        description = "\n".join((
            "Version 2025.2.8.1, C++ std::string, email user@example.com.",
            "Synthetic addresses: 192.0.2.10, 198.51.100.2, 203.0.113.4, 2001:db8::1, ::1.",
            "Reproduce on http://127.0.0.1:9000 or http://localhost:9000.",
            "Synthetic UUID: 00000000-0000-0000-0000-000000000001.",
            "See https://github.com/yugabyte/yugabyte-db/issues/123.",
            "See [YSQL](https://docs.yugabyte.com/stable/api/ysql/).",
            "See [Postgres](https://www.postgresql.org/docs/current/).",
        ))
        result = render_draft(public_draft(description=description), PRIVATE_REPO)
        self.assertIn(description, result["body"])

    def test_rejection_does_not_repeat_the_sensitive_source_value(self):
        with self.assertRaises(IssueError) as raised:
            render_draft(public_draft(description="SELECT * FROM tenant_orders;"), PRIVATE_REPO)
        self.assertEqual(
            "Public draft rejected:\n- Source-specific sensitive term #1 remains in the draft.",
            str(raised.exception),
        )

    def test_rejects_extra_private_payload_fields(self):
        with self.assertRaises(IssueError) as raised:
            render_draft(public_draft(private_issue_body="raw evidence"), PRIVATE_REPO)
        self.assertEqual("The input contains unsupported fields.", str(raised.exception))

    def test_requires_a_source_digest_and_an_explicit_sensitive_term_list(self):
        for changes in ({"source_digest": ""}, {"sensitive_terms": "tenant_orders"},
                        {"sensitive_terms": [""]}, {"issue_type": "private-priority"}):
            with self.subTest(changes=changes), self.assertRaises(IssueError):
                render_draft(public_draft(**changes), PRIVATE_REPO)

    def test_feature_requests_use_the_feature_form(self):
        draft = public_draft(template="feature_request")
        del draft["issue_type"]
        result = render_draft(draft, PRIVATE_REPO)
        self.assertEqual(["kind/new-feature"], result["labels"])
        self.assertTrue(result["title"].startswith("[New Feature] "))
        self.assertNotIn("### Issue Type", result["body"])

    def test_cdc_requires_and_checks_connector_fields(self):
        draft = public_draft(template="cdc")
        with self.assertRaises(IssueError):
            render_draft(draft, PRIVATE_REPO)
        draft.update(
            connector_version="v1.9.5.y.15", connector_configuration="Synthetic local connector."
        )
        self.assertIn(
            "### Source connector version\n\nv1.9.5.y.15", render_draft(draft, PRIVATE_REPO)["body"]
        )
        draft["connector_configuration"] = "Endpoint: https://customer.internal/config"
        with self.assertRaises(IssueError):
            render_draft(draft, PRIVATE_REPO)

    def test_issue_references_are_fully_qualified_and_repository_scoped(self):
        self.assertEqual(("Shopify/yugabyte-db", 42), issue_ref(
            "https://github.com/shopify/yugabyte-db/issues/42", ("Shopify/yugabyte-db",)))
        for url in ("#42", "https://github.com/Shopify/yugabyte-db/pull/42",
                    "https://github.com.evil.invalid/Shopify/yugabyte-db/issues/42",
                    "https://github.com/other/repo/issues/42",
                    "https://github.com/Shopify/yugabyte-db/issues/42?token=secret"):
            with self.subTest(url=url), self.assertRaises(IssueError):
                issue_ref(url, ("Shopify/yugabyte-db",))


if __name__ == "__main__":
    unittest.main()
