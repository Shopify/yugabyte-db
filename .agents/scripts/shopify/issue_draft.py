"""Render public issue forms and check known disclosure patterns.

The Shopify skill supplies a semantic rewrite and source-specific sensitive terms.
These checks complement that rewrite; they do not classify arbitrary private facts.
"""

from __future__ import annotations

import hashlib
import html
import ipaddress
import json
import re
import unicodedata
from urllib.parse import unquote, urlsplit


PUBLIC_REPO = "Shopify/yugabyte-db"
UPSTREAM_REPO = "yugabyte/yugabyte-db"
PUBLIC_REPOS = (PUBLIC_REPO, UPSTREAM_REPO)
PRIVATE_LABEL = "Yugabyte Contrib"
ISSUE_TYPES = (
    "kind/bug", "kind/enhancement", "kind/failing-test", "kind/improve-ux",
    "kind/new-feature", "kind/question",
)

# Field names and labels follow .github/ISSUE_TEMPLATE/. The skill reads the
# selected YAML before drafting; changes to those forms need updating here too.
TEMPLATES = {
    "docDB": ("DocDB", ["area/docdb", "status/awaiting-triage"], True),
    "ysql": ("YSQL", ["area/ysql", "status/awaiting-triage"], True),
    "ycql": ("YCQL", ["area/ycql", "status/awaiting-triage"], True),
    "cdc": ("CDCSDK", ["area/cdcsdk", "status/awaiting-triage"], True),
    "docs": ("Docs", ["area/documentation", "status/awaiting-triage"], False),
    "ui": ("UI", ["area/ui"], False),
    "yugabyted": ("yugabyted", ["area/ecosystem", "area/ybd", "status/awaiting-triage"], False),
    "feature_request": ("New Feature", ["kind/new-feature"], False),
}

PUBLIC_HOSTS = {
    "docs.yugabyte.com", "www.yugabyte.com", "yugabyte.com",
    "www.postgresql.org", "postgresql.org",
    "example.com", "example.org", "example.net", "localhost",
}
PUBLIC_CODE_REPOS = {repo.casefold() for repo in PUBLIC_REPOS} | {"postgres/postgres"}
EXAMPLE_NETWORKS = tuple(ipaddress.ip_network(value) for value in (
    "192.0.2.0/24", "198.51.100.0/24", "203.0.113.0/24", "2001:db8::/32",
))
URL_PATTERN = re.compile(r'''\b[a-z][a-z0-9+.-]*://[^\s<>\[\]{}"']+''', re.I)
EMAIL_PATTERN = re.compile(r"\b[\w.+-]+@(?:[\w-]+\.)+[a-z]{2,}\b", re.I)
IP_PATTERN = re.compile(
    r"(?<![\w.])(?:\d{1,3}\.){3}\d{1,3}(?!\w|\.\d)|"
    r"(?<![\w:])(?:[0-9a-f]{0,4}:){2,}[0-9a-f:.]*(?![\w:])", re.I)
# Legacy 2.x release numbers overlap IPv4 syntax; require an explicit version context.
YUGABYTE_VERSION_PATTERN = re.compile(
    r"\b(?:YugabyteDB(?:\s+version)?|version)(?:\s*[:=]\s*|\s+)`?"
    r"v?2(?:\.\d+){3}(?:-b\d+)?(?![\w:/]|\.\d)", re.I)
UUID_PATTERN = re.compile(r"\b[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}\b", re.I)
SECRET_PATTERN = re.compile(
    r"-----BEGIN (?:[A-Z ]*PRIVATE KEY|CERTIFICATE)-----|"
    r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b|"
    r"\b(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,})\b|"
    r"\bxox[baprs]-[A-Za-z0-9-]{10,}\b|"
    r"\bAIza[A-Za-z0-9_-]{30,}\b|"
    r"\bBearer\s+[A-Za-z0-9._~+/-]{12,}", re.I)
INTERNAL_HOST_PATTERN = re.compile(
    r"\b(?:[a-z0-9_-]+\.)*(?:shopify\.io|shopifycloud\.com|myshopify\.com|"
    r"shopify\.com)\b|\b[a-z0-9_-]+(?:\.[a-z0-9_-]+)*\.(?:internal|local|corp)\b", re.I)


class IssueError(ValueError):
    """An input or workflow condition needs correction before proceeding."""


def fingerprint(value: object) -> str:
    return hashlib.sha256(json.dumps(
        value, sort_keys=True, ensure_ascii=False, separators=(",", ":")
    ).encode()).hexdigest()


def require_keys(value: object, required: set[str], optional: set[str] | None = None) -> None:
    if not isinstance(value, dict) or not required <= value.keys():
        raise IssueError("The input is missing required fields.")
    if value.keys() - required - (optional or set()):
        raise IssueError("The input contains unsupported fields.")


def text_field(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise IssueError(f"{name} must be nonempty text.")
    if any(unicodedata.category(char) == "Cc" and char not in "\n\t\r" for char in value):
        raise IssueError(f"{name} contains control characters.")
    return value.strip()


def issue_ref(value: str, allowed_repos: tuple[str, ...]) -> tuple[str, int]:
    if not isinstance(value, str):
        raise IssueError("Expected a full GitHub issue URL.")
    match = re.fullmatch(
        r"https://github\.com/([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)/issues/([1-9][0-9]*)/?",
        value,
    )
    if match:
        for repo in allowed_repos:
            if match[1].casefold() == repo.casefold():
                return repo, int(match[2])
    raise IssueError(
        "Expected an issue URL in the configured private tracker or an allowed public repository."
    )


def issue_url(repo: str, number: int) -> str:
    return f"https://github.com/{repo}/issues/{number}"


def normalized(text: str) -> str:
    for _ in range(3):
        text = html.unescape(unquote(text))
    text = unicodedata.normalize("NFKC", text)
    return "".join(char for char in text if unicodedata.category(char) != "Cf")


def example_address(value: str) -> bool:
    try:
        address = ipaddress.ip_address(value.rstrip("."))
    except ValueError:
        return True  # A version number or C++ qualifier, rather than an address.
    return address.is_loopback or any(
        address.version == network.version and address in network for network in EXAMPLE_NETWORKS)


def allowed_public_url(value: str) -> bool:
    try:
        parsed = urlsplit(value.rstrip(".,;"))
        if (parsed.scheme not in ("http", "https") or parsed.username or parsed.password
                or parsed.query or "\\" in value or set(parsed.path.split("/")) & {".", ".."}):
            return False
        host = (parsed.hostname or "").casefold()
        if host == "github.com":
            return "/".join(parsed.path.strip("/").split("/")[:2]).casefold() in PUBLIC_CODE_REPOS
        if host in PUBLIC_HOSTS:
            return True
        address = ipaddress.ip_address(host)
        return address.is_loopback or any(
            address.version == network.version and address in network
            for network in EXAMPLE_NETWORKS)
    except ValueError:
        return False


def check_disclosures(content: str, sensitive_terms: list[str], private_repo: str) -> None:
    content = normalized(content)
    findings = []
    if (INTERNAL_HOST_PATTERN.search(content)
            or private_repo.casefold() in content.casefold()
            or private_repo.split("/")[1].casefold() in content.casefold()):
        findings.append("Internal host or private repository reference.")
    if re.search(r"\b(?:PLAT|INC|INCIDENT)-[0-9]+\b", content):
        findings.append("Internal ticket identifier.")
    if SECRET_PATTERN.search(content):
        findings.append("Credential or certificate pattern.")
    if re.search(r"\b(?:password|passwd|api[_-]?key|(?:access[_-]?)?token|secret)"
                 r"\s*[=:]\s*[\"']?[^\s\"'<>]{6,}",
                 content, re.I):
        findings.append("Credential assignment; use a placeholder without a credential value.")
    address_text = YUGABYTE_VERSION_PATTERN.sub(" ", content)
    if any(not example_address(match[0]) for match in IP_PATTERN.finditer(address_text)):
        findings.append("An address outside the documentation or loopback ranges.")
    if any(match[0].split("@", 1)[1].casefold() not in {"example.com", "example.org", "example.net"}
           for match in EMAIL_PATTERN.finditer(content)):
        findings.append("An email address outside the example domains.")
    if any(not match[0].startswith("00000000-0000-0000-0000-")
           for match in UUID_PATTERN.finditer(content)):
        findings.append("A deployment identifier; use a synthetic zero-prefixed UUID.")
    if re.search(r"!\[|<!--|<\s*(?:a|img|iframe|script|video|object|embed|svg|style|"
                 r"span|div|details|summary|meta|link|form|input|textarea)\b", content, re.I):
        findings.append(
            "Embedded media, HTML links, or hidden comments require a text-only rewrite."
        )
    urls = [match[0].rstrip(").,") for match in URL_PATTERN.finditer(content)]
    # Check Markdown destinations too, including relative, protocol-relative and data URLs.
    urls += re.findall(r"\]\(\s*<?([^\s)>]+)", content)
    urls += re.findall(r"(?m)^\s*\[[^\]]+\]:\s*<?([^\s>]+)", content)
    if any(not url.startswith("#") and not allowed_public_url(url) for url in urls):
        findings.append(
            "A link is outside the supported public documentation, code, or example locations."
        )
    if not isinstance(sensitive_terms, list) or any(
            not isinstance(term, str) or len(term.strip()) < 3 for term in sensitive_terms):
        raise IssueError(
            "sensitive_terms must be a list of source identifiers, each at least three characters."
        )
    folded = " ".join(content.casefold().split())
    for index, term in enumerate(sensitive_terms, 1):
        term = " ".join(normalized(term).casefold().split())
        if re.search(r"(?<![a-z0-9])" + re.escape(term) + r"(?![a-z0-9])", folded):
            findings.append(f"Source-specific sensitive term #{index} remains in the draft.")
    if findings:
        raise IssueError("Public draft rejected:\n- " + "\n- ".join(findings))


def render_draft(draft: dict, private_repo: str) -> dict:
    required = {"source_digest", "template", "title", "description", "sensitive_terms"}
    require_keys(draft, required, {
        "issue_type", "connector_version", "connector_configuration", "yugabyte_version",
    })
    if (not isinstance(draft["source_digest"], str)
            or not re.fullmatch(r"[a-f0-9]{64}", draft["source_digest"])):
        raise IssueError("source_digest must come from a fresh inspect operation.")
    if not isinstance(draft["template"], str) or draft["template"] not in TEMPLATES:
        raise IssueError("Select one of the supported upstream issue templates.")
    prefix, labels, has_type = TEMPLATES[draft["template"]]
    title = text_field(draft["title"], "title")
    if "\n" in title or "\r" in title or len(title) > 200 or title.startswith("["):
        raise IssueError(
            "Use a single-line title of at most 200 characters without a component prefix."
        )
    sections = [("Description", text_field(draft["description"], "description"))]
    extra = {"connector_version", "connector_configuration", "yugabyte_version"}
    if draft["template"] == "cdc":
        for field, heading in (("connector_version", "Source connector version"),
                               ("connector_configuration", "Connector configuration")):
            sections.append((heading, text_field(draft.get(field), field)))
        if "yugabyte_version" in draft:
            sections.append((
                "YugabyteDB version", text_field(draft["yugabyte_version"], "yugabyte_version")
            ))
    elif extra & draft.keys():
        raise IssueError("Connector fields are only supported by the CDC template.")
    if has_type:
        if draft.get("issue_type") not in ISSUE_TYPES:
            raise IssueError("Select a supported issue_type from the upstream template.")
        sections.append(("Issue Type", draft["issue_type"]))
        labels = labels + [draft["issue_type"]]
    elif "issue_type" in draft:
        raise IssueError("The selected upstream template does not have an Issue Type field.")
    sections.append((
        "Warning: Please confirm that this issue does not contain any sensitive information",
        "- [x] I confirm this issue does not contain any sensitive information.",
    ))
    result = {"title": f"[{prefix}] {title}",
              "body": "\n\n".join(f"### {heading}\n\n{text}" for heading, text in sections),
              "labels": list(labels)}
    if len(result["body"].encode()) > 60000:
        raise IssueError(
            "The public issue is too large; reduce it to the problem and a minimal reproduction."
        )
    check_disclosures(
        result["title"] + "\n" + result["body"], draft["sensitive_terms"], private_repo
    )
    return result
