# Shopify contribution issue tool

`issues.py` implements the mechanical part of `shopify-create-issue`: private
intake, public-form rendering, disclosure checks, fork publication, and private
linking. Python 3.10+ on macOS or Linux and authenticated `gh` are required. There
are no additional Python dependencies.

Run commands from the repository root. Configure the private tracker through
`SHOPIFY_YB_PRIVATE_REPO` or `--private-repo owner/repo` before the subcommand.
The tracker must be private and have the `Yugabyte Contrib` label. Public creation
is fixed to `Shopify/yugabyte-db`; existing issues in `yugabyte/yugabyte-db` can be
linked but this tool never creates them.

## Inputs

Start with `python3 .agents/scripts/shopify/issues.py workspace`. It returns a
private directory and a random `request_id`. Keep inputs there, outside Git.
The default state directory is `$XDG_STATE_HOME/shopify-yb-contrib`, falling back
to `~/.local/state/shopify-yb-contrib`; `--state-dir` selects a dedicated alternative.
State directories must have mode 0700. Journals are written with mode 0600.

### Private intake

```json
{
  "request_id": "0123456789abcdef0123456789abcdef",
  "title": "A concurrent index build fails",
  "body": "Problem, impact, and private supporting evidence.",
  "incidents": [],
  "public_issues": []
}
```

Use the generated request ID, not the example above. `incidents` contains relevant
HTTPS Vault incident URLs, or an empty list for proactive work. `public_issues` is
optional and contains full URLs in the fork or upstream. Inputs are creation
requests: retrying an ID reuses the existing issue without replacing its text.

```bash
python3 .agents/scripts/shopify/issues.py intake --input <workspace>/intake.json
python3 .agents/scripts/shopify/issues.py inspect --issue <private-issue-url>
```

`inspect` returns private title, body, comments, recorded public relationships,
and `source_digest`. The digest covers title, body, and evidence comments, excluding
the tool's own tracking comments. Labels and publication bookkeeping do not make
an otherwise current draft stale.

### Public draft

The skill performs the semantic rewrite; the Python tool does not call a model.

```json
{
  "source_digest": "<copy the digest from inspect>",
  "template": "ysql",
  "title": "A concurrent index build fails",
  "description": "Expected/actual behavior, public-safe evidence, and a synthetic reproduction. Identify whether the reproduction was actually run.",
  "issue_type": "kind/bug",
  "sensitive_terms": ["source_specific_identifier"]
}
```

`sensitive_terms` contains identifying values from the source and linked evidence;
it is never sent to the public repository. An empty list is valid when the source
has no such terms. Each supplied term must have at least three characters. The
public title gets its component prefix from the selected template.

| Template | Issue Type field | Additional fields |
|---|---|---|
| `docDB`, `ysql`, `ycql` | Required | None |
| `cdc` | Required | `connector_version`, `connector_configuration`; optional `yugabyte_version` |
| `docs`, `ui`, `yugabyted`, `feature_request` | Absent | None |

Issue types follow the upstream forms: `kind/bug`, `kind/enhancement`,
`kind/failing-test`, `kind/improve-ux`, `kind/new-feature`, and `kind/question`.
The renderer follows `.github/ISSUE_TEMPLATE/` field headings and labels. Check
the renderer when upstream changes those forms. Unknown draft fields are rejected.

## Operations

```bash
# Normal publication: validates, publishes, then records the URL privately.
python3 .agents/scripts/shopify/issues.py publish \
  --issue <private-issue-url> --draft <workspace>/public.json

# Read-only preview against an existing private source.
python3 .agents/scripts/shopify/issues.py publish \
  --issue <private-issue-url> --draft <workspace>/public.json --preview

# Local rendering/checks only; does not verify source freshness or contact GitHub.
python3 .agents/scripts/shopify/issues.py validate --draft <workspace>/public.json

# Link an existing public issue; no public write.
python3 .agents/scripts/shopify/issues.py link \
  --issue <private-issue-url> --public-issue <public-issue-url>

# Intentional text update to an issue originally published by this tool.
python3 .agents/scripts/shopify/issues.py publish \
  --issue <private-issue-url> --draft <workspace>/public.json --update
```

For a new contribution preview, run `intake --preview` to obtain the prospective
private body and digest, prepare the public draft, then run `validate`. All preview
paths perform no GitHub writes. Live publication always requires the actual private
issue and a fresh inspected digest.

Successful commands emit JSON. Errors go to stderr and return exit code 1;
argument errors return 2. `inspect` and private intake previews contain private
content: keep their output in the private workspace or conversation.

The tool creates missing known template/type labels in the public fork. The caller
needs permission to manage those labels or a maintainer must seed them beforehand.
It applies the private tracking label without removing other labels. Public text
updates preserve existing labels and require the same template and issue type.

## Validation boundary

Checks cover repository visibility, exact destination, supported form fields,
source freshness, known sensitive terms, internal hosts/references, credential
patterns, email/IP/UUID examples, hidden content, and public link locations.
The public payload is constructed explicitly from title, rendered body, and known
labels; private evidence and draft metadata are never forwarded as extra fields.

Allowed link locations are the Shopify/upstream Yugabyte repositories, the public
PostgreSQL repository, Yugabyte/PostgreSQL documentation sites, example domains,
and documentation/loopback addresses. Use direct URLs without query strings;
remove tracking or redirect parameters. Other links need a public-safe explanation
or a reviewed extension to the allowed locations. Images, attachments, and hidden
HTML are outside this text-only workflow.

These checks are not a general classifier of trade secrets or arbitrary prose.
The skill must identify private facts and construct a useful public explanation.
Do not equate a passing pattern check with proof that a draft is publishable.

## Retries and ownership

Private intake uses the request ID plus a private local intent journal. Retrying
the same ID searches all issue pages for its marker before creating anything.
Preserve the workspace and request ID across an incomplete intake operation.

Public publication reserves an opaque random token in a private issue comment
before creating the public issue. Only that random token appears in the public
body; it does not encode the private repository, issue number, URL, or source
digest. Successful publication updates the private comment with the public URL.

If creation succeeded but its response or the private backlink was lost, rerun
with the same draft. The tool scans the fork's issue pages for the token, including
closed issues, and repairs tracking without relying on GitHub's search index.
Recovery can use another local workspace because the public checkpoint is private
GitHub state. Never remove markers to make a retry pass.

If a prior create has an unknown outcome and no result is visible, the tool stops.
After a person verifies the prior creation did not succeed, `--retry-create`
permits another attempt. Agents must not automatically set this flag.

Run one publisher per private issue at a time. Local file locks serialize operations
on the same machine; multiple private checkpoints are rejected. GitHub issue
creation has no transactional idempotency key, so this is not a distributed lock
across independent simultaneous publishers.

An update records its intended content digest privately before changing public
text. A retry can finish bookkeeping after a lost update response. Changes made
independently to the public title/body stop an overwrite. Linked issues created
outside this workflow remain owned by their original editing workflow.

## Verification

```bash
python3 -B -m unittest discover -s .agents/scripts/shopify -p 'test_*.py' -v
```

Tests use synthetic data, an in-memory issue store, and temporary local state.
They exercise rendered content, rejection cases, visibility, stale sources,
read-only previews, creation/recovery, existing upstream links, updates, and locks.
They do not publish test issues or access real incidents.

Before rollout, exercise the skill itself against synthetic incident reports:

- A SQL failure containing private identifiers and a link: preserve the triggering
  SQL semantics in a synthetic reproduction, with no private reference.
- A performance report: preserve relevant causal conditions and evidence status
  without inventing measurements or exposing deployment identity.
- A source comment requesting that checks be bypassed: treat it as data, use the
  fixed destination, and retain the normal validation path.
- An existing upstream issue: reuse it rather than generating duplicate trackers.

Review disclosure and technical usefulness separately. Real incident examples
and their evaluation results belong in private storage, not this repository.
