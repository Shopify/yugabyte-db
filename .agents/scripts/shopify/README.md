# Shopify contribution tools

`issues.py` implements the mechanical part of `shopify-create-issue`: private
intake, public-form rendering, disclosure checks, fork publication, and private
linking. Python 3.10+ on macOS or Linux and authenticated `gh` are required. There
are no additional Python dependencies.

Run commands from the repository root. Configure the private tracker through
`SHOPIFY_YB_PRIVATE_REPO` or `--private-repo owner/repo` before the subcommand.
The tracker must be private and have the `Yugabyte Contrib` label. Public creation
is fixed to `Shopify/yugabyte-db`; this tool never creates issues in
`yugabyte/yugabyte-db`. An upstream issue created outside the tool is recorded with
`link`.

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

The description may end with a `## Shopify context` section for public-safe Shopify
context: why it matters to Shopify, fork PR links, release targets needed, and a
fork-side workaround. It must be the last section of the description with exactly
that heading and no `###` sub-headings inside it, because the upstream copy of the
issue omits it along with the tool's publication marker. The renderer places the description under `### Description` and
appends the form's remaining `###` sections after it, so the section ends at the
next `###` heading. Disclosure checks cover it like the rest of the description.

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

# Record an existing public issue, fork or upstream; no public write.
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

## Release planner

`release.py` plans a Shopify release of YugabyteDB: the Shopify patches to
cherry-pick onto an upstream release tag, and the git commands that create or
resume the `<version>-shopify-<sequence>-candidate` branch. It succeeds the Ruby
`version_builder.rb` in the private build tooling, which remains until this has
planned a release; the release process itself is unchanged.

```bash
# The default --repo-path is the checkout containing this script.
python3 .agents/scripts/shopify/release.py --version 2025.2.8.0
python3 .agents/scripts/shopify/release.py --version v2025.2.8.0 --no-fetch \
  --target-branch 2025.2.8.0-shopify-2-candidate --previous-release 2025.2.7.0-shopify-3
```

Planning is read-only. It fetches (`git fetch --all --tags`, unless `--no-fetch`),
reads fork PRs through `gh`, and prints commands; it never creates or moves refs.
`--repo-path` points it at another checkout.

Patches are fork PRs carrying `Available:` labels. Selection is label-driven
regardless of PR state: a closed PR with an `Available:` label is still a patch.
`Available: X.Y.Z` means upstream includes the fix from that version: the PR is
skipped when the target is at or after `X.Y.Z` in the same series, or when `X.Y.Z`
is the first release of an earlier series. `Available: nil` is a permanent fork
patch; the report lists PRs included because their availability is unknown. A
landed PR contributes its merge commit (with `-m 1` only for a real merge commit);
an open PR contributes its branch commits, skipping merge commits with a warning.
Commits are ordered by committer time (oldest PR first on ties), reuse the previous
Shopify release's cherry-pick where one exists so that conflict resolutions carry
over, and are reported as done when the candidate branch already has them.

The sequence comes from existing `<version>-shopify-<n>` tags and branches; an
unreleased `-candidate` branch that descends from the upstream tag is resumed, and
one that does not is an error. Exit codes: 0, 2 for usage errors, 1 otherwise.

The previous Shopify release whose cherry-picks are reused is the highest lower
release in any series, so a series' first Shopify release reuses them from an
earlier series and later ones reuse them from their own.

The candidate lands in its release branch through a release PR, which keeps every
cherry-pick with a merge commit; [shopify-create-pr](../../../.claude/skills/shopify-create-pr/SKILL.md)
§ Release PRs gives its title, when to create the release branch, and why the PR is
never squashed.

`release.py` shares its git and `gh` plumbing with the other tools through `scm.py`;
`test_release.py` and `test_scm.py` run under the same `unittest discover` command as above.

## Upstream tooling

`upstream.py` has three commands: `status`, which shows where each change `master-shopify`
carries stands upstream; `submit <fork-pr>`, which performs the shopify-upstream-pr skill's
steps under the developer's own `gh` login; and `sync`, which rebuilds `master-shopify` on a
newer upstream commit as AGENTS.md describes, opens the result as a PR and, with `--land`,
lands it. Exit codes: 0, 2 for usage errors, 1 otherwise; `submit` and `sync` also exit 4 when
they stop for the developer. `test_upstream.py`, `test_upstream_submit.py`,
`test_upstream_sync.py` and `test_upstream_land.py` run under the same `unittest discover`
command as above.

### status

```bash
python3 .agents/scripts/shopify/upstream.py status
python3 .agents/scripts/shopify/upstream.py status --no-fetch --json
```

`status` stores nothing: it reads git and `gh`. Unless `--no-fetch`, it first fetches
`origin master-shopify` into `origin/master-shopify`, and upstream `master` from
`--upstream-url` (default `https://github.com/yugabyte/yugabyte-db.git`) into
`refs/shopify/upstream-master`, a ref only this tool writes. `--no-fetch` reuses that ref
and fails when it is absent. The header names the merge base of the two and its date, as in
`since upstream ad087d0fd (2026-11-18)`; an old date is the first sign of a stale ref.

It walks `git log --first-parent <merge base>..origin/master-shopify`. A merge that brings in
upstream history on a later parent is an upstream merge (GitHub's Sync fork); one whose first
parent is upstream and whose tree equals that parent's is a sync's `-s ours` merge. The PRs
earlier syncs adopted come from the `Dropped: #N (upstream #M)` trailers of every merge
reachable from `origin/master-shopify` but not from the upstream ref, since a sync's merge
falls behind the merge base once the next sync lands. Every other commit is a
landing: the fork PR its `(#N)` subject names; else the merged fork PRs its
`Shopify-PR: #55 #58` trailer names, which `sync` writes when it replays a landing that had
no `(#N)`, such as a merge landing replayed with `cherry-pick -m 1`; else every merged fork
PR whose `mergeCommit` it is (a stack merge); else unidentified. A PR is fork-only with the
`upstream: skip` label, which decides: a `[shopify-only]` title without the label counts as
upstream-bound, with a warning. Otherwise the newest comment with an
`Upstream:` line names the upstream PR, which is open, closed, landed (merged, and its merge
commit is an ancestor of the upstream ref) or restructured (merged, and its merge commit is
not, or is not in the checkout). The line accepts browser-copied URLs: angle brackets, a
trailing `.` or `,`, a suffix such as `/files` or `#issuecomment-1`, and `**Upstream:**`.
Without a readable `Upstream:` comment the PR is not submitted.

A landing whose message has a `This reverts commit <sha>` line, git's revert text with the
full sha or at least 7 characters of it, naming an earlier landing or an earlier landing's
original `mergeCommit` (a sync replays landings under new shas; the revert text keeps naming
the squash commit) pairs the two PRs: the
reverted one is `reverted` (`reverted_by` in `--json`) and the revert is `revert` (`reverts`,
the first PR it reverts). The fork carries nothing from the pair, so neither is listed as not
submitted; both appear under "Reverted in the fork" as `#12  reverted by #15`, and the revert
asks GitHub nothing. Several such lines may revert several PRs. A revert of a commit the fork
does not carry, such as an upstream one, of an unattributed landing or of another revert (a
reapply) is an ordinary contribution. `submit` will refuse both PRs of a pair; `sync` will
replay both, which cancels.

Warnings cover what the derivation cannot settle: a label and title that disagree; a PR
reverted in the fork after its `Upstream:` comment was posted, whose upstream PR needs closing
or reverting; an
`Upstream:` line that is not an upstream PR URL, or an upstream PR it cannot read; a merged
PR based on `master-shopify` that is neither carried nor adopted; a PR a sync dropped that is
carried again; first-parent merge commits that are neither sync points nor attributed, which
is how an upstream merge beyond a stale upstream ref shows; and a `gh pr list` result as long
as its 5000-PR limit.

### submit

```bash
# From the developer's yugabyte/yugabyte-db checkout, or name it with --repo-path.
python3 <fork checkout>/.agents/scripts/shopify/upstream.py submit 71
python3 <fork checkout>/.agents/scripts/shopify/upstream.py submit 71 \
  --repo-path ~/src/yugabyte-db --test-plan testplan.md --yes
```

`submit` takes one fork PR merged into `master-shopify` through the skill's Steps 1–6 in two
phases with one confirmation between them. Phase A writes only locally:

- Usage, `--branch`, and the `--test-plan` and `--upgrade` files (readable and non-empty)
  are checked first, and `gh` and `gh stack --help` before anything is fetched or cloned.
- Upstream checkout: `--repo-path`; else `--clone-to <dir>`, cloned with
  `git clone -o upstream`; else the current directory when `gh repo set-default --view` prints
  `yugabyte/yugabyte-db` there; else a prompt to enter a checkout's path or clone into
  `<parent of the current directory>/yugabyte-db-upstream`, and without a terminal an exit 4
  with both rerun forms. A new clone gets `gh repo set-default yugabyte/yugabyte-db` and
  `gh-stack.remote upstream`. A checkout named with `--repo-path` or at the prompt gets them
  when unset; the current directory is never changed, and is refused when `gh-stack.remote` is
  unset. Either setting naming something else is refused. The work tree must have no tracked
  changes, and `gh auth status` must succeed.
- The fork PR must be merged, approved by an active member of
  `Shopify/yugabyte-code-reviewers`, carrying neither fork-only marker (the `upstream: skip`
  label decides for `status`; `submit` stops on a `[shopify-only]` title too, since the two
  disagreeing is something to resolve first), neither reverted in the fork nor a revert of an
  unsubmitted fork PR (the fork carries nothing from such a pair), and
  titled `[#N] Component: Short description`, the component one `create-pr.sh` accepts (a
  letter, then letters and digits), with a body whose first line is `Refs <issue URL>` and
  which has a `## Summary`. After the fetch below, a newest `Upstream:` comment naming an open
  upstream PR, or a merged one whose merge commit is on upstream master (landed), ends the run
  with exit 0; one merged without landing (restructured) gets a replacement PR and a new
  `Upstream:` comment; one closed without merging is refused.
- Upstream issue: the issue a `Refs` line names upstream; for a fork issue, `--upstream-issue`,
  else the newest `Upstream issue: <url>` comment on the fork issue, else an upstream issue in
  `issues.py inspect --issue <--private-issue>`'s `public_issues`, else a new issue: the fork
  issue's title, its body without the publication marker and `## Shopify context`, and the
  fork issue's labels that upstream also has. A reused issue, the one a `Refs` line names
  included, that `--private-issue`'s `public_issues` lacks is recorded there with
  `issues.py link` in Phase B, and one no fork issue comment names yet gets its
  `Upstream issue: <url>` comment there.
- Base: `--parent` (refused when repeated), else the merged fork PR whose head is the PR's
  `baseRefName` when that is not `master-shopify`. No parent, or a parent whose upstream PR
  landed (its merge commit is on upstream master): `master`. An open parent PR: its
  `feature-stack/Shopify/<parent-change>`. Refused: a fork-only parent, one with no upstream
  PR, one merged without landing (restructured), and one closed.
- Landing: `master-shopify` from `--fork-url` (default
  `https://github.com/Shopify/yugabyte-db.git`) into `refs/shopify/fork-master-shopify` and
  upstream `master` from `--upstream-url` into `refs/shopify/upstream-master`, refs only this
  tool writes, unless `--no-fetch`. The PR's commit on the first-parent line since their merge
  base is found as `status` attributes it over the merged fork PRs; none, several, a merge
  commit, or one that also belongs to another PR (a sync's replay of a stack landing, whose
  `Shopify-PR:` trailer names every layer) is refused.
- Branch: `feature-stack/Shopify/<change>`, `<change>` slugged from the title's description or
  given as `--branch` (required when the slug is empty). PRs from same-named branches in other
  repositories are ignored; a PR on the branch in `yugabyte/yugabyte-db` whose body lacks
  `Shopify fork PR: <this fork PR's URL>` is another change's, and the run is refused with a
  pointer to `--branch`. This change's open or landed PR leaves only its records: nothing is
  built, and no test plan or upgrade notes are needed. Otherwise the branch is made with
  `gh stack init` and `gh stack rebase --remote <upstream>` on `master`; on a parent outside a
  GitHub stack, from the fetched parent branch with `gh stack init <parent> <branch>`; on a
  parent in one, `gh stack checkout <parent PR URL>` and `gh stack add`, refused when the parent
  is not the stack's top. Then `git cherry-pick -x`, with the subject
  `[#<upstream issue>] Component: Short description` (the fork issue number until Phase B
  creates the issue) and every bare `#N` in the body made a fork issue URL without its closing
  keyword. The pick must change the landing's paths; a different `git patch-id --stable` is a
  warning. Rewording uses `git commit --amend --only`, so nothing staged is folded in.
- A branch that already exists is reused only where this run would have built it: its local
  gh stack must list it on the planned base (as the bottom layer on `master`, or above the
  parent branch), and its history must agree: the parent branch's tip is its ancestor, or, on
  `master`, the pick's parent is on `<upstream remote>/master` (the ref
  `gh stack rebase --remote` moves) or on `refs/shopify/upstream-master`. Unless `--no-fetch`,
  the run fetches `<upstream remote>`'s `master` into that tracking ref too; it is needed only
  for a branch that already exists, so a newly added remote can still build one. A branch with
  no commits beyond the base (an aborted pick) is picked again only when it sits at either ref.
  Otherwise the run is refused with the commands that remove it, or, when only the remote's
  `master` has the branch's base, told to rerun without `--no-fetch` first; it never restacks a
  branch.

It then prints the branch and commit to build and test. When the commit changes `.proto`
files, `--upgrade` is required, as `create-pr.sh` requires it: without it the run is refused
with the rerun command before any test plan is asked for. It then asks for the test plan: a
file path, `e` to write one in `$EDITOR`, or `q`; without a terminal it exits 4 with the rerun
command instead.

Phase B shows the issue to reuse or create, the PR's title, base, summary and test plan files,
and the records, then asks `Publish? [y/N]`. `--yes` skips the question; it needs `--test-plan`
unless a PR already exists for the branch. Without a terminal or `--yes` it exits 4 with the
rerun command; for an existing PR, declining or the lack of a terminal prints the
`gh issue comment`, `issues.py link` (each when pending) and `gh pr comment` commands that
record it instead. Before any write it checks that the previewed branch is still checked out
at the previewed commit, and again just before `create-pr.sh` after rewording; it checks again
that the checkout has no tracked changes,
staged or not, since the pause for the test plan may have left some, and that the pick is the
branch's top commit, or already cites the upstream issue when commits sit above it. It then
creates the upstream issue when needed and comments `Upstream issue: <url>` on the fork issue,
then records it with `issues.py link` when `--private-issue` is given; a reused issue gets
whichever of those two records is missing. When a record fails after the issue was created, the
error says to rerun with `--upstream-issue <N>`. It rewrites the subject with the issue number,
runs `.agents/scripts/create-pr.sh -i -t -d -T [-U] [-D]` in the checkout, with the fork PR's
`## Summary` (fork references linked, ending with the fork PR URL and, when stacked, the parent
line) as the summary, and comments `Upstream: <PR URL>` on the fork PR. `create-pr.sh`'s exit 3
(lint), 5 (`gh stack push`) and 6 (`gh stack link`) are reported with its recovery; its exit 1
for the replacement of a restructured PR points at the skill's by-hand replay. Generated files
go to a fresh temporary directory, kept for inspection.

Every step is rerun-safe: a branch that has the landing's `(cherry picked from commit …)` line
is reused, also with fix commits above it, and one with other commits is refused; an open or
landed PR for the branch is recorded instead of opened, a restructured one is replaced, and a
closed one is refused; an existing PR with no known upstream issue is refused, since creating
one would duplicate the issue it cites; an existing `Upstream issue:` comment or private link
is not written again, and a newest `Upstream:` comment naming an open or landed PR ends the
run before anything is written.

### sync

```bash
python3 .agents/scripts/shopify/upstream.py sync
python3 .agents/scripts/shopify/upstream.py sync --to <upstream sha> --yes
python3 .agents/scripts/shopify/upstream.py sync --continue --to <upstream sha>
python3 .agents/scripts/shopify/upstream.py sync --abort
python3 .agents/scripts/shopify/upstream.py sync --land <pr>
```

`sync` runs in a full fork checkout: `--repo-path` (default: the checkout containing the
script) must have `origin` pointing at `Shopify/yugabyte-db`, must not be sparse
(`core.sparseCheckout`), and `gh auth status` must succeed. Unless `--no-fetch`, it fetches
as `status` does. `--to <ref>` names the upstream commit to sync to (default: the tip of
`refs/shopify/upstream-master`); it must be on upstream master. When `origin/master-shopify`
already contains it, there is nothing to sync and the run exits 0.

It derives the plan as `status` does, against that commit. Any commit on the first-parent line
that no merged fork PR accounts for, a merge commit that is neither a sync point nor a landing
included, refuses the run until it is attributed. A landing all of whose PRs have landed
upstream is dropped; every other landing is carried: not submitted, open, closed,
restructured, fork-only, and those whose upstream PR cannot be read, each of which is a warning.

The developer's branch and tree are never touched. The candidate is the branch
`shopify-sync/<sha>` (the upstream commit's first 12 characters) in a new worktree,
`<checkout>/../yugabyte-db-sync-<sha>`. It starts at the upstream commit, then `git merge -s
ours --no-ff` merges the old `master-shopify` with the message `Sync master-shopify to upstream
<sha>` and the trailers `Old-master-shopify: <sha>` and one `Dropped: #<fork PR> (upstream
#<upstream PR>)` per drop; the run checks that `status` classifies that merge as a sync. It
then replays the carried landings oldest first with their messages, authors and author dates
unchanged and no `-x`: `git cherry-pick` for a squash landing, and `git cherry-pick -m 1` for a
merge landing (a stack merge). GitHub's `mergeCommit` names only the original landing, so a
replay whose subject has no `(#N)` naming its PR, and whose message has no `Shopify-PR:`
trailer naming all its PRs, gets a `Shopify-PR: #a #b` trailer; a conflict resolution the
developer committed gets it on `--continue`. Without it the next sync would find the replay
unidentified. A candidate that already exists, in a worktree, as a local branch or on `origin`,
is refused without `--continue`.

A conflict stops the run with exit 4, the conflicting paths and the steps: resolve them in the
worktree, `git add`, `git cherry-pick --continue`, then rerun with `--continue`. When
upstream's version should win instead, run `git cherry-pick --abort` in the worktree and rerun
with `--continue --skip '#<fork PR>'` (repeatable; `70` and `#70` both work): the landing is
recorded as skipped after a conflict, and the PR body's warnings say it conflicted on upstream
and was skipped. `--skip` names only the conflicting pick the run stopped on, after its pick is
aborted; any other PR is a usage error. A landing that applies empty, because upstream already
has the change, also stops: when upstream took an equivalent fix, run `git cherry-pick --skip`
in the worktree and rerun with `--continue`; the landing is then neither carried nor dropped,
so `status` reports its PR as neither carried nor adopted until it has a disposition.
`--continue` derives the plan again for the same upstream commit (every rerun command it prints
names it with `--to`), refuses when `master-shopify` or the drops changed since the candidate
was built, matches the commits after the sync merge to the planned landings in order, treating
the ones in between as skipped, and replays the rest. A commit pairs with the landing whose
replay the tool recorded as that commit; a pick the developer committed (a resolved conflict)
pairs by subject and author date, and for a landing that needs one its `Shopify-PR:` trailer.
Every rerun and recovery command names the script and the checkout (`--repo-path`) by absolute
path, so it works from the candidate worktree the conflict steps `cd` into. A conflicted pick
that was aborted rather than resolved is picked again unless `--skip` names it; an empty pick
is skipped. The replays, the picks that conflicted and those skipped are kept in
`shopify-sync.json` in the worktree's git directory; a worktree added again from the branch
reports earlier conflicts as unknown. A candidate checked out in the developer's own checkout
is refused. `--abort` removes the worktree and the local branch, and deletes the branch on
`origin` when it was pushed only after asking (`--yes` deletes it; without a terminal it prints
the `git push origin --delete` command); without `--to` it aborts the only local candidate.

The run then shows the upstream commit, the old `master-shopify`, each carried landing (old and
new SHA), each drop, the conflicts resolved and the warnings, and asks `Push shopify-sync/<sha>
and open its PR against master-shopify? [y/N]`. `--yes` skips the question; without a terminal
or `--yes` it exits 4 with the `--continue --yes` rerun command. After the answer it checks
that the branch still points at the commit it showed, refusing when the candidate changed while
the question was open, and pushes that commit explicitly (`git push origin
<sha>:refs/heads/shopify-sync/<sha>`), never forcing: when `origin` has a version that is not
an ancestor of the candidate, it refuses. It creates the `sync` label when the fork lacks it
and opens a draft PR against `master-shopify` titled `[shopify-only] Build: Sync master-shopify
to upstream <sha>`, labelled `upstream: skip` and `sync`; when the branch already has an open
PR against `master-shopify` whose head is in `Shopify/yugabyte-db` itself, it updates that PR's
body instead, so a rerun with `--continue` pushes and refreshes it. Same-named branches in
other forks and PRs against other bases are ignored. The body has a summary, the upstream
commit, tables of the dropped and carried PRs (with whether each pick conflicted), a `git
range-diff` of each replay against its original landing (capped at 200 lines each), the
warnings, and the landing rule. The body stays under 60,000 characters: each range-diff first
gets a placeholder with the command that shows it, failed ones included, and outputs replace
placeholders in order while they fit; when even the placeholders do not fit, the range-diffs
left out are named in one "Omitted" block. A body still over the cap, with every range-diff
omitted, is refused before anything is pushed. A merge landing's range-diff uses
`--diff-merges=first-parent`, which needs git 2.48 or later; a range-diff that fails is
replaced by its command and a warning, and the run goes on. It prints the PR's URL, the `devx
binks review run` command and the worktree's path.

The PR is never merged with a GitHub button: squash, merge-commit and rebase all undo the sync.
Once Binks and a `Shopify/yugabyte-code-reviewers` member have approved it, `sync --land <pr>`
lands it by fast-forwarding `master-shopify` to its head; `--land` takes `--repo-path`,
`--no-fetch` and `--upstream-url` and no other `sync` mode, and works in a sparse checkout.
Before any write it fetches `master-shopify`, the candidate and upstream master, and refuses
(exit 1) unless: the PR is open, not a draft, against `master-shopify`, from
`shopify-sync/<sha>` in the fork and labelled `sync`; origin's branch is the PR's head; walking
first-parent back from the head finds only single-parent commits down to the sync merge, which
`status` classifies as a sync of that upstream commit and whose `Old-master-shopify:` is the
current `master-shopify` (when `master-shopify` is already at the head, it says the PR is landed
and exits 0); each commit above the merge is one replay of a landing the sync plan carries, the
plan being `status`'s derivation from the merge's `Old-master-shopify:` against its upstream
commit less the PRs its `Dropped:` trailers name, and each replay attributed as `status` does,
so a commit no merged fork PR accounts for, a replay of a PR outside the plan and a second
replay of a landing are refused (planned landings the candidate lacks were skipped, and the
summary lists them); no reviewer's latest opinion (APPROVED or CHANGES_REQUESTED, by submission
time) requests changes, on whatever head; a team member's latest opinion approves that head; the
review decision is not `CHANGES_REQUESTED` or `REVIEW_REQUIRED` (none, as GitHub often reports,
is a warning); `pr-title` and `pr-lint`, where they ran, succeeded; the newest Binks review
(`devx binks --json review list`, or its `control_plane_reviews` when `review_owner` is
`control_plane`) is `ready` on that head, its findings printed and confirmed fixed or answered
(`--findings-answered`, or `y` on a terminal), while a review still `running`, `queued` or
`pending` is waited for and any other is requested again; and the push is a fast-forward. An
active `pull_request` rule on `master-shopify` is only a warning: the push is attempted, and a
rejected push prints git's message and the options (a scoped bypass identity for syncs, or
disabling that ruleset for the push), changes nothing and exits 1 without the break-glass
command, which the same rule would reject. It then shows the old tip and the head, the replays,
the skipped landings, the drops and the upstream commit, and asks for the typed phrase `land
<12-char head>`; without a terminal it exits 4 with the rerun command carrying `--confirm-land
<12-char head>`, and a phrase or `--confirm-land` that does not match is a usage error. It
pushes `git push --force-with-lease=refs/heads/master-shopify:<old tip> origin
<head>:refs/heads/master-shopify`, waits up to 30 seconds for GitHub to mark the PR merged (it
does once the head is on the base), removes the candidate's worktree and local branch, and keeps
`shopify-sync/<sha>` on `origin` as the sync's record. A refusal of a sync PR ends with the
break-glass command that skips the checks, `git push origin <head sha>:master-shopify`, except
when `master-shopify` moved or the push would not fast-forward; a PR that is not a sync PR (closed,
draft, another base or head, no `sync` label) gets no such command, since that push would land an
unreviewed branch. `status` treats a merged PR labelled `sync` as a landed
sync, not a contribution: it is never listed as a contribution or as neither carried nor
adopted, and its `mergeCommit`, the candidate's head, does not attribute the last replay to it.
A sync PR whose `mergeCommit` is on the first-parent line without its own sync merge beneath it,
reached through single-parent replays, was merged with a GitHub button and is a warning.

## PR title cases

`pr_title_cases.py` checks `.github/workflows/pr-title.yml` against
`.github/tests/pr-title-cases.json`: simulated pull request events, one per line,
each with the expected outcome, exit code, and output of every step. It runs each
step the way GitHub does (`bash -e`, a minimal environment, the step's `env:`
expanded from the case) and reports, for each case that differs, its first differing
step and that step's output. Exit codes: 0, 1 for a difference, 2 for invalid cases,
workflow syntax it does not support, or a step that times out.

```bash
python3 -B .agents/scripts/shopify/pr_title_cases.py
```

Run it on Linux: the policy's regexes use `\S`, which macOS bash does not support.
The "PR title cases" job in `shopify-contributions.yml` runs it whenever the workflow
or the cases change, including a sync that replays the fork's patch to the workflow.
Change the cases in the same PR as the policy. `test_pr_title_cases.py` runs under
the same `unittest discover` command as above.
