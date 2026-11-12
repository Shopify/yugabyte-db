---
name: shopify-upstream-pr
description: >-
  Submit a Shopify contribution merged into master-shopify to yugabyte/yugabyte-db
  as the original developer: reuse or derive the upstream issue, choose the base or
  stack parent, cherry-pick onto feature-stack/Shopify/<change>, open the PR with
  create-pr.sh, and record it on the fork PR. Also covers the upstream PR's later
  rebase, corrections, replacement, closure, and opt-out.
---

# Shopify upstream PRs

A contribution merged into `master-shopify` with an approving review from
`Shopify/yugabyte-code-reviewers` goes upstream unless its fork PR carries the
`upstream: skip` label. The original developer submits it under their own identity:
upstream squash-merges, so the PR opener becomes the commit author. This skill is the
Shopify overlay on [create-pr](../create-pr/SKILL.md) (confidentiality, lint, push, PR
creation) and [stack-prs](../stack-prs/SKILL.md) (stack setup, syncing, landing): it
states only what Shopify adds or changes, and every rule in those skills holds here.
Step 1 takes judgement and Steps 2–6 are mechanics; the `submit` command will run all
six (see [Tooling](#tooling)).

## Preconditions

- The fork PR is merged into `master-shopify` with the approving review and has no
  `upstream: skip` label:
  `gh pr view <n> --repo Shopify/yugabyte-db --json state,baseRefName,reviewDecision,labels,title,body`.
  Every fork PR, a stack layer included, merges before it is submitted. Its title cites
  the upstream issue or a fork issue in `[#N]`, and its body's first line,
  `Refs <full issue URL>`, names which; a title carrying `[shopify-only]` is fork-only
  work: stop.
- You are in the developer's clone of `yugabyte/yugabyte-db`, set up per stack-prs
  § Setup: a remote pointing at `yugabyte/yugabyte-db` (`<upstream>` below), `gh stack`
  installed, `git config gh-stack.remote <upstream>`. `gh auth status` succeeds for the
  developer's own account. `gh repo set-default --view` prints `yugabyte/yugabyte-db`
  (run `gh repo set-default yugabyte/yugabyte-db` if none is set): `gh stack` sends its
  API calls to that default repository, so this is not the fork-only clone that
  `shopify-create-pr` uses.
- The branch is the two-segment `feature-stack/Shopify/<change>`; `git-push.sh` warns on
  any other shape.
- `.agents/scripts/shopify/` exists only on the fork's `master-shopify`: when Step 1 needs
  `issues.py`, run it from a checkout of that branch before Step 3 switches to the stack
  branch. From an upstream-only clone:
  `git fetch https://github.com/Shopify/yugabyte-db.git master-shopify && git worktree add <dir> FETCH_HEAD`.
- create-pr § Confidentiality applies to the issue, branch name, commit, title, body,
  and comments. Shopify specifics: no private tracker or incident links, no internal
  hostnames. Fork issue and fork PR text are already public-safe: copy, add nothing.

## 1. Upstream issue

The upstream PR cites the upstream issue the fork PR's title promised. When the title
cites an upstream issue, reuse it.

When the title cites a fork issue, the work started from private evidence, and the
upstream issue is derived from that fork issue. Look for an existing one first: an
`Upstream issue: <url>` comment on the fork issue
(`gh issue view <N> --repo Shopify/yugabyte-db --comments`) and, when a private tracker
exists, the `public_issues` reported by
`python3 .agents/scripts/shopify/issues.py inspect --issue <private-issue-url>`. Reuse an
upstream issue that already covers the problem
(`gh issue list --repo yugabyte/yugabyte-db --state all --search "<keywords>"`) rather
than creating a duplicate.

Otherwise derive one. Write the fork issue body to a file
(`gh issue view <N> --repo Shopify/yugabyte-db --json body -q .body > <file>`) and
remove two things: the trailing `<!-- shopify-yb-publication:… -->` line, and the
`## Shopify context` section, from that heading up to the next `###` heading. Keep the
title verbatim. The body already follows the upstream form headings. Pass no labels:
triage is upstream's.

```bash
gh issue create --repo yugabyte/yugabyte-db --title "<fork issue title>" --body-file <file>
```

Record it: when a private tracker exists,
`python3 .agents/scripts/shopify/issues.py link --issue <private-issue-url> --public-issue <upstream-issue-url>`;
always `gh issue comment <N> --repo Shopify/yugabyte-db --body "Upstream issue: <url>"`.

## Submission mechanics

Steps 2–6 take no decisions beyond the stops they name. The `submit` command will run
them; until it exists, run them by hand.

### 2. Base or stack parent

The fork stack names the dependency: a layer depends on the layer below it in its fork
stack. A merged layer keeps that layer's branch as its recorded base, so a `baseRefName`
other than `master-shopify` from `gh pr view <n> --repo Shopify/yugabyte-db --json baseRefName`
names the parent, and
`gh pr list --repo Shopify/yugabyte-db --state merged --head <baseRefName> --json number,url`
finds its fork PR. A change outside a fork stack has a parent when its Step 3 cherry-pick
does not apply on upstream `master` without an unlanded Shopify change; that change is
the parent.

Upstream state then decides. Read the latest `Upstream:` comment on the parent's fork PR
and check that upstream PR (`gh pr view <url> --json state,mergeCommit,headRefName`;
`headRefName` is the `feature-stack/Shopify/<parent-change>` to stack on). A merged PR
has landed only when its merge commit is on upstream `master`:
`gh api repos/yugabyte/yugabyte-db/compare/master...<merge-commit> --jq .ahead_by`
prints `0`.

- No parent, or it has landed: base `master`.
- It is merged but has not landed: it was restructured (see After opening). Wait for
  the replacement in a new `Upstream:` comment.
- It has an open upstream PR: stack on it; the base is `feature-stack/Shopify/<parent-change>`.
- It has no upstream PR yet: submit it first. This change waits.
- It carries `upstream: skip`: this change cannot go upstream as is. Stop and raise it.
- This change depends on several unlanded upstream PRs: stop and decide the arrangement
  with the team, one chain in dependency order.

Stacking on another developer's PR puts both changes in one GitHub stack, which
stack-prs works from one clone: agree who runs that stack's rebases and pushes.

### 3. Branch and commit

Name the branch `feature-stack/Shopify/<change-name>`: the feature segment is literally
`Shopify`, and `<change-name>` is a short lowercase hyphenated slug proposed from the
fork PR title. Layers of a multi-layer Shopify stack share a stem and carry a layer
number, such as `uniq-idx-1-backfill-job-mode`. Every submission uses this namespace, a
single independent change included (a one-layer gh stack), so it can become a stack
parent later. Shopify developers have push access there.

- Base `master`: `gh stack init feature-stack/Shopify/<change>`, then
  `gh stack rebase --remote <upstream>`: init starts from the local `master`; rebase
  fetches, fast-forwards it, and moves the branch.
- Stacked on a parent whose PR is not yet in a GitHub stack
  (`gh api repos/yugabyte/yugabyte-db/pulls/<pr> --jq .stack` prints nothing; a GitHub
  stack needs two PRs, so this is every first submission): fetch its branch, create the
  local branch from `<upstream>/feature-stack/Shopify/<parent-change>`, then
  `gh stack init feature-stack/Shopify/<parent-change> feature-stack/Shopify/<change>`.
- Stacked on a parent already in a GitHub stack: `gh stack checkout <parent PR url>`
  when this clone does not track the stack, then `gh stack add feature-stack/Shopify/<change>`
  adds the layer on the stack's top; a top that is not the parent is the
  several-dependencies case of Step 2.

Pick the PR's current commit on `master-shopify`'s first-parent line, authorship
preserved:

```bash
git fetch https://github.com/Shopify/yugabyte-db.git master-shopify
git log --basic-regexp --first-parent --format='%H %s' --grep '(#<n>)$' \
  "$(git merge-base FETCH_HEAD <upstream>/master)..FETCH_HEAD"
```

A sync that rebuilds `master-shopify` on a newer upstream base replays the carried
commits, so the copy found there is adapted to current upstream; the PR's `mergeCommit`
stays reachable but older. Until a sync has done so, and for a PR merged another way,
the commit is the `mergeCommit` from
`gh pr view <n> --repo Shopify/yugabyte-db --json mergeCommit,commits`, and its shape
decides the pick. Fork PRs are squash-merged
([shopify-create-pr](../shopify-create-pr/SKILL.md)), but the fork also allows merge
and rebase merges:

- `mergeCommit` has two parents (merge commit): `git cherry-pick -n <c1> <c2>` with
  every `commits[].oid`, oldest first, skipping any merge commit among them.
- `mergeCommit` has one parent, the PR has `<count>` commits, and the `<count>` commits
  ending at `mergeCommit` carry the PR's commit subjects in order
  (`git log --reverse --format=%s <mergeCommit>~<count>..<mergeCommit>` against
  `gh api --paginate repos/Shopify/yugabyte-db/pulls/<n>/commits --jq '.[].commit.message | split("\n")[0]'`;
  `commits[].messageHeadline` truncates long subjects): a rebase merge.
  `git cherry-pick -n <mergeCommit>~<count>..<mergeCommit>`.
- Any other one-parent `mergeCommit` is a squash and holds the whole PR:
  `git cherry-pick -x <mergeCommit>`.

The `-n` cases end in one `git commit --author="<original author>" -F <message-file>`
with a `Co-authored-by:` trailer per other author; a single-commit pick with `-x` takes
`git commit --amend` for the subject. Confirm with `git diff <base>` against
`gh pr diff <n> --repo Shopify/yugabyte-db`. The subject is the Step 4 title, and any
bare `#N` in copied text becomes a full fork URL without its closing keyword: a fork
number pushed upstream links to an unrelated upstream issue, and upstream's squash
message takes the PR body.

### 4. Title and body

The title copies the fork PR's title when that cites the upstream issue; when it cites
a fork issue, the Step 1 upstream issue replaces that number. Either way it follows the
grammar `.github/workflows/pr-title.yml` checks here and upstream,
`[#<upstream-issue>] <Component>: <Short description>` with a 10–100 character
description and no extra whitespace. Pass `-i <upstream-issue>` and
`-t "<Component>: <Short description>"` to `create-pr.sh`.

The summary and test plan go in separate files, as create-pr Step 5 describes. Reuse
the fork PR's `## Summary` and `## Test plan` verbatim; shopify-create-pr writes them
for upstream. The summary ends with
`Shopify fork PR: https://github.com/Shopify/yugabyte-db/pull/<n>` and, for a stacked
layer, `Stacked on #<parent-pr>, based on feature-stack/Shopify/<parent-change>, so the
diff is exactly this PR's commit.`

### 5. Open the PR

```bash
.agents/scripts/create-pr.sh -i <upstream-issue> -t "<Component>: <Short description>" \
  -d <summary-file> -T <test-plan-file> [-U <upgrade-file>] [-D]
```

Do not pass `-b`; the base comes from `gh stack`. `-U` is required by the script when the
change touches `.proto` files. Pass `-D` only when the developer wants to read the PR
first, then `gh pr ready <num> -R yugabyte/yugabyte-db`. Exit codes and recovery are
create-pr's; exit `4` never happens for a stack branch.

`create-pr.sh` refuses (exit 1) while a layer above already has a PR, which is the
restructure case below. Then replay the script's own calls by hand, the one exception
to create-pr § Notes' rule against `gh pr create`: `.agents/scripts/git-push.sh`; a body
file with the script's `## Summary`, optional `## Upgrade/Rollback safety`, and
`## Test plan` sections;
`gh pr create --repo yugabyte/yugabyte-db --base <layer below, or master>
--head feature-stack/Shopify/<change> --title "[#<upstream-issue>] <Component>: <Short description>"
--body-file <body-file> --draft`; `gh stack link --remote <upstream> --base master <PRs bottom-up>`;
then `gh pr ready <num> -R yugabyte/yugabyte-db`, so CI first sees a stack member.

### 6. Record it on the fork PR

```bash
gh pr comment <fork-pr> --repo Shopify/yugabyte-db --body "Upstream: <upstream PR url>"
```

Add a new such comment whenever the upstream PR is replaced. Nothing of ours reads
labels on upstream PRs.

## After opening

- A parent squash-merging deletes its branch upstream and GitHub retargets the child
  onto the new base. Rebase and push with the stack-prs § Syncing commands; Jenkins
  follows stack-prs § Jenkins.
- Upstream review asks for changes. When the fork needs the correction before upstream
  accepts it, land it first as a follow-up fork PR into `master-shopify`
  (`shopify-create-pr`) whose body links the original fork PR and this upstream PR. Once
  it merges, cherry-pick it onto this branch as a new commit with its subject rewritten
  per Step 3 (no fork `#N` upstream), push per stack-prs § Updating a layer, and comment
  `Upstream: <this upstream PR url>` on the follow-up fork PR (Step 6) so it is not
  submitted again. Otherwise change only the upstream PR: the synchronization that
  brings the accepted commit into the fork adopts it. Yugabyte Infrastructure decides
  whether a follow-up needs backporting.
- A merged PR has landed only when its merge commit is on upstream `master` (the Step 2
  check); the base GitHub records does not decide it. A stack merge records upper
  layers against their parent branch although they land on `master`. A force-push that
  reorders a stack can mark a PR merged into its parent's branch without landing it:
  that is a restructure. Open the replacement PR (Step 5) and record it (Step 6).
- A PR closed without merging is not silently replaced: note the outcome on the fork
  PR and raise it with the team.
- Opting out later: `gh pr edit <n> --repo Shopify/yugabyte-db --add-label "upstream: skip"`
  (`gh pr edit` works on the fork). An already open upstream PR is not closed by that.
  Removing the label makes an unsubmitted change upstream-bound again.

## Tooling

`submit <fork-pr>`, a command in a script beside `issues.py`, will perform Steps 1–6
under the developer's own `gh` login. It applies Step 2's dependency rule, takes
`--parent` to override it, stops where the steps stop, and is rerun after a failure.
Until it exists, follow the steps by hand as written here.

## Commands

```bash
gh issue create --repo yugabyte/yugabyte-db --title "<fork issue title>" --body-file <file>   # 1, title cites a fork issue
python3 .agents/scripts/shopify/issues.py link --issue <private-issue-url> --public-issue <upstream-issue-url>
gh issue comment <N> --repo Shopify/yugabyte-db --body "Upstream issue: <upstream-issue-url>"
gh pr view <n> --repo Shopify/yugabyte-db --json baseRefName                                  # 2: fork stack parent
gh pr list --repo Shopify/yugabyte-db --state merged --head <baseRefName> --json number,url
gh pr view <parent upstream PR url> --json state,mergeCommit,headRefName
gh api repos/yugabyte/yugabyte-db/compare/master...<merge-commit> --jq .ahead_by           # 0 = landed
gh stack init feature-stack/Shopify/<change> && gh stack rebase --remote <upstream>   # 3: base master; see Step 3 for the stacked forms
git fetch https://github.com/Shopify/yugabyte-db.git master-shopify
git log --basic-regexp --first-parent --format='%H %s' --grep '(#<n>)$' "$(git merge-base FETCH_HEAD <upstream>/master)..FETCH_HEAD"
git cherry-pick -x <commit>                             # one commit; see Step 3 for rebase and merge commits
.agents/scripts/create-pr.sh -i <upstream-issue> -t "<Component>: <Short description>" -d <summary-file> -T <test-plan-file> [-U <upgrade-file>]   # 5
gh pr comment <fork-pr> --repo Shopify/yugabyte-db --body "Upstream: <upstream PR url>"      # 6
gh stack rebase --remote <upstream> && .agents/scripts/git-push.sh && gh stack sync --remote <upstream> --prune   # after a parent lands
```
