---
name: shopify-create-pr
description: >-
  Open, review, and merge any pull request in Shopify/yugabyte-db: classify the
  change as upstream-bound or fork-only, cite the upstream or fork issue, base it on
  master-shopify or a release base, build it as gh stack layers, lint, push with
  gh stack submit, and squash-merge after review, a feature stack whole. Use for
  every PR into the fork; the inherited create-pr opens PRs in yugabyte/yugabyte-db.
---

# Shopify fork PRs

Every PR into `Shopify/yugabyte-db` goes through this skill. The inherited
[create-pr](../create-pr/SKILL.md) opens PRs in `yugabyte/yugabyte-db`, and its scripts
cannot push to the fork. This skill is the fork overlay on create-pr (confidentiality,
components, body) and [stack-prs](../stack-prs/SKILL.md) (gh stack setup and operation):
every rule in those skills holds here except the ones named below. Submitting a merged
contribution upstream is [shopify-upstream-pr](../shopify-upstream-pr/SKILL.md); a backport
to a release base is [shopify-backport](../shopify-backport/SKILL.md).

## Preconditions

- A full checkout of a clone whose only remote is `Shopify/yugabyte-db` (`<fork>` below),
  such as `gh repo clone Shopify/yugabyte-db --no-upstream`, kept apart from the clone
  shopify-upstream-pr uses: `gh stack` sends PR, stack, and merge API calls to the clone's
  default repository rather than to `--remote`, and `gh stack init` has no `--remote`.
  `build-support/lint.sh` reads the changed files from the working tree. `gh auth status`
  succeeds for the developer's own account.
- `gh stack` installed and the gh-stack skill loaded, per stack-prs § Setup, except that
  `gh-stack.remote`, if set in this clone, names `<fork>`. Pass branch names, and
  `--remote <fork>` wherever a `gh stack` command takes it, so no command prompts.
- Raw `git push` is denied to agents; `gh stack` does every push below.
- create-pr § Confidentiality applies to the issue, branch name, commit, title, body, and
  comments: the fork is public. Shopify specifics: no private tracker or incident links,
  no internal hostnames.

## 1. Classify and cite the issue

Every merge into `master-shopify` is upstream-bound by default, tooling and CI included.
Fork-only work carries `[shopify-only]` in the title, in place of or after the issue ID,
and the `upstream: skip` label; the title check does not enforce the pairing.

Upstream-bound work cites, as `[#N]`, the upstream issue its upstream PR will carry: an
existing one, or one the developer opens in `yugabyte/yugabyte-db` when the problem
carries no private context. Work that starts from private evidence goes through
[shopify-create-issue](../shopify-create-issue/SKILL.md); when no upstream issue covers
the problem, that skill publishes a sanitized fork issue, the fork PR cites it, and
shopify-upstream-pr derives the upstream issue from it. Fork-only work needs no issue.

## 2. Base and branch

The base is `master-shopify`. A stacked layer's base is its parent's branch, set by
`gh stack`. Release bases (`<version>-shopify`, `<version>-shopify-N`,
`<version>-shopify-N-candidate`) take only
release-specific changes, with the release tag in the title; backports are
[shopify-backport](../shopify-backport/SKILL.md)'s. The fork's `master` mirrors upstream
and is never a base.

Name the branch for the change, in any shape; it need not match the upstream branch. Keep
it outside `feature-stack/`, the upstream submission namespace. A single PR is a
one-layer stack.

```bash
gh stack init --base master-shopify <branch>   # adopts an existing branch; creates a missing one from the local trunk
gh stack rebase --remote <fork>                # fetches master-shopify, fast-forwards the local trunk, moves the branch
gh stack add <layer-branch>                    # per dependent layer, from the top of the stack
```

## 3. Commit and title

One commit per layer, written with `git commit -F <file>`. Its subject is the PR title:
`gh stack submit --auto` takes a single commit's subject as the title and its body as the
PR body, and the fork's squash merge takes the PR title, with `(#N)` appended, and the
commit messages. The title follows `.github/workflows/pr-title.yml`:

- Upstream-bound: `[#<issue>] <Component>: <Short description>`, with Step 1's issue.
- Fork-only: `[shopify-only] <Component>: <Short description>` or `[#N][shopify-only] ...`.
- Release base: `[<base>]` first, matching the base branch exactly. `[BACKPORT <base>]` is
  a backport's and needs an `Original commit: <sha> / #<PR>` body line.

The description is 10–100 characters with no extra whitespace; components are create-pr
Step 4's. A layer with several commits gets a humanized branch name as its title: squash
first, or repair with `gh pr edit <n> --repo Shopify/yugabyte-db --title "<title>"`.

## 4. Body

Write the body to a file: `Refs <full issue URL>` as the first line whenever the title
cites an issue, since a bare `#N` in a fork title links to the fork, then `## Summary`,
`## Upgrade/Rollback safety` when create-pr's `-U` rule applies (`.proto` changes make it
mandatory upstream), and `## Test plan`.

- The summary describes the change, not the fork, so shopify-upstream-pr reuses it
  upstream verbatim: motivation, what changed, what the reader must act on. Issue
  references are full URLs; a bare `#N` names a different issue once the text is upstream.
- The test plan says what was run, on what. The fork has no build CI, so the test plan is
  the build and test evidence reviewers get.
- No closing keywords: GitHub interprets `Fixes #N` only on PRs into the default branch,
  which in the fork is `master`
  ([Linking a pull request to an issue](https://docs.github.com/en/issues/tracking-your-work-with-issues/using-issues/linking-a-pull-request-to-an-issue)).

## 5. Lint, push, open (mechanics)

This step takes no decisions, so a script command can replace it without changing the
rest of this skill.

```bash
git fetch <fork> <trunk>                     # <trunk>: master-shopify, or the release base
build-support/lint.sh --rev <fork>/<trunk>   # from the top layer; the check git-push.sh runs
gh stack submit --auto --remote <fork>       # pushes every layer with leases; opens missing PRs as drafts
gh pr edit <n> --repo Shopify/yugabyte-db --body-file <body-file> \
  --add-reviewer Shopify/yugabyte-code-reviewers [--add-label "upstream: skip"]
```

`gh stack submit`, `push`, and `sync` are the fork's exception to stack-prs' rule against
them: `git-push.sh` and `create-pr.sh` lint, push, and look up PRs through a
`yugabyte/yugabyte-db` remote, which the fork clone lacks; pointed at the fork with
`GH_REPO`, they push only to a personal fork, whose PRs GitHub cannot stack.
`gh pr edit` works on the fork, so the REST workarounds in create-pr § Notes and stack-prs
do not apply here; run the edit once per new PR.

## 6. Review and merge

PRs open as drafts; `gh pr ready <n> --repo Shopify/yugabyte-db` when the developer says
so. Review changes go into the layer's commit (stack-prs' history-rewrite rule applies:
every fork PR is a gh stack layer), then `gh stack rebase --upstack --remote <fork>`, the
Step 5 lint, and `gh stack push --remote <fork>`.

The author merges after an approving review from a `Shopify/yugabyte-code-reviewers`
member with `pr-title` and `pr-lint` passing. No branch protection enforces any of this,
so check before merging:

```bash
gh pr view <n> --repo Shopify/yugabyte-db --json reviewDecision,latestReviews,statusCheckRollup
gh api orgs/Shopify/teams/yugabyte-code-reviewers/memberships/<login> --jq .state   # active; 404 = not a member, whatever scope gh hints
```

`bld-db` stays queued (the fork has no build runner) and `bld-none` is skipped; neither is
a gate, and there is no Jenkins to trigger. Squash every PR: a squash commit is one PR's,
where a merge commit is shared by every PR merged with it.

Every fork PR merges before it is submitted upstream, stack layers included. A feature
stack whose layers are useless apart lands whole once every layer is approved; an
independent fix inside a stack lands on its own, with the layers below it.

```bash
gh pr merge <n> --repo Shopify/yugabyte-db --squash   # a PR not in a GitHub stack (stack-prs § Landing)
gh stack merge <top-pr> --yes --squash                 # a stack: atomic, one squash commit per PR
gh stack merge <fix-pr> --yes --squash                 # an independent fix in a stack, and the layers below it
```

## After merge

- Upstream-bound: [shopify-upstream-pr](../shopify-upstream-pr/SKILL.md), a stack layer
  by layer from the bottom. Fork-only: nothing more.
- Layers still open above an independent fix that merged: `gh stack rebase --remote <fork>`,
  the Step 5 lint, `gh stack push --remote <fork>`, then
  `gh stack sync --remote <fork> --prune` (stack-prs § Syncing with the fork remote).
- Opting out later: `gh pr edit <n> --repo Shopify/yugabyte-db --add-label "upstream: skip"`.
  Removing the label makes an unsubmitted change upstream-bound again.

## Commands

```bash
gh stack init --base master-shopify <branch> && gh stack rebase --remote <fork>                    # 2; then gh stack add per layer
git fetch <fork> <trunk> && build-support/lint.sh --rev <fork>/<trunk>                             # 5
gh stack submit --auto --remote <fork>
gh pr edit <n> --repo Shopify/yugabyte-db --body-file <body-file> --add-reviewer Shopify/yugabyte-code-reviewers [--add-label "upstream: skip"]
gh pr ready <n> --repo Shopify/yugabyte-db                                                          # 6
gh pr view <n> --repo Shopify/yugabyte-db --json reviewDecision,latestReviews,statusCheckRollup
gh api orgs/Shopify/teams/yugabyte-code-reviewers/memberships/<login> --jq .state                  # active = member
gh pr merge <n> --repo Shopify/yugabyte-db --squash   # or, for a stack: gh stack merge <top-pr> --yes --squash
gh stack rebase --remote <fork> && git fetch <fork> <trunk> && build-support/lint.sh --rev <fork>/<trunk> \
  && gh stack push --remote <fork> && gh stack sync --remote <fork> --prune   # after an independent fix merges, from the top layer
```
