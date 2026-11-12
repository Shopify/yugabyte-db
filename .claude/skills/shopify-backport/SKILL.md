---
name: shopify-backport
description: >-
  Backport a merged change to a Shopify release line in Shopify/yugabyte-db once
  Yugabyte Infrastructure has selected the target: check the target, cherry-pick
  the change with a trailer naming its original commit onto a <series>-shopify or
  <maintenance>-shopify base, write the [BACKPORT <target>] title and Original
  commit body, then push, review, and squash-merge through shopify-create-pr.
  Replaces the inherited backport-commit for the fork, which pushes to a fork of
  yugabyte/yugabyte-db.
---

# Shopify backports

A backport copies a merged change onto a `<series>-shopify` branch (`2025.2-shopify`:
series integration, preview builds, backport continuity) or a `<maintenance>-shopify`
branch (`2025.2.8-shopify`: a supported maintenance line plus selected backports). This
skill is the overlay on [shopify-create-pr](../shopify-create-pr/SKILL.md) (clone, branch,
lint, push, open, review, merge) and [REVIEW.md](../../../REVIEW.md) § Backport PRs (PR
format and review mode, with `master` read as `master-shopify` for fork-origin changes);
every rule there holds here. The inherited `backport-commit` command pushes to a fork of
`yugabyte/yugabyte-db` and opens PRs in `yugabyte/yugabyte-db`, and `review-backport` reads
Phorge; neither applies to the fork.

## Preconditions

- Yugabyte Infrastructure has named the target branches. Contributors who need a backport
  ask them; nobody else picks targets or guesses which upstream version will carry the
  fix. A backport proceeds after the Shopify merge while upstream review continues.
- The original is merged. Fork-origin: a fork PR into `master-shopify` with its approving
  review (`gh pr view <n> --repo Shopify/yugabyte-db --json state,baseRefName,mergeCommit,commits,title`);
  `<sha>` below is its original landing commit, the `mergeCommit` of the squash merge.
  Upstream-origin (an upstream commit a Shopify release line needs that its upstream
  branch lacks): `<sha>` is the upstream commit, cited with its upstream PR.
- shopify-create-pr § Preconditions: the fork-only clone (`<fork>`), `gh stack`, no raw
  `git push`, create-pr § Confidentiality.

## 1. Check the target

```bash
git fetch <fork> <target>
git log <fork>/<target> --oneline --grep "cherry picked from commit <sha>"          # an earlier backport
git log <fork>/<target> --oneline --grep "#<upstream PR>" --grep "<fork PR url>"   # a synchronized upstream fix
```

The greps match substrings, so inspect each hit. When one is the change, the target
already carries it: stop and report it.

## 2. Branch

One PR per target: `gh stack init --base <target> <backport-branch>` then
`gh stack rebase --remote <fork>`, shopify-create-pr Step 2 with the target as the base and
a branch name outside `feature-stack/`. A change that depends on code the target lacks
needs its prerequisites backported first, each as its own backport PR merged before this
one is branched, or a report to Yugabyte Infrastructure. Backports are not stacked:
`pr-title.yml` rejects a release tag on a PR whose base is another layer.

## Backport mechanics

Steps 3–6 take no decisions except Step 3's conflict resolutions, which stop the block
until someone resolves them; a script command can replace the rest without changing
this skill's other sections.

### 3. Cherry-pick and conflicts

Pick the PR's current commit on `master-shopify`'s first-parent line, never an earlier
backport:

```bash
git fetch <fork> master-shopify
git fetch https://github.com/yugabyte/yugabyte-db.git master   # second: FETCH_HEAD is now upstream master
git log --basic-regexp --first-parent --format='%H %s' --grep '(#<n>)$' \
  "$(git merge-base FETCH_HEAD <fork>/master-shopify)..<fork>/master-shopify"
```

Release builds skip a commit already on a branch, and reuse an earlier release's
cherry-pick, by its `(cherry picked from commit <sha>)` trailer, so the trailer always
names the original landing commit `<sha>`. Until a sync replays the change, the commit
found is `<sha>`, and `git cherry-pick -x <sha>` writes that trailer. A sync that rebuilds
`master-shopify` on a newer upstream base leaves a replayed copy there instead: pick it
with `git cherry-pick <copy>`, end the message with `(cherry picked from commit <sha>)`,
and name the copy in Step 5. An upstream-origin change is the upstream commit itself:
`git fetch https://github.com/yugabyte/yugabyte-db.git <upstream branch>`, then
`git cherry-pick -x <sha>`. An older fork PR merged as a merge or rebase merge has no
`(#N)` subject; it follows shopify-upstream-pr Step 3's table, whose `-n` pick adds no
trailer, so end the message with one such line per original commit.

Resolve conflicts against the target, `git add` each file, `git cherry-pick --continue
--no-edit`, and record every resolution, trivial or not, as
`<path>:<line> — <one-line description>` for Step 5. A hunk that needs code the target
lacks is the prerequisite case of Step 2.

### 4. Title and commit

The commit subject is the PR title: `[BACKPORT <target>]`, then the original title's issue
or tag and description, as in `[BACKPORT 2025.2-shopify][#37457] DocDB: ...` or
`[BACKPORT <target>][shopify-only] Build: ...`; `pr-title.yml` requires the tag to equal the
base branch. Amend with `git commit --amend --cleanup=whitespace -F <file>` (a
`commit.cleanup=strip` setting would delete the `## ` headings): the subject, a blank
line, the Step 5 body, and the `(cherry picked from commit <sha>)` line last. The fork's
squash merge keeps the commit messages, so the trailer lands with the commit.

### 5. Body

REVIEW.md's backport format under shopify-create-pr Step 4's rules (full issue URLs, no
closing keywords):

```markdown
Refs <full issue URL>

## Summary
<the original PR's summary>

## Merge conflicts
- `<path>:<line>` — <one-line description>

Picked from: <copy>
https://github.com/yugabyte/yugabyte-db/pull/<PR>
Original commit: <sha> / #<PR>

## Test plan
<what ran on the target>
```

Keep the `Refs` line only when the title cites an issue, `## Merge conflicts` only when
Step 3 resolved any, `Picked from:` only when Step 3 picked a replayed copy, and the
upstream PR URL line only for an upstream-origin change, whose `#<PR>` would otherwise
resolve against the fork.
`Original commit:` sits immediately above `## Test plan`; several originals take
`Original commits:` with one `- <sha> / #<PR>` line each, and `pr-title.yml` fails a
`[BACKPORT ...]` PR without the header and an entry. The fork has no build CI, so the test
plan is the build evidence on the target. The `#<PR>` mention of a fork PR cross-references
the original PR's timeline; no other record is needed.

### 6. Push and open

shopify-create-pr Step 5 with `<trunk>` the target and no `--add-label`.

## Review and merge

shopify-create-pr Step 6. Reviewers use REVIEW.md's backport mode: the picked commit
(`git show` of the `Picked from:` copy when the body names one, otherwise of `<sha>`) against
`gh pr diff <n> --repo Shopify/yugabyte-db` is byte-identical except for the documented
resolutions. Merge after the approving `Shopify/yugabyte-code-reviewers` review, squash
only, so the commit and its trailer land as one.

## Labels

- No `upstream: skip`: a backport copies an already merged change and is never submitted
  upstream; the label is for merges into `master-shopify`.
- Never `Available:`. `Available: X.Y.Z` on a fork PR means the patch is needed until
  upstream X.Y.Z includes the fix, and release builds cherry-pick every PR carrying one
  into builds of lower versions (`Available: nil`: a permanent patch). It belongs on the
  original PR, set by Yugabyte Infrastructure once the including version is known and
  never guessed; on a backport PR it would be picked as a second patch.

## Follow-ups and out of scope

- A correction B to a backported change A is backported the same way, its own PR with its
  own pick and trailer, when Yugabyte Infrastructure decides B needs those targets.
- Numbered releases `<full-version>-shopify-<N>` and their `-candidate` branches come from
  Shopify's release builds, not from backport PRs.
- Synchronization from upstream branches is not a backport, and it keeps each backport's
  commit message, trailer included. Release-only changes with no original are
  shopify-create-pr's, with the `[<target>]` tag.

## Commands

```bash
git fetch <fork> <target> && git log <fork>/<target> --oneline --grep "cherry picked from commit <sha>" --grep "#<upstream PR>" --grep "<fork PR url>"   # 1
gh stack init --base <target> <backport-branch> && gh stack rebase --remote <fork>   # 2; prerequisites merge first as their own PRs
git fetch <fork> master-shopify && git fetch https://github.com/yugabyte/yugabyte-db.git master   # 3; in this order: FETCH_HEAD = upstream master
git log --basic-regexp --first-parent --format='%H %s' --grep '(#<n>)$' "$(git merge-base FETCH_HEAD <fork>/master-shopify)..<fork>/master-shopify"
git cherry-pick -x <sha>                                                              # or a replayed copy: git cherry-pick <copy>, trailer naming <sha>
git add <files> && git cherry-pick --continue --no-edit                               # after resolving conflicts
git commit --amend --cleanup=whitespace -F <message-file>                             # 4: title, body, trailer last
git fetch <fork> <target> && build-support/lint.sh --rev <fork>/<target>              # 6
gh stack submit --auto --remote <fork>
gh pr edit <n> --repo Shopify/yugabyte-db --body-file <body-file> --add-reviewer Shopify/yugabyte-code-reviewers
gh pr merge <n> --repo Shopify/yugabyte-db --squash                                   # a one-layer backport is not a GitHub stack
```
