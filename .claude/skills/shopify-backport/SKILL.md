---
name: shopify-backport
description: >-
  Backport a merged change to a Shopify release line in Shopify/yugabyte-db once
  Yugabyte Infrastructure has selected the target: check the target, cherry-pick
  the change with a trailer naming its original commit onto a <series>-shopify or
  <maintenance>-shopify base, write the [BACKPORT <target>] title and Original
  commit body, then push, run a Binks review, review, and squash-merge through
  shopify-create-pr.
  Replaces the inherited backport-commit for the fork, which pushes to a fork of
  yugabyte/yugabyte-db.
---

# Shopify backports

A backport copies a merged change onto a `<series>-shopify` branch (`2025.2-shopify`:
series integration, preview builds, backport continuity) or a `<maintenance>-shopify`
branch (`2025.2.8-shopify`: a supported maintenance line plus selected backports). This
skill is the overlay on [shopify-create-pr](../shopify-create-pr/SKILL.md) (clone, branch,
lint, push, open, Binks review, review, merge) and [REVIEW.md](../../../REVIEW.md)
§ Backport PRs (PR format and review mode, with `master` read as `master-shopify` for
fork-origin changes); every rule there holds here. The inherited `backport-commit` command pushes to a fork of
`yugabyte/yugabyte-db` and opens PRs in `yugabyte/yugabyte-db`, and `review-backport` reads
Phorge; neither applies to the fork.

## Preconditions

- Yugabyte Infrastructure has named the target branches. Contributors who need a backport
  ask them; nobody else picks targets or guesses which upstream version will carry the
  fix. A backport proceeds after the Shopify merge while upstream review continues.
- The original is merged. Fork-origin: a fork PR into `master-shopify` with its approving
  review (`gh pr view <n> --repo Shopify/yugabyte-db --json state,baseRefName,mergeCommit,commits,title`);
  `<sha>` below is its original landing commit, the PR's `mergeCommit`.
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
until someone resolves them, and its choice of source for a change a sync has dropped;
a script command can replace the rest without changing this skill's other sections.

### 3. Cherry-pick and conflicts

Pick the PR's current commit on `master-shopify`'s first-parent line, never an earlier
backport:

```bash
git fetch <fork> master-shopify
git fetch https://github.com/yugabyte/yugabyte-db.git master   # second: FETCH_HEAD is now upstream master
git log --basic-regexp --first-parent --no-merges --format='%H %s' --grep '(#<n>)$' \
  "$(git merge-base FETCH_HEAD <fork>/master-shopify)..<fork>/master-shopify"
```

The `(cherry picked from commit <sha>)` trailer always names the original landing commit
`<sha>`: Step 1 finds earlier backports by it, and `release.py` identifies a PR's patch
by the same commit when it looks for earlier cherry-picks. Until a sync replays the
change, the commit found is `<sha>`, and `git cherry-pick -x <sha>` writes that trailer.
A sync that rebuilds `master-shopify` on a newer upstream base leaves a replayed copy
there instead: pick it with `git cherry-pick <copy>`, end the message with
`(cherry picked from commit <sha>)`, and name the copy in Step 5. When a sync has
dropped the change because its upstream PR landed, the search finds nothing. Pick `<sha>`
with `git cherry-pick -x <sha>`, or, when upstream's landed version suits the target
better (upstream review changed it, or it applies more cleanly), the commit the latest
`Upstream:` PR landed as (`gh pr view <upstream PR url> --json mergeCommit`), picked
like a replayed copy: without `-x`, the message ending with
`(cherry picked from commit <sha>)`, and named in Step 5. An upstream-origin change is
the upstream commit itself:
`git fetch https://github.com/yugabyte/yugabyte-db.git <upstream branch>`, then
`git cherry-pick -x <sha>`. An older fork PR merged another way has no `(#N)` subject. A
merge commit that brings in only that PR's commits
(`git rev-list --count <mergeCommit>^1..<mergeCommit>^2` equals the PR's commit count) is
picked whole with `git cherry-pick -x -m 1 <mergeCommit>`: that keeps the landed change,
merge resolutions included, and a trailer naming the commit `release.py` uses for the
PR. A merge commit shared with other PRs (a stack merge), or a rebase merge, is
cherry-picked from the PR's own commits with `-n` and recommitted; `-n` adds no trailer,
so end the message with one such line per picked commit.

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
Step 3 resolved any, `Picked from:` only when Step 3 picked a commit other than `<sha>`,
and the upstream PR URL line only for an upstream-origin change, whose `#<PR>` would
otherwise resolve against the fork.
`Original commit:` sits immediately above `## Test plan`; several originals take
`Original commits:` with one `- <sha> / #<PR>` line each, and `pr-title.yml` fails a
`[BACKPORT ...]` PR without the header and an entry. The fork has no build CI, so the test
plan is the build evidence on the target. The `#<PR>` mention of a fork PR cross-references
the original PR's timeline; no other record is needed.

### 6. Push and open

shopify-create-pr Step 5 with `<trunk>` the target and no `--add-label`.

## Review and merge

shopify-create-pr Steps 6 and 7: a Binks review, then human review. A Binks finding on a
hunk identical to the original is answered by naming the original PR, as Step 6 there
says; a real defect in a documented conflict resolution is fixed here. Reviewers use
REVIEW.md's backport mode: the picked commit (`git show` of the `Picked from:` copy when
the body names one, otherwise of `<sha>`, with `--first-parent` when `<sha>` is a merge
commit) against
`gh pr diff <n> --repo Shopify/yugabyte-db` is byte-identical except for the documented
resolutions. Merge after the approving `Shopify/yugabyte-code-reviewers` review, with the
Binks review covering the PR's current changes, squash only, so the commit and its
trailer land as one.

## Labels

- No `upstream: skip`: a backport copies an already merged change and is never submitted
  upstream; the label is for merges into `master-shopify`.
- No `Available:` on the backport of a fork PR. `release.py` plans each Shopify release
  from every fork PR that carries an `Available:` label, whatever its state or base,
  unless the label says the target's upstream version already includes the change; a
  lone `Available: nil` is always included (`.agents/scripts/shopify/README.md`
  § Release planner). The label belongs on the original PR, set by Yugabyte
  Infrastructure and never guessed; on a backport PR, `release.py` would pick the
  backport as a second patch.
- An upstream-origin change has no fork PR to carry the label, and `release.py` builds
  each release from the upstream tag, not from a release line. For the change to reach
  numbered releases whose tag lacks it, Yugabyte Infrastructure labels its first
  backport PR as the record; later backports of the same change stay unlabelled.
  `release.py` orders patches by landing time, so it plans a record that landed after
  a fork change needing it after that change. On a candidate whose tag lacks the
  record, cherry-pick the record just before that change, then keep to the plan:
  `release.py` accepts the reordered candidate only once every patch it planned up to
  the record is on it, so run it again no earlier.

## Follow-ups and out of scope

- A correction B to a backported change A is backported the same way, its own PR with its
  own pick and trailer, when Yugabyte Infrastructure decides B needs those targets.
- Numbered releases `<version>-shopify-<n>` are built on their `-candidate` branches from
  the commands `release.py` prints and land through a release PR
  (shopify-create-pr § Release PRs), not through backport PRs.
- Synchronization from upstream branches is not a backport, and it keeps each backport's
  commit message, trailer included. Release-only changes with no original are
  shopify-create-pr's, with the `[<target>]` tag.

## Commands

```bash
git fetch <fork> <target> && git log <fork>/<target> --oneline --grep "cherry picked from commit <sha>" --grep "#<upstream PR>" --grep "<fork PR url>"   # 1
gh stack init --base <target> <backport-branch> && gh stack rebase --remote <fork>   # 2; prerequisites merge first as their own PRs
git fetch <fork> master-shopify && git fetch https://github.com/yugabyte/yugabyte-db.git master   # 3; in this order: FETCH_HEAD = upstream master
git log --basic-regexp --first-parent --no-merges --format='%H %s' --grep '(#<n>)$' "$(git merge-base FETCH_HEAD <fork>/master-shopify)..<fork>/master-shopify"
git cherry-pick -x <sha>                                                              # or a replayed copy: git cherry-pick <copy>, trailer naming <sha>
git add <files> && git cherry-pick --continue --no-edit                               # after resolving conflicts
git commit --amend --cleanup=whitespace -F <message-file>                             # 4: title, body, trailer last
git fetch <fork> <target> && build-support/lint.sh --rev <fork>/<target>              # 6
gh stack submit --auto --remote <fork>
gh pr edit <n> --repo Shopify/yugabyte-db --body-file <body-file>
devx binks --json review run https://github.com/Shopify/yugabyte-db/pull/<n> --expect-head "$(git rev-parse <backport-branch>)"   # review: shopify-create-pr Step 6
devx binks --json review list --pr https://github.com/Shopify/yugabyte-db/pull/<n>
gh pr edit <n> --repo Shopify/yugabyte-db --add-reviewer Shopify/yugabyte-code-reviewers           # shopify-create-pr Step 7
gh pr merge <n> --repo Shopify/yugabyte-db --squash                                   # a one-layer backport is not a GitHub stack
```
