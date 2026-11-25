---
name: shopify-sync-resolve
description: >-
  Resolve a conflict or an empty pick that stopped `upstream.py sync` while it replayed
  master-shopify's carried landings onto a newer upstream commit: read both sides, keep
  the fork's change and upstream's change, verify, continue the sync, then build and test
  the candidate and leave a resolution note on the sync PR before review is requested.
---

# Resolving a sync stop

`upstream.py sync` rebuilds `master-shopify` on a newer upstream commit by replaying every
carried landing (`.agents/scripts/shopify/README.md` § sync). A replay that conflicts, or that
applies empty because upstream already has the change, stops the run with exit 4, the
conflicting paths, and the rerun command. This skill takes it from that stop to a sync PR
that a reviewer can approve. The person who ran the sync drives it; the candidate's worktree
and the tool's state (`shopify-sync.json` in the worktree's git directory) exist only on
that machine.

The fork's carried change and upstream's change are both reviewed code. The job is to keep
both; it is not to improve either.

## 1. Read the stop

The stop names the worktree (`<checkout>/../yugabyte-db-sync-<sha>`), the landing that
stopped, its fork PRs, the paths, and a rerun command with absolute paths. In the worktree:

```bash
git status                                                # unmerged paths (UU, AA, DU, ...)
git log -1 --format='%H %s' CHERRY_PICK_HEAD              # the fork landing being replayed
git diff CHERRY_PICK_HEAD^ CHERRY_PICK_HEAD -- <path>     # the fork's change to that path (first parent, so a stack merge works too)
git log --oneline <old upstream base>..HEAD -- <path>     # upstream's changes to it since the last sync
```

The old upstream base is the one `upstream.py status` names in its header before the sync
(`since upstream <sha>`).

Read the fork PR that the landing belongs to, and the upstream commits that touched the path.
Know what each side was for before editing anything.

## 2. Decide which case it is

- **Both changes still apply**: the usual case. Resolve (step 3).
- **The stop says the replay applied empty**: upstream already has the change. Confirm by
  comparing the fork PR's diff with upstream's commit, then `git cherry-pick --skip` in the
  worktree and rerun with `--continue`. The landing is neither carried nor dropped, and
  `status` warns about its PR until it has a disposition; note it on the fork PR and raise
  it with the team.
- **The stop is a conflict, but upstream turns out to have an equivalent change**: the tool
  saved this stop as a conflict, so a plain `git cherry-pick --skip` would replay the same
  landing and stop again. With the developer's agreement, `git cherry-pick --abort` and rerun
  with `--continue --skip '#<fork PR>'`; the PR body records it as skipped after a conflict.
  Note on the fork PR which upstream commit made it redundant.
- **Upstream's version should win**: upstream reworked the code so that the fork's change is
  obsolete or must be rewritten. Do not decide this alone: it drops a carried change. With
  the developer's agreement, `git cherry-pick --abort` and rerun with
  `--continue --skip '#<fork PR>'`; the PR body's warnings record it. A rewrite of the fork's
  change is a new fork PR after the sync lands.
- **The two contradict**, so keeping both is impossible: stop and ask.

## 3. Resolve

- Edit only the paths the stop names. No other file changes in the replay.
- Keep upstream's change exactly as upstream wrote it, and the fork's change with the same
  behaviour it had; adapt the fork's change to upstream's new code only as far as needed to
  keep that behaviour.
- Never edit upstream code into a replay, and never fix upstream code seen in the diff,
  however wrong it looks: report it upstream with create-issue or `upstream.py submit`.
- Prefer placing the fork's lines where a later replay of the same file will still apply
  cleanly; an unchanged line between the two edits lets the next landing that touches the
  file merge without a second stop.
- Remove every conflict marker: `git diff --check` and
  `git grep -n -e '^<<<<<<< ' -e '^>>>>>>> ' -- <path>` print nothing.

## 4. Verify before continuing

- `git diff CHERRY_PICK_HEAD -- <path>` shows only upstream's change; `git diff HEAD -- <path>`
  shows only the fork's change. Anything else is a third change: undo it.
- `.github/workflows/pr-title.yml`: run the fixture against the resolved workflow on Linux;
  macOS's regex library lacks `\S`, so every case fails there:

  ```bash
  docker run --rm -v "$PWD:/repo:ro" -w /repo python:3.12-slim \
    sh -c 'apt-get -qq update >/dev/null && apt-get -qq install -y bash >/dev/null && \
           python3 -B .agents/scripts/shopify/pr_title_cases.py'
  ```

  Add a case to `.github/tests/pr-title-cases.json` in a later fork PR when upstream's
  change adds behaviour the fixture does not cover.
- `.agents/scripts/shopify/**`: `python3 -B -m unittest discover -s .agents/scripts/shopify -p 'test_*.py'`.
- `src/`, `java/`, `managed/`: build the affected targets and run the tests the fork PR's test
  plan names, in the worktree, before continuing.
- `build-support/lint.sh --rev HEAD` on the resolved paths.

## 5. Continue

```bash
git add <path>...
git cherry-pick --continue --no-edit
<the rerun command the stop printed>          # upstream.py sync --continue --to <sha> --repo-path <checkout>
```

Keep the replay's message as it was; `--continue` adds the `Shopify-PR:` trailer where the
replay needs one. The run replays the remaining landings, and a later stop repeats steps
1–5. It then shows the plan and asks before pushing and opening (or refreshing) the PR.

## 6. Before review is requested

- Build the candidate and run the tests. Until the sync PR carries build and test statuses,
  this is the resolver's job: the candidate `shopify-sync/<sha>` on the custom YugabyteDB
  builder, plus the tests named by every carried landing that touches `src/`, `java/` or
  `managed/`. Post what ran and the result on the sync PR.
- Leave a resolution note on the sync PR, one per resolved landing:

  ```markdown
  Resolution: #<fork PRs> (<old sha> -> <new sha>), conflicted with upstream <commits>
  - <path>: kept upstream's <what>; kept the fork's <what>; <where the fork's lines went and why>
  - Verified: <fixture / tests / build that ran, and their result>
  ```

  The approver reviews the sync from its body (dropped and carried tables, the range-diff of
  each `Conflict: yes` row) and these notes, not from Files changed, which is upstream's diff.
- Binks: as for any sync PR. Its findings on upstream code are answered as a class in one PR
  comment. A finding on a resolution means rebuilding the candidate, since the tool never
  rewrites a pushed one (`--continue` refuses when `origin`'s head is not an ancestor of the
  local one):
  1. Save the affected replay's version of each resolved file, not the final candidate's,
     which carries later landings' changes: `git show <replay sha>:<path> > <dir outside the
     worktree>/<file>`, with the replay's new SHA from the PR's carried table.
  2. Leave the worktree, which the abort removes: `cd <checkout>`.
  3. `python3 <checkout>/.agents/scripts/shopify/upstream.py sync --abort --to <sha>
     --repo-path <checkout> --yes` removes the worktree, the local branch and the branch on
     `origin`, which closes the PR.
  4. `python3 <checkout>/.agents/scripts/shopify/upstream.py sync --to <sha> --repo-path
     <checkout>` stops at the same conflict.
  5. Resolve again from step 3, starting from the saved version and fixing the finding, then
     verify (step 4) and continue (step 5). The rerun opens a new PR; link the old one from it.

Landing is `upstream.py sync --land <pr>` after approval (`.agents/scripts/shopify/README.md`
§ sync); never a merge button.

## Worked example

Sync to upstream `b9ab12f8769a` (2026-11-24) stopped on the replay of the `#41 #42 #43 #46`
stack landing in `.github/workflows/pr-title.yml`. Upstream had added a `STACK_BACKPORT_RE`
block to the "Check Backport title" step, mapping a backport-stack layer's base
(`feature-stack/backport-<branch>-...`) to `<branch>`; the fork had replaced that step's base
test with `RELEASE_BRANCH_RE`, adding `-shopify` release names. Resolution: the fork's two
regexes and its comment kept; upstream's six lines inserted after the fork's comment, with
`(-shopify)?` added to the version group so a backport stack onto a Shopify release line maps
too. The unchanged comment line between the two edits let the later replay of the `#55`–`#64`
stack, which rewrote the two regex lines, apply without a second stop. Verified with the
fixture runner on Linux (targeted cases, then the full fixture in the PR's `PR title cases`
check). Shopify/yugabyte-db#140.
