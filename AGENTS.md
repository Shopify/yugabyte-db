# AGENTS.md

This document provides a guide for agents working on YugabyteDB

### Shopify contributions

For Shopify-originated issue intake, public issue publication, or private/public
issue linking, use
[shopify-create-issue](.claude/skills/shopify-create-issue/SKILL.md). Its public
publication target is `Shopify/yugabyte-db`. Source evidence stays in the configured
private tracker; normal sanitized publication does not require a separate approval.
Use the Shopify tool for publication and recovery rather than the inherited
upstream `create-issue` workflow. Existing public upstream issues are reused as the
public record, and the fork PR cites them directly.

A Shopify contribution is a reviewed PR merged into `master-shopify` with an
approving review from `Shopify/yugabyte-code-reviewers`. Series and maintenance
branches use the `<release>-shopify` suffix. Every merge into `master-shopify` is
upstream-bound by default, tooling and CI changes included. The `upstream: skip`
label on the fork PR opts out: the change stays in the fork and is not submitted,
and removing the label makes a change that has not been submitted upstream-bound again.

Every fork PR merges into `master-shopify` before it is submitted upstream, stack
layers included. Fork PRs squash-merge, so each lands as one `(#N)` commit. A feature
stack whose layers are useless apart lands whole with
`gh stack merge <top-pr> --yes --squash`; independent fixes land on their own.

Submission is developer-forward: the original developer runs one command, `submit`,
which performs shopify-upstream-pr's steps under their own identity. Until `submit`
exists, developers do these steps by hand following the skills; the
`shopify-upstream-pr`, `shopify-create-pr`, and `shopify-backport` skills are being
added in following changes.

Upstream PRs are opened in `yugabyte/yugabyte-db` from branches under
`feature-stack/Shopify/<slug>`, where Shopify developers have push access.
Dependants are stacked: a dependant PR's base is its parent's branch. A PR has
landed only when its merge commit is on upstream `master`, whatever base GitHub
records: a stack merge records upper layers against their parent branch, and a stack
reorder can mark a PR merged without landing it, which is a restructure. After a
parent squash-merges, the developer rebases and force-pushes their branch.

Fork-only work needs no issue. Its PR title carries `[shopify-only]` in place of or
after the issue ID, paired with the `upstream: skip` label; CI checks the title, and
the pairing is a convention. Upstream requires an issue reference on every PR, a
GitHub issue or a YB Jira ID, and Shopify cannot create YB Jira tickets. An
upstream-bound fork PR cites an upstream issue, the number its upstream PR will
carry: an existing one, or one the developer opens upstream when the problem carries
no private context. Work that starts from private evidence goes through
shopify-create-issue; when no upstream issue covers the problem, that skill publishes
a sanitized fork issue, the fork PR cites it, and the upstream issue is derived from
it at submission. The body's first line is `Refs <full issue URL>`, because a bare
`#N` in a fork title links to the fork. Fork PR titles follow `.github/workflows/pr-title.yml`:
`[#issue] area: Short description`, with a `[BACKPORT <branch>]` or `[<branch>]` tag
on release bases and none on `master-shopify` or feature bases.

Evidence lives on the fork PR. After opening the upstream PR, comment
`Upstream: <upstream PR url>` there; add a new comment if the upstream PR is
replaced. `Available: <version>` labels keep recording backport availability as
today. Nothing is read from upstream PR labels.

### Deploying and running

For agents that want to deploy, configure and run YugabyteDB refer to instructions at ./docs/content/stable/quick-start

### Repo Structure

| Directory | What it contains |
|---|---|
| `src/` | Core database code: PostgreSQL fork (`src/postgres/`), YugabyteDB C++ storage engine (`src/yb/`), Odyssey connection pooler (`src/odyssey/`) |
| `java/` | Java client library, CDC connector, and DB tests |
| `managed/` | YugabyteDB Anywhere (YBA) platform — orchestration UI, CLI, node agent, and backend (Scala/Java) |
| `docs/` | Source files for the docs website (docs.yugabyte.com) |
| `python/` | Python build utilities and test infrastructure scripts |
| `build-support/` | Build system scripts, linting, and third-party dependency tooling |
| `cmake_modules/` | CMake modules for locating dependencies and custom build functions |
| `cloud/` | Docker, Kubernetes, and Grafana deployment configurations |
| `yugabyted-ui/` | Yugabyted web UI (React frontend + Go API server) |
| `architecture/` | Internal design documents and architecture specs |
| `troubleshoot/` | Troubleshooting framework backend and UI |

### Coding and Development

When working on DB code (`src/`), refer to `src/AGENTS.md` for build and test guidance

When working on YBA (`managed/`), refer to `managed/AGENTS.md`

### Meko datapacks (`MEKO.md`)

A `MEKO.md` lists the Meko datapacks that hold context for the folder it sits in, with each one's
ID and what it covers; the root's [MEKO.md](MEKO.md) covers the whole repo. Use every `MEKO.md`
from the root down to the files you're working on. Search their datapacks before any work here,
even before reading the code.

Save new facts to the datapack in those `MEKO.md` files that owns the topic. A datapack its row
says not to write to, such as the gotchas datapack, never owns the topic. A datapack that starts
holding context for part of the repo gets a row in that part's `MEKO.md`.

What to save in Meko, and what not to:

- **Don't save** secrets (passwords, tokens, keys, connection strings with credentials), what's in
  flight this session (that's for plans and tasks), anything the code or `git log` already says,
  or the story of a fixed bug: its cause, fix and check live in the commit and the issue. Keep
  only a trap the fix doesn't prevent, in one sentence.
- **Do save** facts the code doesn't hold, such as why a design is the way it is and the
  alternatives it rejected, and the sharp edges a future session would otherwise rediscover the
  hard way.

## Cursor Cloud specific instructions

### Environment

The VM image is `yugabyteci/yb_build_infra_almalinux9_x86_64` which ships with: Clang, JDK 17, Go, Python 3.11, CMake, Ninja, SBT, Node.js 22, Rust, and all C++ build dependencies.

### Building from source

```bash
./yb_build.sh release daemons initdb --sj --skip-pg-parquet --no-odyssey --no-ybc
```

This is the Cloud environment `install` command. Builds snapshot `build/latest` (yb-master, yb-tserver, postgres, initdb). Agents booting from a successful Build already have those binaries; rerun only if the branch changes build inputs. Do not start yugabyted in `install`; Builds keep files, not processes.

Flags: `--sj` skips Java, `--skip-pg-parquet` skips the parquet extension, `--no-odyssey` skips the connection pooler, `--no-ybc` skips the backup controller. Add targets back as needed. First compile is ~15-20 minutes on 8 vCPUs; later Builds reuse `/opt/yb-build` and incremental objects.

### Running the database

If `build/latest` is present, start the cluster. Otherwise compile first.

```bash
python3 bin/yugabyted start --advertise_address 127.0.0.1 --base_dir /tmp/yb-data
```

For a multi-node cluster, start additional nodes with `--join <first-node-address>` and a distinct `--advertise_address` and `--base_dir` each.

Ports: YSQL on 5433, YCQL on 9042, yb-master UI on 7000, yb-tserver UI on 9000.

Connect: `build/latest/postgres/bin/ysqlsh -U yugabyte -d yugabyte -h 127.0.0.1`

### Yugabyted UI

`yugabyted-ui/ui/`: `npm ci` (Node.js >= 22.18), `npm start` on port 3000, `npm run typecheck`.

### Prose discipline — write for the reader, not for volume

AI-written text runs long: PR and diff descriptions that narrate the diff, comments that
restate the line below them, design docs padded with editorial framing. It reads as
thorough, but it costs review time, and every extra sentence is one more claim that has to
stay true as the code moves.

There are no hard limits here. Judge each piece by what a reader who already knows this
repo needs:

- **Code comments** — don't restate the code. Comment the *why* when it isn't obvious: an
  invariant, a cross-component contract, a locking or ordering constraint, a workaround and
  the upstream bug behind it. Not a label for the block below it, not narration of the
  change you just made.
- **PR / Phorge diff descriptions** — the motivation (a reviewer who doesn't know why is
  the expensive case), what changed, and whatever the reader must *act* on: new gflags,
  upgrade/rollback consequences, migration steps. Not a narration of the diff. The test
  plan is a separate section — keep it to what was run.
- **Design docs (`architecture/`) and agent docs (`AGENTS.md`, `.claude/skills/`)** — how
  the system is wired today, why it's built that way, and what the reader has to do. Cut
  restated context and the history of what the code used to do.
- **Commit messages** — subject, plus the why when the why isn't obvious.

The test for a sentence: would a reader who knows this repo do or believe anything
differently without it? If not, cut it.

**Motivation is not filler** — why a design is the way it is is exactly what a reader can't
recover from the code; what's being cut is text that says the same thing twice, not text
that explains. And don't over-correct: the goal is *fewer, load-bearing* words, not
stripped docs. Deleting a section that describes live behavior is a worse outcome than
leaving it wordy, and this applies to your own diff, not a cleanup tour of files you didn't
touch.

This does **not** apply to the user-facing docs website (`docs/`), which follows its own
editorial style guide and is written for readers who do *not* know this repo.

Trim before you publish, not in review: the `create-pr` and `create-diff` skills re-check
this as a step.
