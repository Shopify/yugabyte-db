# Online Schema Changes via Async Shadow Generations

This design has been reorganized into focused component documents.

Start with [Online Schema Changes](online-schema-changes/README.md).

The milestone plan is in
[online-schema-changes/roadmap.md](online-schema-changes/roadmap.md), and the
landed prototype status is in
[online-schema-changes/implementation-status.md](online-schema-changes/implementation-status.md).

This compatibility page is retained so existing links continue to resolve.

## Decisions and Scope

See [Overview](online-schema-changes/overview.md) and
[Compatibility and scope](online-schema-changes/compatibility-scope.md).

## Prototype Findings

See [Implementation status](online-schema-changes/implementation-status.md).

## Section 0: Migration Identity, State Tracking, and Observability

See [Job and SQL API](online-schema-changes/job-and-sql-api.md).

## Section 1: Shadow Generation and Distributed Copy

See [Physical generations](online-schema-changes/physical-generations.md) and
[Copy: clone and restore](online-schema-changes/copy-clone-restore.md).

## Section 2: Streaming Fresh Data with WAL/CDC

See [Change capture and replay](online-schema-changes/change-capture-replay.md).

## Section 3: Atomic Storage Switch

See [Cutover and fencing](online-schema-changes/cutover-and-fencing.md).

## Roadmap

See [Roadmap](online-schema-changes/roadmap.md).

## Required Testing

See [Testing](online-schema-changes/testing.md).

## Open Decisions

Unresolved work is tracked in each component document and consolidated in the
[Roadmap](online-schema-changes/roadmap.md).
