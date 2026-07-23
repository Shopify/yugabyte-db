# Implementation Status

This page is the source of truth for **landed prototype behavior**. Component
documents describe both current and target designs; use this page to distinguish
them.

## Status summary

| Area | Status | Notes |
|---|---|---|
| Durable migration job | Landed | Master sys-catalog entity, failover reload, cancel, status/progress surfaces |
| Physical-generation metadata | Landed | `ACTIVE`, `SHADOW`, `RETIRED`; shadow visibility exclusions |
| Hidden shadow creation | Landed | Regular, non-colocated, non-index YSQL table; same schema/partitions as source |
| Bulk copy | Landed prototype | Source snapshot + per-tablet clone + target restore; synchronous phase |
| Internal change capture | Landed prototype | Slot-less CDCSDK stream, `PG_FULL`, bound to source table |
| Logical replay | Landed prototype | WALSENDER `pg_ql_value`; per-record UPSERT/DELETE; drains to `F` |
| Master role flip | Landed prototype | Shadow becomes `ACTIVE`, source becomes `RETIRED` in one sys-catalog batch |
| PostgreSQL storage repoint | Landed prototype | Manual `yb_finalize_online_schema_change` updates `pg_class.relfilenode` |
| Final write fence | Not implemented | Current safe operation requires external quiescing |
| Atomic role flip + relfilenode repoint | Not implemented | Master and PostgreSQL catalog steps are separate |
| Target-schema construction | Not implemented | Shadow currently copies the source schema; submitted DDL is not applied |
| Durable transform plan | Not implemented | No compiled expression/dependency contract shared by copy and replay |
| Catalog/dependent-object bundle | Not implemented | Indexes, triggers, FKs, policies, privileges, and validation are not migrated by the prototype |
| Concurrent DDL lease/preflight | Incomplete | Unsupported shapes and plan-invalidating DDL are not comprehensively fenced/rejected |
| Transaction-preserving replay | Not implemented | Records are applied and flushed one at a time; BEGIN/COMMIT are ignored |
| Durable per-tablet replay checkpoint | Not implemented | Replay restarts from `S`; only stream id, `S`, and `F` are persisted |
| CDC stream cleanup | Not implemented | Terminal migrations can retain WAL/history until cleanup is added |
| External CDC handoff | Not implemented | Required before production use with logical/gRPC CDC |
| Backup/PITR awareness | Not implemented | Required before production use with backup schedules/PITR |
| Partitioned/geo/colocated/index bundles | Not implemented | Prototype supports one regular non-colocated heap |
| Performance/retention observability | Incomplete | Summary phases exist; production metrics, pressure policy, tuning, and support bundle do not |

## What the current tests prove

- Baseline PostgreSQL rewrite tests prove that `pg_class.oid` survives a physical
  rewrite and that views/foreign keys bound to the original OID continue to work.
  The new migration-driven finalize path still needs equivalent dependent tests.
- A hidden shadow is omitted from normal table listing.
- RF1 and RF3/multi-tablet clone + restore produce physical data parity.
- Finalizing the relation makes reads use the shadow relfilenode.
- INSERT, UPDATE, and DELETE committed after copy snapshot `S` are replayed into
  the shadow before cutover (`ReplayCapturesConcurrentWrites`).
- A raw per-tablet `GetChanges` request in WALSENDER mode returns clean
  `pg_ql_value` values without a PostgreSQL replication slot.
- Durable job admission, request idempotency, cancellation, phase reporting,
  and master-failover reload work.

## Important current safety boundary

`ReplayGenerationChanges` chooses `F` when the REPLAYING phase begins, then
drains every source tablet until `safe_hybrid_time >= F`. Writes can still commit
to `G0` after `F`. The master role flip and SQL relfilenode finalize happen later.

Therefore, the prototype is only data-safe if writes are quiesced before `F` and
remain quiesced until finalize commits. Current downtime is approximately:

```text
remaining replay drain + master role flip + manual SQL finalize
```

The intended downtime is only the final fence/drain/switch critical section;
that work is next in [Cutover and fencing](cutover-and-fencing.md).

## Preview controls

The backend is test/preview-only and gated by flags including:

- `TEST_enable_schema_migration_admission`
- `TEST_schema_migration_create_shadow`
- `TEST_pause_schema_migration_in_running`
- `TEST_pause_schema_migration_before_replaying`
- `TEST_schema_migration_stop_before_cutover`

These are test flags, not a production feature contract.
