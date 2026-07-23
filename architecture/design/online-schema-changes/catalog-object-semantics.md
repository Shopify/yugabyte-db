# Catalog Objects and Dependency Semantics

Online table rewrite is more than copying heap rows. PostgreSQL relation identity
is referenced by catalogs, parsed expression trees, indexes, constraints,
replication metadata, privileges, and extension state. The OID-preserving design
reduces this surface, but does not remove the need to classify every object.

## Core rule

The source `pg_class.oid` remains the logical identity `L`. At cutover, the
target schema and physical generation are adopted by `L`; dependent objects are
not retargeted to a different table OID.

Objects fall into four groups:

1. **Preserve:** remain attached to `L` unchanged.
2. **Rewrite metadata:** update the logical schema/definition on `L` at cutover.
3. **Build physical target:** populate target storage before cutover, then map it
   to stable or new logical object identities.
4. **Stage/validate:** suppress side effects during copy/replay, then enable only
   after validation through the final barrier.

## Object handling matrix

| Object | Primary catalog/identity | Target behavior |
|---|---|---|
| Table identity | `pg_class.oid` | Preserve `L`; change physical fields/schema metadata atomically |
| Columns and types | `pg_attribute`, table row type | Install target attributes on `L`; preserve the table row-type identity where PostgreSQL permits |
| Defaults/generated expressions | `pg_attrdef` | Compile target definitions; do not execute user defaults as a side effect of mirror apply unless transform semantics require it |
| Primary/unique indexes | `pg_index`, index `pg_class` rows | Build/validate target physical index storage; preserve logical index OID only when definition/semantics are unchanged |
| Non-unique indexes | `pg_index`, index `pg_class` rows | Build target index bundle before cutover; changed definitions require dependency updates |
| CHECK / NOT NULL | `pg_constraint`, `pg_attribute` | Enforce on post-admission target writes where possible; validate historical rows through `F` |
| Foreign keys from this table | `pg_constraint.conrelid` | Preserve logical relation reference; stage `NOT VALID` when historical validation is required, then validate through `F` |
| Foreign keys to this table | `pg_constraint.confrelid` | Preserve because referenced table OID remains `L`; revalidate only if referenced key/type semantics changed |
| Triggers | `pg_trigger.tgrelid` | Preserve on `L`; do not fire user triggers for internal copy/replay; ensure target schema remains trigger-compatible at cutover |
| Rules/views | `pg_rewrite` parsed trees | Preserve OID references to `L`; invalidate/replan if target column/type shape changes |
| RLS policies | `pg_policy.polrelid` | Preserve on `L`; verify expressions against target schema |
| Inheritance/partitions | `pg_inherits`, `pg_partitioned_table`, bounds | Treat root/leaves/indexes as one generation group; topology changes require atomic hierarchy metadata update |
| Publications | `pg_publication_rel.prrelid` | Preserve logical membership; coordinate external CDC generation/schema transition |
| Subscriptions | `pg_subscription_rel.srrelid` | Preserve local relation identity where applicable; invalidate cached schema and define checkpoint transition |
| Sequences owned by columns | `pg_depend`, sequence catalogs | Preserve existing ownership/sequence identity where semantics are unchanged |
| Identity columns | attributes + owned sequence | Avoid allocating sequence values during retrying copy; define initialization for pre-existing rows separately |
| Table row/composite type | `pg_type.typrelid` | Preserve identity; update tuple descriptor and invalidate dependent plans/functions |
| ACLs/owner/default privileges | `pg_class`, `pg_init_privs` | Preserve on logical relation; do not inherit accidental privileges from an expert target template |
| Comments/security labels | `pg_description`, `pg_seclabel` | Preserve unless DDL explicitly changes them; provider-specific external OID state remains tied to `L` |
| Statistics | `pg_statistic`, `pg_statistic_ext` | Existing values may be invalid after transform; clear/recompute or transform only when proven safe |
| reloptions/tablespace/placement | `pg_class`, YB placement metadata | Install target policy at activation; validate capacity and placement before `F` |
| Extension membership | `pg_depend` | Preserve membership of `L`; reject unsupported extension-owned rewrite semantics |
| FDW/foreign-table metadata | FDW catalogs/provider state | Not an initial heap-rewrite target; explicitly reject or define provider-specific behavior |

This matrix is a design checklist, not a claim that every row is implemented.
Preflight must reject any object class without a defined migration action.

## Why stable OID matters

Many dependents store relation OIDs in catalog rows or serialized parse trees,
not reconstructible table names. A rename-style swap to a separately created
table would require retargeting or rebuilding:

- views/materialized-view query trees;
- incoming foreign keys and RI triggers;
- inheritance/partition links;
- publication/subscription membership;
- row/composite-type users;
- parsed SQL-function bodies and cached plans;
- extension/provider metadata keyed by OID.

Preserving `L` avoids most retargeting. Schema-changing dependents still need
validation, invalidation, or deliberate rebuild.

## Target object lifecycle

### During shadow creation

Create the physical objects needed to validate and serve `G1`, but keep the
logical user object attached to `L`. A same-schema clone can copy physical schema
metadata; a real transform must compile an explicit target bundle.

### During copy and replay

Internal writes must not fire user-visible side effects. Examples:

- audit/notification triggers must not fire twice;
- user rules must not rewrite internal replay statements;
- external CDC must filter mirror origins;
- sequence defaults must not allocate values merely because a copied row is
  retried; and
- target constraints must either be safely enforceable on each replayed row or
  remain staged for final validation.

Internal write authorization should be tied to `migration_id`/epoch or a scoped
capability. A broad session GUC is acceptable only as prototype plumbing, not as
the final trust boundary.

### Validation

Historical validation must complete before the final fence whenever possible:

1. Validate copied rows at a stable frontier.
2. Keep target indexes/constraints current during replay.
3. At the final fence, drain to `F` and verify validation frontiers cover `F`.
4. Enable staged constraints/objects in the authoritative cutover transaction.

For foreign keys, a typical strategy is create/stage as `NOT VALID`, maintain new
writes, then validate historical data after copy. Exact YSQL distributed-FK
semantics and lock levels need dedicated tests.

## Trigger semantics

User triggers logically belong to `L`, not to each physical generation. Internal
copy/replay must bypass them, while ordinary source writes continue to fire them
exactly once before cutover and ordinary target writes fire them exactly once
after cutover.

Preflight must verify that trigger functions remain valid against the target
tuple descriptor. A DDL that drops or changes columns referenced by a trigger
must update/drop the trigger explicitly or fail before copy begins.

Statement-level triggers and transition tables are especially sensitive: replay
may batch or split source statements differently. Preserving source transaction
boundaries alone does not preserve statement-trigger semantics. The target
design should apply logical user effects only on the authoritative source before
`K`, suppressing target trigger execution for mirror writes.

## Indexes and uniqueness

Target indexes can be maintained eagerly during copy/replay or built after base
copy using online index-backfill machinery. The choice affects write
amplification, cutover latency, and semantic error timing.

Uniqueness errors discovered asynchronously cannot reject an already committed
source transaction. The migration must keep `G0` authoritative, stop advancement,
record a semantic failure, and require repair/cancel rather than activating an
invalid target.

## Defaults, generated columns, and identity

Different row populations need distinct semantics:

- pre-existing rows copied at `S`;
- source rows inserted after admission;
- updates whose source record lacks a newly added target column; and
- replay retries.

The transform plan must define whether a target value is preserved, derived, or
initialized once. PostgreSQL default evaluation at replay time is not sufficient
for volatile expressions or sequences because retry can produce a new value.

## Direct DML and reads on the shadow

Normal SQL lookup must not expose `G1`. Direct DML to a target template or guessed
physical id is disallowed while the migration owns it. Such writes would be
outside CDC ordering and could be overwritten or make validation meaningless.

Optional target inspection should use a read-only, migration-scoped API that
reports the applied frontier. It must not turn `G1` into a second writable
authority.

## Concurrent DDL policy

The migration holds a durable logical schema lease/object lock over the full
source/target hierarchy, even though it does not hold `ACCESS EXCLUSIVE` for the
whole copy. Conflicting DDL must block or fail while the migration is active:

- source/target schema changes;
- index/constraint add/drop affecting the bundle;
- partition attach/detach or topology changes;
- tablespace/placement changes; and
- publication/replica-identity changes that affect capture.

Non-conflicting metadata operations require an explicit compatibility matrix;
the safe initial policy is to reject all unclassified DDL.

## Cutover transaction

The authoritative cutover must update one consistent bundle:

- target `pg_attribute`/defaults/constraints/index definitions;
- physical relfilenodes and master generation roles;
- staged object validity/enabled flags;
- catalog version and invalidation messages;
- generation/CDC transition metadata; and
- old-generation retention ownership.

If all changes cannot share one transaction, they need a durable prepared
decision and idempotent roll-forward protocol. Reporting `SUCCEEDED` before this
bundle is authoritative is not valid production behavior.

## Preflight output

Preflight should persist a plan/report listing every discovered object and its
action:

```text
PRESERVE | REWRITE_METADATA | BUILD_TARGET | VALIDATE | REJECT
```

The report is useful for correctness review, user explainability, support, and
ensuring newly introduced PostgreSQL object types do not silently bypass the
migration classifier.
