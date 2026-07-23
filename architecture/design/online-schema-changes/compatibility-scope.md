# Compatibility and Scope

## Operation classes

The new path is intended for operations that require a physical rewrite or
reshape. Existing fast paths remain preferred.

| Class | Examples | Shadow flow |
|---|---|---|
| Metadata-only | rename, default change, nullable column add, ordinary column drop | No |
| Validation | `SET NOT NULL`, validate CHECK/FK | No; use validation flow |
| Online index | `CREATE INDEX` / uniqueness backfill | No; use index backfill |
| Full rewrite | volatile default, non-compatible type change, add/drop primary key | Yes |
| Reshape | repartition, tablet/geo/tablegroup/colocation change | Eventually |

## Prototype support

Current backend support is intentionally narrow:

- YSQL only;
- one regular, non-colocated heap table;
- no indexes in the migration bundle;
- source and shadow have identical schema and partition/tablet layout;
- RF1 and RF3/multi-tablet clone paths are tested;
- auto-commit INSERT/UPDATE/DELETE replay is tested;
- no active xCluster/backup/PITR/external CDC compatibility contract.

Production behavior must reject unsupported shapes in preflight rather than
partially execute. Current preflight validation is incomplete.

## Access and concurrent DDL policy

`G0` remains the only user-writable authority before cutover and `G1` becomes the
only user-writable authority after cutover. Direct user DML against the shadow or
an expert target template is disallowed while the migration owns it. Optional
target inspection must be read-only and expose the target apply frontier.

The migration needs a durable schema/object lease over the full relation,
partition, and index hierarchy. Conflicting DDL must block or fail for the
duration of the migration even though ordinary DML remains online. The safe
initial policy is to reject unclassified concurrent DDL, including schema,
index/constraint, partition topology, placement, publication, and replica-
identity changes that invalidate the migration plan.

Detailed object rules are in
[Catalog objects and dependency semantics](catalog-object-semantics.md).

## Target table shapes

| Shape | Target behavior |
|---|---|
| Regular table | Parallel copy/replay by source range |
| Existing partitioned table | Treat root, leaves, and indexes as one generation group |
| Regular -> partitioned | Route source rows through target partition tree |
| Partitioned -> regular | Merge all source leaves into target routing |
| Repartitioned | Independent source/target range and tablet sets |
| Geo-partitioned | Create target leaves in final placement; throttle per region |
| Colocated -> colocated | Allocate a new target colocation prefix; never delete shared tablet |
| Colocated <-> non-colocated | Transform between shared-prefix and normal tablet routing |

## External CDC

Internal copy/replay writes must not appear as user changes. External CDC should
observe:

```text
G0 user transactions before K
one schema/generation transition at K
G1 user transactions after K
```

Required work:

- durable origin on backfill/mirror writes and server-side filtering;
- logical-replication schema transition with restart semantics around `K`;
- gRPC CDC generation-transition record containing old/new tablets and
  checkpoints;
- retention of old tablets until every relevant consumer crosses `K`;
- deterministic behavior when existing rows are transformed (consumer transform,
  rebootstrap, or reject).

xCluster-managed tables remain out of scope and must be rejected. An OSC copy or
mirror write is not automatically equivalent to a user write on a remote
universe: external-HybridTime/origin filtering, target schema timing, generation
activation, and checkpoint handoff would need coordinated execution on both
universes. Running the local generation switch independently could leave remote
storage/schema inconsistent.

## Backup and PITR

A backup snapshot at HybridTime `H` must select exactly one generation:

```text
H < K  -> G0
H >= K -> G1
```

Selection must use catalog state as of `H`, not current enumeration followed by
a later snapshot. Restore produces an ordinary database with one active
generation and no resumed migration.

PITR must physically retain both generations while its restore window spans `K`:

```text
restore T < K  -> G0 active
restore T >= K -> G1 active
```

Incremental backup is SST-based; the first chain crossing `K` can approach a full
table. Initial safe policy must be explicit (force full or reject crossing) until
generation-replacement manifests exist.

## Upgrade and rollback

Production enablement requires:

- mixed-version readers/writers understand generation metadata;
- old code cannot expose or mutate a shadow;
- rolling rollback refuses to proceed with an in-flight generation unless the
  feature can cleanly cancel it;
- wire/protobuf additions remain backward-compatible;
- catalog changes define forward and rollback behavior around `K`.

## Related workflows

- **Automated online DDL:** expected primary product workflow, layered over the
  durable job without changing ordinary DDL result semantics.
- **Expert target definition:** possible for reshapes, but the admitted target
  must become internally owned/read-only rather than a second user authority.
- **Manual synchronization:** possible future policy; it still retains WAL and
  requires the same fenced final drain.
- **Bidirectional writes:** separate replication/conflict-resolution feature,
  not an initial OSC rollback mechanism.
- **CREATE TABLE AS:** may reuse distributed snapshot scan/transform/routing
  workers but creates a new logical object rather than switching generations.
- **REFRESH MATERIALIZED VIEW:** may reuse generation activation for simple
  plans, but maintaining arbitrary query results incrementally is outside table
  CDC replay semantics.

See [API and workflow evolution](api-and-workflows.md).

## Non-goals

- Trigger-based synchronous mirroring as the core mechanism.
- MySQL-style rename swap that changes the logical table OID.
- Replacing metadata-only or existing online-index paths.
- Changing ordinary DDL response semantics to return a migration id.
- Supporting xCluster table migrations in the initial feature.
- Bidirectional user writes to both generations.
- General incremental maintenance for arbitrary CTAS/materialized-view queries.
