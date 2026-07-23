# Physical Generation Metadata (Online Schema Changes)

Detail companion to
`../online-schema-changes-async-shadow-roadmap.md` (Milestone A, "Generation
metadata + hidden shadow"). This document records the in-tree design and
findings for how a hidden second physical copy of a live table (a *shadow
generation*) is represented and kept invisible to everything user-facing.

## Concept

An online `ALTER TABLE` builds a second DocDB table (a new `relfilenode` /
physical generation) that shares the source table's stable logical identity
(`pg_class.oid`, carried by `SysTablesEntryPB.pg_table_id`). Until cutover the
new generation must be completely invisible to users and to every subsystem that
enumerates tables, yet remain addressable by internal machinery (the migration
job).

We model this with a **physical-generation role** on the table entity, distinct
from the existing `HideState`:

| Concept | Field | Meaning |
|---|---|---|
| Dropped/hidden user object (PITR, drop) | `SysTablesEntryPB.hide_state` (`VISIBLE`/`HIDING`/`HIDDEN`) | The USER object is gone or being hidden. |
| Physical generation role (this doc) | `SysTablesEntryPB.physical_generation_role` (`ACTIVE`/`SHADOW`/`RETIRED`) | Which physical copy of a *live* logical table this is. |

The two are orthogonal: a `SHADOW` generation is `VISIBLE` in the hide-state
sense (it is not a dropped object) but must still never appear to users. Keeping
them separate avoids overloading the drop/PITR machinery and its GC.

### Roles

- `ACTIVE` (default, and the value for every ordinary table): the user-visible
  generation.
- `SHADOW`: a hidden target generation being populated by a migration. Excluded
  from SQL lookup/listing, CDC discovery, backup enumeration, DDL events, and
  hidden-object GC.
- `RETIRED`: a former `ACTIVE` generation kept only for rollback/retention after
  a cutover; not user-visible.

`owning_migration_id` links a `SHADOW`/`RETIRED` generation to the
schema-migration job (`SysSchemaMigrationEntryPB`) that owns it, so access can be
fenced and cleanup driven by the job.

## Implementation

Fields (`src/yb/master/catalog_entity_info.proto`, `SysTablesEntryPB`):

```proto
enum PhysicalGenerationRole { ACTIVE = 0; SHADOW = 1; RETIRED = 2; }
optional PhysicalGenerationRole physical_generation_role = 43 [ default = ACTIVE ];
optional bytes owning_migration_id = 44;
```

Accessors (`src/yb/master/catalog_entity_info.h`, `PersistentTableInfo`):

- `physical_generation_role()`, `is_active_generation()`,
  `is_shadow_generation()`, `is_retired_generation()`.
- `visible_to_client()` now also requires `is_active_generation()`, so every
  caller that already gated on client visibility excludes shadow/retired
  generations for free.

Explicit exclusions added at the enumeration seams that do NOT go through
`visible_to_client()`:

- **`ListTables`** (`catalog_manager.cc`): skip non-active generations even when
  `include_not_running` is set (that flag otherwise surfaces PREPARING tables).
- **CDC discovery** (`IsTableEligibleForCDCSDKStream`, `xrepl_catalog_manager.cc`):
  a non-active generation is ineligible. The active generation carries the
  logical table's CDC; the migration performs the generation/schema handoff at
  cutover (roadmap Section 3.3).
- **Backup enumeration** (`catalog_manager_ext.cc`): the namespace-snapshot table
  filter already uses `visible_to_client()`, so shadows are excluded. Explicit
  single-table backup of a shadow id is rejected in migration preflight (future
  work).

## Findings and rationale

- **Reuse `visible_to_client()` as the choke point.** Most user-facing paths
  (SQL relcache lookups, `ListTables`, namespace backup) already funnel through
  it. Adding the generation check there covers them with one edit and avoids
  scattering role checks. The only places needing an explicit check are the ones
  that deliberately bypass client-visibility (`include_not_running`, CDC
  eligibility).
- **Do not reuse `HIDDEN`.** A shadow is not a dropped object; marking it
  `HIDDEN` would entangle it with PITR retention and the hidden-table GC, which
  would try to delete it. A `VISIBLE` + `SHADOW` combination keeps GC away
  (hidden-object GC only looks at `hide_state`).
- **Default `ACTIVE`** means the field is upgrade-safe: existing tables and old
  masters that never set it read as `ACTIVE`, i.e. unchanged behavior.

## Upgrade / rollback safety

- Proto fields 43/44 are appended; the `sys.catalog` physical schema is
  unchanged. Old masters ignore the unknown fields and treat every table as
  `ACTIVE` (its default), which is the pre-feature behavior.
- No shadow generation is ever created unless an online schema change runs
  (gated, preview). On rollback with no in-flight migration there is nothing to
  reconcile. A rollback with a live shadow present requires the migration to be
  cancelled first (tracked with the job lifecycle, not here).

## Not yet done (drives later steps)

- Creating a shadow generation via `CreateTable` with `pg_table_id` set to the
  source logical id and `physical_generation_role = SHADOW` (Step: wire into the
  migration job).
- Adopting a caught-up shadow into the source OID at cutover (roadmap Section 3,
  the `tablecmds.c` `make_new_heap`/`finish_heap_swap` seam).
- Migration-driven GC of `SHADOW` (on cancel) and `RETIRED` (after retention).
- Preflight rejection of explicit operations targeting a shadow physical id.
