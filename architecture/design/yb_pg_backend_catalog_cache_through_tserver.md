# YSQL backend catalog-cache misses through PgResponseCache

## Summary

Backend catalog-cache-miss reads can reuse the tserver-side `PgResponseCache` that already serves
sys-table prefetch/connection-warming requests.  When `ysql_enable_read_request_caching` is enabled,
eligible backend catalog read `Perform` RPCs attach `PgPerformOptionsPB::caching_info`.  A tserver
cache hit avoids a master read RPC; a miss is fetched from master, stored in the existing cache, and
returned to the backend.

The implementation deliberately does not add protobuf fields, schema changes, GUCs, or gflags.

## Phase 1a decisions

### Accessor mechanism

The backend's local catalog version lives in PostgreSQL backend state in
`src/postgres/src/backend/utils/misc/pg_yb_utils.c` as `yb_catalog_cache_version`.  `pggate` has a
catalog read time accessor (`YBCGetPgCatalogReadTime` / `PgSession::catalog_read_time`) but no
existing pggate-owned local catalog-version source.  Therefore the implementation adds a
`YbcPgCallbacks::GetCatalogVersionForResponseCache` callback.

Initializer sites:

- `pg_yb_utils.c`: designated initializer sets `GetCatalogVersionForResponseCache` to
  `YbGetCatalogCacheVersionForResponseCache`.
- `src/yb/yql/pggate/test/pggate_test.cc`: initializes `YbcPgCallbacks callbacks = {}` and assigns
  the test stub, preventing uninitialized callback slots.

`YbGetCatalogCacheVersionForResponseCache` must only read local backend state.  Its code comment
forbids `YbGetMasterCatalogVersion`, `YbInvalidateCatalogSnapshot`, and any helper that performs a
master RPC.

### Effective read time

`PgSession::Perform` computes `ops_read_time` with `GetReadTime(ops.operations())` and uses
`ops_read_time ? ops_read_time : catalog_read_time_` as the effective catalog read time serialized
into `PgPerformOptionsPB::read_time`.  Backend-miss response-cache keys are built from this same
effective value so the key represents the exact snapshot requested by the outbound RPC.

### Key group and invalidation

`PgResponseCache::CachingInfoPB::key_group` remains the PostgreSQL database oid.  `PgSession` uses
`options.namespace_id` as the single-database gate and converts it back to a database oid with
`GetPgsqlDatabaseOid`.  This preserves compatibility with existing invalidation paths that call
`PgResponseCache::Disable(db_oid)` from `PgClientSession` for temp-table / silently-altered-db
cases.  The existing Disable-path test in `pg_catalog_perf-test.cc` is
`ResponseCacheInvalidationOnDiscardTempTables`.

### Paging, template1-only, and mixed database batches

Backend catalog-cache-miss paging is out of scope for v1.  The eligibility predicate rejects any
read request whose top-level or nested index request already has an inbound `paging_state`.

Template1-only batches do not set `options.namespace_id` because `PgSession::Perform` ignores
`kTemplate1Oid` while computing the namespace id.  Such batches are ineligible in v1.

Mixed-database batches also fail the `options.namespace_id` gate: `PgSession::Perform` clears the
database oid if it sees non-template1 relations from multiple databases, so no namespace id is set.

### Startup and prefetch counter compatibility

The sys-table prefetcher already supplies caller-owned `CacheOptions`; the backend-miss path only
runs when no caller-supplied cache options are present.  Therefore prefetcher cache keys and exact
prefetcher response-cache counter expectations remain unchanged.  Connection startup is covered by
the existing `pg_catalog_perf-test.cc` exact counter tests; if future startup code creates additional
ordinary backend catalog-cache misses, those tests will fail and the predicate should be narrowed.

### Failed responses and zero-row responses

`PgResponseCache::Data::Set` stores only successful responses as valid entries: `IsOk` returns false
when the response has a top-level status, and `Data::IsValid` rejects failed entries.  Empty row data
is still a successful response when the top-level status is OK, so zero-row responses are cacheable
end-to-end.  Fault-injection coverage for failed backend-miss responses is deferred unless an
existing precise fault knob is identified during test work; the code-level invariant is the `IsOk`
gate in `src/yb/tserver/pg_response_cache.cc`.

## Eligibility predicate

`GetBackendCatalogReadCacheKeyGroup` in `src/yb/yql/pggate/pg_session.cc` is the named predicate.  It
requires:

- `ysql_enable_read_request_caching` is true.
- `PgPerformOptionsPB::use_catalog_session` is true (`SessionType::kCatalog`).
- No caller-supplied `CacheOptions` are present; the prefetcher path is left unchanged.
- The transaction manager is not in DDL mode.
- `yb_non_ddl_txn_for_sys_tables_allowed` is false.
- The session is not major-PG-version-upgrade mode.
- `yb_read_time == 0`.
- The effective catalog read time is valid.
- The local catalog version is initialized (`version != 0`).
- `options.namespace_id` resolves to exactly one non-template1 database oid.
- Every operation is a read op and is not a row-locking read.
- No top-level or nested index read request has inbound `paging_state`.

When the predicate is false, `PgPerformOptionsPB::caching_info` is not attached.

## Cache-key helper

`src/yb/yql/pggate/pg_response_cache_key.{h,cc}` contains the shared encoder.  It takes a read-request
provider callback so both `PgSysTablePrefetcher` and `PgSession::Perform` feed the same implementation
without duplicating encoding logic.

The encoding preserves the previous prefetcher byte format:

1. one byte for per-database catalog version mode (`'1'` or `'0'`),
2. varint catalog version,
3. length-prefixed catalog read time protobuf (or zero length),
4. each length-prefixed read request after temporarily clearing insignificant fields
   (`stmt_id`, `metrics_capture`).

Parameterized byte-equality coverage should include: empty op list; single op; 2-op; 4-op; ops with
and without index requests; ops with and without ybctid-style filters; varied catalog read times;
varied version values; and both values of `is_db_catalog_version_mode`.

## Tests

Core pgwrapper tests live in `src/yb/yql/pgwrapper/pg_catalog_perf-test.cc` and use fixtures derived
from `PgCatalogPerfTestBase`, which pins `NumTabletServers() == 1`.  Feature-on fixtures set
`response_cache_size_bytes` to a positive value such as `kResponseCacheSize5MB`; feature-off fixtures
leave it as `std::nullopt` because `0` means unlimited cache, not disabled.

Metrics windows should wrap one targeted SQL statement and should not include `Connect()` /
`ConnectToDB()`.  A no-op drain statement after connection creation is used to keep connection-start
catalog activity out of the measured window.

Positive cache-hit assertions require both `pg_response_cache_hits > 0` and `master_read_rpc == 0` in
the same single-statement window.  Negative attachment assertions use `pg_response_cache_queries == 0`;
this is a valid proxy because `PgClientSession::Perform` calls `response_cache().Get` only when
`options.has_caching_info()` is true.

## Rollback

The behavior is gated by `DEFINE_NON_RUNTIME_bool(ysql_enable_read_request_caching, ...)`.  Disabling
it requires restarting tservers.  PostgreSQL backends inherit the flag values from their parent
tserver process, so a tserver restart plus natural reconnect is sufficient.  No wire-format, catalog,
schema, master-side, or on-disk rollback is required.

## Out of scope / follow-ups

- ysql_conn_mgr-specific coverage.
- Mid-query catalog-version refresh races.
- Multi-page backend catalog read caching.
- Dedicated failed-response fault-injection test if a precise existing knob is identified.
