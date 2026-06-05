# Decoupling CDCSDK reconciliation from the catalog-manager background thread

## Status

Proposed. Implemented incrementally:

1. **Commit 1** — move CDCSDK metadata reconciliation off the single catalog-manager
   background thread onto its own scheduled task. (This document's primary subject.)
2. **Commit 2** — narrow the drop-table cleanup so it no longer scans the entire
   `cdc_state` table on every run. (A separable follow-up; see "Second step".)

## Summary

The YB-Master runs one background thread (`CatalogManagerBgTasks::Run`) that
*serially* executes the cluster's most important steady-state control-plane work:
load balancing, tablet assignment, leader-affinity enforcement, deleted-table
cleanup, clone-state management, transaction-table autoscaling — and, today, CDCSDK
metadata reconciliation. Because the loop is serial and runs under a single
`SCOPED_LEADER_SHARED_LOCK`, any slow step *starves every other step*.

CDCSDK reconciliation is exactly such a step, and it gets slow on large clusters.
The result is that CDC pressure manifests as **cluster-wide "high catalog-manager
times"** — degraded load balancing and tablet assignment — rather than as localized
CDC slowness. This is the failure mode that contributed to a staging incident.

This change moves CDCSDK reconciliation onto its own self-rescheduling task (running
on the existing `background_tasks_thread_pool_`), using the same idiom already
established for `xrepl_parent_tablet_deletion_task_`. It does **not** make the CDC
work itself cheaper — that is Commit 2's job and is deliberately separate. What it
does is **contain the blast radius**: CDC slowness can no longer block load
balancing, tablet assignment, or leader affinity.

## Background: what runs on the single thread today

`CatalogManagerBgTasks::RunOnceAsLeader` (`catalog_manager_bg_tasks.cc`) runs every
`catalog_manager_bg_task_wait_ms` (default 1s) while this master is leader, holding
`SCOPED_LEADER_SHARED_LOCK` for the whole iteration. In order, it does roughly:

- `ClearDeadTServerMetrics`, YSQL manager bg tasks, metrics reporting, task-tracker
  cleanup, unresponsive-tserver marking;
- pending tablet assignment (`ProcessPendingAssignmentsPerTable`);
- backfill resumption;
- **load balancing** (`MaybeRunClusterBalancer`);
- **tablet splitting** (`MaybeDoSplitting`);
- clone-state management;
- deleted-table cleanup;
- **leader-affinity enforcement** (`SysCatalogRespectLeaderAffinity`);
- `RunXReplBgTasks` — which includes `CleanUpCDCSDKStreamsMetadata` (drop-table
  cleanup, a full `cdc_state` scan — see Commit 2);
- `cdcsdk_manager_->RunBgTasks` — dynamic table addition, non-eligible / unqualified
  / ineligible table cleanup (each scans streams and writes `cdc_state`);
- xCluster bg tasks;
- transaction-status-table autoscaling.

The two CDCSDK items — `CleanUpCDCSDKStreamsMetadata` and
`cdcsdk_manager_->RunBgTasks` — are the ones that scale with
`(#streams x #tablets)` and with DDL activity. On a cluster with ~16K tablets and
~30 streams, `cdc_state` holds ~480K rows, and the drop-table path scans all of them
on every tick that any stream has a dropped table. While that scan runs, the load
balancer does not.

## Why decoupling is the right first step

**1. It fixes the actual observed failure mode.**
The incident wasn't "CDC is slow"; it was "the master is slow at everything because
CDC is slow." The root cause is *serialization on one thread*, not the absolute cost
of the CDC work. Removing CDC from that thread directly addresses the observed
symptom — load balancing / tablet assignment / leader affinity stop being held
hostage by CDC.

**2. It is low-risk and uses an existing, proven idiom.**
The master already runs periodic XRepl work off the critical thread:
`xrepl_parent_tablet_deletion_task_` is a `ScheduledTaskTracker` that self-reschedules
onto `background_tasks_thread_pool_` (`ScheduleXReplParentTabletDeletionTask` /
`ProcessXReplHiddenObjectDeletionPeriodically`). This change mirrors that pattern
exactly for CDCSDK metadata reconciliation. No new threading primitives, no new
locking discipline — the moved functions already take a `LeaderEpoch` and already
acquire their own locks (`EXCLUDES(mutex_)`).

**3. It is correct under leadership changes.**
The task only does work while `CheckIsLeaderAndReady()` holds, captures the current
`LeaderEpoch` per iteration, and the underlying reconciliation functions validate
the epoch on every `sys_catalog` write (a stale epoch fails the write safely). When
leadership is lost the task stops rescheduling itself; it is re-armed the next time
this master runs `RunXReplBgTasks` as leader — identical to the parent-deletion task.

**4. It separates "where the work runs" from "how expensive the work is."**
Whether the full `cdc_state` scan is actually a problem is an open question. By
landing the decoupling first and the scan-narrowing second, we can measure each
independently: Commit 1 should flatten catalog-manager latency spikes even if the CDC
work stays exactly as expensive; Commit 2 then reduces the CDC work's own cost.

**5. It does not change semantics.**
The same functions run, with the same inputs, at a comparable cadence
(`cdcsdk_metadata_bg_task_interval_ms`, defaulted to match the old ~1s tick). Dynamic
table onboarding and drop-table cleanup behave as before; they simply no longer share
a thread with the load balancer. Because the task is now independently paced,
operators can raise its interval to shed master load without touching
`catalog_manager_bg_task_wait_ms` (which governs load balancing and must stay
responsive).

## What this change does *not* do

- It does not reduce the cost of `CleanUpCDCSDKStreamsMetadata` (still a full
  `cdc_state` scan — see Commit 2).
- It does not touch the registration path (`CreateCDCStream` still sets retention on
  every table and still writes one `cdc_state` row per `(stream, tablet)`).
- It does not change retention semantics, the `cdc_state` schema, or any external/RPC
  surface.

These are intentionally out of scope. This is the smallest change that removes CDC
from the critical control-plane thread.

## The larger arc (context, not part of this change)

The strategic end state for "CDC consumers can register and scale" is to stop
coupling CDC bookkeeping to the catalog/DDL lifecycle at all: a stateless,
client-owned-cursor read path that registers no stream object and writes no
`cdc_state` rows, with retention pinned lazily and leader-locally by the act of
polling (reusing the lease that already exists for intent retention via
`cdc_sdk_min_checkpoint_op_id_expiration_`). In that world the catalog manager does
*zero* CDC work, because there is no per-`(stream, tablet)` state for it to reconcile,
scan, or expire.

That is a much larger change with real residual design questions (schema/history
retention floor; new-tablet discovery window). This document's change is the
pragmatic, ship-now step that de-toxifies the incident path while that larger arc is
designed. The two are complementary: decoupling is valuable regardless of whether the
stateless path lands.

## Design

### New scheduled task

Mirror `xrepl_parent_tablet_deletion_task_`:

- `cdcsdk_metadata_bg_task_` — a `rpc::ScheduledTaskTracker`, bound to the messenger
  scheduler at the same place the parent-deletion task is bound.
- `cdcsdk_metadata_bg_task_running_` — an `std::atomic<bool>` guard so only one
  instance is in flight.
- `StartCDCSDKMetadataBgTaskIfStopped()` — called from `RunXReplBgTasks` (leader-only)
  to arm the task; no-op if already running or if disabled by interval `<= 0`.
- `ScheduleCDCSDKMetadataBgTask()` — schedules the next run after
  `cdcsdk_metadata_bg_task_interval_ms`, submitting onto `background_tasks_thread_pool_`.
- `RunCDCSDKMetadataBgTaskPeriodically()` — checks leadership, runs the reconciliation
  with the current epoch, then reschedules.

### Work moved off the critical thread

- `cdcsdk_manager_->RunBgTasks(epoch)` — removed from
  `CatalogManagerBgTasks::RunOnceAsLeader`.
- `CleanUpCDCSDKStreamsMetadata(epoch)` — removed from `RunXReplBgTasks`; both now run
  inside `RunCDCSDKMetadataBgTaskPeriodically`.

xCluster bg tasks (`CleanUpDeletedXReplStreams`, `ClearFailedUniverse`,
`ClearFailedReplicationBootstrap`, `GetXClusterManager()->RunBgTasks`) are left where
they are; they are a separate concern and out of scope for this CDC-focused change.

### New flag

- `cdcsdk_metadata_bg_task_interval_ms` (runtime, default 1000) — cadence of the new
  task. Setting it `<= 0` disables the task. Defaulted to the old effective cadence so
  behavior is preserved; operators may raise it to shed master load now that it is
  off the critical path.
