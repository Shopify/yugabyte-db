# Heat-based load balancer

## Overview

The heat-based load balancer extends the master-side cluster balancer so it can react to observed
tablet traffic, not just tablet counts and leader counts.

The feature is implemented as an alternate balancing strategy,
`load_balancer_strategy=heat_aware_experimental`. The existing count-based strategy remains the
default and continues to be the fallback when operators do not opt in, when telemetry is absent,
or when heat-specific safety checks reject a move.

At a high level, the feature adds:

- leader heat balancing, so hot leaders can be moved even when leader counts are already close;
- tablet placement balancing based on replicated write load, so follower-side write cost is spread
  more evenly across tablet servers;
- hysteresis, cooldowns, and count/global-load guards so heat-driven decisions do not oscillate or
  fight existing balancing policies.

## Capabilities

### Heat-aware leader moves

The balancer now ranks tablet servers by weighted leader heat instead of only by leader counts when
the heat-aware strategy is active. Heat is derived from per-leader `read_ops_per_sec` and
`write_ops_per_sec`, with separate weights for reads and writes.

This lets the balancer move leadership away from hot servers even when the count-based balancer
would consider the cluster balanced by leader count.

### Heat-aware tablet moves

Tablet moves use a different signal from leader moves. They operate on **placement heat**, which is
the write load borne by every replica of a tablet:

- leader-side write heat for tablets led by the server; and
- follower-side replicated write heat for replicas hosted by the server.

Read heat is intentionally excluded from tablet moves because moving a follower replica does not
redistribute leader reads.

This allows the balancer to spread write-heavy replica placement more evenly, including cases where
tablet counts are already flat.

### Safety and rollout controls

The feature is deliberately gated and conservative:

- Telemetry publication is controlled by the `enable_load_balancer_heat_telemetry` AutoFlag.
- Strategy selection is controlled by `load_balancer_strategy`.
- Heat is bucketed with hysteresis instead of compared directly, which avoids churn on small metric
  fluctuations.
- Leader moves and tablet moves have separate cooldowns keyed by `(tablet, from_ts, to_ts)`.
- Heat-driven tablet moves are rejected when they would worsen count imbalance, fight global load
  balancing, or overshoot the current replicated-write gap.
- Count-based balancing remains available as the default behavior and as the fallback path inside
  the heat-aware strategy.

## High-level implementation changes

### 1. Tablet servers now export leader heat telemetry

The tserver heartbeat metrics path was extended so leaders report per-tablet
`leader_read_ops_per_sec` and `leader_write_ops_per_sec` in `TabletLeaderMetricsPB`.

To support that:

- `Tablet` now maintains monotonic read/write operation counters;
- read paths increment the read counter and successful writes increment the write counter;
- the heartbeat metrics provider samples those counters on each metrics heartbeat and emits
  per-second rates only for peers that currently hold the leader lease.

### 2. The master caches and aggregates heat per run

The master now owns a `ClusterBalanceHeatCache` on `CatalogManager`.

The heartbeat service updates that cache whenever a leader reports fresh rates. At the start of a
balancer run, the load balancer snapshots fresh cache entries and builds:

- per-tserver leader heat (`heat_by_ts_`);
- per-tablet heat records (`heat_by_tablet_`) so in-run decisions can project changes without
  re-reading heartbeat state; and
- per-tserver replicated-write placement heat (`tablet_heat_by_ts_`) by fanning each tablet's
  write rate across every server that currently hosts a replica.

### 3. The balancer now has an explicit strategy/scoring seam

The existing cluster balancer logic was refactored behind `LoadBalancerStrategy` and `LoadScorer`.

This adds two concrete strategies:

- `count_based`, which preserves the pre-existing balancing behavior; and
- `heat_aware_experimental`, which overlays heat-aware ordering and move selection while retaining
  the existing count-based tie-breakers and safety rules.

This split keeps the old behavior intact while making it possible to reason about heat-aware
ordering and move assessment independently.

### 4. Heat-aware leader balancing was added

Leader balancing now has a heat-aware path that:

- orders candidates by heat bucket;
- still respects leader blacklist handling and preferred-zone logic;
- suppresses ordinary count-based moves when the only reason to enter the loop was a heat override
  of `leader_balance_threshold`; and
- records recent heat-driven leader moves so the same move is not immediately proposed again.

The balancer also projects successful leader moves into the in-run heat aggregates so later
iterations make decisions against the updated leader distribution instead of a stale start-of-run
snapshot.

### 5. Heat-aware tablet placement balancing was added

Tablet balancing now has a placement-heat-aware path that:

- orders servers by placement-heat bucket;
- identifies hot/cold server pairs even when tablet counts are tied;
- selects a tablet whose write heat reduces the pair's replicated-write gap without flipping it;
- projects both the add and the eventual paired remove into the in-run placement heat aggregates;
- uses cooldowns to avoid repeating the same heat-driven move; and
- biases later cleanup so the paired over-replication remove drains the original source rather than
  undoing the heat-driven add.

### 6. Pending-task replay and in-run projections were updated

Heat-aware balancing depends on the balancer's in-memory view staying consistent with work that is
already in flight. The branch therefore updates global heat state when replaying pending add,
remove, and leader-stepdown tasks, and also when successful heat-related decisions are made during
the current run.

Without these projections, the heat-aware path would keep ranking servers using stale placement or
leadership ownership for the remainder of the run.

### 7. New focused tests were added

The branch adds:

- unit tests for the heat cache;
- extensive strategy tests for heat-aware leader and tablet balancing behavior; and
- new Python tests for unrelated build/version helper changes that are also part of the branch.

## Current behavior summary

With telemetry enabled and `load_balancer_strategy=heat_aware_experimental`, the balancer can:

- move hot leaders away from busy servers even when leader counts look balanced;
- move write-heavy tablets to spread replica-side write cost across servers;
- keep existing placement, blacklist, global balancing, and preferred-leader semantics; and
- fall back to count-based behavior when heat is unavailable or when a heat-driven move would be
  unsafe.
