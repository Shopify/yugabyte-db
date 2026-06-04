# Tablet Change Feed Prototype

This prototype adds a narrow, streamless tablet-local change feed for internal
consumers that do not need the full CDCSDK contract.

The API is `CDCService.ReadTabletChanges`. It reads committed row changes from a
tablet leader, returns an opaque per-tablet cursor token, and does not create CDC
streams, write `cdc_state`, or install per-consumer retention barriers. Empty
cursor tokens start at the current leader tip.

The intended retention model is static:

- set WAL retention with `--log_min_seconds_to_retain`
- set transactional intent retention with
  `--tablet_change_feed_intents_retention_secs`
- store returned cursor tokens externally after downstream acknowledgement
- treat `CHANGE_FEED_CURSOR_TOO_OLD` as a consumer-lag policy failure

The response intentionally contains only committed row-change data:

- tablet id
- table id
- schema version
- commit hybrid time
- operation type
- encoded primary key
- changed columns
- stable duplicate key

This deliberately excludes CDCSDK transaction envelopes, DDL bootstrap records,
publication filtering, before images, safe-point records, and server-side
checkpoint persistence. The first implementation reuses the existing CDCSDK
decoder internally and projects its output into this narrower response shape;
a production version should move the cutpoint lower so record caps and batched
multi-tablet polling can be enforced before CDCSDK envelopes are materialized.
