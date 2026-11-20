# Guarantees and how they are tested

| guarantee | mechanism | evidence (`test/integration`, real PostgreSQL 16/17/18) |
|---|---|---|
| No lost or duplicated effects across crashes | effects + checkpoint LSN in one sink transaction; units at or below the checkpoint are skipped; feedback reports only the durable LSN | `TestFailpointCrashes` (kill exactly before sink commit and after commit before feedback, 4 crashes each, concurrent writers, then full verify); `TestConcurrentWritesAbortsAndKills` (SIGKILL loop under 4 writers with ~20 % aborted transactions) |
| Feedback never ahead of durable progress, also on keepalives | status updates carry the durable LSN only; idle positions go through the committer | `assertFeedbackNotAhead` after every crash (slot `confirmed_flush_lsn` ≤ checkpoint); `TestIdleProgressAndState` (checkpoint advances on unpublished WAL) |
| Aborted transactions have no effect | pgoutput only delivers committed transactions; the planner never applies partial ones | concurrent writer tests with rollbacks |
| Zero missing, extra or stale chunks | deterministic chunk IDs, fingerprints, trim of obsolete chunks, range deletes in backfill windows | `embedsync verify` after every scenario compares every row's expected chunks (content + fingerprint) with the sink at one barrier |
| Backfill under concurrent writes never resurrects deletes or keeps stale rows | watermark windows; snapshot rows yield to stream changes inside the window; range delete with keep set | `TestBackfillDuringWrites` (2,500 rows, 37-row windows, 4 writers, deletes, key changes); property test `TestPropertyBackfillInterleavings` (1,000 random interleavings incl. TOAST deferrals and window discards, mutation-checked) |
| Truncate | stream truncate clears state and every live generation in the unit | `TestStreamCorrectness` |
| Only allowlisted columns leave the source | publication column list + column-level grants + explicit SELECT lists | `TestLeastPrivilegeAndColumnAllowlist` (secret column never reaches sink, metrics or logs; reader role cannot read `source_state`) |
| Unchanged TOAST values | filled from `source_state`, deferred to backfill, or pause | `TestUnchangedToast` (45 KB body, title-only updates, key change) |
| Embedding outage loses nothing | retries with jittered backoff; nothing acknowledged | `TestEmbeddingOutage` |
| Poison rows | quarantine atomically with versioned DLQ records, newer edits/deletes supersede, replay respects them; or pause | `TestPoisonQuarantineAndReplay` |
| Incompatible schema change is never acknowledged | relation signature check in the stream and at startup | `TestSchemaChangePauses` |
| Slot loss is detected and recoverable | identity checks; `rebuild` into a fresh generation | `TestSlotLossRebuild` |
| Model migration is pinned, gated, atomic and reversible | per-generation encoder identity; gates at one snapshot; serving row swap; rollback window | `TestMigrationPromoteRejectRollback` (continuous reader: zero errors, zero wrong results, generation sequence v1→v2→v1; regression rejected with an inspectable report); `TestMigrationResume` |

What is **not** guaranteed:

* Search results are eventually consistent with the source: a committed row is
  searchable after the measured commit-to-searchable latency, not at commit.
* A paused pipeline (schema change, poison row with `on_poison: pause`, unresolvable
  TOAST) stops freshness for the whole table until an operator acts; WAL accumulates
  meanwhile (bounded by `max_slot_wal_keep_size`).
* Quarantined rows are absent from search until resolved (by design: never serve an
  outdated version), and are reported as unresolved.
* One table per process; no joins across tables.
