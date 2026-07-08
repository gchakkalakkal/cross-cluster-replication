# CCR Fix Summary

**Date:** July 8, 2026  
**Codebase:** OpenSearch Cross-Cluster Replication (CCR) Plugin

---

## Overview

This document summarizes all fixes and enhancements made to the CCR plugin. The changes address three primary problem areas:

1. **Role-Transition (Warm-Attach) Support** — Allow a cluster that was previously a leader to become a follower without a full snapshot restore.
2. **Checkpoint Persistence** — Persist leader/follower sequence numbers so that role transitions and task restarts can resume from the last known position.
3. **2 GB Batch Size Limit Fix** — Automatically reduce batch size when a `ReleasableBytesStreamOutput` overflow is encountered, preventing hard failures.

---

## 1. Checkpoint Persistence — Data Model Changes

### 1.1 `ReplicationMetadata.kt`
**File:** `src/main/kotlin/org/opensearch/replication/metadata/store/ReplicationMetadata.kt`

**Changes:**
- Added five new fields to the `ReplicationMetadata` class to support bidirectional checkpoint tracking and checkpoint configuration:

| Field | Type | Default | Purpose |
|---|---|---|---|
| `leaderSequenceNumber` | `Long` | `-1` (`UNASSIGNED_SEQ_NO`) | Last confirmed leader global checkpoint |
| `followerAppliedSequence` | `Long` | `-1` (`UNASSIGNED_SEQ_NO`) | Last applied follower local checkpoint |
| `checkpointPersistenceEnabled` | `Boolean` | `true` | Feature flag to enable/disable checkpoint tracking |
| `checkpointRetentionPeriod` | `String` | `"24h"` | How long to retain checkpoint data after stop |
| `roleTransitionResumeMode` | `String` | `"CHECKPOINT"` | Resume strategy on role transition |

- Updated `toXContent()` to serialize all five new fields (`leader_sequence_number`, `follower_applied_sequence`, `checkpoint_persistence_enabled`, `checkpoint_retention_period`, `role_transition_resume_mode`).
- Updated the `METADATA_PARSER` (`ObjectParser`) to deserialize all new fields from the index document.
- Added `UNASSIGNED_SEQ_NO = -1L` companion constant for sentinel comparison.

### 1.2 `replication-metadata-store.json`
**File:** `src/main/resources/mappings/replication-metadata-store.json`

**Changes:**
- Bumped `schema_version` from `1` to `2`.
- Added five new field mappings:
  - `leader_sequence_number` → `type: long`
  - `follower_applied_sequence` → `type: long`
  - `checkpoint_persistence_enabled` → `type: boolean`
  - `checkpoint_retention_period` → `type: keyword`
  - `role_transition_resume_mode` → `type: keyword`

---

## 2. Checkpoint Persistence — Store & Manager

### 2.1 `ReplicationMetadataStore.kt`
**File:** `src/main/kotlin/org/opensearch/replication/metadata/store/ReplicationMetadataStore.kt`

**Changes:**
- Added `@Volatile private var mappingUpToDate = false` flag to skip redundant cluster-state reads after the first successful mapping confirmation in a JVM lifecycle.
- Added `getCurrentSchemaVersion()` — a pure cluster-state read that returns the `schema_version` stored in the live index mapping without a network call.
- Added `checkAndUpdateMapping()` — called before every read and write. Detects when `REPLICATION_STORE_MAPPING_VERSION > currentSchemaVersion` and automatically issues a `PutMappingRequest` to upgrade the index to the latest schema. Sets `mappingUpToDate = true` on success.
- Modified `doStart()` — On plugin startup, proactively calls `ensureSystemIndexReady()` on the `GENERIC` thread pool. If a cluster manager is not yet elected, registers a one-shot `ClusterStateListener` that fires the moment one becomes available.
- Added `ensureSystemIndexReady()` — runs synchronously on the `GENERIC` thread. Creates the system index if it does not exist, or upgrades its mapping if the stored schema version is stale. Handles concurrent creation (`ResourceAlreadyExistsException`) gracefully.
- `addMetadata()` and `updateMetadata()` now call `checkAndUpdateMapping()` before every write so checkpoint fields are available from the first operation after a plugin upgrade.
- `getMetadata()` (suspend version) now calls `checkAndUpdateMapping()` before every read.

### 2.2 `ReplicationMetadataManager.kt`
**File:** `src/main/kotlin/org/opensearch/replication/metadata/ReplicationMetadataManager.kt`

**Changes:**
- Added `updateCheckpointSequences(patternName, connectionName, leaderSequenceNumber, followerAppliedSequence)` — updates checkpoint fields on an `AUTO_FOLLOW` type metadata document. Checks `checkpointPersistenceEnabled` before writing.
- Added `updateIndexCheckpointSequences(followerIndex, leaderSequenceNumber, followerAppliedSequence)` — updates checkpoint fields on an `INDEX` type metadata document. No optimistic locking (last-write-wins), which is safe because all concurrent shard-task writes carry approximately correct values.
- Modified `deleteIndexReplicationMetadata()` — **no longer deletes the INDEX document on stop**. Instead:
  - If `leaderSequenceNumber > UNASSIGNED_SEQ_NO`, transitions `overallState` to `STOPPED` and calls `updateMetadata()`, preserving both checkpoint fields for role-transition resume.
  - Falls back to actual deletion only if no checkpoint has been recorded or if the preservation itself throws an exception.
  - Logs the preserved checkpoint values at `INFO` level.
- Added `addAutofollowMetadata()` overload parameters: `checkpointPersistenceEnabled`, `checkpointRetentionPeriod`, `roleTransitionResumeMode` — forwarded into the `ReplicationMetadata` constructor.

---

## 3. Checkpoint Persistence — Shard-Level

### 3.1 `ShardReplicationTask.kt`
**File:** `src/main/kotlin/org/opensearch/replication/task/shard/ShardReplicationTask.kt`

**Changes:**
- Added `checkpointOpsCounter: AtomicLong` to track cumulative operation count across batch fetches.
- Added constant `CHECKPOINT_OPS_INTERVAL = 1_000L` — checkpoint is persisted to the metadata store every ~1,000 replicated operations.
- Added `persistShardCheckpoint(leaderCheckpoint: Long)` suspend function — reads `followerCheckpoint` from `FollowerClusterStats` and calls `replicationMetadataManager.updateIndexCheckpointSequences()`. Failures are logged as `WARN` but do not interrupt replication.
- In `replicate()`: Added an immediate `persistShardCheckpoint(indexShard.lastSyncedGlobalCheckpoint)` call right after initializing stats. This handles the case where a fully caught-up follower receives no new operations after a restart — the initial persist ensures the metadata store reflects the current position.
- In `getChanges()`: After each successful batch, advances `checkpointOpsCounter`. When the counter crosses a `CHECKPOINT_OPS_INTERVAL` boundary (`prev / interval < curr / interval`), calls `persistShardCheckpoint()`.

> **Why in ShardReplicationTask rather than IndexReplicationTask?**  
> `ShardReplicationTask` always runs co-located with its shard (persistent task pinned to the shard's node), so checkpoint values read from `FollowerClusterStats` are always accurate. `IndexReplicationTask` reads node-local stats and can miss shards assigned to remote nodes in a multi-node cluster.

---

## 4. Checkpoint Persistence — Index-Level (Secondary Path)

### 4.1 `IndexReplicationTask.kt`
**File:** `src/main/kotlin/org/opensearch/replication/task/index/IndexReplicationTask.kt`

**Changes:**
- Added `checkpointPollCounter: Int` and `CHECKPOINT_PERSIST_INTERVAL = 12` constant — checkpoint is persisted every 12 monitoring polls (~60 seconds at 5 s/poll).
- Added `persistCheckpointSequences()` suspend function — aggregates `leaderCheckpoint` and `followerCheckpoint` across all shards of the index from `FollowerClusterStats` (max values), then calls `replicationMetadataManager.updateIndexCheckpointSequences()`. Only persists when at least one shard has reported real data (`maxLeaderCheckpoint >= 0`).
- In `pollShardTaskStatus()`: Increments `checkpointPollCounter` and calls `persistCheckpointSequences()` when the interval threshold is reached.

---

## 5. Role-Transition (Warm-Attach) Support

### 5.1 `ReplicateIndexRequest.kt`
**File:** `src/main/kotlin/org/opensearch/replication/action/index/ReplicateIndexRequest.kt`

**Changes:**
- Added `isRoleTransitionResume: Boolean = false` field. Set by `AutoFollowTask` when a role-transition scenario is detected. Propagated through the request pipeline to allow downstream logic to bypass the "follower index already exists" guard and skip snapshot restore.

### 5.2 `TransportReplicateIndexAction.kt`
**File:** `src/main/kotlin/org/opensearch/replication/action/index/TransportReplicateIndexAction.kt`

**Changes:**
- Modified the `REPLICATED_INDEX_SETTING` guard in `doExecute()`:
  - **Before:** Always threw `IllegalArgumentException("Cannot Replicate a Replicated Index")` when the leader carried the setting.
  - **After:** Checks if the local cluster has the same-named index as a **regular (non-follower)** index (`localFollowerSetting.isNullOrBlank()`). If so, logs a role-transition detection message and allows the operation to continue as a warm-attach. The `REPLICATED_INDEX_SETTING` on the new leader is not cleared here because it is an `InternalIndex` setting that cannot be changed via `UpdateSettingsRequest` — it will be cleared when `StopIndexReplicationAction` is eventually called on the new leader cluster.

### 5.3 `TransportReplicateIndexClusterManagerNodeAction.kt`
**File:** `src/main/kotlin/org/opensearch/replication/action/index/TransportReplicateIndexClusterManagerNodeAction.kt`

**Changes:**
- Modified `clusterManagerOperation()` follower-index-exists check:
  - **Before:** Threw `IllegalArgumentException("Cant use same index again for replication")` if the follower index existed in the routing table.
  - **After:** Reads `REPLICATED_INDEX_SETTING` on the existing index. Only rejects if the index is **already a CCR follower**. If the index has no `REPLICATED_INDEX_SETTING` (i.e., it was previously a leader), logs a warm-attach notice and allows the operation to proceed.
- Added stale persistent task cleanup: Before calling `persistentTasksService.startTask()`, checks for an existing task with the same ID (`replication:index:<followerIndex>`) in the cluster state. If found, removes it via `persistentTasksService.removeTask()` to prevent `ResourceAlreadyExistsException` during warm-attach restarts.
- Made the `waitForTaskCondition` lambda use safe-null (`?.`) and safe-cast (`as?`) to handle null or unexpected state types during the warm-attach transition without throwing `NullPointerException` or `ClassCastException`.

### 5.4 `IndexReplicationTask.kt` — Warm-Attach INIT State
**File:** `src/main/kotlin/org/opensearch/replication/task/index/IndexReplicationTask.kt`

**Changes:**
- In the `INIT` state handler, when `isResumed()` returns `true` (follower index already exists in routing table):
  - Checks if `REPLICATED_INDEX_SETTING` is already present on the index.
  - If missing (the index was a regular/leader index before), applies it via a direct `AckedClusterStateUpdateTask` cluster state update (same approach used by `StopReplicationTask`). Overrides `timeout()` to return a safe 60-second default to avoid NPE when `request` is null.
  - Logs the warm-attach detection and skips `setupAndStartRestore()` — goes directly to `InitFollowState`, reusing the local index data.

### 5.5 `AutoFollowTask.kt` — Role-Transition Detection
**File:** `src/main/kotlin/org/opensearch/replication/task/autofollow/AutoFollowTask.kt`

**Changes:**
- In `startReplication()`:
  - Added checkpoint lookup: Reads the per-index `INDEX` type metadata (not the pattern-level `AUTO_FOLLOW` metadata, which never carries checkpoint data) to retrieve `savedLeaderSeqNo` and `savedFollowerSeqNo`.
  - Added `isRoleTransition` boolean: `true` when `checkpointPersistenceEnabled && roleTransitionResumeMode == "CHECKPOINT" && savedLeaderSeqNo > UNASSIGNED_SEQ_NO && local index exists`.
  - If a role transition is detected and the local index exists, instead of returning early with "Cannot replicate — index already exists", proceeds to issue a `ReplicateIndexRequest` with `isRoleTransitionResume = true`.
  - Logs the detected checkpoint values (`leaderSeqNo`, `followerSeqNo`) at `INFO` level before issuing the request.
- In `startReplicationJobs()`:
  - Added `ResourceNotFoundException` catch block: When the status check for a tracked index throws `ResourceNotFoundException` (metadata was externally cleaned up while the index was still in the queue), adds it to `completedJobs` and removes it from the queue, preventing a continuous error flood on every poll cycle.
- Added `clearLeaderReplicatedSetting(leaderIndex: String)` private suspend function — fetches the `REPLICATED_INDEX_SETTING` on the new leader via the remote client and clears it via `UpdateSettingsRequest` if present. Useful in role-transition scenarios where the stale setting needs to be removed.

---

## 6. Stop Replication — STOPPED State Validation

### 6.1 `TransportStopIndexReplicationAction.kt`
**File:** `src/main/kotlin/org/opensearch/replication/action/stop/TransportStopIndexReplicationAction.kt`

**Changes:**
- In `validateReplicationStateOfIndex()`: Added `ReplicationOverallState.STOPPED.name` to the valid-state list. This allows `StopIndexReplicationAction` to succeed when called on an index whose metadata was already transitioned to `STOPPED` state (e.g., by a previous stop that preserved checkpoint data for role-transition purposes).

---

## 7. Batch Size Settings — 2 GB Limit Fix

### 7.1 `BatchSizeSettings.kt` (New File)
**File:** `src/main/kotlin/org/opensearch/replication/task/shard/BatchSizeSettings.kt`

**New class** that encapsulates batch size management with fallback priority:

| Method | Description |
|---|---|
| `getBatchSize()` | Returns index-level setting if configured, otherwise cluster-level setting |
| `hasIndexLevelSetting()` | Whether `index.plugins.replication.follower.ops_batch_size` is set |
| `getBatchSizeSource()` | Returns `"index-level"` or `"cluster-level"` for logging |
| `getEffectiveBatchSize()` | Returns `dynamicBatchSize` if set (after reduction), otherwise `getBatchSize()` |
| `reduceBatchSize()` | Halves the current effective batch size, floored at `MIN_OPS_BATCH_SIZE = 16` |
| `resetBatchSize()` | Clears the dynamic override, reverting to the configured batch size |
| `isDynamicallyReduced()` | `true` when a dynamic reduction is active |

### 7.2 `ShardReplicationChangesTracker.kt`
**File:** `src/main/kotlin/org/opensearch/replication/task/shard/ShardReplicationChangesTracker.kt`

**Changes:**
- Replaced direct `replicationSettings.batchSize` usage with `BatchSizeSettings(indexShard.indexSettings(), replicationSettings)`.
- `requestBatchToFetch()` now calls `batchSizeSettings.getEffectiveBatchSize()` to respect dynamic reductions. Logs whether the batch size has been dynamically reduced.
- Added `reduceBatchSize()` — delegates to `batchSizeSettings.reduceBatchSize()` and logs the new effective size.
- Added `resetBatchSize()` — delegates to `batchSizeSettings.resetBatchSize()` and logs.
- Added `batchSizeSettings()` — exposes the `BatchSizeSettings` instance for external introspection.

### 7.3 `ShardReplicationTask.kt` — 2 GB Exception Handler
**File:** `src/main/kotlin/org/opensearch/replication/task/shard/ShardReplicationTask.kt`

**Changes:**
- In the `catch (e: Exception)` block inside `replicate()`:
  - Added a specific check for `IllegalArgumentException` with message containing `"ReleasableBytesStreamOutput cannot hold more than"`.
  - On match: logs the error at `ERROR` level, calls `changeTracker.reduceBatchSize()`, logs the new reduced size, calls `changeTracker.updateBatchFetched(false, ...)`, and **returns immediately without delay** to retry with the smaller batch size.
  - This prevents replication from hard-failing when a large batch exceeds the 2 GB serialization limit.

### 7.4 `ReplicationPlugin.kt`
**File:** `src/main/kotlin/org/opensearch/replication/ReplicationPlugin.kt`

**Changes:**
- Added constant `MIN_OPS_BATCH_SIZE = 16` — the absolute minimum batch size allowed after dynamic reductions.
- Added `REPLICATION_FOLLOWER_OPS_BATCH_SIZE_INDEX` index-scoped setting (`index.plugins.replication.follower.ops_batch_size`, default `50000`, minimum `16`, `Dynamic + IndexScope`) — allows per-index batch size override independent of the cluster-level setting.

---

## 8. AutoFollowMetadata — Checkpoint Configuration Support

### 8.1 `ReplicationMetadataManager.kt` — `addAutofollowMetadata()`
**File:** `src/main/kotlin/org/opensearch/replication/metadata/ReplicationMetadataManager.kt`

**Changes:**
- `addAutofollowMetadata()` now accepts three additional optional parameters:
  - `checkpointPersistenceEnabled: Boolean = true`
  - `checkpointRetentionPeriod: String = "24h"`
  - `roleTransitionResumeMode: String = "CHECKPOINT"`
- These are forwarded into the `ReplicationMetadata` constructor so that autofollow patterns created with custom checkpoint configuration are correctly persisted.

---

## Summary Table

| # | Component | File(s) Changed | Type |
|---|---|---|---|
| 1 | Checkpoint data model | `ReplicationMetadata.kt`, `replication-metadata-store.json` | New fields / Schema v2 |
| 2 | Metadata store auto-migration | `ReplicationMetadataStore.kt` | Schema upgrade on startup |
| 3 | Checkpoint write on stop | `ReplicationMetadataManager.kt` | Behavior change (no delete → STOPPED) |
| 4 | Checkpoint write methods | `ReplicationMetadataManager.kt` | New methods |
| 5 | Shard-level periodic checkpoint | `ShardReplicationTask.kt` | New logic (every 1k ops) |
| 6 | Index-level periodic checkpoint | `IndexReplicationTask.kt` | New logic (every ~60 s) |
| 7 | Warm-attach transport layer | `TransportReplicateIndexAction.kt`, `TransportReplicateIndexClusterManagerNodeAction.kt` | Guard bypass for role transition |
| 8 | Warm-attach INIT state | `IndexReplicationTask.kt` | Skip restore, stamp setting |
| 9 | Role-transition detection | `AutoFollowTask.kt`, `ReplicateIndexRequest.kt` | Checkpoint lookup + flag |
| 10 | Stop state validation | `TransportStopIndexReplicationAction.kt` | Accept STOPPED state |
| 11 | 2 GB batch limit fix | `BatchSizeSettings.kt` (new), `ShardReplicationChangesTracker.kt`, `ShardReplicationTask.kt`, `ReplicationPlugin.kt` | Dynamic batch reduction |

---

## Flow: Role Transition (Cluster A leader → follower of Cluster B)

```
1. Cluster A was the leader; Cluster B was the follower.
2. Role switch: Cluster B now has the up-to-date data; Cluster A needs to follow Cluster B.

On Cluster A (new follower):
  AutoFollowTask.pollForIndices()
    └─ startReplication(leaderIndex)
         ├─ Read INDEX metadata for leaderIndex from .replication-metadata-store
         │   └─ savedLeaderSeqNo, savedFollowerSeqNo (preserved from stop)
         ├─ isRoleTransition = true
         │   (checkpoint > -1 AND local index exists AND CHECKPOINT mode)
         ├─ Set request.isRoleTransitionResume = true
         └─ client.suspendExecute(ReplicateIndexAction, request)

TransportReplicateIndexAction.doExecute()
  ├─ Leader carries REPLICATED_INDEX_SETTING?
  │   └─ Local index is regular (no REPLICATED_INDEX_SETTING) → allow warm-attach
  └─ Forward to cluster manager node action

TransportReplicateIndexClusterManagerNodeAction.clusterManagerOperation()
  ├─ Follower index exists? Check REPLICATED_INDEX_SETTING → not a current follower → allow
  ├─ Remove stale persistent task if present
  ├─ addIndexReplicationMetadata() → new RUNNING document (preserves no checkpoint here;
  │   ShardReplicationTask will start updating immediately)
  └─ startTask(IndexReplicationExecutor)

IndexReplicationTask.execute() → INIT state
  ├─ isResumed() == true (follower index in routing table)
  ├─ REPLICATED_INDEX_SETTING missing? → apply via cluster state update
  └─ Return InitFollowState (skip snapshot restore)
       └─ addIndexBlockForReplication()
       └─ startNewOrMissingShardTasks()
            └─ ShardReplicationTask starts from local shard checkpoint
```

---

## Flow: 2 GB Batch Size Auto-Reduction

```
ShardReplicationTask.replicate()
  └─ getChanges(fromSeqNo, toSeqNo)
       └─ throws IllegalArgumentException("ReleasableBytesStreamOutput cannot hold more than ...")
            ├─ logError("Hit 2GB limit ...")
            ├─ changeTracker.reduceBatchSize()  → batchSize = max(current/2, MIN_OPS_BATCH_SIZE=16)
            ├─ logError("Reduced batch size to X. Retrying immediately.")
            ├─ updateBatchFetched(false, ...)
            └─ return@launch  ← no delay, retry immediately with smaller batch
```

---

## Key Constants & Settings

| Setting / Constant | Default | Scope | Description |
|---|---|---|---|
| `plugins.replication.follower.index.ops_batch_size` | `50000` | Cluster, Dynamic | Cluster-level batch size for change fetching |
| `index.plugins.replication.follower.ops_batch_size` | `50000` | Index, Dynamic | Per-index batch size override |
| `MIN_OPS_BATCH_SIZE` | `16` | Code constant | Floor for dynamic batch size reduction |
| `CHECKPOINT_OPS_INTERVAL` | `1000` | Code constant | Ops between shard-level checkpoint persists |
| `CHECKPOINT_PERSIST_INTERVAL` | `12` | Code constant | Monitoring polls between index-level checkpoint persists (~60 s) |
| `REPLICATION_CONFIG_SYSTEM_INDEX` | `.replication-metadata-store` | System index | Stores all replication metadata |
| `REPLICATION_STORE_MAPPING_VERSION` | `2` | Code constant | Current schema version of metadata store |

