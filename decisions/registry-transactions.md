# Registry transactions and garbage collection

## Content addressing and publication

An upload is read in 64 KiB chunks, hashed with SHA256, and capped at 256 MiB. Staging uses a unique private filename containing the creating process ID. The complete file is flushed and fsynced before publication. An optional caller digest is checked before any CAS path is created.

The destination is `blobs/<first-two-digest-characters>/<64-character-digest>`. Digest syntax and resolved directory containment are checked. `os.link` creates the destination atomically without overwrite; an existing object must independently verify before deduplication succeeds. A SQLite `BEGIN IMMEDIATE` transaction covers publication metadata and a fresh ingest lease. GC uses the same lock, so it cannot collect a newly published object between visibility and its lease. The stream itself is outside the lock; unfinished staging is not a published artifact.

Publication is O(blob bytes), with bounded chunk memory. Returning bytes is separately bounded and hashes exactly the returned buffer. Promotion verifies every reachable content object, so it is intentionally O(total lineage bytes) and can hold the writer lock during I/O. This favors transparent correctness over a scalable remote registry design.

## Immutable versions and lineage

Manifests contain artifact kind, name/version, direct blob IDs, parent manifest IDs, and finite JSON metadata. Their IDs hash canonical payloads. Names have monotonically increasing versions; stale expected heads are rejected inside the writer transaction. Database triggers reject manifest update/delete.

Every parent must already exist. Consequently creation order is a topological order; cycles cannot be introduced through the API. A lineage traversal visits each manifest once and gathers direct blob references, with a 10,000-manifest budget. The persisted parent relation and blob-reference tables make closure traversal O(V+E) in metadata size. Historical manifests remain immutable metadata even when their unreferenced bytes are collected.

## Promotion, rollback, and audit

An environment stores `(revision, manifest)`. A release reads the expected revision under the writer lock, validates transitive materialization and digests, changes the reference, and appends an audit entry in one transaction. Concurrent writers with the same expected revision have exactly one winner. The local SQLite lock serializes writers; CAS detects stale application decisions.

Each audit hash covers the prior global audit hash and the complete release record. Update/delete triggers protect log rows; `audit()` independently recomputes the chain. Rollback resolves an earlier environment revision, then creates a new monotonic release revision. It never rewrites history. If retention/leases allowed old bytes to be collected, rollback fails explicitly until those same content-addressed bytes are re-ingested.

These hashes are integrity/mismatch checks, not signatures or proof that a model was trained on truthful data. The database directory is application-owned; an actor who can replace the database or drop its triggers is outside the authorization boundary.

## Leases and safe mark/sweep

Publication and manifest creation produce leases. A lease can be released or renewed while still active; expired leases cannot revive removed data. Expiry uses an injected epoch clock in tests and wall time in ordinary operation. Clock synchronization across remote hosts is not provided.

GC acquires the writer lock and marks:

1. Current environment manifest references.
2. A configured number of recent environment releases (default two).
3. Active manifest leases and all their parent/blob lineage.
4. Active blob leases.

It quarantines every present unmarked blob, marks its metadata absent, and commits. Committed trash is deleted afterward. Immutable metadata/audit are retained. GC costs O(reachable lineage + registered blobs); it is not an incremental distributed collector. Expired lease rows are pruned in the same transaction.

All operations that can create reachability or renew protection use the same writer lock. Therefore a promotion/GC race either commits a valid protected release, or collection wins and promotion reports unavailable content. It cannot publish a dangling active reference.

## Crash recovery

- **Exit after blob publication, before metadata commit:** recovery verifies and registers the orphan object. Re-publication of an earlier tombstoned digest is also recognized.
- **Exit after quarantine, before GC commit:** SQLite rolls metadata back; recovery restores quarantined bytes whose metadata still says present.
- **Exit after GC commit, before deleting trash:** metadata says absent, so recovery discards trash instead of resurrecting collected objects.
- **Metadata claims a missing file:** recovery marks it absent. Reads/promotions fail visibly.
- **Dead upload process:** process-tagged staging files are removed only when local liveness checks prove the process is gone. PID reuse/permission uncertainty retains files conservatively.

The tests use actual subprocess exits in those windows. This establishes local process-crash behavior, not power-loss durability, storage-device correctness, remote filesystem semantics, or distributed exactly-once execution. File fsync and SQLite FULL synchronous mode are used, but the repository does not claim exhaustive power-failure testing.
