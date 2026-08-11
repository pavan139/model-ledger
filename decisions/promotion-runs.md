# Promotion and collection runs

`python -m model_ledger evaluate --output artifacts/evaluation.json` creates temporary real blob files and a SQLite WAL database, then runs eight labeled checks. Artifact and manifest IDs derive from their bytes/payloads. Lease IDs are random capabilities, and timing/race order can vary. The lease-boundary fixture uses a controlled clock rather than sleeping.

The experiment publishes dataset/config/model manifests, promotes two model versions, rolls back as a new release, verifies the audit chain, and collects an expired unreferenced object while preserving active lineage. A separate four-thread/four-connection release race verifies exactly one expected-revision winner. Deliberately corrupted bytes must be rejected.

[release_records/evaluation.json](../release_records/evaluation.json) is an executed report with denominators, race outcomes, actual wall time, and raw lifecycle records. [release_records/demo.json](../release_records/demo.json) shows the detailed artifact graph and releases. Do not interpret the synthetic weight bytes as a trained checkpoint or the pass count as model accuracy.

Crash experiments live in the unit suite because they intentionally terminate child processes with explicit exit codes. They verify orphan recovery, uncommitted-quarantine restoration, committed-trash deletion, and re-publication of a tombstoned digest. The processes use only project-controlled Python snippets; the public API does not execute user programs.

For larger experiments, stream independently generated files through `Registry.put`, retain the returned lease, create manifests, then measure verification/promotion/GC time separately. Report filesystem, SQLite/Python versions, blob sizes, lineage size, cache state, and durability assumptions. Avoid comparing a metadata-only reference update against a release that rehashes all lineage bytes without naming that difference.
