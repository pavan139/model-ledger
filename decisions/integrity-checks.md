# Integrity and recovery checks

Executed on Python 3.12 using the standard-library implementation:

```bash
PYTHONPATH=src python -m unittest discover -s checks -v
PYTHONPATH=src python -m model_ledger demo --output release_records/demo.json
PYTHONPATH=src python -m model_ledger evaluate --output release_records/evaluation.json
ruff check --select E9,F src checks
ruff format --check src checks
```

The suite has **17 passing tests**. Coverage includes expected-digest rejection, streaming/deduplication, immutable/versioned manifests, missing parents, transitive reachability, stale environment CAS, rollback/audit chain, corruption rejection, lease expiry/renewal, and a four-promoter race. Real child processes exit after blob publication, during uncommitted quarantine, and after GC commit. Another test races promotion against collection and checks that a successful active reference still has verified bytes.

Review exposed and fixed a recovery edge: re-publication of a formerly collected digest could leave a valid CAS file alongside a tombstoned metadata row if the process exited before commit. Recovery now verifies and re-registers that state. Tests retain this regression. Returned-byte verification was also made independent of a prior file pass, and clock values are normalized to finite floats before audit hashing.

The default evaluation records **8/8 actual local lifecycle checks**, including one committed promoter and three stale-revision outcomes. Those are software contract checks; no trained model, semantic dataset quality, authentication, or cloud throughput was measured.

Remaining limits: private single-host directory ownership, no authentication/signatures/replication, no resource admission for total registry disk use, bounded individual blobs/manifests, and conservative retention of uncertain staging owners. No exhaustive mutation score or power-loss certification is claimed.
