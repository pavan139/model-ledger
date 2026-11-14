# Changing the ledger

Start with the transaction boundary you are changing in [registry-transactions.md](decisions/registry-transactions.md). Blob publication, manifest creation, promotion and collection share reachability assumptions. A change that saves a database query can still break the upload-to-manifest lease or quarantine recovery.

Keep integrity and failure cases in `checks/test_registry.py`. For storage changes, test both sides of commit with an actual subprocess exit. For promotion or collection changes, retain the competing-connection test and verify the winning release's reachable bytes. An expected exception should leave a usable registry, not just an error message.

```bash
PYTHONPATH=src python -m unittest discover -s checks -v
python -m ruff check src checks
python -m ruff format --check src checks
```

When changing a measured workload, write a new release record and explain its blob sizes, lineage, retention and clock assumptions in `decisions/promotion-runs.md`. Keep runtime registries under ignored `artifacts/` or outside the checkout. The small byte fixtures in version control should remain inspectable without downloading a model.
