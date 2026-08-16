# Sharing a registry implementation

Publish the source and its small release records, not the mutable registry directory used by an application. A live registry includes SQLite state, staging files, quarantine and content-addressed bytes whose lifecycle belongs to that application.

Before sharing a revision:

```bash
python -m pip install -e . -r requirements-dev.txt
python tools/check_registry.py --require-lint --wheel
python -m model_ledger evaluate --output artifacts/release-check.json
```

Inspect the evaluation's eight lifecycle checks and promoter race. Keep `release_records/` if it still describes the workload documented in `promotion-runs.md`; a newer local timing does not silently replace the context of an older measurement. The verifier records a normal wheel installation outside the checkout in [verification.json](../reports/verification.json).

This directory is already an independent Git repository in the workspace. An extracted ZIP has no Git internals, so initialize it and make a reviewed initial commit before adding your own remote. Do not invent earlier release history for the demonstration manifests.

```bash
git remote add origin https://github.com/YOUR_USERNAME/model-ledger.git
git push -u origin HEAD
```

The CI workflow installs the package, runs `tools/check_registry.py` and uploads the verification report. The local checks do not mean the hosted workflow has run; its status becomes visible after a push. Users of the registry still need to choose retention, ownership and storage policies for their own artifact directory.
