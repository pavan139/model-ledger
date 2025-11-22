"""Real local filesystem/SQLite experiments, not a model-quality benchmark."""

from __future__ import annotations

from dataclasses import asdict
import hashlib
import io
from pathlib import Path
import tempfile
import threading
import time

from .registry import Registry, LedgerError


def demo(root):
    clock = [1000.0]
    with Registry(root, clock=lambda: clock[0]) as registry:
        data = registry.put(io.BytesIO(b"sample_id,label\n1,red\n2,blue\n"))
        config = registry.put(b'{"algorithm":"linear","seed":7}')
        dataset = registry.create_manifest("training-data", "dataset", [data.sha256])
        configuration = registry.create_manifest("training-config", "config", [config.sha256])
        first = registry.put(b"weights-version-one")
        model1 = registry.create_manifest("classifier", "model", [first.sha256], [dataset["id"], configuration["id"]])
        release1 = registry.promote("production", model1["id"], expected_revision=0)
        second = registry.put(b"weights-version-two")
        model2 = registry.create_manifest(
            "classifier", "model", [second.sha256], [dataset["id"], configuration["id"]], expected_version=1
        )
        registry.promote("production", model2["id"], expected_revision=1)
        rollback = registry.rollback("production", 1, expected_revision=2)
        orphan = registry.put(b"unreferenced-experiment", lease_ttl=5)
        protected = registry.gc()
        clock[0] += 6
        collected = registry.gc()
        return {
            "experiment": "actual_local_content_registry",
            "model_inference_performed": False,
            "dataset": dataset,
            "configuration": configuration,
            "model_versions": [model1, model2],
            "first_release": release1,
            "rollback": rollback,
            "environment": registry.environment("production"),
            "audit_chain_verified": len(registry.audit()),
            "leased_blob": asdict(orphan),
            "gc_before_lease_expiry": protected,
            "gc_after_lease_expiry": collected,
            "active_model_verified": registry.verify(first.sha256),
            "limitations": [
                "This is a local artifact-control-plane experiment, not trained-model accuracy.",
                "Private trusted filesystem and SQLite ownership are assumed; no cloud auth, signed provenance, or distributed replication is implemented.",
            ],
        }


def evaluate():
    started = time.perf_counter()
    with tempfile.TemporaryDirectory() as directory:
        report = demo(Path(directory) / "demo")
        race_root = Path(directory) / "race"
        with Registry(race_root) as registry:
            receipt = registry.put(b"race-weights")
            manifest = registry.create_manifest("race-model", "model", [receipt.sha256])
        barrier, lock, outcomes = threading.Barrier(4), threading.Lock(), []

        def writer():
            with Registry(race_root) as registry:
                barrier.wait(timeout=10)
                try:
                    registry.promote("staging", manifest["id"], expected_revision=0)
                    result = "committed"
                except LedgerError as error:
                    result = error.code
                with lock:
                    outcomes.append(result)

        threads = [threading.Thread(target=writer) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=15)
        with Registry(race_root) as registry:
            audit_count = len(registry.audit())
            registry._path(receipt.sha256).write_bytes(b"corruption")
            try:
                registry.verify(receipt.sha256)
                corrupt = "not_detected"
            except LedgerError as error:
                corrupt = error.code
        cases = {
            "stream_digest": report["active_model_verified"]["sha256"]
            == hashlib.sha256(b"weights-version-one").hexdigest(),
            "rollback_revision": report["environment"]["revision"] == 3,
            "audit_chain": report["audit_chain_verified"] == 3,
            "active_lease_protected": report["leased_blob"]["sha256"]
            not in report["gc_before_lease_expiry"]["swept_blobs"],
            "expired_unreachable_swept": report["leased_blob"]["sha256"]
            in report["gc_after_lease_expiry"]["swept_blobs"],
            "active_lineage_retained": report["active_model_verified"]["verified"],
            "concurrent_cas": outcomes.count("committed") == 1
            and outcomes.count("stale_revision") == 3
            and audit_count == 1,
            "corruption_detected": corrupt == "corrupt_blob",
        }
        return {
            "experiment": "executed_filesystem_sqlite_registry_checks",
            "case_count": len(cases),
            "passed": sum(cases.values()),
            "cases": cases,
            "promotion_race_outcomes": sorted(outcomes),
            "wall_seconds": time.perf_counter() - started,
            "demo": report,
            "limits": [
                "Process-crash injection and quarantine recovery are additionally exercised by the test suite.",
                "One local run does not establish distributed scalability or power-loss durability.",
            ],
        }
