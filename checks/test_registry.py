import hashlib
import io
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest

from model_ledger import Registry, LedgerError
from model_ledger.experiment import evaluate


class RegistryTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.time = [1000.0]
        self.registry = Registry(self.root, clock=lambda: self.time[0])

    def tearDown(self):
        self.registry.close()
        self.directory.cleanup()

    def artifact(self, name, payload=b"weights", parents=(), expected=0):
        blob = self.registry.put(payload)
        manifest = self.registry.create_manifest(name, "model", [blob.sha256], parents, expected_version=expected)
        return blob, manifest

    def test_streaming_expected_digest_and_content_dedup(self):
        payload = b"x" * 150000
        digest = hashlib.sha256(payload).hexdigest()
        receipt = self.registry.put(io.BytesIO(payload), expected_sha256=digest)
        duplicate = self.registry.put(payload)
        self.assertEqual((receipt.sha256, receipt.size), (digest, len(payload)))
        self.assertEqual(duplicate.sha256, digest)
        self.assertEqual(self.registry.db.execute("SELECT COUNT(*) FROM blobs").fetchone()[0], 1)
        self.assertEqual(self.registry.read(digest), payload)
        self.assertEqual(self.registry.verify(digest)["size"], len(payload))

    def test_wrong_digest_and_text_stream_never_publish(self):
        for source, expected in ((b"bytes", "0" * 64), (io.StringIO("text"), None)):
            with self.subTest(expected=expected), self.assertRaises(LedgerError):
                self.registry.put(source, expected_sha256=expected)
        self.assertEqual(list(self.registry.blobs.glob("*/*")), [])
        self.assertEqual(list(self.registry.staging.iterdir()), [])

    def test_lineage_hashes_are_immutable_and_versions_never_reused(self):
        blob, data = self.artifact("data")
        _, model = self.artifact("model", b"model", (data["id"],))
        payload = self.registry.manifest(model["id"])
        self.assertEqual(payload["parents"], [data["id"]])
        with self.assertRaises(sqlite3.IntegrityError):
            self.registry.db.execute("UPDATE manifests SET payload='{}'")
        with self.assertRaises(LedgerError):
            self.artifact("data", b"second", expected=0)
        _, second = self.artifact("data", b"second", expected=1)
        self.assertEqual(second["payload"]["version"], 2)

    def test_missing_or_invalid_parent_is_rejected_without_new_version(self):
        blob = self.registry.put(b"model")
        with self.assertRaises(LedgerError):
            self.registry.create_manifest("model", "model", [blob.sha256], ["0" * 64])
        self.assertEqual(self.registry.db.execute("SELECT COUNT(*) FROM manifests").fetchone()[0], 0)

    def test_promote_rollback_and_audit_chain(self):
        _, one = self.artifact("model", b"one")
        _, two = self.artifact("model", b"two", expected=1)
        self.registry.promote("prod", one["id"], expected_revision=0)
        self.registry.promote("prod", two["id"], expected_revision=1)
        self.registry.rollback("prod", 1, expected_revision=2)
        self.assertEqual(self.registry.environment("prod"), {"revision": 3, "manifest": one["id"]})
        self.assertEqual([row["action"] for row in self.registry.audit()], ["promote", "promote", "rollback"])
        with self.assertRaises(sqlite3.IntegrityError):
            self.registry.db.execute("DELETE FROM audit")

    def test_stale_environment_revision_cannot_move_reference(self):
        _, one = self.artifact("model")
        self.registry.promote("prod", one["id"], expected_revision=0)
        with self.assertRaises(LedgerError) as error:
            self.registry.promote("prod", one["id"], expected_revision=0)
        self.assertEqual(error.exception.code, "stale_revision")
        self.assertEqual(len(self.registry.audit()), 1)

    def test_gc_independent_reachability_and_expiring_leases(self):
        data_blob, data = self.artifact("dataset", b"dataset")
        config_blob, config = self.artifact("config", b"config")
        model_blob, model = self.artifact("model", b"model", (data["id"], config["id"]))
        orphan = self.registry.put(b"orphan", lease_ttl=2)
        self.registry.promote("prod", model["id"], expected_revision=0)
        self.time[0] += 1000
        result = self.registry.gc(retain_history=0)
        expected_roots = {data_blob.sha256, config_blob.sha256, model_blob.sha256}
        actual_present = {row[0] for row in self.registry.db.execute("SELECT digest FROM blobs WHERE present=1")}
        self.assertEqual(actual_present, expected_roots)
        self.assertEqual(result["swept_blobs"], [orphan.sha256])

    def test_lease_renewal_protects_content_and_expired_lease_cannot_revive(self):
        blob = self.registry.put(b"lease", lease_ttl=10)
        self.time[0] += 9
        self.registry.renew(blob.lease_id, ttl=10)
        self.time[0] += 2
        self.assertNotIn(blob.sha256, self.registry.gc()["swept_blobs"])
        self.time[0] += 9
        with self.assertRaises(LedgerError):
            self.registry.renew(blob.lease_id)
        self.assertIn(blob.sha256, self.registry.gc()["swept_blobs"])

    def test_corruption_detected_before_read_or_promotion(self):
        blob, manifest = self.artifact("model")
        self.registry._path(blob.sha256).write_bytes(b"corrupted")
        for operation in (
            lambda: self.registry.read(blob.sha256),
            lambda: self.registry.promote("prod", manifest["id"], expected_revision=0),
        ):
            with self.subTest(operation=operation), self.assertRaises(LedgerError) as error:
                operation()
            self.assertEqual(error.exception.code, "corrupt_blob")
        self.assertEqual(self.registry.environment("prod")["revision"], 0)

    def test_concurrent_promoters_have_one_cas_winner(self):
        _, manifest = self.artifact("model")
        barrier, outcomes = threading.Barrier(4), []

        def writer():
            with Registry(self.root) as registry:
                barrier.wait(timeout=10)
                try:
                    registry.promote("prod", manifest["id"], expected_revision=0)
                    outcomes.append("committed")
                except LedgerError as error:
                    outcomes.append(error.code)

        threads = [threading.Thread(target=writer) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=15)
        self.assertEqual(outcomes.count("committed"), 1)
        self.assertEqual(outcomes.count("stale_revision"), 3)

    def test_process_crash_after_blob_link_recovers_verified_orphan(self):
        code = "from model_ledger import Registry; import os,sys; r=Registry(sys.argv[1],fault_hook=lambda s: os._exit(73) if s=='after_blob_publish' else None); r.put(b'crash-payload')"
        result = subprocess.run(
            [sys.executable, "-c", code, str(self.root)],
            env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)},
            timeout=10,
        )
        self.assertEqual(result.returncode, 73)
        recovery = self.registry.recover()
        self.assertEqual(recovery["orphan_blobs_registered"], 1)
        self.assertEqual(recovery["dead_process_staging_removed"], 1)
        self.assertEqual(list(self.registry.staging.iterdir()), [])
        self.assertEqual(self.registry.read(hashlib.sha256(b"crash-payload").hexdigest()), b"crash-payload")

    def test_process_crash_during_gc_restores_rolled_back_quarantine(self):
        blob = self.registry.put(b"quarantine-crash")
        self.registry.release(blob.lease_id)
        code = "from model_ledger import Registry; import os,sys; r=Registry(sys.argv[1],fault_hook=lambda s: os._exit(74) if s=='after_gc_quarantine' else None); r.gc(retain_history=0)"
        result = subprocess.run(
            [sys.executable, "-c", code, str(self.root)],
            env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)},
            timeout=10,
        )
        self.assertEqual(result.returncode, 74)
        self.assertFalse(self.registry._path(blob.sha256).exists())
        self.assertEqual(self.registry.recover()["restored_quarantined"], 1)
        self.assertEqual(self.registry.read(blob.sha256), b"quarantine-crash")

    def test_crashed_republication_of_tombstoned_blob_is_reconciled(self):
        blob = self.registry.put(b"crash-payload")
        self.registry.release(blob.lease_id)
        self.registry.gc(retain_history=0)
        code = "from model_ledger import Registry; import os,sys; r=Registry(sys.argv[1],fault_hook=lambda s: os._exit(73) if s=='after_blob_publish' else None); r.put(b'crash-payload')"
        result = subprocess.run(
            [sys.executable, "-c", code, str(self.root)],
            env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)},
            timeout=10,
        )
        self.assertEqual(result.returncode, 73)
        self.assertEqual(self.registry.recover()["republished_blobs_registered"], 1)
        self.assertEqual(self.registry.read(blob.sha256), b"crash-payload")

    def test_crash_after_gc_commit_does_not_resurrect_collected_object(self):
        blob = self.registry.put(b"gc-committed")
        self.registry.release(blob.lease_id)
        code = "from model_ledger import Registry; import os,sys; r=Registry(sys.argv[1],fault_hook=lambda s: os._exit(75) if s=='after_gc_commit' else None); r.gc(retain_history=0)"
        result = subprocess.run(
            [sys.executable, "-c", code, str(self.root)],
            env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)},
            timeout=10,
        )
        self.assertEqual(result.returncode, 75)
        self.assertEqual(self.registry.recover()["discarded_quarantined"], 1)
        with self.assertRaises(LedgerError):
            self.registry.read(blob.sha256)

    def test_gc_and_promotion_race_cannot_create_a_dangling_active_reference(self):
        blob, manifest = self.artifact("model")
        self.registry.release(blob.lease_id)
        self.registry.release(manifest["lease_id"])
        barrier, outcomes = threading.Barrier(2), []

        def promoter():
            with Registry(self.root) as registry:
                barrier.wait(timeout=10)
                try:
                    registry.promote("prod", manifest["id"], expected_revision=0)
                    outcomes.append("promoted")
                except LedgerError as error:
                    outcomes.append(error.code)

        def collector():
            with Registry(self.root) as registry:
                barrier.wait(timeout=10)
                registry.gc(retain_history=0)

        threads = [threading.Thread(target=promoter), threading.Thread(target=collector)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=15)
        if self.registry.environment("prod")["revision"]:
            self.assertEqual(outcomes, ["promoted"])
            self.assertEqual(self.registry.read(blob.sha256), b"weights")
        else:
            self.assertEqual(outcomes, ["artifact_unavailable"])

    def test_collected_old_revision_cannot_be_silently_rolled_back(self):
        first_blob, one = self.artifact("model", b"one")
        _, two = self.artifact("model", b"two", expected=1)
        self.registry.promote("prod", one["id"], expected_revision=0)
        self.registry.promote("prod", two["id"], expected_revision=1)
        self.time[0] += 1000
        self.registry.gc(retain_history=0)
        with self.assertRaises(LedgerError) as error:
            self.registry.rollback("prod", 1, expected_revision=2)
        self.assertEqual(error.exception.code, "artifact_unavailable")
        self.registry.put(b"one", expected_sha256=first_blob.sha256)
        self.registry.rollback("prod", 1, expected_revision=2)
        self.assertEqual(self.registry.environment("prod")["manifest"], one["id"])

    def test_executed_registry_experiment_checks_all_cases(self):
        report = evaluate()
        self.assertEqual(report["passed"], report["case_count"])


if __name__ == "__main__":
    unittest.main()
