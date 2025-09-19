"""Filesystem content addressing with transactional lineage and release references."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import io
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import tempfile
import time
import uuid

CHUNK_BYTES = 65536
MAX_BLOB_BYTES = 256 * 1024 * 1024
MAX_MANIFESTS = 10000
KINDS = {"model", "dataset", "config", "evaluation"}


class LedgerError(ValueError):
    def __init__(self, code, message):
        self.code = code
        super().__init__(message)


def canonical(value):
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError, RecursionError) as error:
        raise LedgerError("invalid_json", "Metadata must be finite JSON") from error


def digest_id(value):
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise LedgerError("invalid_digest", "Expected a lowercase SHA256 digest")
    return value


def name_id(value):
    if not isinstance(value, str) or re.fullmatch(r"[a-z][a-z0-9_.-]{0,63}", value) is None:
        raise LedgerError("invalid_name", "Names require 1..64 lowercase identifier characters")
    return value


def _ttl(value):
    if type(value) not in (int, float) or not 0 < value <= 86400:
        raise LedgerError("invalid_ttl", "Lease TTL must be in (0,86400]")
    return value


@dataclass(frozen=True)
class BlobReceipt:
    sha256: str
    size: int
    lease_id: str
    lease_expires_at: float


class Registry:
    """One connection per thread/instance. The local registry directory is trusted.

    A SQLite writer transaction serializes publication, references, leases, and
    GC classification. Staging bytes are never considered published artifacts.
    """

    def __init__(self, root, *, clock=time.time, fault_hook=None):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.blobs = self.root / "blobs"
        self.staging = self.root / "staging"
        self.trash = self.root / "trash"
        for directory in (self.blobs, self.staging, self.trash):
            directory.mkdir(exist_ok=True)
        self.clock, self.fault_hook = clock, fault_hook
        self.db = sqlite3.connect(self.root / "ledger.sqlite", timeout=5, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS blobs(digest TEXT PRIMARY KEY,size INTEGER NOT NULL,present INTEGER NOT NULL,created REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS manifests(id TEXT PRIMARY KEY,name TEXT NOT NULL,kind TEXT NOT NULL,version INTEGER NOT NULL,payload TEXT NOT NULL,created REAL NOT NULL,UNIQUE(name,version));
        CREATE TABLE IF NOT EXISTS edges(child TEXT NOT NULL REFERENCES manifests(id),parent TEXT NOT NULL REFERENCES manifests(id),PRIMARY KEY(child,parent));
        CREATE TABLE IF NOT EXISTS manifest_blobs(manifest TEXT NOT NULL REFERENCES manifests(id),digest TEXT NOT NULL REFERENCES blobs(digest),PRIMARY KEY(manifest,digest));
        CREATE TABLE IF NOT EXISTS leases(id TEXT PRIMARY KEY,kind TEXT NOT NULL,target TEXT NOT NULL,expires REAL NOT NULL);
        CREATE INDEX IF NOT EXISTS ix_leases_expiry ON leases(expires);
        CREATE TABLE IF NOT EXISTS environments(name TEXT PRIMARY KEY,revision INTEGER NOT NULL,manifest TEXT NOT NULL REFERENCES manifests(id));
        CREATE TABLE IF NOT EXISTS audit(sequence INTEGER PRIMARY KEY,environment TEXT NOT NULL,revision INTEGER NOT NULL,previous_manifest TEXT,target_manifest TEXT NOT NULL REFERENCES manifests(id),action TEXT NOT NULL,actor TEXT NOT NULL,created REAL NOT NULL,previous_hash TEXT NOT NULL,entry_hash TEXT NOT NULL,UNIQUE(environment,revision));
        CREATE TRIGGER IF NOT EXISTS immutable_manifest BEFORE UPDATE ON manifests BEGIN SELECT RAISE(ABORT,'immutable_manifest'); END;
        CREATE TRIGGER IF NOT EXISTS immutable_manifest_delete BEFORE DELETE ON manifests BEGIN SELECT RAISE(ABORT,'immutable_manifest'); END;
        CREATE TRIGGER IF NOT EXISTS immutable_audit BEFORE UPDATE ON audit BEGIN SELECT RAISE(ABORT,'immutable_audit'); END;
        CREATE TRIGGER IF NOT EXISTS immutable_audit_delete BEFORE DELETE ON audit BEGIN SELECT RAISE(ABORT,'immutable_audit'); END;
        """)
        self.last_recovery = self.recover()

    def _now(self):
        value = self.clock()
        if type(value) not in (int, float) or not math.isfinite(value):
            raise LedgerError("invalid_clock", "Clock must return finite epoch seconds")
        return float(value)

    def close(self):
        self.db.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    @contextmanager
    def _write(self):
        if self.db.in_transaction:
            raise LedgerError("nested_transaction", "Nested writer transactions are unsupported")
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.db.execute("COMMIT")
        except BaseException:
            if self.db.in_transaction:
                self.db.execute("ROLLBACK")
            raise

    def _fault(self, stage):
        if self.fault_hook is not None:
            self.fault_hook(stage)

    def _path(self, digest):
        digest_id(digest)
        path = self.blobs / digest[:2] / digest
        if not path.resolve().is_relative_to(self.blobs.resolve()):
            raise LedgerError("unsafe_path", "CAS path escaped the registry")
        return path

    def _lease(self, kind, target, ttl):
        expires = self._now() + _ttl(ttl)
        identifier = uuid.uuid4().hex
        self.db.execute("INSERT INTO leases VALUES(?,?,?,?)", (identifier, kind, target, expires))
        return identifier, expires

    def _verify(self, digest):
        path = self._path(digest)
        if not path.is_file() or path.stat().st_size > MAX_BLOB_BYTES:
            raise LedgerError("artifact_unavailable", "Blob is absent or exceeds the size contract")
        hasher, size = hashlib.sha256(), 0
        with path.open("rb") as stream:
            while chunk := stream.read(CHUNK_BYTES):
                size += len(chunk)
                if size > MAX_BLOB_BYTES:
                    raise LedgerError("blob_too_large", "Blob grew beyond the size limit")
                hasher.update(chunk)
        if hasher.hexdigest() != digest:
            raise LedgerError("corrupt_blob", "Stored bytes do not match their content address")
        return size

    def put(self, source, *, expected_sha256=None, lease_ttl=300):
        if expected_sha256 is not None:
            digest_id(expected_sha256)
        _ttl(lease_ttl)
        if isinstance(source, bytes):
            source = io.BytesIO(source)
        if not hasattr(source, "read"):
            raise LedgerError("invalid_source", "Expected bytes or a binary stream")
        descriptor, temporary = tempfile.mkstemp(dir=self.staging, prefix=f"upload-{os.getpid()}-")
        temporary = Path(temporary)
        hasher, size = hashlib.sha256(), 0
        try:
            with os.fdopen(descriptor, "wb") as output:
                while True:
                    chunk = source.read(CHUNK_BYTES)
                    if not isinstance(chunk, bytes):
                        raise LedgerError("invalid_source", "Binary streams must return bytes")
                    if not chunk:
                        break
                    size += len(chunk)
                    if size > MAX_BLOB_BYTES:
                        raise LedgerError("blob_too_large", "Upload exceeds 256 MiB")
                    hasher.update(chunk)
                    output.write(chunk)
                output.flush()
                os.fsync(output.fileno())
            digest = hasher.hexdigest()
            if expected_sha256 is not None and digest != expected_sha256:
                raise LedgerError("digest_mismatch", "Upload does not match the expected digest")
            with self._write():
                destination = self._path(digest)
                destination.parent.mkdir(exist_ok=True)
                try:
                    os.link(temporary, destination)
                except FileExistsError:
                    if self._verify(digest) != size:
                        raise LedgerError("corrupt_blob", "Existing object has incompatible size")
                self._fault("after_blob_publish")
                self.db.execute(
                    "INSERT INTO blobs VALUES(?,?,1,?) ON CONFLICT(digest) DO UPDATE SET present=1",
                    (digest, size, self._now()),
                )
                lease, expires = self._lease("blob", digest, lease_ttl)
            return BlobReceipt(digest, size, lease, expires)
        finally:
            temporary.unlink(missing_ok=True)

    def verify(self, digest):
        digest_id(digest)
        with self._write():
            self._available({digest})
            return {"sha256": digest, "size": self._verify(digest), "verified": True}

    def read(self, digest, *, max_bytes=8 * 1024 * 1024):
        if type(max_bytes) is not int or not 0 <= max_bytes <= MAX_BLOB_BYTES:
            raise LedgerError("invalid_budget", "Read budget must be in [0,256MiB]")
        with self._write():
            self._available({digest})
            size = self._path(digest).stat().st_size
            if size > max_bytes:
                raise LedgerError("read_budget", "Use streaming verification for larger objects")
            with self._path(digest).open("rb") as stream:
                payload = stream.read(max_bytes + 1)
            if len(payload) > max_bytes:
                raise LedgerError("read_budget", "Blob grew beyond the read budget")
            if hashlib.sha256(payload).hexdigest() != digest:
                raise LedgerError("corrupt_blob", "Returned bytes do not match their content address")
            return payload

    def _closure(self, roots):
        pending, visited, blobs = list(roots), set(), set()
        while pending:
            identifier = digest_id(pending.pop())
            if identifier in visited:
                continue
            if (
                len(visited) >= MAX_MANIFESTS
                or self.db.execute("SELECT 1 FROM manifests WHERE id=?", (identifier,)).fetchone() is None
            ):
                raise LedgerError("invalid_lineage", "Missing manifest or lineage traversal budget exceeded")
            visited.add(identifier)
            blobs.update(
                row[0] for row in self.db.execute("SELECT digest FROM manifest_blobs WHERE manifest=?", (identifier,))
            )
            pending.extend(row[0] for row in self.db.execute("SELECT parent FROM edges WHERE child=?", (identifier,)))
        return visited, blobs

    def _available(self, digests, *, verify=False):
        for digest in digests:
            digest_id(digest)
            row = self.db.execute("SELECT present FROM blobs WHERE digest=?", (digest,)).fetchone()
            if row is None or not row[0] or not self._path(digest).is_file():
                raise LedgerError("artifact_unavailable", "Referenced content has been removed or was never registered")
            if verify:
                self._verify(digest)

    def create_manifest(
        self, name, kind, blob_digests=(), parents=(), *, expected_version=0, metadata=None, lease_ttl=300
    ):
        name_id(name)
        if not isinstance(kind, str) or kind not in KINDS or type(expected_version) is not int or expected_version < 0:
            raise LedgerError("invalid_manifest", "Invalid artifact kind or expected version")
        if (
            not isinstance(blob_digests, (list, tuple))
            or not isinstance(parents, (list, tuple))
            or len(blob_digests) > 64
            or len(parents) > 64
        ):
            raise LedgerError("invalid_manifest", "At most 64 blobs and parents are supported")
        for identifier in (*blob_digests, *parents):
            digest_id(identifier)
        if len(set(blob_digests)) != len(blob_digests) or len(set(parents)) != len(parents):
            raise LedgerError("invalid_manifest", "Manifest references must be unique")
        metadata = {} if metadata is None else metadata
        if not isinstance(metadata, dict) or len(canonical(metadata)) > 16384:
            raise LedgerError("invalid_metadata", "Metadata requires a JSON object of at most 16KiB")
        _ttl(lease_ttl)
        with self._write():
            latest = self.db.execute(
                "SELECT version,kind FROM manifests WHERE name=? ORDER BY version DESC LIMIT 1", (name,)
            ).fetchone()
            version = latest["version"] if latest else 0
            if version != expected_version:
                raise LedgerError("stale_version", "Manifest head compare-and-swap failed")
            if latest and latest["kind"] != kind:
                raise LedgerError("kind_mismatch", "A manifest name cannot change artifact kind")
            if self.db.execute("SELECT COUNT(*) FROM manifests").fetchone()[0] >= MAX_MANIFESTS:
                raise LedgerError("manifest_limit", "Registry manifest limit exceeded")
            _, inherited = self._closure(parents)
            self._available(inherited | set(blob_digests), verify=True)
            payload = {
                "schema_version": 1,
                "name": name,
                "kind": kind,
                "version": version + 1,
                "blobs": sorted(blob_digests),
                "parents": sorted(parents),
                "metadata": metadata,
            }
            encoded = canonical(payload)
            identifier = hashlib.sha256(encoded.encode()).hexdigest()
            self.db.execute(
                "INSERT INTO manifests VALUES(?,?,?,?,?,?)", (identifier, name, kind, version + 1, encoded, self._now())
            )
            self.db.executemany("INSERT INTO edges VALUES(?,?)", [(identifier, parent) for parent in parents])
            self.db.executemany(
                "INSERT INTO manifest_blobs VALUES(?,?)", [(identifier, digest) for digest in blob_digests]
            )
            lease, expires = self._lease("manifest", identifier, lease_ttl)
            return {"id": identifier, "payload": payload, "lease_id": lease, "lease_expires_at": expires}

    def manifest(self, identifier):
        digest_id(identifier)
        row = self.db.execute("SELECT payload FROM manifests WHERE id=?", (identifier,)).fetchone()
        if row is None:
            raise LedgerError("missing_manifest", "Manifest does not exist")
        payload = json.loads(row[0])
        if hashlib.sha256(canonical(payload).encode()).hexdigest() != identifier:
            raise LedgerError("corrupt_manifest", "Manifest content-address verification failed")
        return payload

    def lease(self, kind, target, *, ttl=300):
        if kind not in ("blob", "manifest"):
            raise LedgerError("invalid_lease", "Lease target must be blob or manifest")
        digest_id(target)
        with self._write():
            if kind == "manifest":
                _, blobs = self._closure([target])
                self._available(blobs)
            else:
                self._available({target})
            identifier, expires = self._lease(kind, target, ttl)
        return {"id": identifier, "expires_at": expires}

    def release(self, lease):
        with self._write():
            return self.db.execute("DELETE FROM leases WHERE id=?", (lease,)).rowcount == 1

    def renew(self, lease, *, ttl=300):
        _ttl(ttl)
        with self._write():
            row = self.db.execute("SELECT * FROM leases WHERE id=?", (lease,)).fetchone()
            if row is None or row["expires"] <= self._now():
                raise LedgerError("lease_expired", "Missing or expired leases cannot be revived")
            _, blobs = self._closure([row["target"]]) if row["kind"] == "manifest" else (set(), {row["target"]})
            self._available(blobs)
            expires = self._now() + ttl
            self.db.execute("UPDATE leases SET expires=? WHERE id=?", (expires, lease))
            return {"id": lease, "expires_at": expires}

    def environment(self, environment):
        name_id(environment)
        row = self.db.execute("SELECT revision,manifest FROM environments WHERE name=?", (environment,)).fetchone()
        return dict(row) if row else {"revision": 0, "manifest": None}

    def promote(self, environment, manifest, *, expected_revision, actor="local", action="promote"):
        name_id(environment)
        name_id(actor)
        digest_id(manifest)
        if type(expected_revision) is not int or expected_revision < 0 or action not in ("promote", "rollback"):
            raise LedgerError("invalid_promotion", "Invalid expected revision or action")
        with self._write():
            return self._promote(environment, manifest, expected_revision, actor, action)

    def _promote(self, environment, manifest, expected, actor, action):
        previous = self.environment(environment)
        if previous["revision"] != expected:
            raise LedgerError("stale_revision", "Environment compare-and-swap failed")
        _, blobs = self._closure([manifest])
        self._available(blobs, verify=True)
        revision = expected + 1
        last = self.db.execute("SELECT entry_hash FROM audit ORDER BY sequence DESC LIMIT 1").fetchone()
        entry = {
            "environment": environment,
            "revision": revision,
            "previous_manifest": previous["manifest"],
            "target_manifest": manifest,
            "action": action,
            "actor": actor,
            "created": self._now(),
            "previous_hash": last[0] if last else "0" * 64,
        }
        entry_hash = hashlib.sha256(canonical(entry).encode()).hexdigest()
        self.db.execute(
            "INSERT INTO environments VALUES(?,?,?) ON CONFLICT(name) DO UPDATE SET revision=excluded.revision,manifest=excluded.manifest",
            (environment, revision, manifest),
        )
        self.db.execute(
            "INSERT INTO audit(environment,revision,previous_manifest,target_manifest,action,actor,created,previous_hash,entry_hash) VALUES(?,?,?,?,?,?,?,?,?)",
            tuple(entry.values()) + (entry_hash,),
        )
        return {**entry, "entry_hash": entry_hash}

    def rollback(self, environment, target_revision, *, expected_revision, actor="local"):
        name_id(environment)
        name_id(actor)
        if (
            type(expected_revision) is not int
            or expected_revision < 1
            or type(target_revision) is not int
            or not 1 <= target_revision < expected_revision
        ):
            raise LedgerError("invalid_rollback", "Rollback must target an earlier positive revision")
        with self._write():
            row = self.db.execute(
                "SELECT target_manifest FROM audit WHERE environment=? AND revision=?", (environment, target_revision)
            ).fetchone()
            if row is None:
                raise LedgerError("missing_revision", "Rollback target revision does not exist")
            return self._promote(environment, row[0], expected_revision, actor, "rollback")

    def audit(self):
        rows = [dict(row) for row in self.db.execute("SELECT * FROM audit ORDER BY sequence")]
        previous = "0" * 64
        for row in rows:
            payload = {
                key: row[key]
                for key in (
                    "environment",
                    "revision",
                    "previous_manifest",
                    "target_manifest",
                    "action",
                    "actor",
                    "created",
                    "previous_hash",
                )
            }
            if (
                row["previous_hash"] != previous
                or hashlib.sha256(canonical(payload).encode()).hexdigest() != row["entry_hash"]
            ):
                raise LedgerError("corrupt_audit", "Audit chain verification failed")
            previous = row["entry_hash"]
        return rows

    def gc(self, *, retain_history=2):
        if type(retain_history) is not int or not 0 <= retain_history <= 100:
            raise LedgerError("invalid_retention", "History retention must be in [0,100]")
        quarantined = []
        with self._write():
            now = self._now()
            roots = {row[0] for row in self.db.execute("SELECT manifest FROM environments")}
            for (environment,) in self.db.execute("SELECT name FROM environments"):
                roots.update(
                    row[0]
                    for row in self.db.execute(
                        "SELECT target_manifest FROM audit WHERE environment=? ORDER BY revision DESC LIMIT ?",
                        (environment, retain_history),
                    )
                )
            roots.update(
                row[0]
                for row in self.db.execute("SELECT target FROM leases WHERE kind='manifest' AND expires>?", (now,))
            )
            visited, marked = self._closure(roots)
            marked.update(
                row[0] for row in self.db.execute("SELECT target FROM leases WHERE kind='blob' AND expires>?", (now,))
            )
            candidates = [
                row[0] for row in self.db.execute("SELECT digest FROM blobs WHERE present=1") if row[0] not in marked
            ]
            for digest in candidates:
                path = self._path(digest)
                if path.exists():
                    target = self.trash / (digest + "." + uuid.uuid4().hex)
                    os.replace(path, target)
                    quarantined.append(target)
                self.db.execute("UPDATE blobs SET present=0 WHERE digest=?", (digest,))
            self._fault("after_gc_quarantine")
            expired_leases = self.db.execute("DELETE FROM leases WHERE expires<=?", (now,)).rowcount
        self._fault("after_gc_commit")
        for path in quarantined:
            path.unlink(missing_ok=True)
        return {
            "manifest_roots": len(roots),
            "reachable_manifests": len(visited),
            "protected_blobs": len(marked),
            "swept_blobs": sorted(candidates),
            "retained_history_per_environment": retain_history,
            "expired_leases_removed": expired_leases,
        }

    def recover(self):
        restored = discarded = missing = orphans = republished = staging_removed = 0
        with self._write():
            for path in list(self.trash.iterdir()):
                digest = path.name.split(".", 1)[0]
                digest_id(digest)
                row = self.db.execute("SELECT present FROM blobs WHERE digest=?", (digest,)).fetchone()
                destination = self._path(digest)
                if row is not None and row[0] and not destination.exists():
                    destination.parent.mkdir(exist_ok=True)
                    os.replace(path, destination)
                    self._verify(digest)
                    restored += 1
                else:
                    path.unlink()
                    discarded += 1
            for row in self.db.execute("SELECT digest FROM blobs WHERE present=1").fetchall():
                if not self._path(row[0]).is_file():
                    self.db.execute("UPDATE blobs SET present=0 WHERE digest=?", (row[0],))
                    missing += 1
            for path in self.blobs.glob("*/*"):
                if not path.is_file():
                    continue
                digest = digest_id(path.name)
                registered = self.db.execute("SELECT present FROM blobs WHERE digest=?", (digest,)).fetchone()
                if registered is None:
                    size = self._verify(digest)
                    self.db.execute("INSERT INTO blobs VALUES(?,?,1,?)", (digest, size, self._now()))
                    orphans += 1
                elif not registered[0]:
                    self._verify(digest)
                    self.db.execute("UPDATE blobs SET present=1 WHERE digest=?", (digest,))
                    republished += 1
            for path in self.staging.iterdir():
                match = re.fullmatch(r"upload-([0-9]+)-.+", path.name)
                if match is None or not 1 <= int(match[1]) <= 2**31 - 1:
                    continue
                try:
                    os.kill(int(match[1]), 0)
                except ProcessLookupError:
                    path.unlink(missing_ok=True)
                    staging_removed += 1
                except PermissionError:
                    pass  # Uncertain ownership/liveness: retain conservatively.
        return {
            "restored_quarantined": restored,
            "discarded_quarantined": discarded,
            "missing_marked_absent": missing,
            "orphan_blobs_registered": orphans,
            "republished_blobs_registered": republished,
            "dead_process_staging_removed": staging_removed,
        }
