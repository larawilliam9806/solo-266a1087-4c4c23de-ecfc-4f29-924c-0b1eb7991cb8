"""Unit tests for the package lease registry (no servers involved)."""

from __future__ import annotations

import pytest

from app.packages import PackageRegistry, PackageRetired


def test_unpinned_retire_deletes_immediately(tmp_path):
    registry = PackageRegistry()
    pkg = tmp_path / "pkg.tar.gz"
    pkg.write_bytes(b"old")
    registry.retire(str(pkg))
    assert not pkg.exists()
    assert registry.pinned() == {}
    assert registry.retired() == set()


def test_pinned_retire_defers_until_last_release(tmp_path):
    registry = PackageRegistry()
    pkg = tmp_path / "pkg.tar.gz"
    pkg.write_bytes(b"old")
    lease1 = registry.acquire(str(pkg))
    lease2 = registry.acquire(str(pkg))
    registry.retire(str(pkg))
    assert pkg.exists(), "deleted while downloads held it"
    assert registry.pinned()[str(pkg)] == 2

    lease1.release()
    assert pkg.exists(), "deleted before the last download finished"
    assert registry.pinned()[str(pkg)] == 1

    lease2.release()
    assert not pkg.exists(), "not deleted after the last download finished"
    assert registry.pinned() == {}
    assert registry.retired() == set()


def test_acquire_after_retire_is_refused(tmp_path):
    registry = PackageRegistry()
    pkg = tmp_path / "pkg.tar.gz"
    pkg.write_bytes(b"old")
    lease = registry.acquire(str(pkg))
    registry.retire(str(pkg))
    with pytest.raises(PackageRetired):
        registry.acquire(str(pkg))
    lease.release()
    assert not pkg.exists()


def test_lease_release_is_idempotent(tmp_path):
    registry = PackageRegistry()
    pkg = tmp_path / "pkg.tar.gz"
    pkg.write_bytes(b"old")
    lease = registry.acquire(str(pkg))
    lease.release()
    lease.release()
    assert registry.pinned() == {}
    # A double release must not have corrupted the count for a later lease.
    again = registry.acquire(str(pkg))
    assert registry.pinned()[str(pkg)] == 1
    again.release()
    assert registry.pinned() == {}


def test_retire_is_idempotent_and_missing_file_tolerated(tmp_path):
    registry = PackageRegistry()
    missing = tmp_path / "nope.tar.gz"
    registry.retire(str(missing))  # never existed: no error
    pkg = tmp_path / "pkg.tar.gz"
    pkg.write_bytes(b"old")
    registry.retire(str(pkg))
    registry.retire(str(pkg))  # already gone: still no error
    assert not pkg.exists()


def test_current_package_is_never_retired_implicitly(tmp_path):
    """A lease on a live (non-retired) package never triggers deletion."""
    registry = PackageRegistry()
    pkg = tmp_path / "pkg.tar.gz"
    pkg.write_bytes(b"current")
    lease = registry.acquire(str(pkg))
    lease.release()
    assert pkg.exists()
