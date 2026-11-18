"""Lease registry for published package files, and the response that serves them.

The race this module closes
---------------------------
A download is accepted (HTTP 200, Content-Length and Digest promised) and,
concurrently, a refresh publishes a newer package and deletes the old one.
If the old file disappears while the response is being set up or streamed,
the client is left with a broken download that already returned 200.

The registry makes deletion lease-aware:

* ``acquire`` pins a package path for one in-flight download.  A path that
  was already retired (replaced) refuses new leases, so a late request is
  redirected to the current package instead of a vanishing one.
* ``retire`` marks a package as replaced/abandoned.  An unpinned file is
  deleted immediately; a pinned file is deleted exactly when its last lease
  is released — never earlier, never later, and never while pinned.
* ``LeasedPackageResponse`` streams from a file descriptor opened *before*
  the 200 is sent, so the body, Content-Length and Digest always describe
  the same inode even if the path itself is unlinked or replaced mid-stream.
  The lease is released exactly once, on completion, error or disconnect.

Refresh never waits for downloads: ``retire`` is O(1) and non-blocking; the
deferred unlink happens on the download's own completion path.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from pathlib import Path
from typing import IO

from starlette.responses import Response
from starlette.types import Receive, Scope, Send

log = logging.getLogger("collector.packages")


class PackageRetired(Exception):
    """The path was replaced before this download was accepted."""


class PackageLease:
    """One in-flight download's hold on a package path.  Idempotent release."""

    def __init__(self, registry: "PackageRegistry", path: str):
        self._registry = registry
        self.path = path
        self._released = False

    def release(self) -> None:
        if not self._released:
            self._released = True
            self._registry.release(self.path)

    def __enter__(self) -> "PackageLease":
        return self

    def __exit__(self, *exc_info) -> None:
        self.release()


class PackageRegistry:
    """Tracks in-flight downloads per package path and defers deletion.

    All methods are non-blocking and safe to call from the event loop or
    from worker code; file deletion happens outside the lock.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._pins: dict[str, int] = {}
        self._retired: set[str] = set()

    def acquire(self, path: str) -> PackageLease:
        """Pin ``path`` for a new download; raise if it was already replaced."""
        with self._lock:
            if path in self._retired:
                raise PackageRetired(path)
            self._pins[path] = self._pins.get(path, 0) + 1
        return PackageLease(self, path)

    def release(self, path: str) -> None:
        """End one download; delete the file if it was retired and this was last."""
        delete = False
        with self._lock:
            remaining = self._pins.get(path, 0) - 1
            if remaining > 0:
                self._pins[path] = remaining
            else:
                self._pins.pop(path, None)
                if path in self._retired:
                    self._retired.discard(path)
                    delete = True
        if delete:
            self._unlink(path)

    def retire(self, path: str) -> None:
        """Mark a package as replaced/abandoned; delete now or on last release."""
        delete = False
        with self._lock:
            if self._pins.get(path, 0) > 0:
                self._retired.add(path)
                log.info("package %s retired; deletion deferred until downloads finish", path)
            else:
                delete = True
        if delete:
            self._unlink(path)

    def pinned(self) -> dict[str, int]:
        """Snapshot of active lease counts (observability/tests)."""
        with self._lock:
            return dict(self._pins)

    def retired(self) -> set[str]:
        """Snapshot of paths awaiting their last release (observability/tests)."""
        with self._lock:
            return set(self._retired)

    @staticmethod
    def _unlink(path: str) -> None:
        try:
            Path(path).unlink(missing_ok=True)
            log.info("deleted retired package %s", path)
        except OSError:
            log.warning("could not delete retired package %s", path, exc_info=True)


class LeasedPackageResponse(Response):
    """Streams an already-open, leased package file.

    The descriptor is opened by the endpoint *before* this response is
    returned, so the promised Content-Length and Digest always match the
    bytes actually sent, regardless of what happens to the path afterwards.
    """

    chunk_size = 256 * 1024

    def __init__(
        self,
        *,
        handle: IO[bytes],
        size: int,
        lease: PackageLease,
        filename: str,
        sha256: str,
    ):
        self._handle = handle
        self._lease = lease
        headers = {
            "Content-Length": str(size),
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Digest": f"sha-256={sha256}",
            "ETag": f'"{sha256}"',
            "Cache-Control": "no-cache",
        }
        super().__init__(
            content=None, status_code=200, headers=headers, media_type="application/gzip"
        )

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        try:
            await send(
                {
                    "type": "http.response.start",
                    "status": self.status_code,
                    "headers": self.raw_headers,
                }
            )
            if scope["type"] == "http" and scope["method"].upper() == "HEAD":
                await send({"type": "http.response.body", "body": b"", "more_body": False})
                return
            while True:
                chunk = await asyncio.to_thread(self._handle.read, self.chunk_size)
                more = bool(chunk)
                await send({"type": "http.response.body", "body": chunk, "more_body": more})
                if not more:
                    break
        finally:
            # Runs on completion, on streaming errors and on client disconnect:
            # the descriptor is closed and a retired file's deferred deletion
            # (if this was its last lease) happens here.
            self._handle.close()
            self._lease.release()
