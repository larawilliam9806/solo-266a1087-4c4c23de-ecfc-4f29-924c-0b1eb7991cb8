from __future__ import annotations

import asyncio
import hashlib
import time

import httpx
import pytest

from app.collector import Collector
from app.config import Settings
from app.main import create_app
from mock_source.app import app as source_app, state as source_state
from tests.conftest import free_port, run_server


class Stack:
    def __init__(self, source_url: str, api_url: str):
        self.source_url = source_url
        self.api_url = api_url


async def _wait(client: httpx.AsyncClient, api: str, task_id: str, states: set[str], timeout: float = 60):
    deadline = time.monotonic() + timeout
    view = {}
    while time.monotonic() < deadline:
        view = (await client.get(f"{api}/tasks/{task_id}")).json()
        if view["state"] in states:
            return view
        await __import__("asyncio").sleep(0.05)
    raise AssertionError(f"timeout; last={view}")


@pytest.fixture
async def stack(tmp_path):
    source_state.revisions.clear()
    source_state.payload_kb.clear()
    source_state.delay_seconds = 0.0
    source_state.chunk_delay_seconds = 0.0
    source_state.failing.clear()
    source_state.corrupting.clear()
    source_state.no_range.clear()
    source_state.requests.clear()

    source_port = free_port()
    api_port = free_port()
    settings = Settings(
        source_base_url=f"http://127.0.0.1:{source_port}",
        data_dir=tmp_path / "data",
        http_timeout=15,
        chunk_size=8 * 1024,
        max_source_resyncs=3,
    )
    collector = Collector(settings)
    api_app = create_app(settings, collector)
    async with run_server(source_app, source_port):
        async with run_server(api_app, api_port):
            async with httpx.AsyncClient(timeout=30) as client:
                yield Stack(f"http://127.0.0.1:{source_port}", f"http://127.0.0.1:{api_port}"), client, settings, collector


async def set_delay(stack: Stack, client: httpx.AsyncClient, seconds: float, chunk: float = 0.0):
    await client.post(
        f"{stack.source_url}/admin/delay",
        json={"seconds": seconds, "chunk_seconds": chunk},
    )


async def test_full_flow_download_verified(stack):
    st, client, settings, collector = stack
    source, api = st.source_url, st.api_url
    resp = await client.post(f"{api}/tasks", json={"station": "54511", "date": "2026-11-17"})
    assert resp.status_code == 201
    task_id = resp.json()["id"]
    assert resp.json()["state"] == "queued"

    await client.post(f"{api}/tasks/{task_id}/start")
    view = await _wait(client, api, task_id, {"completed", "failed"})
    assert view["state"] == "completed", view
    assert view["percent"] == 100.0
    assert view["verified_size"] == view["expected_size"]
    assert view["verified_sha256"] == view["expected_sha256"]

    directory = (await client.get(f"{source}/directory/54511/2026-11-17")).json()
    entry = directory["entries"][0]
    package = await client.get(f"{api}/tasks/{task_id}/package")
    assert package.status_code == 200
    assert len(package.content) == entry["size"]
    assert hashlib.sha256(package.content).hexdigest() == entry["sha256"]
    assert package.headers["ETag"] == f'"{entry["sha256"]}"'

    # Part file is gone; exactly one package remains.
    assert not list(settings.parts_dir.glob("*.part"))
    assert len(list(settings.packages_dir.glob("*.tar.gz"))) == 1


async def test_duplicate_station_date_returns_same_task(stack):
    st, client, _s, _c = stack
    api = st.api_url
    first = (await client.post(f"{api}/tasks", json={"station": "A1", "date": "2026-01-02"})).json()
    second = (await client.post(f"{api}/tasks", json={"station": "A1", "date": "2026-01-02"})).json()
    assert first["id"] == second["id"]
    listed = (await client.get(f"{api}/tasks")).json()
    assert len(listed) == 1


async def test_pause_then_late_response_cannot_complete(stack):
    st, client, settings, collector = stack
    api = st.api_url
    await set_delay(st, client, 0.0, 0.05)
    task_id = (
        await client.post(f"{api}/tasks", json={"station": "LATE", "date": "2026-11-01"})
    ).json()["id"]
    await client.post(f"{api}/tasks/{task_id}/start")

    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        view = (await client.get(f"{api}/tasks/{task_id}")).json()
        if view["downloaded_bytes"] > 0:
            break
        await __import__("asyncio").sleep(0.02)
    paused = (await client.post(f"{api}/tasks/{task_id}/pause")).json()
    assert paused["state"] == "paused"
    bytes_at_pause = paused["downloaded_bytes"]
    gen_at_pause = paused["generation"]

    # Give any in-flight response ample time to try to advance the task.
    await __import__("asyncio").sleep(1.0)
    after = (await client.get(f"{api}/tasks/{task_id}")).json()
    assert after["state"] == "paused"
    assert after["generation"] == gen_at_pause
    assert after["downloaded_bytes"] == bytes_at_pause
    assert after["completed_at"] is None

    # Package endpoint must refuse while paused.
    refused = await client.get(f"{api}/tasks/{task_id}/package")
    assert refused.status_code == 409

    await client.post(f"{api}/tasks/{task_id}/resume")
    done = await _wait(client, api, task_id, {"completed", "failed"})
    assert done["state"] == "completed", done
    package = await client.get(f"{api}/tasks/{task_id}/package")
    assert hashlib.sha256(package.content).hexdigest() == done["verified_sha256"]


async def test_resume_after_restart_uses_range_and_keeps_progress(stack):
    st, client, settings, collector = stack
    api = st.api_url
    task_id = (
        await client.post(f"{api}/tasks", json={"station": "REST", "date": "2026-11-02"})
    ).json()["id"]
    await client.post(f"{api}/tasks/{task_id}/start")
    await set_delay(st, client, 0.0, 0.05)
    while True:
        view = (await client.get(f"{api}/tasks/{task_id}")).json()
        if view["downloaded_bytes"] > 100_000:
            break
        await __import__("asyncio").sleep(0.02)
    await client.post(f"{api}/tasks/{task_id}/pause")
    await __import__("asyncio").sleep(0.5)

    parts = list(settings.parts_dir.glob(f"{task_id}-*.part"))
    assert len(parts) == 1
    durable = parts[0].stat().st_size
    assert durable > 0

    # Simulate a process restart: collector shutdown flips running -> paused, and
    # a brand-new Collector instance reads the same SQLite database.
    await collector.shutdown()
    fresh = Collector(settings)
    await fresh.startup()
    reloaded = fresh.store.get(task_id)
    assert reloaded["state"] in {"paused", "queued"}
    assert fresh.store.get(task_id)["downloaded_bytes"] > 0

    await set_delay(st, client, 0.0, 0.0)
    await fresh.start(task_id)
    done = await _wait(client, api, task_id, {"completed", "failed"})
    assert done["state"] == "completed", done

    events = [e["event"] for e in (await client.get(f"{api}/tasks/{task_id}/events")).json()]
    assert "resume" in events
    assert hashlib.sha256((await client.get(f"{api}/tasks/{task_id}/package")).content).hexdigest() == done["verified_sha256"]
    await fresh.shutdown()


async def test_revision_change_discards_old_part_and_recollects(stack):
    st, client, settings, collector = stack
    source, api = st.source_url, st.api_url
    station, date = "REV", "2026-11-03"
    task_id = (await client.post(f"{api}/tasks", json={"station": station, "date": date})).json()["id"]
    await client.post(f"{api}/tasks/{task_id}/start")
    await set_delay(st, client, 0.0, 0.06)
    while True:
        view = (await client.get(f"{api}/tasks/{task_id}")).json()
        if view["downloaded_bytes"] > 200_000:
            break
        await __import__("asyncio").sleep(0.02)
    await client.post(f"{api}/tasks/{task_id}/pause")
    await __import__("asyncio").sleep(0.4)
    old_parts = list(settings.parts_dir.glob(f"{task_id}-*.part"))
    assert old_parts and old_parts[0].stat().st_size > 0
    old_sha = view["expected_sha256"]
    old_bytes = view["downloaded_bytes"]

    await client.post(
        f"{source}/admin/revision", json={"station": station, "date": date, "revision": "v2"}
    )
    await set_delay(st, client, 0.0, 0.0)
    await client.post(f"{api}/tasks/{task_id}/resume")
    done = await _wait(client, api, task_id, {"completed", "failed"})
    assert done["state"] == "completed", done
    assert done["verified_sha256"] != old_sha
    assert done["source_revision"] == "v2"
    # No bytes from the old revision survive.
    assert not any(p.stat().st_size >= old_bytes for p in old_parts if p.exists())
    entry = (await client.get(f"{source}/directory/{station}/{date}")).json()["entries"][0]
    package = await client.get(f"{api}/tasks/{task_id}/package")
    assert len(package.content) == entry["size"]
    assert hashlib.sha256(package.content).hexdigest() == entry["sha256"]
    events = [e["event"] for e in (await client.get(f"{api}/tasks/{task_id}/events")).json()]
    assert "source_changed" in events or "resume" in events
    assert done["resyncs"] >= 0


async def test_corrupt_source_fails_and_never_exposes_package(stack):
    st, client, settings, _c = stack
    source, api = st.source_url, st.api_url
    station, date = "BAD", "2026-11-04"
    await client.post(f"{source}/admin/fault", json={"station": station, "date": date, "mode": "corrupt"})
    task_id = (await client.post(f"{api}/tasks", json={"station": station, "date": date})).json()["id"]
    await client.post(f"{api}/tasks/{task_id}/start")
    done = await _wait(client, api, task_id, {"completed", "failed"}, timeout=60)
    assert done["state"] == "failed", done
    assert done["error"]
    assert done["verified_sha256"] is None
    refused = await client.get(f"{api}/tasks/{task_id}/package")
    assert refused.status_code == 409
    assert not list(settings.packages_dir.glob("*.tar.gz"))


async def test_source_outage_fails_with_reason_and_can_retry(stack):
    st, client, settings, _c = stack
    source, api = st.source_url, st.api_url
    station, date = "OUT", "2026-11-05"
    await client.post(f"{source}/admin/fault", json={"station": station, "date": date, "mode": "fail"})
    task_id = (await client.post(f"{api}/tasks", json={"station": station, "date": date})).json()["id"]
    await client.post(f"{api}/tasks/{task_id}/start")
    failed = await _wait(client, api, task_id, {"failed", "completed"})
    assert failed["state"] == "failed"
    assert "503" in (failed["error"] or "")

    await client.post(f"{source}/admin/fault", json={"station": station, "date": date, "mode": "clear"})
    await client.post(f"{api}/tasks/{task_id}/start")
    done = await _wait(client, api, task_id, {"completed", "failed"})
    assert done["state"] == "completed", done
    assert done["error"] is None


async def test_refresh_replaces_package_when_source_updates(stack):
    st, client, settings, _c = stack
    source, api = st.source_url, st.api_url
    station, date = "UPD", "2026-11-06"
    task_id = (await client.post(f"{api}/tasks", json={"station": station, "date": date})).json()["id"]
    await client.post(f"{api}/tasks/{task_id}/start")
    first = await _wait(client, api, task_id, {"completed", "failed"})
    assert first["state"] == "completed"
    first_pkg = await client.get(f"{api}/tasks/{task_id}/package")
    first_sha = hashlib.sha256(first_pkg.content).hexdigest()

    await client.post(f"{source}/admin/revision", json={"station": station, "date": date, "revision": "v2"})
    refreshed = (await client.post(f"{api}/tasks/{task_id}/refresh")).json()
    assert refreshed["state"] in {"running", "completed"}
    second = await _wait(client, api, task_id, {"completed", "failed"})
    assert second["state"] == "completed", second
    second_pkg = await client.get(f"{api}/tasks/{task_id}/package")
    second_sha = hashlib.sha256(second_pkg.content).hexdigest()
    assert second_sha != first_sha
    assert second_sha == second["verified_sha256"]
    assert len(list(settings.packages_dir.glob("*.tar.gz"))) == 1

    await client.post(f"{source}/admin/revision", json={"station": station, "date": date, "revision": "v3"})
    await client.post(f"{api}/tasks/{task_id}/refresh")
    third = await _wait(client, api, task_id, {"completed", "failed"})
    assert third["verified_sha256"] != second["verified_sha256"]


async def test_invalid_inputs_are_rejected(stack):
    st, client, _s, _c = stack
    api = st.api_url
    bad = await client.post(f"{api}/tasks", json={"station": "../etc", "date": "2026-11-17"})
    assert bad.status_code == 422
    bad_date = await client.post(f"{api}/tasks", json={"station": "OK", "date": "17-11-2026"})
    assert bad_date.status_code == 422
    missing = await client.get(f"{api}/tasks/nope")
    assert missing.status_code == 404


async def test_pause_before_start_is_stable(stack):
    st, client, _s, _c = stack
    api = st.api_url
    task_id = (await client.post(f"{api}/tasks", json={"station": "Q", "date": "2026-11-07"})).json()["id"]
    paused = (await client.post(f"{api}/tasks/{task_id}/pause")).json()
    assert paused["state"] == "paused"
    await __import__("asyncio").sleep(0.3)
    still = (await client.get(f"{api}/tasks/{task_id}")).json()
    assert still["state"] == "paused"
    assert still["downloaded_bytes"] == 0
    assert still["completed_at"] is None


async def test_resume_actually_sends_range_and_if_range(stack):
    st, client, settings, _c = stack
    source, api = st.source_url, st.api_url
    station, date = "RANGE", "2026-11-08"
    await set_delay(st, client, 0.0, 0.05)
    task_id = (await client.post(f"{api}/tasks", json={"station": station, "date": date})).json()["id"]
    await client.post(f"{api}/tasks/{task_id}/start")
    while True:
        view = (await client.get(f"{api}/tasks/{task_id}")).json()
        if view["downloaded_bytes"] > 100_000:
            break
        await __import__("asyncio").sleep(0.02)
    paused = (await client.post(f"{api}/tasks/{task_id}/pause")).json()
    assert paused["state"] == "paused"
    await __import__("asyncio").sleep(0.4)

    await set_delay(st, client, 0.0, 0.0)
    await client.post(f"{api}/tasks/{task_id}/resume")
    done = await _wait(client, api, task_id, {"completed", "failed"})
    assert done["state"] == "completed", done

    state_info = (await client.get(f"{source}/admin/state")).json()
    ranged = [r for r in state_info["recent_requests"] if r["range"]]
    assert ranged, state_info
    assert all(r["if_range"] for r in ranged)
    assert all(r["range"].startswith("bytes=") for r in ranged)
    assert not any(r["range"] == "bytes=0-" for r in ranged)


async def test_retry_after_timeout_resumes_from_stored_progress(stack, monkeypatch):
    st, client, settings, collector = stack
    source, api = st.source_url, st.api_url
    station, date = "TIMEOUT", "2026-11-09"
    await set_delay(st, client, 0.0, 0.08)
    task_id = (await client.post(f"{api}/tasks", json={"station": station, "date": date})).json()["id"]

    # Force the first transfer to fail with a read timeout after some bytes.
    real_stream = httpx.AsyncClient.stream

    calls = {"n": 0}

    def flaky_stream(self, method, url, **kwargs):
        calls["n"] += 1
        ctx = real_stream(self, method, url, **kwargs)
        if calls["n"] == 1:
            class Exploding:
                async def __aenter__(inner):
                    response = await ctx.__aenter__()

                    class Wrap:
                        status_code = response.status_code
                        headers = response.headers

                        async def aiter_bytes(inner2, size=0):
                            count = 0
                            async for chunk in response.aiter_bytes(size):
                                yield chunk
                                count += 1
                                if count >= 3:
                                    raise httpx.ReadTimeout("simulated timeout")
                        async def __aenter__(inner2):
                            return inner2
                        async def __aexit__(inner2, *a):
                            return False
                    return Wrap()

                async def __aexit__(inner, *a):
                    await ctx.__aexit__(*a)
            return Exploding()
        return ctx

    monkeypatch.setattr(httpx.AsyncClient, "stream", flaky_stream)
    await client.post(f"{api}/tasks/{task_id}/start")
    failed = await _wait(client, api, task_id, {"failed", "completed"}, timeout=60)
    monkeypatch.setattr(httpx.AsyncClient, "stream", real_stream)
    assert failed["state"] == "failed", failed
    assert "ReadTimeout" in (failed["error"] or "")
    assert failed["downloaded_bytes"] > 0
    assert (await client.get(f"{api}/tasks/{task_id}/package")).status_code == 409

    await set_delay(st, client, 0.0, 0.0)
    await client.post(f"{api}/tasks/{task_id}/start")
    done = await _wait(client, api, task_id, {"completed", "failed"})
    assert done["state"] == "completed", done
    package = await client.get(f"{api}/tasks/{task_id}/package")
    assert hashlib.sha256(package.content).hexdigest() == done["verified_sha256"]

    state_info = (await client.get(f"{source}/admin/state")).json()
    assert any(r["range"] and r["range"] != "bytes=0-" for r in state_info["recent_requests"])


async def test_server_ignoring_range_restarts_cleanly(stack):
    st, client, settings, _c = stack
    source, api = st.source_url, st.api_url
    station, date = "NORNG", "2026-11-10"
    await set_delay(st, client, 0.0, 0.05)
    task_id = (await client.post(f"{api}/tasks", json={"station": station, "date": date})).json()["id"]
    await client.post(f"{api}/tasks/{task_id}/start")
    while True:
        view = (await client.get(f"{api}/tasks/{task_id}")).json()
        if view["downloaded_bytes"] > 100_000:
            break
        await __import__("asyncio").sleep(0.02)
    await client.post(f"{api}/tasks/{task_id}/pause")
    await __import__("asyncio").sleep(0.4)

    # Source now ignores Range (still strong ETag): the collector must truncate
    # and re-download rather than append or fail.
    await client.post(f"{source}/admin/fault", json={"station": station, "date": date, "mode": "no_range"})
    await set_delay(st, client, 0.0, 0.0)
    await client.post(f"{api}/tasks/{task_id}/resume")
    done = await _wait(client, api, task_id, {"completed", "failed"})
    assert done["state"] == "completed", done
    package = await client.get(f"{api}/tasks/{task_id}/package")
    assert hashlib.sha256(package.content).hexdigest() == done["verified_sha256"]
    events = [e["event"] for e in (await client.get(f"{api}/tasks/{task_id}/events")).json()]
    assert "range_ignored" in events


async def test_pause_before_late_response_arrives_cannot_progress(stack):
    st, client, settings, _c = stack
    api = st.api_url
    station, date = "SLOW", "2026-11-11"
    # Headers (and body) are delayed 2s, so the response arrives well after the
    # pause call has already returned.
    await set_delay(st, client, 2.0, 0.0)
    task_id = (await client.post(f"{api}/tasks", json={"station": station, "date": date})).json()["id"]
    await client.post(f"{api}/tasks/{task_id}/start")
    await __import__("asyncio").sleep(0.4)
    paused = (await client.post(f"{api}/tasks/{task_id}/pause")).json()
    assert paused["state"] == "paused"
    assert paused["downloaded_bytes"] == 0
    gen = paused["generation"]

    # Let the delayed response arrive and be drained by the worker.
    await __import__("asyncio").sleep(3.0)
    after = (await client.get(f"{api}/tasks/{task_id}")).json()
    assert after["state"] == "paused"
    assert after["generation"] == gen
    assert after["downloaded_bytes"] == 0
    assert after["completed_at"] is None
    assert after["verified_sha256"] is None
    assert (await client.get(f"{api}/tasks/{task_id}/package")).status_code == 409
    assert not list(settings.packages_dir.glob("*.tar.gz"))

    await set_delay(st, client, 0.0, 0.0)
    await client.post(f"{api}/tasks/{task_id}/resume")
    done = await _wait(client, api, task_id, {"completed", "failed"})
    assert done["state"] == "completed", done
    package = await client.get(f"{api}/tasks/{task_id}/package")
    assert hashlib.sha256(package.content).hexdigest() == done["verified_sha256"]


# ---------------------------------------------------------------------------
# Download/refresh race: an accepted download must deliver the package it
# selected, no matter how a concurrent refresh ends; superseded packages must
# be released once no download holds them.
# ---------------------------------------------------------------------------

async def _collect_completed(client: httpx.AsyncClient, api: str, task_id: str) -> dict:
    await client.post(f"{api}/tasks/{task_id}/start")
    view = await _wait(client, api, task_id, {"completed", "failed"}, timeout=120)
    assert view["state"] == "completed", view
    return view


async def _wait_file_gone(path, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while path.exists() and time.monotonic() < deadline:
        await asyncio.sleep(0.05)
    assert not path.exists(), f"{path} was not released"


async def test_download_in_flight_survives_successful_refresh(stack):
    st, client, settings, collector = stack
    source, api = st.source_url, st.api_url
    station, date = "RACE", "2026-11-12"
    # 16 MiB: larger than any socket/transport buffer, so the response is
    # still streaming (lease held) when the refresh completes.
    await client.post(
        f"{source}/admin/size",
        json={"station": station, "date": date, "payload_kb": 16 * 1024},
    )
    task_id = (
        await client.post(f"{api}/tasks", json={"station": station, "date": date})
    ).json()["id"]
    first = await _collect_completed(client, api, task_id)
    old_sha, old_size = first["verified_sha256"], first["verified_size"]
    old_file = settings.packages_dir / f"{station}-{date}-{old_sha[:12]}.tar.gz"
    assert old_file.exists()

    async with client.stream("GET", f"{api}/tasks/{task_id}/package") as resp:
        assert resp.status_code == 200
        assert resp.headers["content-length"] == str(old_size)
        assert resp.headers["digest"] == f"sha-256={old_sha}"
        chunks = resp.aiter_bytes(64 * 1024)
        head = await chunks.__anext__()
        assert len(head) == 64 * 1024

        # While this download is in flight, the source moves and a refresh
        # replaces the package.
        await client.post(
            f"{source}/admin/revision",
            json={"station": station, "date": date, "revision": "v2"},
        )
        await client.post(f"{api}/tasks/{task_id}/refresh")
        # A new download requested mid-refresh still obeys the task state.
        mid = await client.get(f"{api}/tasks/{task_id}/package")
        assert mid.status_code == 409
        done = await _wait(client, api, task_id, {"completed", "failed"}, timeout=120)
        assert done["state"] == "completed", done
        assert done["verified_sha256"] != old_sha

        # The refresh finished, yet the old file must NOT have been deleted
        # under the accepted download: its lease defers the deletion.
        assert old_file.exists(), "old package deleted while a download held it"
        assert collector.packages.pinned().get(str(old_file), 0) >= 1

        # A download accepted after the refresh serves the NEW package.
        new_pkg = await client.get(f"{api}/tasks/{task_id}/package")
        assert new_pkg.status_code == 200
        assert hashlib.sha256(new_pkg.content).hexdigest() == done["verified_sha256"]

        # Finish the in-flight download: it must deliver the ORIGINAL package,
        # exactly matching the length and digest promised in its headers.
        hasher = hashlib.sha256(head)
        total = len(head)
        async for chunk in chunks:
            hasher.update(chunk)
            total += len(chunk)
    assert total == old_size, (total, old_size)
    assert hasher.hexdigest() == old_sha

    # The last lease is gone: the deferred deletion of the old package runs.
    await _wait_file_gone(old_file)
    assert not collector.packages.pinned()
    remaining = list(settings.packages_dir.glob("*.tar.gz"))
    assert len(remaining) == 1
    assert remaining[0].name.endswith(f"{done['verified_sha256'][:12]}.tar.gz")


async def test_download_in_flight_survives_failed_refresh(stack):
    st, client, settings, collector = stack
    source, api = st.source_url, st.api_url
    station, date = "RACEF", "2026-11-13"
    await client.post(
        f"{source}/admin/size",
        json={"station": station, "date": date, "payload_kb": 4 * 1024},
    )
    task_id = (
        await client.post(f"{api}/tasks", json={"station": station, "date": date})
    ).json()["id"]
    first = await _collect_completed(client, api, task_id)
    old_sha, old_size = first["verified_sha256"], first["verified_size"]
    old_file = settings.packages_dir / f"{station}-{date}-{old_sha[:12]}.tar.gz"
    assert old_file.exists()

    async with client.stream("GET", f"{api}/tasks/{task_id}/package") as resp:
        assert resp.status_code == 200
        chunks = resp.aiter_bytes(64 * 1024)
        head = await chunks.__anext__()

        # The source publishes v2 but serves corrupt bytes for it: the refresh
        # fails terminally and retires the old package, yet this accepted
        # download must still complete intact.
        await client.post(
            f"{source}/admin/revision",
            json={"station": station, "date": date, "revision": "v2"},
        )
        await client.post(
            f"{source}/admin/fault",
            json={"station": station, "date": date, "mode": "corrupt"},
        )
        await client.post(f"{api}/tasks/{task_id}/refresh")
        done = await _wait(client, api, task_id, {"failed"}, timeout=120)
        assert done["state"] == "failed", done

        hasher = hashlib.sha256(head)
        total = len(head)
        async for chunk in chunks:
            hasher.update(chunk)
            total += len(chunk)
    assert total == old_size, (total, old_size)
    assert hasher.hexdigest() == old_sha

    # The failed refresh retired the old package; after the download ended the
    # file is gone and new downloads are refused (task is failed).
    await _wait_file_gone(old_file)
    refused = await client.get(f"{api}/tasks/{task_id}/package")
    assert refused.status_code == 409


async def test_interleaved_downloads_and_disconnect_release_old_package(stack):
    st, client, settings, collector = stack
    source, api = st.source_url, st.api_url
    station, date = "RACEX", "2026-11-14"
    await client.post(
        f"{source}/admin/size",
        json={"station": station, "date": date, "payload_kb": 16 * 1024},
    )
    task_id = (
        await client.post(f"{api}/tasks", json={"station": station, "date": date})
    ).json()["id"]
    first = await _collect_completed(client, api, task_id)
    old_sha, old_size = first["verified_sha256"], first["verified_size"]
    old_file = settings.packages_dir / f"{station}-{date}-{old_sha[:12]}.tar.gz"

    url = f"{api}/tasks/{task_id}/package"
    dl1 = client.stream("GET", url)
    dl2 = client.stream("GET", url)
    resp1 = await dl1.__aenter__()
    resp2 = await dl2.__aenter__()
    try:
        assert resp1.status_code == 200 and resp2.status_code == 200
        it1 = resp1.aiter_bytes(64 * 1024)
        it2 = resp2.aiter_bytes(64 * 1024)
        head1 = await it1.__anext__()
        head2 = await it2.__anext__()
        assert len(head1) == len(head2) == 64 * 1024

        # Refresh to v2 while both downloads are in flight.
        await client.post(
            f"{source}/admin/revision",
            json={"station": station, "date": date, "revision": "v2"},
        )
        await client.post(f"{api}/tasks/{task_id}/refresh")
        done = await _wait(client, api, task_id, {"completed", "failed"}, timeout=120)
        assert done["state"] == "completed", done

        # Two leases pin the old package.
        assert old_file.exists()
        assert collector.packages.pinned().get(str(old_file)) == 2

        # Client 1 disconnects mid-stream: one lease remains, the file stays.
        await dl1.__aexit__(None, None, None)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if collector.packages.pinned().get(str(old_file), 0) <= 1:
                break
            await asyncio.sleep(0.05)
        assert collector.packages.pinned().get(str(old_file), 0) == 1
        assert old_file.exists(), "old package deleted while a download still held it"

        # Client 2 reads to the end: full original body, then the deferred
        # deletion finally runs.
        hasher = hashlib.sha256(head2)
        total = len(head2)
        async for chunk in it2:
            hasher.update(chunk)
            total += len(chunk)
        assert total == old_size
        assert hasher.hexdigest() == old_sha
    finally:
        await dl1.__aexit__(None, None, None)
        await dl2.__aexit__(None, None, None)

    await _wait_file_gone(old_file)
    assert not collector.packages.pinned()


async def test_package_response_streams_even_if_file_vanishes(stack):
    """Defense in depth: the response serves an already-open descriptor.

    Deletes the package file in the exact window where the old implementation
    broke (right after the 200 status line went out) by driving the real ASGI
    app with a hooked ``send``.
    """
    st, client, settings, collector = stack
    api = st.api_url
    station, date = "GONE", "2026-11-15"
    task_id = (
        await client.post(f"{api}/tasks", json={"station": station, "date": date})
    ).json()["id"]
    done = await _collect_completed(client, api, task_id)
    sha, size = done["verified_sha256"], done["verified_size"]
    (pkg,) = list(settings.packages_dir.glob("*.tar.gz"))

    direct_app = create_app(settings, collector)
    messages = []

    async def send(message):
        messages.append(message)
        if message["type"] == "http.response.start":
            pkg.unlink()  # vanish right after the 200 was sent

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": f"/tasks/{task_id}/package",
        "raw_path": f"/tasks/{task_id}/package".encode(),
        "query_string": b"",
        "root_path": "",
        "headers": [],
        "client": ("127.0.0.1", 12345),
        "server": ("127.0.0.1", 80),
    }
    await direct_app(scope, receive, send)
    start = next(m for m in messages if m["type"] == "http.response.start")
    assert start["status"] == 200
    body = b"".join(
        m.get("body", b"") for m in messages if m["type"] == "http.response.body"
    )
    assert len(body) == size
    assert hashlib.sha256(body).hexdigest() == sha


async def test_startup_sweeps_orphan_packages(stack):
    st, client, settings, collector = stack
    api = st.api_url
    task_id = (
        await client.post(f"{api}/tasks", json={"station": "SWEEP", "date": "2026-11-16"})
    ).json()["id"]
    await _collect_completed(client, api, task_id)
    (current,) = list(settings.packages_dir.glob("*.tar.gz"))
    orphan = settings.packages_dir / "ORPH-2020-01-01-deadbeefdead.tar.gz"
    orphan.write_bytes(b"stale")
    temp = settings.packages_dir / ".publish-abandoned"
    temp.write_bytes(b"half")

    # A fresh collector over the same data dir sweeps orphans at startup.
    fresh = Collector(settings)
    await fresh.startup()
    try:
        assert current.exists(), "referenced package must survive the sweep"
        assert not orphan.exists(), "unreferenced package must be swept"
        assert not temp.exists(), "abandoned publish temp must be swept"
    finally:
        await fresh.shutdown()
