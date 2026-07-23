import asyncio
import os
import sys

import aiohttp
import pytest

sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "airflow", "dags", "dependencies"),
)

import import_genome_notes as ign


class FakeResponse:
    def __init__(self, status, *, content_type="application/json", payload=None, text=""):
        self.status = status
        self.content_type = content_type
        self.headers = {}
        self._payload = payload
        self._text = text

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def raise_for_status(self):
        if self.status >= 400:
            raise aiohttp.ClientResponseError(
                request_info=None, history=(), status=self.status
            )

    async def json(self):
        return self._payload

    async def text(self):
        return self._text


class FakeSession:
    """Returns queued responses in order; records how many gets happened."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = 0

    def get(self, url, headers=None):
        self.calls += 1
        return self._responses.pop(0)


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    async def instant(_):
        return None

    monkeypatch.setattr(ign.asyncio, "sleep", instant)


def test_fetch_retries_on_429_then_succeeds():
    session = FakeSession(
        [
            FakeResponse(429),
            FakeResponse(429),
            FakeResponse(200, payload={"ok": True}),
        ]
    )
    result = asyncio.run(ign.fetch(session, "http://x"))
    assert result == {"ok": True}
    assert session.calls == 3


def test_fetch_raises_after_exhausting_retries():
    session = FakeSession([FakeResponse(429) for _ in range(ign.MAX_RETRIES)])
    with pytest.raises(aiohttp.ClientResponseError):
        asyncio.run(ign.fetch(session, "http://x"))
    assert session.calls == ign.MAX_RETRIES


def test_fetch_does_not_retry_permanent_status():
    # 403/404 are permanent: fetch must fail immediately, not burn retries.
    session = FakeSession([FakeResponse(403)])
    with pytest.raises(aiohttp.ClientResponseError):
        asyncio.run(ign.fetch(session, "http://x"))
    assert session.calls == 1


def test_fetch_force_text_ignores_json_content_type():
    # HTML files mislabelled as application/json must be read as text, not parsed.
    session = FakeSession(
        [FakeResponse(200, content_type="application/json", text="<html></html>")]
    )
    result = asyncio.run(ign.fetch(session, "http://x", force_text=True))
    assert result == "<html></html>"


def test_fetch_html_returns_none_on_failure(monkeypatch):
    async def boom(*args, **kwargs):
        raise aiohttp.ClientError("boom")

    monkeypatch.setattr(ign, "fetch", boom)
    result = asyncio.run(ign.fetch_html(object(), "http://x"))
    assert result is None


def test_main_reraises_instead_of_returning_none(monkeypatch):
    async def boom(_session):
        raise RuntimeError("429 Too Many Requests")

    monkeypatch.setattr(ign, "get_auth_token", boom)
    with pytest.raises(RuntimeError, match="429"):
        asyncio.run(ign.main())
