import asyncio

import pytest
from nats.js.errors import APIError

from video_analyzer import publisher
from video_analyzer.frames import Frame

FRAME = Frame(index=7, jpeg=b"\xff\xd8")


class FakeJetStream:
    def __init__(self, failures: list[Exception]) -> None:
        self.failures = failures
        self.published: list[tuple[str, bytes, dict[str, str]]] = []

    async def publish(
        self, subject: str, payload: bytes, headers: dict[str, str]
    ) -> None:
        if self.failures:
            raise self.failures.pop(0)
        self.published.append((subject, payload, headers))


@pytest.fixture(autouse=True)
def fast_poll(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(publisher, "MAX_WAIT_SECONDS", 0.03)
    monkeypatch.setattr(publisher, "POLL_SECONDS", 0.01)


def test_retries_while_stream_is_full() -> None:
    js = FakeJetStream([APIError(err_code=publisher.STREAM_FULL)])
    asyncio.run(publisher.publish(js, "v", FRAME))
    assert js.published == [("frames", b"\xff\xd8", {"video_id": "v", "frame_id": "7"})]


def test_gives_up_after_max_wait() -> None:
    js = FakeJetStream([APIError(err_code=publisher.STREAM_FULL)] * 10)
    with pytest.raises(TimeoutError):
        asyncio.run(publisher.publish(js, "v", FRAME))
    assert js.published == []


def test_other_api_errors_propagate() -> None:
    js = FakeJetStream([APIError(err_code=10059)])
    with pytest.raises(APIError):
        asyncio.run(publisher.publish(js, "v", FRAME))
