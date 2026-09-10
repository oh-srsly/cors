import asyncio
from unittest.mock import AsyncMock, Mock

import pytest
from nats.js.errors import APIError

from video_analyzer import publisher
from video_analyzer.frames import Frame

FRAME = Frame(index=7, jpeg=b"\xff\xd8")
FULL = APIError(err_code=publisher.STREAM_FULL)


@pytest.fixture(autouse=True)
def fast_poll(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(publisher, "MAX_WAIT_SECONDS", 0.03)
    monkeypatch.setattr(publisher, "POLL_SECONDS", 0.01)


def test_retries_while_stream_is_full() -> None:
    js = Mock(publish=AsyncMock(side_effect=[FULL, None]))
    asyncio.run(publisher.publish(js, "v", FRAME))
    js.publish.assert_awaited_with(
        "frames", b"\xff\xd8", headers={"video_id": "v", "frame_id": "7"}
    )


def test_gives_up_after_max_wait() -> None:
    js = Mock(publish=AsyncMock(side_effect=FULL))
    with pytest.raises(TimeoutError):
        asyncio.run(publisher.publish(js, "v", FRAME))


def test_other_api_errors_propagate() -> None:
    js = Mock(publish=AsyncMock(side_effect=APIError(err_code=10059)))
    with pytest.raises(APIError):
        asyncio.run(publisher.publish(js, "v", FRAME))
