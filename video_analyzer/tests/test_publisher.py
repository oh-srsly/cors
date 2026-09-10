import fakeredis
import pytest

from video_analyzer import publisher
from video_analyzer.frames import Frame

FRAME = Frame(index=0, jpeg=b"\xff\xd8")


def test_publishes_frame_fields() -> None:
    client = fakeredis.FakeRedis()
    publisher.publish(client, "v", Frame(index=13, jpeg=b"\xff\xd8"))
    [(_, fields)] = client.xrange(publisher.STREAM)
    assert fields == {b"video_id": b"v", b"frame_id": b"13", b"jpeg": b"\xff\xd8"}


def test_full_backlog_times_out(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(publisher, "MAX_BACKLOG", 2)
    monkeypatch.setattr(publisher, "MAX_WAIT_SECONDS", 0.02)
    monkeypatch.setattr(publisher, "POLL_SECONDS", 0.01)
    client = fakeredis.FakeRedis()
    publisher.publish(client, "v", FRAME)
    with pytest.raises(TimeoutError):
        publisher.publish(client, "v", FRAME)
    assert client.xlen(publisher.STREAM) == 2
