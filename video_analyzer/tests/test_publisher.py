import fakeredis
import pytest

from video_analyzer import publisher as publisher_module
from video_analyzer.frames import Frame
from video_analyzer.publisher import FramePublisher


def frames(count: int) -> list[Frame]:
    return [Frame(index=i, jpeg=b"\xff\xd8") for i in range(count)]


def test_publishes_every_frame() -> None:
    client = fakeredis.FakeRedis()
    assert (
        FramePublisher(client, "frames", max_backlog=100).publish("v", frames(3)) == 3
    )
    entries = client.xrange("frames")
    assert [f[b"frame_id"] for _, f in entries] == [b"0", b"1", b"2"]
    assert all(f[b"video_id"] == b"v" for _, f in entries)


def test_full_backlog_times_out(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(publisher_module, "MAX_WAIT_SECONDS", 0.02)
    monkeypatch.setattr(publisher_module, "POLL_SECONDS", 0.01)
    client = fakeredis.FakeRedis()
    publisher = FramePublisher(client, "frames", max_backlog=2)
    publisher.publish("v", frames(2))
    with pytest.raises(TimeoutError):
        publisher.publish("v", frames(1))
    assert client.xlen("frames") == 2
