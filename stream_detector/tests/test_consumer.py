import cv2
import fakeredis
import numpy as np
import pytest

from stream_detector import main
from stream_detector.detector import BoundingBox, StreamFaceDetector
from stream_detector.detector_response_handling import RespObject
from stream_detector.main import GROUP, STREAM


class FailingDetector(StreamFaceDetector):
    def detect_faces(self, frame: np.ndarray) -> list[BoundingBox]:
        raise RuntimeError("model exploded")


@pytest.fixture
def client() -> fakeredis.FakeRedis:
    return fakeredis.FakeRedis()


@pytest.fixture
def sent(monkeypatch: pytest.MonkeyPatch) -> list[RespObject]:
    results: list[RespObject] = []
    monkeypatch.setattr(main, "send_results_next_service", results.extend)
    return results


@pytest.fixture(autouse=True)
def fast_block(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(main, "BLOCK_MS", 1)


def make_consumer(
    client: fakeredis.FakeRedis,
    name: str = "worker-1",
    detector: StreamFaceDetector | None = None,
) -> main.FrameConsumer:
    consumer = main.FrameConsumer(client, detector or StreamFaceDetector(), name)
    consumer.ensure_group()
    return consumer


def jpeg_bytes() -> bytes:
    ok, buffer = cv2.imencode(".jpg", np.zeros((48, 64, 3), dtype=np.uint8))
    assert ok
    return buffer.tobytes()


def add_frame(
    client: fakeredis.FakeRedis, frame_id: object, jpeg: bytes | None = None
) -> None:
    payload = jpeg_bytes() if jpeg is None else jpeg
    client.xadd(STREAM, {"video_id": "clip", "frame_id": frame_id, "jpeg": payload})


def drain(consumer: main.FrameConsumer) -> int:
    processed = 0
    while (n := consumer.process_once()) > 0:
        processed += n
    return processed


def test_detects_and_acks_each_entry(
    client: fakeredis.FakeRedis, sent: list[RespObject]
) -> None:
    consumer = make_consumer(client)
    for frame_id in (0, 12, 25):
        add_frame(client, frame_id)

    assert consumer.process_once() == 1
    assert client.xpending(STREAM, GROUP)["pending"] == 0
    assert drain(consumer) == 2

    assert [r.frame_id for r in sent] == [0, 12, 25]
    assert all(r.video_id == "clip" and len(r.faces) == 2 for r in sent)
    assert client.xlen(STREAM) == 0


def test_bad_entries_are_dropped_not_fatal(
    client: fakeredis.FakeRedis, sent: list[RespObject]
) -> None:
    consumer = make_consumer(client)
    add_frame(client, 0, jpeg=b"garbage")
    add_frame(client, 1, jpeg=b"")
    add_frame(client, "x")
    add_frame(client, 2)

    assert drain(consumer) == 4

    assert [r.frame_id for r in sent] == [2]
    assert client.xlen(STREAM) == 0


def test_detector_exception_drops_entry(
    client: fakeredis.FakeRedis, sent: list[RespObject]
) -> None:
    consumer = make_consumer(client, detector=FailingDetector())
    add_frame(client, 7)

    assert consumer.process_once() == 1

    assert sent == []
    assert client.xlen(STREAM) == 0
    assert client.xpending(STREAM, GROUP)["pending"] == 0


def test_forwarding_exception_drops_entry(
    client: fakeredis.FakeRedis, monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken_forward(results: list[RespObject]) -> None:
        raise ConnectionError("next service down")

    monkeypatch.setattr(main, "send_results_next_service", broken_forward)
    consumer = make_consumer(client)
    add_frame(client, 7)

    assert consumer.process_once() == 1
    assert client.xpending(STREAM, GROUP)["pending"] == 0


def test_idle_stream_returns_zero(
    client: fakeredis.FakeRedis, sent: list[RespObject]
) -> None:
    assert make_consumer(client).process_once() == 0
    assert sent == []


def test_ensure_group_is_idempotent(client: fakeredis.FakeRedis) -> None:
    make_consumer(client)
    make_consumer(client, name="worker-2")


def test_stale_pending_entries_are_reclaimed(
    client: fakeredis.FakeRedis, sent: list[RespObject], monkeypatch: pytest.MonkeyPatch
) -> None:
    crashed = make_consumer(client, name="crashed")
    add_frame(client, 7)
    crashed._read_new()  # delivered but never acked, as if the worker died mid-frame
    assert client.xpending(STREAM, GROUP)["pending"] == 1

    monkeypatch.setattr(main, "CLAIM_IDLE_MS", 0)
    survivor = make_consumer(client, name="survivor")
    assert survivor.process_once() == 1

    assert [r.frame_id for r in sent] == [7]
    assert client.xpending(STREAM, GROUP)["pending"] == 0
    assert client.xlen(STREAM) == 0


def test_make_detector_rejects_unknown() -> None:
    from stream_detector.detector import make_detector

    with pytest.raises(ValueError):
        make_detector("yolo")
