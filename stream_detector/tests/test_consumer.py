import cv2
import fakeredis
import numpy as np
import pytest

from stream_detector import main
from stream_detector.detector import StreamFaceDetector
from stream_detector.detector_response_handling import RespObject

STREAM = "frames"
GROUP = "detectors"


@pytest.fixture
def client() -> fakeredis.FakeRedis:
    return fakeredis.FakeRedis()


@pytest.fixture
def sent(monkeypatch: pytest.MonkeyPatch) -> list[list[RespObject]]:
    batches: list[list[RespObject]] = []
    monkeypatch.setattr(main, "send_results_next_service", batches.append)
    return batches


def make_consumer(
    client: fakeredis.FakeRedis, name: str = "worker-1"
) -> main.FrameConsumer:
    consumer = main.FrameConsumer(
        client, StreamFaceDetector(), STREAM, GROUP, name, batch_size=8, block_ms=1
    )
    consumer.ensure_group()
    return consumer


def jpeg_bytes() -> bytes:
    ok, buffer = cv2.imencode(".jpg", np.zeros((48, 64, 3), dtype=np.uint8))
    assert ok
    return buffer.tobytes()


def test_detects_and_acks_batch(
    client: fakeredis.FakeRedis, sent: list[list[RespObject]]
) -> None:
    consumer = make_consumer(client)
    for frame_id in (0, 12, 25):
        client.xadd(
            STREAM, {"video_id": "clip", "frame_id": frame_id, "jpeg": jpeg_bytes()}
        )

    assert consumer.process_once() == 3

    assert len(sent) == 1
    assert [r.frame_id for r in sent[0]] == [0, 12, 25]
    assert all(r.video_id == "clip" and len(r.faces) == 2 for r in sent[0])
    assert client.xlen(STREAM) == 0
    assert client.xpending(STREAM, GROUP)["pending"] == 0


def test_malformed_entries_are_dropped(
    client: fakeredis.FakeRedis, sent: list[list[RespObject]]
) -> None:
    consumer = make_consumer(client)
    client.xadd(STREAM, {"video_id": "clip", "frame_id": 0, "jpeg": b"garbage"})
    client.xadd(STREAM, {"video_id": "clip", "frame_id": "x", "jpeg": jpeg_bytes()})
    client.xadd(STREAM, {"video_id": "clip", "frame_id": 1, "jpeg": jpeg_bytes()})

    assert consumer.process_once() == 3

    assert [r.frame_id for r in sent[0]] == [1]
    assert client.xlen(STREAM) == 0


def test_idle_stream_returns_zero(
    client: fakeredis.FakeRedis, sent: list[list[RespObject]]
) -> None:
    assert make_consumer(client).process_once() == 0
    assert sent == []


def test_ensure_group_is_idempotent(client: fakeredis.FakeRedis) -> None:
    make_consumer(client)
    make_consumer(client, name="worker-2")


def test_stale_pending_entries_are_reclaimed(
    client: fakeredis.FakeRedis, sent: list[list[RespObject]]
) -> None:
    crashed = make_consumer(client, name="crashed")
    client.xadd(STREAM, {"video_id": "clip", "frame_id": 7, "jpeg": jpeg_bytes()})
    crashed._read_new()  # delivered but never acked, as if the worker died mid-batch
    assert client.xpending(STREAM, GROUP)["pending"] == 1

    survivor = main.FrameConsumer(
        client,
        StreamFaceDetector(),
        STREAM,
        GROUP,
        "survivor",
        block_ms=1,
        claim_idle_ms=0,
    )
    assert survivor.process_once() == 1

    assert [r.frame_id for r in sent[0]] == [7]
    assert client.xpending(STREAM, GROUP)["pending"] == 0
    assert client.xlen(STREAM) == 0
