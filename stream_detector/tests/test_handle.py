import asyncio

import cv2
import numpy as np
import pytest

from stream_detector import main
from stream_detector.detector import BoundingBox, StreamFaceDetector, make_detector
from stream_detector.detector_response_handling import RespObject


class FakeMsg:
    def __init__(self, headers: dict[str, str] | None, data: bytes) -> None:
        self.headers = headers
        self.data = data
        self.outcome: str | None = None

    async def ack(self) -> None:
        self.outcome = "ack"

    async def term(self) -> None:
        self.outcome = "term"


class FailingDetector(StreamFaceDetector):
    def detect_faces(self, frame: np.ndarray) -> list[BoundingBox]:
        raise RuntimeError("model exploded")


def jpeg_bytes() -> bytes:
    ok, buffer = cv2.imencode(".jpg", np.zeros((48, 64, 3), dtype=np.uint8))
    assert ok
    return buffer.tobytes()


def frame_msg(frame_id: str = "12", data: bytes | None = None) -> FakeMsg:
    payload = jpeg_bytes() if data is None else data
    return FakeMsg({"video_id": "clip", "frame_id": frame_id}, payload)


@pytest.fixture
def sent(monkeypatch: pytest.MonkeyPatch) -> list[RespObject]:
    results: list[RespObject] = []
    monkeypatch.setattr(main, "send_results_next_service", results.extend)
    return results


def test_detects_forwards_and_acks(sent: list[RespObject]) -> None:
    msg = frame_msg()
    asyncio.run(main.handle(msg, StreamFaceDetector()))
    assert msg.outcome == "ack"
    assert [(r.video_id, r.frame_id, len(r.faces)) for r in sent] == [("clip", 12, 2)]


@pytest.mark.parametrize(
    "msg",
    [
        frame_msg(data=b"garbage"),
        frame_msg(data=b""),
        frame_msg(frame_id="x"),
        FakeMsg(None, jpeg_bytes()),
        FakeMsg({"video_id": "clip"}, jpeg_bytes()),
    ],
)
def test_bad_frames_are_terminated(sent: list[RespObject], msg: FakeMsg) -> None:
    asyncio.run(main.handle(msg, StreamFaceDetector()))
    assert msg.outcome == "term"
    assert sent == []


def test_detector_exception_terminates(sent: list[RespObject]) -> None:
    msg = frame_msg()
    asyncio.run(main.handle(msg, FailingDetector()))
    assert msg.outcome == "term"
    assert sent == []


def test_forwarding_exception_terminates(monkeypatch: pytest.MonkeyPatch) -> None:
    def broken_forward(results: list[RespObject]) -> None:
        raise ConnectionError("next service down")

    monkeypatch.setattr(main, "send_results_next_service", broken_forward)
    msg = frame_msg()
    asyncio.run(main.handle(msg, StreamFaceDetector()))
    assert msg.outcome == "term"


def test_make_detector_rejects_unknown() -> None:
    with pytest.raises(ValueError):
        make_detector("yolo")
