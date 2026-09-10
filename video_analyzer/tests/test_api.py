from collections.abc import Iterable
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from video_analyzer import main
from video_analyzer.frames import Frame


class RecordingPublisher:
    def __init__(self) -> None:
        self.published: list[tuple[str, int]] = []

    def publish(self, video_id: str, frames: Iterable[Frame]) -> int:
        before = len(self.published)
        self.published.extend((video_id, frame.index) for frame in frames)
        return len(self.published) - before


@pytest.fixture
def publisher(monkeypatch: pytest.MonkeyPatch) -> RecordingPublisher:
    recording = RecordingPublisher()
    monkeypatch.setattr(main, "publisher", recording)
    return recording


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> TestClient:
    monkeypatch.setattr(main, "VIDEOS_DIR", tmp_path)
    return TestClient(main.app)


def test_happy_path(
    client: TestClient, publisher: RecordingPublisher, video_path: Path
) -> None:
    response = client.post("/analyze", json={"file_path": video_path.name, "fps": 2})
    assert response.status_code == 200
    body = response.json()
    assert body["video_id"].startswith("clip-")
    assert body["frames_dispatched"] == 4
    assert [i for _, i in publisher.published] == [0, 13, 25, 38]
    assert {v for v, _ in publisher.published} == {body["video_id"]}


def test_each_request_gets_its_own_video_id(
    client: TestClient, publisher: RecordingPublisher, video_path: Path
) -> None:
    body = {"file_path": video_path.name, "fps": 2}
    first = client.post("/analyze", json=body).json()["video_id"]
    second = client.post("/analyze", json=body).json()["video_id"]
    assert first != second


@pytest.mark.parametrize("fps", [1, 3, 30, "2", 2.0, True, None])
def test_rejects_fps_other_than_2_or_4(
    client: TestClient, publisher: RecordingPublisher, fps: object
) -> None:
    response = client.post("/analyze", json={"file_path": "clip.avi", "fps": fps})
    assert response.status_code == 422
    assert publisher.published == []


def test_rejects_unknown_fields(
    client: TestClient, publisher: RecordingPublisher
) -> None:
    response = client.post(
        "/analyze", json={"file_path": "clip.avi", "fps": 2, "extra": 1}
    )
    assert response.status_code == 422


def test_missing_video_is_404(
    client: TestClient, publisher: RecordingPublisher
) -> None:
    response = client.post("/analyze", json={"file_path": "nope.mp4", "fps": 2})
    assert response.status_code == 404


@pytest.mark.parametrize("file_path", ["../../etc/passwd", "a\x00b", "x" * 5000])
def test_bad_paths_are_400(
    client: TestClient, publisher: RecordingPublisher, file_path: str
) -> None:
    response = client.post("/analyze", json={"file_path": file_path, "fps": 2})
    assert response.status_code == 400
    assert publisher.published == []


def test_unreadable_video_is_400(
    client: TestClient, publisher: RecordingPublisher, tmp_path: Path
) -> None:
    (tmp_path / "bogus.mp4").write_bytes(b"not a video")
    response = client.post("/analyze", json={"file_path": "bogus.mp4", "fps": 4})
    assert response.status_code == 400
