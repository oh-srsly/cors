from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from video_analyzer import main
from video_analyzer.frames import Frame


@pytest.fixture
def published(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, int]]:
    records: list[tuple[str, int]] = []

    async def fake_publish(js: object, video_id: str, frame: Frame) -> None:
        records.append((video_id, frame.index))

    monkeypatch.setattr(main, "publish", fake_publish)
    return records


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> TestClient:
    monkeypatch.setattr(main, "VIDEOS_DIR", tmp_path)
    main.app.state.js = None
    return TestClient(main.app)


def test_happy_path(
    client: TestClient, published: list[tuple[str, int]], video_path: Path
) -> None:
    response = client.post("/analyze", json={"file_path": video_path.name, "fps": 2})
    assert response.status_code == 200
    body = response.json()
    assert body["video_id"].startswith("clip-")
    assert body["frames_dispatched"] == 4
    assert [i for _, i in published] == [0, 13, 25, 38]
    assert {v for v, _ in published} == {body["video_id"]}


def test_each_request_gets_its_own_video_id(
    client: TestClient, published: list[tuple[str, int]], video_path: Path
) -> None:
    body = {"file_path": video_path.name, "fps": 2}
    first = client.post("/analyze", json=body).json()["video_id"]
    second = client.post("/analyze", json=body).json()["video_id"]
    assert first != second


def test_full_stream_reports_partial_dispatch(
    client: TestClient, monkeypatch: pytest.MonkeyPatch, video_path: Path
) -> None:
    calls = 0

    async def stuck_publish(js: object, video_id: str, frame: Frame) -> None:
        nonlocal calls
        calls += 1
        if calls > 2:
            raise TimeoutError("stream stayed full")

    monkeypatch.setattr(main, "publish", stuck_publish)
    response = client.post("/analyze", json={"file_path": video_path.name, "fps": 2})
    assert response.status_code == 503
    assert "2 frames already dispatched as clip-" in response.json()["detail"]


@pytest.mark.parametrize("fps", [1, 3, 30, "2", 2.0, True, None])
def test_rejects_fps_other_than_2_or_4(
    client: TestClient, published: list[tuple[str, int]], fps: object
) -> None:
    response = client.post("/analyze", json={"file_path": "clip.avi", "fps": fps})
    assert response.status_code == 422
    assert published == []


def test_rejects_unknown_fields(
    client: TestClient, published: list[tuple[str, int]]
) -> None:
    response = client.post(
        "/analyze", json={"file_path": "clip.avi", "fps": 2, "extra": 1}
    )
    assert response.status_code == 422


def test_missing_video_is_404(
    client: TestClient, published: list[tuple[str, int]]
) -> None:
    response = client.post("/analyze", json={"file_path": "nope.mp4", "fps": 2})
    assert response.status_code == 404


@pytest.mark.parametrize("file_path", ["../../etc/passwd", "a\x00b", "x" * 5000])
def test_bad_paths_are_400(
    client: TestClient, published: list[tuple[str, int]], file_path: str
) -> None:
    response = client.post("/analyze", json={"file_path": file_path, "fps": 2})
    assert response.status_code == 400
    assert published == []


def test_unreadable_video_is_400(
    client: TestClient, published: list[tuple[str, int]], tmp_path: Path
) -> None:
    (tmp_path / "bogus.mp4").write_bytes(b"not a video")
    response = client.post("/analyze", json={"file_path": "bogus.mp4", "fps": 4})
    assert response.status_code == 400


def test_metrics_exposed(client: TestClient) -> None:
    response = client.get("/metrics")
    assert response.status_code == 200
    assert "frames_dispatched_total" in response.text


def test_startup_updates_stream_when_config_changed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from nats.js.errors import BadRequestError

    class FakeJetStream:
        updated: list[object] = []

        async def add_stream(self, config: object) -> None:
            raise BadRequestError(err_code=10058)

        async def update_stream(self, config: object) -> None:
            self.updated.append(config)

    class FakeConnection:
        def jetstream(self) -> FakeJetStream:
            return FakeJetStream()

        async def drain(self) -> None:
            pass

    async def fake_connect(url: str) -> FakeConnection:
        return FakeConnection()

    monkeypatch.setattr(main.nats, "connect", fake_connect)
    with TestClient(main.app):
        pass
    assert FakeJetStream.updated == [main.STREAM]
