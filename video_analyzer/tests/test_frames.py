from pathlib import Path

from video_analyzer.frames import iter_frames


def test_samples_by_timestamp_at_2fps(video_path: Path) -> None:
    assert [f.index for f in iter_frames(video_path, 2)] == [0, 13, 25, 38]


def test_samples_by_timestamp_at_4fps(video_path: Path) -> None:
    assert [f.index for f in iter_frames(video_path, 4)] == [
        0,
        7,
        13,
        19,
        25,
        32,
        38,
        44,
    ]


def test_frames_are_jpeg(video_path: Path) -> None:
    assert next(iter_frames(video_path, 2)).jpeg[:2] == b"\xff\xd8"
