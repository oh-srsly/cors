from pathlib import Path

import pytest

from video_analyzer.frames import UnreadableVideoError, VideoSource


def test_samples_by_timestamp_at_2fps(video_path: Path) -> None:
    with VideoSource(video_path) as video:
        indices = [frame.index for frame in video.frames(2)]
    assert video.fps == 25
    assert indices == [0, 12, 25, 37]


def test_samples_by_timestamp_at_4fps(video_path: Path) -> None:
    with VideoSource(video_path) as video:
        indices = [frame.index for frame in video.frames(4)]
    assert indices == [0, 6, 12, 18, 25, 31, 37, 43]


def test_frames_are_jpeg(video_path: Path) -> None:
    with VideoSource(video_path) as video:
        first = next(video.frames(2))
    assert first.jpeg[:2] == b"\xff\xd8"


def test_unreadable_file_raises(tmp_path: Path) -> None:
    bogus = tmp_path / "bogus.mp4"
    bogus.write_bytes(b"not a video")
    with pytest.raises(UnreadableVideoError):
        VideoSource(bogus)
