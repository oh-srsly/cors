from pathlib import Path

import pytest

from video_analyzer.frames import iter_frames


@pytest.mark.parametrize(
    ("fps", "indexes"),
    [(2, [0, 13, 25, 38]), (4, [0, 7, 13, 19, 25, 32, 38, 44])],
)
def test_samples_by_index(video_path: Path, fps: int, indexes: list[int]) -> None:
    assert [f.index for f in iter_frames(video_path, fps)] == indexes


def test_frames_are_jpeg(video_path: Path) -> None:
    assert next(iter_frames(video_path, 2)).jpeg[:2] == b"\xff\xd8"
