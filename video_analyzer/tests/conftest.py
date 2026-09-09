from pathlib import Path

import cv2
import numpy as np
import pytest

SOURCE_FPS = 25
FRAME_COUNT = 50


@pytest.fixture
def video_path(tmp_path: Path) -> Path:
    path = tmp_path / "clip.avi"
    writer = cv2.VideoWriter(str(path), cv2.VideoWriter_fourcc(*"MJPG"), SOURCE_FPS, (64, 48))
    for i in range(FRAME_COUNT):
        writer.write(np.full((48, 64, 3), i, dtype=np.uint8))
    writer.release()
    return path
