# Video processing pipeline

Two Python services joined by a Redis Stream.

```
POST /analyze ──► video_analyzer ──XADD──► redis stream "frames" ──XREADGROUP──► stream_detector ×N ──► send_results_next_service
```

- **video_analyzer** (FastAPI) validates the request, samples frames by presentation time at the requested fps, JPEG-encodes them and publishes one stream entry per frame (`video_id`, `frame_id`, `jpeg`). Each request gets its own `video_id` so retries and concurrent runs of the same file stay distinguishable downstream. It returns 200 once every frame is in the stream. If the backlog exceeds `MAX_BACKLOG` (env, default 500) it waits, so a slow detector fleet throttles ingestion instead of exhausting Redis memory. Undecodable frames are skipped, not fatal.
- **stream_detector** is a consumer-group worker. Each replica reads a batch, and per frame runs `StreamFaceDetector.detect_faces`, hands the `RespObject` to `send_results_next_service`, then acks and deletes the entry. A frame that fails to decode, makes the detector raise, or cannot be forwarded is logged and dropped. Entries left pending by a crashed replica are reclaimed via `XAUTOCLAIM`. Redis errors are retried, not fatal. Scale with `--scale stream_detector=N`.

## Run

```bash
docker compose up --build
curl -X POST localhost:8000/analyze -H 'content-type: application/json' \
     -d '{"file_path": "G20_Summit.mp4", "fps": 2}'
```

`file_path` is relative to the `videos/` directory mounted into the analyzer. Responses: 200 with the `video_id` and dispatched frame count, 422 for a malformed body or an fps other than 2/4, 404 for a missing file, 400 for a bad path or an unreadable video, 503 when Redis is down or the backlog does not drain in time.

Each request holds a worker thread for the whole extraction, so the analyzer handles about 40 videos concurrently before requests queue.

## Develop

```bash
uv venv && uv pip install -r requirements-dev.txt
.venv/bin/ruff check . && .venv/bin/pytest
```

## Stage 2 notes

A dropped frame costs a momentarily lower effective fps on one video, so the production design optimizes for availability and throughput, not durability. For hundreds of concurrent videos the analyzer stops decoding inline: `/analyze` returns `202 Accepted` with a job id, videos come from object storage, and a pool of extractor workers pulls jobs from a `videos` queue so extraction and detection scale independently. Redis moves to Sentinel or Cluster so the queue survives a node loss, detector workers autoscale on stream lag, retries get a cap and a dead-letter stream, and per-job progress (frames dispatched, processed, failed) lives in a database so clients can poll and a failed job is retried at the video level.
