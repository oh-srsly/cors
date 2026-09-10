# Video processing pipeline

Two Python services joined by a NATS JetStream work queue.

```
POST /analyze ──► video_analyzer ──publish──► stream "frames" ──pull──► stream_detector ×N ──► send_results_next_service
```

- **video_analyzer** (FastAPI) validates the request, samples every Nth frame for the requested fps, JPEG-encodes each and publishes it with `video_id` and `frame_id` headers. Each request gets its own `video_id`. It returns 200 once the last frame is in the stream. The stream is capped at `MAX_BACKLOG` messages (env, default 500) and refuses publishes when full, so a slow detector fleet throttles ingestion; a request that stays blocked for 60s gets a 503 naming what was already dispatched.
- **stream_detector** pulls one frame at a time from a shared durable consumer, runs `StreamFaceDetector.detect_faces`, hands the `RespObject` to `send_results_next_service`, and acks. A frame that fails to decode, makes the detector raise, or cannot be forwarded is logged, counted and terminated. A frame whose worker dies is redelivered after 60s. Scale with `--scale stream_detector=N`. `DETECTOR=haar` swaps the provided mock for OpenCV's Haar cascade.

## Run

```bash
docker compose up --build
curl -X POST localhost:8000/analyze -H 'content-type: application/json' \
     -d '{"file_path": "G20_Summit.mp4", "fps": 2}'
```

`file_path` is relative to the `videos/` directory mounted into the analyzer. Responses: 200 with the `video_id` and dispatched frame count, 422 for a malformed body or an fps other than 2/4, 404 for a missing file, 400 for a bad path or an unreadable video, 503 when the queue is unavailable or stays full.

Prometheus metrics: the analyzer at `:8000/metrics` (frames dispatched), each detector at `:9100/metrics` inside the compose network (frames processed and dropped, detect latency). Queue depth comes from NATS itself (`nats consumer info frames detectors`).

Limits, on purpose: frames are sampled by index against the container's nominal frame rate, so variable-frame-rate sources drift; a detector killed mid-frame causes one redelivery, so results are at-least-once.

## Develop

```bash
uv venv && uv pip install -r requirements-dev.txt
.venv/bin/ruff check . && .venv/bin/pytest
```

## Stage 2 notes

A dropped frame costs a momentarily lower effective fps on one video, so the production design optimizes for availability and throughput, not durability. For hundreds of concurrent videos the analyzer stops decoding inline: `/analyze` returns `202 Accepted` with a job id, videos come from object storage, and a pool of extractor workers pulls jobs from a second stream so extraction and detection scale independently. NATS runs as a 3-node cluster with replicated streams, detector workers autoscale on consumer `num_pending`, poison frames go to a dead-letter stream via `max_deliver`, and per-job progress lives in a database so clients can poll and a failed job is retried at the video level.
