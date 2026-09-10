.PHONY: test up demo

test: .venv
	.venv/bin/ruff check . && .venv/bin/ruff format --check . && .venv/bin/pytest

.venv: requirements-dev.txt video_analyzer/requirements.txt stream_detector/requirements.txt
	uv venv --clear && uv pip install -r requirements-dev.txt && touch .venv

up:
	docker compose up --build

demo:
	DETECTOR=haar docker compose up --build -d
	@until curl -sf localhost:8000/metrics >/dev/null; do sleep 1; done
	@for f in videos/*.mp4; do \
	  curl -s -X POST localhost:8000/analyze -H 'content-type: application/json' \
	    -d "{\"file_path\": \"$$(basename $$f)\", \"fps\": 2}"; echo; \
	done
	docker compose logs -f stream_detector
