SHELL := /bin/bash
VENV := .venv
PY := $(VENV)/bin/python

export MYSQL_PORT ?= 3320
export REDIS_PORT ?= 6392

.PHONY: help up down clean install bootstrap selftest drill-accuracy drill-merge verify verify-naive bench demo

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

up: ## Start MySQL + Redis and wait until healthy
	docker compose up -d --wait mysql redis

down: ## Stop containers (keep volumes)
	docker compose down

clean: ## Stop containers, drop volumes, remove artifacts
	docker compose down -v
	rm -f *.jsonl *.pid *.log

install: ## Create venv and install pinned dependencies
	@test -d $(VENV) || python3 -m venv $(VENV)
	$(VENV)/bin/pip install -q -r requirements.txt

bootstrap: ## Apply sql/schema.sql (drop + recreate lab14)
	$(PY) scripts/bootstrap.py

selftest: ## Sketch + store selftests (and the corrupted-sketch catch)
	SELFTEST_BREAK=1 $(PY) scripts/sketches.py; test $$? -eq 1
	$(PY) scripts/sketches.py
	$(PY) scripts/topk_stores.py

drill-accuracy: ## All six contenders vs the oracle + the CMS width-sweep cliff
	$(PY) scripts/drill_accuracy.py

drill-merge: ## Bucket merging: linearity, CU divergence, the cross-window trap
	$(PY) scripts/drill_merge.py

verify: ## Invariant gate: replayed truth, one-sided error, guarantees, merge identity
	$(PY) scripts/verify.py

verify-naive: ## Inverted gate: exit 0 iff the starved/naive records VIOLATE the floor
	VERIFY_INVERT=1 $(PY) scripts/verify.py

bench: ## All six contenders on one stream: ops/s, latency, memory, recall
	$(PY) scripts/bench.py

demo: ## The full story end-to-end: selftests -> cliff -> windows -> gate -> bench
	$(MAKE) up install bootstrap
	rm -f *.jsonl
	@echo && echo "=== 1. the building blocks, proven (and the corrupted-sketch catch) ==="
	$(MAKE) selftest
	@echo && echo "=== 2. six contenders vs the oracle; the width-sweep precision cliff ==="
	$(MAKE) drill-accuracy
	@echo && echo "=== 3. minute buckets: merge linearity, CU divergence, the cross-window trap ==="
	$(MAKE) drill-merge
	@echo && echo "=== 4. the gate CATCHES the starved sketch and the naive policy (inverted proof) ==="
	$(MAKE) verify-naive
	@echo && echo "=== 5. the invariant gate: replayed truth, one-sided error, merge identity ==="
	$(MAKE) verify
	@echo && echo "=== 6. the bench: throughput, latency, memory, recall — side by side ==="
	$(MAKE) bench
	@echo && echo "demo complete — every gate passed"
