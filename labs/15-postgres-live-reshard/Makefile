SHELL := /bin/bash
VENV := .venv
PY := $(VENV)/bin/python

export MONO_PORT ?= 5440
export SHARD0_PORT ?= 5441
export SHARD1_PORT ?= 5442

.PHONY: help up down clean install bootstrap seed traffic-start traffic-stop replicate replicate-status reset-shards drill-naive cutover drill-sequence drill-rollback

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

up: ## Start mono + both shards and wait until healthy
	docker compose up -d --wait mono shard0 shard1

down: ## Stop containers (keep volumes)
	docker compose down

clean: ## Stop containers, drop volumes, remove artifacts
	docker compose down -v
	rm -f *.jsonl *.pid *.log router_state.json

install: ## Create venv and install pinned dependencies
	@test -d $(VENV) || python3 -m venv $(VENV)
	$(VENV)/bin/pip install -q -r requirements.txt

bootstrap: ## Apply sql/schema.sql to all 3 nodes (drop + recreate, clear replication)
	$(PY) scripts/bootstrap.py

seed: ## Seed mono with N_ROWS deterministic rows (default 500k)
	$(PY) scripts/seed.py

traffic-start: ## Start continuous journaled write traffic in the background
	@test ! -f traffic.pid || (echo "traffic already running (traffic.pid)"; exit 1)
	@nohup $(PY) scripts/traffic.py >> traffic.log 2>&1 & echo $$! > traffic.pid
	@echo "traffic started (pid $$(cat traffic.pid))"

traffic-stop: ## Stop background traffic
	@test -f traffic.pid && kill $$(cat traffic.pid) 2>/dev/null || true
	@sleep 1 && rm -f traffic.pid && echo "traffic stopped"

replicate: ## Create row-filtered publications + subscriptions, wait for initial sync
	$(PY) scripts/replicate.py setup

replicate-status: ## Per-shard replication state, lag, and row counts
	$(PY) scripts/replicate.py status

reset-shards: ## Back to baseline: mono authoritative, shards re-synced, fresh journal
	$(PY) scripts/replicate.py reset

drill-naive: ## Cut over mid-lag with no gate, decommission — count the lost writes
	$(PY) scripts/drill_naive.py

cutover: ## The gated cutover: quiesce -> LSN catch-up -> atomic flip -> resume
	$(PY) scripts/cutover.py

drill-sequence: ## Sequences don't replicate: the silent duplicate, the setval trap, the fix
	$(PY) scripts/drill_sequence.py

drill-rollback: ## Roll back AFTER the cutover via the reverse stream — prove nothing lost
	$(PY) scripts/drill_rollback.py

verify: ## Invariant gate: checksums, boundaries, uniqueness, journal replay
	$(PY) scripts/verify.py

verify-naive: ## Inverted gate: exit 0 iff the naive drill's damage is real, recounted
	VERIFY_INVERT=1 $(PY) scripts/verify.py
