SHELL := /bin/bash
VENV := .venv
PY := $(VENV)/bin/python

export MYSQL_PORT ?= 3318
export REDIS_PORT ?= 6390
MODE ?= naive

.PHONY: help up down clean install bootstrap seed verify verify-naive drill-burst sweep drill-expiry locks drill-shadow drill-cutover bench

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

up: ## Start MySQL + Redis and wait until healthy
	docker compose up -d --wait mysql redis

down: ## Stop containers (keep volumes)
	docker compose down

clean: ## Stop containers, drop volumes, remove artifacts
	docker compose down -v
	rm -f *.jsonl seed.json phase.txt *.pid *.log

install: ## Create venv and install pinned dependencies
	@test -d $(VENV) || python3 -m venv $(VENV)
	$(VENV)/bin/pip install -q -r requirements.txt

bootstrap: ## Apply sql/schema.sql
	$(PY) scripts/bootstrap.py

seed: ## Reset MySQL + Redis to one item at CAPACITY, phase=redis
	$(PY) scripts/seed.py

verify: ## Invariant gate: exit 0 iff no oversell, slots conserved, exactly-once, stores agree
	$(PY) scripts/verify.py

verify-naive: ## Inverted gate: exit 0 iff verify FAILS (proves the checker catches oversell)
	VERIFY_INVERT=1 $(PY) scripts/verify.py

drill-burst: ## Flash-sale burst against the hot item (MODE=naive|a|b|c|legacy)
	MODE=$(MODE) $(PY) scripts/drill_burst.py

sweep: ## Release reservations past their TTL
	$(PY) scripts/sweep.py

drill-expiry: ## Exhaust pool with 1s TTLs, sweep, prove capacity returns
	$(PY) scripts/drill_expiry.py

locks: ## Show record locks: composite-PK claim vs secondary-index claim
	$(PY) scripts/locks.py

drill-shadow: ## Dual-write shadow mode + comparator (clean, then injected divergence)
	$(PY) scripts/drill_shadow.py

drill-cutover: ## Flip source of truth Redis->MySQL mid-burst, gated on 0 mismatches
	$(PY) scripts/drill_cutover.py

bench: ## Compare strategies a/b/c under identical contention (fresh reseed each)
	$(PY) scripts/bench.py
