SHELL := /bin/bash
VENV := .venv
PY := $(VENV)/bin/python

export MYSQL_PORT ?= 3319
export REDIS_PORT ?= 6391

.PHONY: help up down clean install bootstrap selftest drill-clock drill-exhaustion drill-zombie drill-zombie-naive verify verify-naive bench bench-btree

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

up: ## Start MySQL + Redis and wait until healthy
	docker compose up -d --wait mysql redis

down: ## Stop containers (keep volumes)
	docker compose down

clean: ## Stop containers, drop volumes, remove artifacts
	docker compose down -v
	rm -f *.jsonl clock.txt *.pid *.log

install: ## Create venv and install pinned dependencies
	@test -d $(VENV) || python3 -m venv $(VENV)
	$(VENV)/bin/pip install -q -r requirements.txt

bootstrap: ## Apply sql/schema.sql and seed the worker-lease slots
	$(PY) scripts/bootstrap.py

selftest: ## Layout round-trip + ordering checks (and the broken-layout catch)
	SELFTEST_BREAK=1 $(PY) scripts/snowflake.py
	$(PY) scripts/snowflake.py

verify: ## Invariant gate: unique, monotonic, decodable ids; exclusive lease windows
	$(PY) scripts/verify.py

verify-naive: ## Inverted gate: exit 0 iff the naive journals VIOLATE uniqueness
	VERIFY_INVERT=1 $(PY) scripts/verify.py

drill-clock: ## Backwards clock jump vs naive|error|wait|hold, side by side
	$(PY) scripts/drill_clock.py

drill-exhaustion: ## >4096 ids in one ms: naive wraps (dupes), hardened spins to next ms
	$(PY) scripts/drill_exhaustion.py

drill-zombie: ## SIGSTOP a leased worker past TTL, reuse its id, prove zero duplicates
	$(PY) scripts/drill_zombie.py

drill-zombie-naive: ## Same drill with the lease check disabled: the collision, reproduced
	ZOMBIE_NAIVE=1 $(PY) scripts/drill_zombie.py

bench: ## Snowflake vs UUIDv4/v7 vs AUTO_INCREMENT vs Redis INCR (multi-process)
	$(PY) scripts/bench.py

bench-btree: ## Insert N rows keyed uuid4 vs uuid7 vs snowflake: locality, measured
	$(PY) scripts/bench_btree.py
