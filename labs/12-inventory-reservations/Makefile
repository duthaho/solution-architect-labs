SHELL := /bin/bash
VENV := .venv
PY := $(VENV)/bin/python

export MYSQL_PORT ?= 3318
export REDIS_PORT ?= 6390
MODE ?= naive

.PHONY: help up down clean install bootstrap seed verify verify-naive drill-burst sweep drill-expiry locks drill-shadow drill-cutover bench demo

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

demo: ## The full story end-to-end: bug -> three fixes -> expiry -> locks -> migration -> bench
	$(MAKE) up install bootstrap seed
	@echo && echo "=== 1. the bug: naive check-then-write oversells ==="
	$(MAKE) drill-burst MODE=naive
	$(MAKE) verify-naive
	@echo && echo "=== 2. fix a: FOR UPDATE on the counter row ==="
	$(MAKE) seed && $(MAKE) drill-burst MODE=a && $(MAKE) verify
	@echo && echo "=== 3. fix b: atomic conditional UPDATE ==="
	$(MAKE) seed && $(MAKE) drill-burst MODE=b && $(MAKE) verify
	@echo && echo "=== 4. fix c: capped pool + SKIP LOCKED (Shopify) ==="
	$(MAKE) seed && $(MAKE) drill-burst MODE=c && $(MAKE) verify
	@echo && echo "=== 5. reservations expire: TTL sweep returns capacity ==="
	$(MAKE) seed && $(MAKE) drill-expiry && $(MAKE) verify
	@echo && echo "=== 6. why the composite PK: lock evidence ==="
	$(MAKE) seed && $(MAKE) locks
	@echo && echo "=== 7. legacy store: Redis is correct but unjoinable ==="
	$(MAKE) seed && $(MAKE) drill-burst MODE=legacy && $(MAKE) verify
	@echo && echo "=== 8. shadow mode: dual-write + mismatch metric ==="
	$(MAKE) seed && $(MAKE) drill-shadow && $(MAKE) verify
	@echo && echo "=== 9. cutover mid-burst: flip Redis->MySQL, lose nothing ==="
	$(MAKE) seed && $(MAKE) drill-cutover && $(MAKE) verify
	@echo && echo "=== 10. bench: a vs b vs c ==="
	$(MAKE) bench
	@echo && echo "demo complete — every gate passed"
