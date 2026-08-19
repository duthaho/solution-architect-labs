SHELL := /bin/bash
VENV := .venv
PY := $(VENV)/bin/python
export MYSQL_PORT ?= 3317

MODE ?= naive

.PHONY: help up down clean install bootstrap seed drill-race drill-deadlock \
        verify verify-naive bench demo

help:
	@echo "Targets:"
	@echo "  make up             start MySQL"
	@echo "  make install        install python deps"
	@echo "  make bootstrap      create schema (accounts, transfers, entries, balance_cache)"
	@echo "  make seed           seed accounts (ACCOUNTS=n BALANCE=x to change)"
	@echo "  make clean          destroy everything (containers, volumes, state)"

up:
	docker compose up -d --wait mysql

down:
	docker compose down

clean:
	docker compose down -v
	rm -f *.jsonl *.json *.pid *.log

install:
	@test -d $(VENV) || python3 -m venv $(VENV)
	$(PY) -m pip install -q -r requirements.txt

bootstrap:
	$(PY) scripts/bootstrap.py

seed:
	$(PY) scripts/seed.py

drill-race:
	MODE=$(MODE) $(PY) scripts/drill_race.py

drill-deadlock:
	$(PY) scripts/drill_deadlock.py

verify:
	$(PY) scripts/verify.py

# After a naive run the checker MUST fail — its exit 1 is the proof it can
# see the corruption. This target inverts the exit code.
verify-naive:
	@if $(PY) scripts/verify.py; then \
		echo "ERROR: verify passed after the naive race — checker is blind"; exit 1; \
	else \
		echo "verify FAILED as expected: the naive corruption is visible"; \
	fi

# Resets the world per strategy (reseed), then compares all four under the
# identical contended workload.
bench:
	$(PY) scripts/bench.py

demo: up install bootstrap seed
	@echo ""
	@echo "=== 1. the bug: naive read-modify-write under 8 concurrent writers ==="
	$(MAKE) drill-race MODE=naive
	@echo ""
	@echo "=== 2. the checker sees it: verify MUST fail on the naive world ==="
	$(MAKE) verify-naive
	@echo ""
	@echo "=== 3. deadlock drill: why locks are taken in sorted order ==="
	@echo "    (runs on the already-dirty world: it journals nothing, so it"
	@echo "     must precede the reseed that verify measures from)"
	$(MAKE) drill-deadlock
	@echo ""
	@echo "=== 4. reseed: naive corrupted the world, strategies get a clean one ==="
	$(MAKE) seed
	@echo ""
	@echo "=== 5. the four fixes, same drill ==="
	$(MAKE) drill-race MODE=a
	$(MAKE) drill-race MODE=b
	$(MAKE) drill-race MODE=c
	$(MAKE) drill-race MODE=d
	@echo ""
	@echo "=== 6. verify: conservation + exactly-once across journals and DB ==="
	$(MAKE) verify
	@echo ""
	@echo "=== 7. benchmark: the four fixes under identical contention ==="
	$(MAKE) bench
