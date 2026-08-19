SHELL := /bin/bash
VENV := .venv
PY := $(VENV)/bin/python
export MYSQL_PORT ?= 3317

MODE ?= naive

.PHONY: help up down clean install bootstrap seed drill-race drill-deadlock \
        verify verify-naive bench

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
