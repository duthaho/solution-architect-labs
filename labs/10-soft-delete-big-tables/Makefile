SHELL := /bin/bash
VENV := .venv
PY := $(VENV)/bin/python
export SEED_ROWS ?= 500000
export MYSQL_PORT ?= 3316

FAMILY ?= a

.PHONY: help up down clean install bootstrap seed traffic-start traffic-stop \
        strategy-a strategy-b drill-drift strategy-c archiver-drain \
        archiver-start archiver-stop purge drill-kill-archiver drill-big-delete verify bench demo

help:
	@echo "Targets:"
	@echo "  make up             start MySQL"
	@echo "  make install        install python deps"
	@echo "  make bootstrap      create the three schema families (a, b, c)"
	@echo "  make seed           load $(SEED_ROWS) orders per family (SEED_ROWS=n to change)"
	@echo "  make clean          destroy everything (containers, volumes, state)"

up:
	docker compose up -d --wait mysql

down:
	docker compose down

clean:
	docker compose down -v
	rm -f *.jsonl *.pid *.log

install:
	@test -d $(VENV) || python3 -m venv $(VENV)
	$(PY) -m pip install -q -r requirements.txt

bootstrap:
	$(PY) scripts/bootstrap.py

seed:
	$(PY) scripts/seed.py

traffic-start:
	@if [ -f traffic_$(FAMILY).pid ] && kill -0 $$(cat traffic_$(FAMILY).pid) 2>/dev/null; then \
		echo "traffic[$(FAMILY)] already running (pid $$(cat traffic_$(FAMILY).pid))"; \
	else \
		nohup $(PY) scripts/traffic.py $(FAMILY) > traffic_$(FAMILY).log 2>&1 & echo $$! > traffic_$(FAMILY).pid; \
		echo "traffic[$(FAMILY)] started (pid $$(cat traffic_$(FAMILY).pid), log: traffic_$(FAMILY).log)"; \
	fi

traffic-stop:
	@if [ -f traffic_$(FAMILY).pid ]; then \
		kill -TERM $$(cat traffic_$(FAMILY).pid) 2>/dev/null || true; \
		sleep 2; rm -f traffic_$(FAMILY).pid; echo "traffic[$(FAMILY)] stopped"; \
		tail -1 traffic_$(FAMILY).log; \
	else echo "traffic[$(FAMILY)] not running"; fi

strategy-a:
	$(PY) scripts/strategy_a.py

strategy-b:
	$(PY) scripts/strategy_b.py

drill-drift:
	$(PY) scripts/strategy_b.py drift

strategy-c:
	$(PY) scripts/archiver.py flag

archiver-drain:
	$(PY) scripts/archiver.py run --drain

archiver-start:
	@if [ -f archiver.pid ] && kill -0 $$(cat archiver.pid) 2>/dev/null; then \
		echo "archiver already running (pid $$(cat archiver.pid))"; \
	else \
		nohup $(PY) scripts/archiver.py run > archiver.log 2>&1 & echo $$! > archiver.pid; \
		echo "archiver started (pid $$(cat archiver.pid), log: archiver.log)"; \
	fi

archiver-stop:
	@if [ -f archiver.pid ]; then \
		kill -TERM $$(cat archiver.pid) 2>/dev/null || true; \
		sleep 1; rm -f archiver.pid; echo "archiver stopped"; \
	else echo "archiver not running"; fi

purge:
	$(PY) scripts/archiver.py purge

drill-kill-archiver:
	$(PY) scripts/drill_kill_archiver.py

drill-big-delete:
	$(PY) scripts/drill_big_delete.py

verify:
	$(PY) scripts/verify.py

bench:
	$(PY) scripts/bench.py
