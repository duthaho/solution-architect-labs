SHELL := /bin/bash
VENV := .venv
PY := $(VENV)/bin/python
export SEED_ROWS ?= 10000
LAG_MS ?= 2000

.PHONY: help up down clean install bootstrap seed traffic-start traffic-stop \
        semisync-on semisync-off semisync-status kill-primary failover verify \
        restore drill-async drill-semisync drill-degrade drill-zombie demo

help:
	@echo "Targets:"
	@echo "  make up              start 3 MySQL nodes"
	@echo "  make install         install python deps"
	@echo "  make bootstrap       wire GTID replication primary -> replica1,replica2"
	@echo "  make seed            load $(SEED_ROWS) rows"
	@echo "  make traffic-start   start journaling writer (background)"
	@echo "  make traffic-stop    stop writer"
	@echo "  make semisync-on     enable lossless semi-sync (AFTER_SYNC)"
	@echo "  make semisync-off    back to async replication"
	@echo "  make semisync-status show semi-sync status on all nodes"
	@echo "  make kill-primary    inject $(LAG_MS)ms replica lag, then SIGKILL the primary"
	@echo "  make failover        promote best replica by GTID, repoint, flip router"
	@echo "  make verify          RPO report (acked-but-lost rows) + RTO timeline"
	@echo "  make restore         resurrect the dead node, fence it, rejoin as replica"
	@echo "  make drill-async     full drill, async replication   (expect loss > 0)"
	@echo "  make drill-semisync  same crash with semi-sync       (expect loss = 0)"
	@echo "  make drill-degrade   semi-sync silently degrades -> loss AGAIN"
	@echo "  make drill-zombie    unfenced old primary -> split-brain -> fence -> rebuild"
	@echo "  make demo            drill-async + drill-semisync + comparison table"
	@echo "  make clean           destroy everything"

up:
	docker compose up -d --wait

down:
	docker compose down

clean:
	docker compose down -v
	rm -f journal.jsonl router.json timeline.json results.jsonl \
	      traffic.pid traffic.log zombie-traffic.log

install:
	@test -d $(VENV) || python3 -m venv $(VENV)
	$(PY) -m pip install -q -r requirements.txt

bootstrap:
	$(PY) scripts/bootstrap.py

seed:
	$(PY) scripts/seed.py

traffic-start:
	@if [ -f traffic.pid ] && kill -0 $$(cat traffic.pid) 2>/dev/null; then \
		echo "traffic already running (pid $$(cat traffic.pid))"; \
	else \
		nohup $(PY) scripts/traffic.py > traffic.log 2>&1 & echo $$! > traffic.pid; \
		echo "traffic started (pid $$(cat traffic.pid), log: traffic.log)"; \
	fi

traffic-stop:
	@if [ -f traffic.pid ]; then \
		kill -TERM $$(cat traffic.pid) 2>/dev/null || true; \
		sleep 2; rm -f traffic.pid; echo "traffic stopped"; \
		tail -2 traffic.log; \
	else echo "traffic not running"; fi

semisync-on:
	$(PY) scripts/semisync.py enable

semisync-off:
	$(PY) scripts/semisync.py disable

semisync-status:
	$(PY) scripts/semisync.py status

kill-primary:
	$(PY) scripts/kill_primary.py --lag-ms $(LAG_MS)

failover:
	$(PY) scripts/failover.py

verify:
	$(PY) scripts/verify.py

restore:
	$(PY) scripts/rebuild.py

# ---------------------------------------------------------------- the drills

drill-async: install
	$(PY) scripts/drill.py async

drill-semisync: install
	$(PY) scripts/drill.py semisync

drill-degrade: install
	$(PY) scripts/drill.py degrade

drill-zombie: install
	$(PY) scripts/drill.py zombie

demo: up install bootstrap seed
	rm -f results.jsonl
	$(MAKE) drill-async
	$(MAKE) restore
	$(MAKE) drill-semisync
	$(PY) scripts/report.py
