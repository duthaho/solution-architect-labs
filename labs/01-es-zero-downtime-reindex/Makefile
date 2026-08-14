SHELL := /bin/bash
VENV := .venv
PY := $(VENV)/bin/python
export SEED_DOCS ?= 300000
export ES_PORT ?= 9200
export ES_URL ?= http://localhost:$(ES_PORT)

.PHONY: help up down clean install bootstrap seed traffic-start traffic-stop reindex verify rollback demo

help:
	@echo "Targets:"
	@echo "  make up             start Elasticsearch"
	@echo "  make install        install python deps"
	@echo "  make bootstrap      create v1 index + aliases"
	@echo "  make seed           load $(SEED_DOCS) docs (SEED_DOCS=n to change)"
	@echo "  make traffic-start  start live write traffic (background)"
	@echo "  make traffic-stop   stop live write traffic"
	@echo "  make reindex        run zero-downtime reindex v1 -> v2"
	@echo "  make verify         prove zero data gap"
	@echo "  make rollback       swap aliases back to v1"
	@echo "  make demo           full scenario end-to-end"
	@echo "  make clean          destroy everything (containers, volumes, journal)"

up:
	docker compose up -d --wait elasticsearch

down:
	docker compose down

clean:
	docker compose down -v
	rm -f journal.jsonl traffic.pid traffic.log

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
		tail -1 traffic.log; \
	else echo "traffic not running"; fi

reindex:
	$(PY) scripts/reindex.py

verify:
	$(PY) scripts/verify.py

rollback:
	$(PY) scripts/rollback.py

demo: up install bootstrap seed traffic-start
	@echo "--- letting live traffic run for 10s before migration ---"
	@sleep 10
	$(MAKE) reindex
	@echo "--- letting traffic keep writing to v2 for 5s after cutover ---"
	@sleep 5
	$(MAKE) traffic-stop
	$(MAKE) verify
