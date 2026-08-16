SHELL := /bin/bash
VENV := .venv
PY := $(VENV)/bin/python
export EVENTS ?= 2000
export ACCOUNTS ?= 50
export MYSQL_PORT ?= 3310
export KAFKA_PORT ?= 9096

# The identical crash schedule used by drill-doublecharge and
# drill-idempotent: SIGKILL-equivalent (os._exit) after every N applied
# events, at the worst moment — effect applied, offsets not yet committed.
CRASH_EVERY ?= 350

.PHONY: help up down clean install bootstrap reset produce relay-start relay-stop \
        consume-naive consume-idempotent chaos-stop lag audit \
        drill-doublecharge drill-idempotent drill-ghost-publish drill-poison demo

help:
	@echo "Targets:"
	@echo "  make up                  start MySQL + Kafka"
	@echo "  make install             install python deps"
	@echo "  make bootstrap           create topics, tables, seed accounts"
	@echo "  make reset               wipe ledger/topics/offsets for a fresh drill"
	@echo "  make produce             produce $(EVENTS) payments (PRODUCE_ARGS='--retry-storm')"
	@echo "  make consume-naive       run naive consumer under chaos supervisor (background)"
	@echo "  make consume-idempotent  run idempotent consumer under chaos supervisor (background)"
	@echo "  make chaos-stop          stop the supervised consumer"
	@echo "  make relay-start         start outbox->Kafka relay (background)"
	@echo "  make relay-stop          stop the relay"
	@echo "  make lag                 show consumer group lag per partition"
	@echo "  make audit               ledger audit: expected vs actual balances"
	@echo "  make drill-doublecharge  naive consumer + crashes -> corrupted balances"
	@echo "  make drill-idempotent    same crash schedule -> zero corruption"
	@echo "  make drill-ghost-publish dual-write loses an event; outbox does not"
	@echo "  make drill-poison        poison pill stalls the group; DLQ + safe replay"
	@echo "  make demo                doublecharge vs idempotent comparison table"
	@echo "  make clean               destroy everything (containers, volumes, logs)"

up:
	docker compose up -d --wait mysql kafka

down:
	docker compose down

clean:
	docker compose down -v
	rm -f *.pid *.log audit.json

install:
	@test -d $(VENV) || python3 -m venv $(VENV)
	$(PY) -m pip install -q -r requirements.txt

bootstrap:
	$(PY) scripts/bootstrap.py

reset:
	$(PY) scripts/bootstrap.py --reset

produce:
	$(PY) scripts/producer.py --count $(EVENTS) $(PRODUCE_ARGS)

consume-naive:
	@$(MAKE) --no-print-directory _chaos MODE=naive

consume-idempotent:
	@$(MAKE) --no-print-directory _chaos MODE=idempotent

_chaos:
	@if [ -f chaos.pid ] && kill -0 $$(cat chaos.pid) 2>/dev/null; then \
		echo "chaos supervisor already running (pid $$(cat chaos.pid))"; \
	else \
		nohup $(PY) scripts/chaos.py --mode $(MODE) --crash-every $(CRASH_EVERY) $(CHAOS_ARGS) \
			> chaos.log 2>&1 & echo $$! > chaos.pid; \
		echo "chaos supervisor started: $(MODE) consumer, crash every $(CRASH_EVERY) events (log: chaos.log)"; \
	fi

chaos-stop:
	@if [ -f chaos.pid ]; then \
		kill -TERM $$(cat chaos.pid) 2>/dev/null || true; \
		sleep 2; rm -f chaos.pid; echo "chaos supervisor stopped"; \
	else echo "chaos supervisor not running"; fi

# Wait for the supervised consumer to drain the topic and exit cleanly.
_chaos-wait:
	@while [ -f chaos.pid ] && kill -0 $$(cat chaos.pid) 2>/dev/null; do sleep 1; done; \
	rm -f chaos.pid; tail -3 chaos.log

relay-start:
	@if [ -f relay.pid ] && kill -0 $$(cat relay.pid) 2>/dev/null; then \
		echo "relay already running (pid $$(cat relay.pid))"; \
	else \
		nohup $(PY) scripts/relay.py > relay.log 2>&1 & echo $$! > relay.pid; \
		echo "relay started (pid $$(cat relay.pid), log: relay.log)"; \
	fi

relay-stop:
	@if [ -f relay.pid ]; then \
		kill -TERM $$(cat relay.pid) 2>/dev/null || true; \
		sleep 2; rm -f relay.pid; echo "relay stopped"; \
		tail -1 relay.log; \
	else echo "relay not running"; fi

lag:
	@docker exec lab08-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
		--bootstrap-server localhost:9092 --describe --group balance-consumer 2>/dev/null || true

audit:
	$(PY) scripts/audit.py

# ---------------------------------------------------------------- drills ---

drill-doublecharge: install
	@echo "=== DRILL 1: naive consumer + producer retries + crashes ==="
	$(MAKE) reset
	$(PY) scripts/producer.py --count $(EVENTS) --retry-storm
	$(MAKE) consume-naive
	$(MAKE) _chaos-wait
	$(PY) scripts/audit.py --expect corrupted

drill-idempotent: install
	@echo "=== DRILL 2: idempotent consumer, identical crash schedule ==="
	$(MAKE) reset
	$(PY) scripts/producer.py --count $(EVENTS) --retry-storm
	$(MAKE) consume-idempotent
	$(MAKE) _chaos-wait
	$(PY) scripts/audit.py --expect clean

drill-ghost-publish: install
	@echo "=== DRILL 3a: direct producer crashes between DB write and publish ==="
	$(MAKE) reset
	-$(PY) scripts/producer.py --count 500 --crash-at 250    # exits 1 on purpose
	$(PY) scripts/producer.py --count 250 --start-at 251     # "service restarted"
	$(MAKE) consume-idempotent
	$(MAKE) _chaos-wait
	$(PY) scripts/audit.py --expect corrupted    # ghost + unflushed buffer: acked payments never applied
	@echo ""
	@echo "=== DRILL 3b: same crash, outbox mode ==="
	$(MAKE) reset
	-$(PY) scripts/producer.py --mode outbox --count 500 --crash-at 250
	$(PY) scripts/producer.py --mode outbox --count 250 --start-at 251
	$(MAKE) relay-start
	$(MAKE) consume-idempotent
	$(MAKE) _chaos-wait
	$(MAKE) relay-stop
	$(PY) scripts/audit.py --expect clean

drill-poison: install
	@echo "=== DRILL 4: poison pill -> stall -> DLQ -> safe replay ==="
	$(MAKE) reset
	$(PY) scripts/producer.py --count 500
	$(PY) scripts/inject.py
	$(PY) scripts/producer.py --count 500 --start-at 501
	@echo "--- consumer without DLQ: crash loop, partitions stall ---"
	$(MAKE) consume-idempotent CHAOS_ARGS="--max-restarts 3" CRASH_EVERY=0
	$(MAKE) _chaos-wait
	$(MAKE) lag
	@echo "--- consumer with DLQ: pill quarantined, group drains ---"
	$(MAKE) consume-idempotent CHAOS_ARGS="--dlq" CRASH_EVERY=0
	$(MAKE) _chaos-wait
	$(MAKE) lag
	$(PY) scripts/audit.py --expect corrupted    # pill's payment not yet applied
	@echo "--- replay DLQ (twice, to prove dedupe makes replay safe) ---"
	$(PY) scripts/replay.py --times 2
	$(MAKE) consume-idempotent CRASH_EVERY=0
	$(MAKE) _chaos-wait
	$(PY) scripts/audit.py --expect clean

demo: up install bootstrap
	$(PY) scripts/demo.py
