SHELL := /bin/bash
VENV := .venv
PY := $(VENV)/bin/python
export SEED_ROWS ?= 200000
export MYSQL_PORT ?= 3306
export KAFKA_PORT ?= 29092
export CONNECT_PORT ?= 8083
export ES_PORT ?= 9201

.PHONY: help up down clean install bootstrap seed connector traffic-start traffic-stop \
        projector-start projector-stop status verify reconcile demo

help:
	@echo "Targets:"
	@echo "  make up               start MySQL + Kafka + Connect + Elasticsearch"
	@echo "  make install          install python deps"
	@echo "  make bootstrap        create source table, ES index, Kafka topic"
	@echo "  make seed             load $(SEED_ROWS) rows (SEED_ROWS=n to change)"
	@echo "  make traffic-start    start live write traffic (background)"
	@echo "  make traffic-stop     stop live write traffic"
	@echo "  make connector        register Debezium connector (snapshot + stream)"
	@echo "  make projector-start  start the Kafka->ES projector (background)"
	@echo "  make projector-stop   stop the projector"
	@echo "  make status           connector status + consumer group lag"
	@echo "  make verify           prove zero data gap end-to-end"
	@echo "  make reconcile        full MySQL-vs-ES diff (--repair via RECONCILE_ARGS)"
	@echo "  make demo             full scenario end-to-end"
	@echo "  make clean            destroy everything (containers, volumes, journal)"

up:
	docker compose up -d --wait mysql kafka connect elasticsearch

down:
	docker compose down

clean:
	docker compose down -v
	rm -f journal.jsonl traffic.pid traffic.log projector.pid projector.log

install:
	@test -d $(VENV) || python3 -m venv $(VENV)
	$(PY) -m pip install -q -r requirements.txt

bootstrap:
	$(PY) scripts/bootstrap.py

seed:
	$(PY) scripts/seed.py

connector:
	$(PY) scripts/connector.py

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

projector-start:
	@if [ -f projector.pid ] && kill -0 $$(cat projector.pid) 2>/dev/null; then \
		echo "projector already running (pid $$(cat projector.pid))"; \
	else \
		nohup $(PY) scripts/projector.py > projector.log 2>&1 & echo $$! > projector.pid; \
		echo "projector started (pid $$(cat projector.pid), log: projector.log)"; \
	fi

projector-stop:
	@if [ -f projector.pid ]; then \
		kill -TERM $$(cat projector.pid) 2>/dev/null || true; \
		sleep 2; rm -f projector.pid; echo "projector stopped"; \
		tail -1 projector.log; \
	else echo "projector not running"; fi

status:
	@curl -s localhost:$(CONNECT_PORT)/connectors/orders-source/status | $(PY) -m json.tool || true
	@docker exec lab03-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
		--bootstrap-server localhost:9092 --describe --group es-projector 2>/dev/null || true

verify:
	$(PY) scripts/verify.py

reconcile:
	$(PY) scripts/reconcile.py $(RECONCILE_ARGS)

demo: up install bootstrap seed traffic-start connector projector-start
	@echo "--- pipeline live: snapshot + binlog stream + projector, traffic still writing ---"
	@echo "--- letting it all run for 30s (watch: tail -f projector.log) ---"
	@sleep 30
	$(MAKE) traffic-stop
	$(MAKE) verify
	$(MAKE) reconcile
