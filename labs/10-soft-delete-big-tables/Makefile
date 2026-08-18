SHELL := /bin/bash
VENV := .venv
PY := $(VENV)/bin/python
export SEED_ROWS ?= 500000
export MYSQL_PORT ?= 3316

.PHONY: help up down clean install

help:
	@echo "Targets:"
	@echo "  make up             start MySQL"
	@echo "  make install        install python deps"
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
