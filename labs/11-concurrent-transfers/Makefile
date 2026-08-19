SHELL := /bin/bash
VENV := .venv
PY := $(VENV)/bin/python
export MYSQL_PORT ?= 3317

MODE ?= naive

.PHONY: help up down clean install bootstrap seed drill-race drill-deadlock

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
