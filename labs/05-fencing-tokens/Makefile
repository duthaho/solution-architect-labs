SHELL := /bin/bash
VENV := .venv
PY := $(VENV)/bin/python

.PHONY: help up down clean install \
        drill-happy drill-corrupt drill-check-race drill-fenced demo

help:
	@echo "Targets:"
	@echo "  make up               start redis (:6380) + ledger storage (:8091)"
	@echo "  make install          install python deps (host-side drivers)"
	@echo "  make drill-happy      no chaos: locks 'work' — where false trust comes from"
	@echo "  make drill-corrupt    SIGSTOP holder past TTL, fencing off -> interleaved ledger"
	@echo "  make drill-check-race re-check lock before each write -> STILL corrupts (TOCTOU)"
	@echo "  make drill-fenced     same chaos, fencing on -> 409s, clean ledger"
	@echo "  make demo             corrupt vs fenced, side-by-side verdict"
	@echo "  make clean            destroy everything"

up:
	docker compose build
	docker compose up -d --wait redis storage

down:
	docker compose down --remove-orphans

clean:
	-docker rm -f lab05-worker-a lab05-worker-b 2>/dev/null
	docker compose down -v --remove-orphans
	rm -f results.jsonl

install:
	@test -d $(VENV) || python3 -m venv $(VENV)
	$(PY) -m pip install -q -r requirements.txt

# ---------------------------------------------------------------- the drills
# Drills manage their own containers (fresh storage per run, right FENCING
# mode), so `make up` beforehand is optional — but the image must exist.

drill-happy: install up
	$(PY) scripts/drill.py happy

drill-corrupt: install up
	$(PY) scripts/drill.py corrupt

drill-check-race: install up
	$(PY) scripts/drill.py check-race

drill-fenced: install up
	$(PY) scripts/drill.py fenced

demo: install up
	rm -f results.jsonl
	$(PY) scripts/drill.py corrupt
	$(PY) scripts/drill.py fenced
	$(PY) scripts/drill.py compare
