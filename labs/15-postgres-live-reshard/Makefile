SHELL := /bin/bash
VENV := .venv
PY := $(VENV)/bin/python

export MONO_PORT ?= 5440
export SHARD0_PORT ?= 5441
export SHARD1_PORT ?= 5442

.PHONY: help up down clean install bootstrap

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

up: ## Start mono + both shards and wait until healthy
	docker compose up -d --wait mono shard0 shard1

down: ## Stop containers (keep volumes)
	docker compose down

clean: ## Stop containers, drop volumes, remove artifacts
	docker compose down -v
	rm -f *.jsonl *.pid *.log router_state.json

install: ## Create venv and install pinned dependencies
	@test -d $(VENV) || python3 -m venv $(VENV)
	$(VENV)/bin/pip install -q -r requirements.txt

bootstrap: ## Apply sql/schema.sql to all 3 nodes (drop + recreate, clear replication)
	$(PY) scripts/bootstrap.py
