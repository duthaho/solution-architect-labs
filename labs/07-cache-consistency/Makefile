SHELL := /bin/bash
VENV := .venv
PY := $(VENV)/bin/python
export N_PRODUCTS ?= 1000
HERD_READERS ?= 200

.PHONY: help up up-cdc down clean install bootstrap seed flush \
        drill-stale-set drill-versioned drill-ttl drill-herd drill-cdc \
        soak report demo

help:
	@echo "Setup:"
	@echo "  make up install bootstrap seed    mysql :3315 + redis :6381, schema, 1000 products"
	@echo "  make up-cdc                       + kafka + debezium (only drill-cdc needs them)"
	@echo "Drills:"
	@echo "  make drill-stale-set              deterministic stale-set race vs delete-on-write: STALE, 10/10"
	@echo "  make drill-versioned              the identical interleaving vs versioned keys: FRESH, 10/10"
	@echo "  make drill-ttl                    ttl-only soak: measured staleness ~= the TTL you chose"
	@echo "  make drill-herd                   hot-key expiry x $(HERD_READERS) readers: naive vs singleflight vs swr"
	@echo "  make drill-cdc                    cdc invalidation soak + invalidator outage: lag IS the staleness"
	@echo "Measurement:"
	@echo "  make soak                         journaling soak of ttl/delete/versioned + staleness audit"
	@echo "  make report                       the money table (from measured summaries)"
	@echo "One shot:"
	@echo "  make demo                         races + herd + soak + money table, end to end"
	@echo "  make clean                        destroy everything (containers, volumes, state)"

up:
	docker compose up -d --wait mysql redis

up-cdc:
	docker compose --profile cdc up -d --wait

down:
	docker compose --profile cdc down

clean:
	docker compose --profile cdc down -v
	rm -f journal_writes.jsonl journal_reads.jsonl journal_cdc_lag.jsonl \
	      audit_summary.json herd_summary.json \
	      traffic.pid traffic.log invalidator.pid invalidator.log

install:
	@test -d $(VENV) || python3 -m venv $(VENV)
	$(PY) -m pip install -q -r requirements.txt

bootstrap:
	$(PY) scripts/bootstrap.py

seed:
	$(PY) scripts/seed.py

flush:
	@docker exec lab07-redis redis-cli flushdb > /dev/null && echo "redis flushed"

# --- deterministic races -----------------------------------------------------

# The stale-set race against cache-aside + delete-on-write. The interleaving is
# SCHEDULED (rendezvous hooks), so it must reproduce 10/10 — exit 1 otherwise.
drill-stale-set:
	$(PY) scripts/race.py --strategy delete --expect stale --runs 10

# The IDENTICAL schedule against versioned keys. The stale SET still happens —
# it just lands on a version nobody can read anymore. Must be fresh 10/10.
drill-versioned:
	$(PY) scripts/race.py --strategy versioned --expect fresh --runs 10

# --- measured staleness ------------------------------------------------------

# TTL-only baseline: with TTL=5s the auditor's MAX staleness converges on ~5s.
# The knob is the dial between cost (miss rate) and staleness — not a fix.
drill-ttl: flush
	rm -f journal_writes.jsonl journal_reads.jsonl
	$(PY) scripts/traffic.py --strategy ttl --ttl 5 --duration 25
	$(PY) scripts/auditor.py --strategy ttl --json

drill-herd: flush
	$(PY) scripts/herd.py --readers $(HERD_READERS)

soak: flush
	rm -f journal_writes.jsonl journal_reads.jsonl
	@for s in ttl delete versioned; do \
		echo "=== soak: $$s (20s) ==="; \
		$(PY) scripts/traffic.py --strategy $$s --ttl 15 --duration 20 || exit 1; \
	done
	$(PY) scripts/auditor.py --json

report:
	$(PY) scripts/report.py

# --- cdc ---------------------------------------------------------------------

# Bounded staleness = pipeline health, proved by breaking the pipeline:
#   20s healthy soak -> kill the invalidator -> 20s outage (staleness grows
#   second by second) -> restart -> catch-up erases the backlog. TTL is set
#   to 600s so nothing rescues the cache except the pipeline itself.
drill-cdc: up-cdc install bootstrap seed
	$(PY) scripts/connector.py
	rm -f journal_writes.jsonl journal_reads.jsonl journal_cdc_lag.jsonl
	@nohup $(PY) scripts/invalidator.py > invalidator.log 2>&1 & echo $$! > invalidator.pid
	@sleep 3
	@nohup $(PY) scripts/traffic.py --strategy cdc --ttl 600 --duration 0 > traffic.log 2>&1 & echo $$! > traffic.pid
	@echo "--- 20s healthy soak: invalidation lag = a few ms ---"
	@sleep 20
	@kill -TERM $$(cat invalidator.pid) && rm -f invalidator.pid
	@echo "--- INVALIDATOR DOWN: 20s outage, staleness now grows 1s per second ---"
	@sleep 20
	@nohup $(PY) scripts/invalidator.py >> invalidator.log 2>&1 & echo $$! > invalidator.pid
	@echo "--- invalidator restarted: consuming the backlog, cache heals ---"
	@sleep 15
	@kill -TERM $$(cat traffic.pid); while kill -0 $$(cat traffic.pid) 2>/dev/null; do sleep 0.2; done; rm -f traffic.pid
	@kill -TERM $$(cat invalidator.pid); while kill -0 $$(cat invalidator.pid) 2>/dev/null; do sleep 0.2; done; rm -f invalidator.pid
	$(PY) scripts/auditor.py --strategy cdc --timeline 5 --json --expect-max-at-least 10000
	@echo "✅ drill-cdc: the outage bump is in the timeline above — staleness is bounded by pipeline health, nothing else"

# --- the whole show ----------------------------------------------------------

demo: up install bootstrap seed
	@echo ""
	@echo "=== 1/4: the stale-set race — delete-on-write loses, deterministically ==="
	$(MAKE) drill-stale-set
	@echo ""
	@echo "=== 2/4: the identical interleaving — versioned keys win ==="
	$(MAKE) drill-versioned
	@echo ""
	@echo "=== 3/4: thundering herd on a hot key ==="
	$(MAKE) drill-herd
	@echo ""
	@echo "=== 4/4: journaling soak (ttl / delete / versioned) + staleness audit ==="
	$(MAKE) soak
	$(MAKE) report
	@echo ""
	@echo "🎉 done. For the cdc row of the table: make drill-cdc  (starts kafka+debezium)"
