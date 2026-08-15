SHELL := /bin/bash
VENV := .venv
PY := $(VENV)/bin/python
export SEED_ROWS ?= 500000
export N_USERS ?= 2000

.PHONY: help up down clean install bootstrap seed traffic-start traffic-stop \
        double-write backfill verify shadow-read cutover rollback \
        drill-crash-backfill drill-detect drill-rollback demo

help:
	@echo "Ladder (in order):"
	@echo "  make up install bootstrap seed    infra + 500k rows on mono"
	@echo "  make traffic-start                live user traffic (background)"
	@echo "  make double-write                 rung 1: writes hit mono AND owning shard"
	@echo "  make backfill                     rung 2: chunked resumable copy to shards"
	@echo "  make verify                       rung 3: counts+checksums+misplacement+journal"
	@echo "  make shadow-read                  rung 4: reads rehearsed on shards, diffs journaled"
	@echo "  make cutover                      rung 5: shards authoritative (mono mirrored)"
	@echo "  make rollback                     back to mono (drain queue + atomic flip)"
	@echo "  make traffic-stop                 stop traffic (nonzero exit if any check failed)"
	@echo "Drills:"
	@echo "  make drill-crash-backfill         SIGKILL backfill mid-run; rerun resumes; verify clean"
	@echo "  make drill-detect                 corrupt a shard row; verifier MUST catch it; repair"
	@echo "  make drill-rollback               roll back after cutover under live traffic"
	@echo "One shot:"
	@echo "  make demo                         full ladder under traffic, end-to-end verified"
	@echo "  make clean                        destroy everything (containers, volumes, state)"

up:
	docker compose up -d --wait mono shard0 shard1

down:
	docker compose down

clean:
	docker compose down -v
	rm -f router_state.json journal.jsonl repair_queue.jsonl shadow_diffs.jsonl \
	      backfill_state.json inject_state.json traffic.pid traffic.log backfill.pid backfill.log

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
		while kill -0 $$(cat traffic.pid) 2>/dev/null; do sleep 0.2; done; \
		rm -f traffic.pid; echo "traffic stopped:"; tail -1 traffic.log; \
		if ! tail -1 traffic.log | grep -q " 0 FAILURES,"; then \
			echo "❌ traffic recorded failures (or died early) — see traffic.log"; exit 2; \
		fi \
	else echo "traffic not running"; fi

double-write:
	$(PY) scripts/phase.py double-write

backfill:
	$(PY) scripts/backfill.py --repair

verify:
	$(PY) scripts/verify.py

shadow-read:
	$(PY) scripts/phase.py shadow-read

cutover:
	$(PY) scripts/phase.py sharded

rollback:
	$(PY) scripts/rollback.py

# --- drills ------------------------------------------------------------------

# SIGKILL the backfill mid-run, then rerun it: the chunk journal resumes, the
# replayed chunk is INSERT IGNOREd, and the data-level checks come out clean.
drill-crash-backfill:
	@echo "--- starting throttled backfill, will SIGKILL it mid-run ---"
	@nohup $(PY) scripts/backfill.py --chunk-sleep-ms 40 > backfill.log 2>&1 & echo $$! > backfill.pid
	@sleep 6
	@kill -9 $$(cat backfill.pid) 2>/dev/null && echo "--- backfill SIGKILLed (kill -9) ---" \
		|| { echo "backfill finished before the kill — rerun with a bigger SEED_ROWS"; exit 1; }
	@rm -f backfill.pid; grep -E "RESUMING|rows in" backfill.log | tail -2 || true
	@echo "--- rerunning backfill: must RESUME, not restart ---"
	$(PY) scripts/backfill.py 2>&1 | tee /dev/stderr | grep -q RESUMING \
		|| { echo "❌ backfill did not resume from the chunk journal"; exit 1; }
	$(PY) scripts/verify.py --data-only
	@echo "✅ drill-crash-backfill: crash, resume, clean verify"

# Corrupt one shard row (same row COUNT, different bytes), prove verify FAILS,
# repair from mono, prove verify passes again.
drill-detect:
	$(PY) scripts/inject.py --action corrupt
	@echo "--- verify MUST fail now ---"
	@if $(PY) scripts/verify.py --data-only; then \
		echo "❌ verifier MISSED injected corruption — it is decorative"; exit 1; \
	else echo "✅ verifier caught the corruption"; fi
	$(PY) scripts/inject.py --repair
	$(PY) scripts/verify.py --data-only
	@echo "✅ drill-detect: inject, detect, repair, clean verify"

# After cutover: roll back to mono under live traffic. Reconciliation happens
# INSIDE rollback.py, pre-flip (post-flip mono runs ahead of the shards by
# design). The post-flip proof is the app's own checks + journal replay.
drill-rollback:
	$(PY) scripts/rollback.py
	@echo "--- 5s of traffic against mono as the authoritative node again ---"
	@sleep 5
	$(MAKE) traffic-stop
	$(PY) scripts/verify.py
	@echo "✅ drill-rollback: mono authoritative again, traffic never stopped, journal replays clean"

# --- the whole ladder --------------------------------------------------------

demo: up install bootstrap seed traffic-start
	@echo "=== rung 0: 10s of traffic against the monolith alone ==="
	@sleep 10
	@echo "=== rung 1: double-write (mono authoritative, shards shadowed) ==="
	$(MAKE) double-write
	@sleep 5
	@echo "=== rung 2: backfill (chunked, resumable, INSERT IGNORE) ==="
	$(MAKE) backfill
	@echo "=== rung 3: pre-cutover verification gate (under live traffic) ==="
	$(PY) scripts/verify.py --data-only
	@echo "=== rung 4: shadow-read soak (reads rehearsed on shards) ==="
	$(MAKE) shadow-read
	@sleep 15
	@echo "=== rung 5: CUTOVER — shards authoritative, mono mirrored ==="
	$(MAKE) cutover
	@sleep 10
	$(MAKE) traffic-stop
	$(MAKE) verify
	@echo ""
	@echo "🎉 reshard complete: 1 monolith -> 2 shards, zero downtime, verified."
	@echo "   try: make traffic-start drill-rollback traffic-stop"
