SHELL := /bin/bash
VENV := .venv
PY := $(VENV)/bin/python
export SEED_ROWS ?= 200000
export GATEWAY_PORT ?= 8080

.PHONY: help up down clean install bootstrap seed status \
        traffic-start traffic-stop traffic-stop-any \
        drill-naive reset expand deploy-v15 backfill verify deploy-v2 relax \
        deploy-v3 contract drill-early-contract drill-crash-backfill \
        drill-rollback demo

help:
	@echo "Setup:"
	@echo "  make up install bootstrap seed    infra + 200k users, blue pair on v1"
	@echo "  make traffic-start                correctness-asserting traffic (background)"
	@echo "  make status                       who is deployed, who has traffic"
	@echo "The naive path (watch it burn):"
	@echo "  make drill-naive                  ALTER+deploy coupled, rolling -> counted carnage"
	@echo "  make reset                        back to pristine v1 to run the good path"
	@echo "The expand/contract ladder (in order, under traffic):"
	@echo "  make expand                       add first/last columns (INSTANT, nullable)"
	@echo "  make deploy-v15                   rolling: dual-write, still read old"
	@echo "  make backfill                     chunked resumable backfill of old rows"
	@echo "  make verify                       shape agreement + journal + acked-write audit"
	@echo "  make deploy-v2                    BLUE-GREEN: flip reads to new columns"
	@echo "  make relax                        make name nullable (v3 can't ship before this)"
	@echo "  make deploy-v3                    rolling: stop writing old column"
	@echo "  make contract                     prove no readers/writers of name, then DROP"
	@echo "  make traffic-stop                 stop traffic (nonzero exit if any failure)"
	@echo "Drills:"
	@echo "  make drill-early-contract         drop the column too early -> outage, restore"
	@echo "  make drill-crash-backfill         SIGKILL backfill mid-run; rerun resumes"
	@echo "  make drill-rollback               v2 -> v1.5 -> v2 under traffic, 0 errors"
	@echo "One shot:"
	@echo "  make demo                         naive carnage vs clean ladder, back to back"
	@echo "  make clean                        destroy everything"

# state/ must exist (and the gateway needs a valid upstream set) BEFORE compose
# starts, otherwise docker creates root-owned dirs / nginx refuses to boot.
state/nginx/upstreams.conf:
	mkdir -p state/nginx
	printf 'upstream app {\n    server app-blue-a:8000 max_fails=0;\n    server app-blue-b:8000 max_fails=0;\n}\n' > $@

up: state/nginx/upstreams.conf
	docker compose up -d --build --wait mysql gateway app-blue-a app-blue-b

down:
	docker compose --profile green down

clean:
	docker compose --profile green down -v
	rm -rf state
	rm -f client_journal.jsonl client_state.json backfill_state.json \
	      traffic.pid traffic.log backfill.pid backfill.log demo.log

install:
	@test -d $(VENV) || python3 -m venv $(VENV)
	$(PY) -m pip install -q -r requirements.txt

bootstrap:
	$(PY) scripts/bootstrap.py

seed:
	$(PY) scripts/seed.py

status:
	$(PY) scripts/deploy.py status

# --- traffic -----------------------------------------------------------------

traffic-start:
	@if [ -f traffic.pid ] && kill -0 $$(cat traffic.pid) 2>/dev/null; then \
		echo "traffic already running (pid $$(cat traffic.pid))"; \
	else \
		nohup $(PY) scripts/client.py > traffic.log 2>&1 & echo $$! > traffic.pid; \
		sleep 2; echo "traffic started (pid $$(cat traffic.pid), log: traffic.log)"; \
	fi

# Gated stop: exits nonzero unless the client saw ZERO failures. Use after the
# good path. Drills that EXPECT carnage use traffic-stop-any + verify instead.
traffic-stop: traffic-stop-any
	@if ! tail -1 traffic.log | grep -q " 0 FAILURES,"; then \
		echo "❌ traffic recorded failures (or died early) — see traffic.log"; exit 2; \
	fi

traffic-stop-any:
	@if [ -f traffic.pid ]; then \
		kill -TERM $$(cat traffic.pid) 2>/dev/null || true; \
		while kill -0 $$(cat traffic.pid) 2>/dev/null; do sleep 0.2; done; \
		rm -f traffic.pid; echo "traffic stopped:"; tail -1 traffic.log; \
	else echo "traffic not running"; fi

# --- the naive path ----------------------------------------------------------

# One "deploy": migrate the schema all the way (add + copy + DROP name), then
# roll the pods. Everything in between is the outage, and it is measured.
drill-naive:
	$(MAKE) traffic-start
	@sleep 5
	@echo "--- running the coupled migration (add + copy + DROP) under traffic ---"
	$(PY) scripts/naive.py
	@echo "--- rolling v1 -> v3 while the schema is already contracted ---"
	$(PY) scripts/deploy.py --strategy rolling --version v3 --pause 4
	@sleep 5
	$(MAKE) traffic-stop-any
	$(PY) scripts/verify.py report --expect-carnage
	$(PY) scripts/verify.py audit || true
	@echo ""
	@echo "☠️  that was the naive deploy. run 'make reset' then the ladder."

reset: traffic-stop-any
	$(PY) scripts/reset.py
	$(PY) scripts/seed.py

# --- the expand/contract ladder ----------------------------------------------

expand:
	$(PY) scripts/expand.py

deploy-v15:
	$(PY) scripts/deploy.py --strategy rolling --version v1.5

backfill:
	$(PY) scripts/backfill.py

verify:
	$(PY) scripts/verify.py

deploy-v2:
	$(PY) scripts/deploy.py --strategy bluegreen --version v2

relax:
	$(PY) scripts/contract.py --relax

deploy-v3:
	$(PY) scripts/deploy.py --strategy rolling --version v3

contract:
	$(PY) scripts/contract.py

# --- drills ------------------------------------------------------------------

# Contract while v1.5 (which still reads AND writes `name`) is serving: total
# outage, counted — then prove the dual-write makes it restorable with zero
# acked-write loss. Run after: expand deploy-v15 backfill.
drill-early-contract:
	$(MAKE) traffic-start
	@sleep 3
	@echo "--- DROPPING name while v1.5 still uses it (--force skips the checks) ---"
	$(PY) scripts/contract.py --force
	@sleep 6
	@echo "--- restoring: re-add name, refill from first/last ---"
	$(PY) scripts/contract.py --restore
	@sleep 5
	$(MAKE) traffic-stop-any
	$(PY) scripts/verify.py report --expect-carnage
	@echo "--- but: dual-write means NO acked write was lost ---"
	$(PY) scripts/verify.py audit
	$(PY) scripts/verify.py shapes
	@echo "✅ drill-early-contract: outage counted, restore proven lossless"

# SIGKILL the backfill mid-run, rerun it: must RESUME from the chunk journal,
# and the shape check must come out clean.
drill-crash-backfill:
	@echo "--- starting throttled backfill, will SIGKILL it mid-run ---"
	@nohup $(PY) scripts/backfill.py --chunk-sleep-ms 40 > backfill.log 2>&1 & echo $$! > backfill.pid
	@sleep 6
	@kill -9 $$(cat backfill.pid) 2>/dev/null && echo "--- backfill SIGKILLed ---" \
		|| { echo "backfill finished before the kill — rerun with bigger SEED_ROWS"; exit 1; }
	@rm -f backfill.pid
	@echo "--- rerunning: must RESUME, not restart ---"
	@$(PY) scripts/backfill.py 2>&1 | tee /dev/stderr | grep -q RESUMING \
		|| { echo "❌ backfill did not resume from its journal"; exit 1; }
	$(PY) scripts/verify.py shapes
	@echo "✅ drill-crash-backfill: crash, resume, clean shapes"

# Roll back from v2 (reads new) to v1.5 (reads old) and forward again, under
# traffic, zero errors — possible ONLY because both versions dual-write.
# Run after deploy-v2.
drill-rollback:
	$(MAKE) traffic-start
	@sleep 3
	@echo "--- rollback: blue-green flip back to v1.5 ---"
	$(PY) scripts/deploy.py --strategy bluegreen --version v1.5
	@sleep 5
	@echo "--- forward again: v2 ---"
	$(PY) scripts/deploy.py --strategy bluegreen --version v2
	@sleep 5
	$(MAKE) traffic-stop
	@echo "✅ drill-rollback: v2 -> v1.5 -> v2, zero client failures"

# --- the whole show ----------------------------------------------------------

demo: up install bootstrap seed
	@echo ""
	@echo "==================== ACT 1: the naive deploy ===================="
	$(MAKE) drill-naive
	$(MAKE) reset
	@echo ""
	@echo "==================== ACT 2: expand -> migrate -> contract ===================="
	$(MAKE) traffic-start
	@sleep 5
	@echo "=== step 1: EXPAND — add nullable first/last (INSTANT) ==="
	$(MAKE) expand
	@echo "=== step 2: deploy v1.5 rolling — dual-write, read old ==="
	$(MAKE) deploy-v15
	@echo "=== step 3: MIGRATE — chunked resumable backfill ==="
	$(MAKE) backfill
	$(PY) scripts/verify.py shapes
	@echo "=== step 4: deploy v2 BLUE-GREEN — flip reads to new columns ==="
	$(MAKE) deploy-v2
	@sleep 5
	@echo "=== step 5: RELAX — name nullable, so code may stop writing it ==="
	$(MAKE) relax
	@echo "=== step 6: deploy v3 rolling — stop writing name ==="
	$(MAKE) deploy-v3
	@echo "=== step 7: CONTRACT — prove name is dead, then DROP it ==="
	$(MAKE) contract
	@sleep 5
	$(MAKE) traffic-stop
	$(PY) scripts/verify.py report --expect-clean
	$(PY) scripts/verify.py audit
	@echo ""
	@echo "🎉 same schema change, twice: naive = counted outage + lost writes;"
	@echo "   expand/contract = 0 failures, 0 lost writes, column gone."
	@echo "   try: make drill-early-contract   (after re-running the ladder)"
