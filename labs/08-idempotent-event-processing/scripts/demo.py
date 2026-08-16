"""`make demo`: run drill 1 (naive) and drill 2 (idempotent) back to back —
same producer duplicates, same crash schedule — and print the before/after
comparison table. Each drill writes audit.json; this script just collects.
"""
import json
import subprocess
import sys

from common import LAB_DIR, log


def run_drill(target: str) -> dict:
    log.info("### make %s", target)
    rc = subprocess.run(["make", "--no-print-directory", target], cwd=LAB_DIR).returncode
    if rc != 0:
        log.error("make %s failed (rc=%d)", target, rc)
        sys.exit(rc)
    return json.loads((LAB_DIR / "audit.json").read_text())


def main() -> None:
    naive = run_drill("drill-doublecharge")
    idem = run_drill("drill-idempotent")

    print()
    print("=" * 70)
    print("SAME KAFKA, SAME DUPLICATES, SAME CRASH SCHEDULE — ONE DIFFERENCE:")
    print("the dedupe INSERT rides the same DB transaction as the effect.")
    print("=" * 70)
    print(f"{'':28}{'naive consumer':>18}{'idempotent':>18}")
    print(f"{'unique payments':28}{naive['payments']:>18}{idem['payments']:>18}")
    print(f"{'accounts corrupted':28}{naive['accounts_corrupted']:>18}{idem['accounts_corrupted']:>18}")
    print(f"{'money over-applied':28}{'$' + format(naive['overcharge_cents'] / 100, ',.2f'):>18}"
          f"{'$' + format(idem['overcharge_cents'] / 100, ',.2f'):>18}")
    print(f"{'money lost':28}{'$' + format(naive['lost_cents'] / 100, ',.2f'):>18}"
          f"{'$' + format(idem['lost_cents'] / 100, ',.2f'):>18}")
    print(f"{'verdict':28}{naive['verdict']:>18}{idem['verdict']:>18}")
    print("=" * 70)
    print("Next: make drill-ghost-publish, make drill-poison. Then break it yourself.")


if __name__ == "__main__":
    main()
