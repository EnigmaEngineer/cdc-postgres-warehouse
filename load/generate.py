"""Run a workload against the source database and print what it did as JSON.

Usage:
    python -m load.generate --seed 7 --steps 2000 --customers 200 --products 80

This script reads arguments and prints. Everything it reports is computed in
load/workload.py and load/apply.py, which have tests over them. A figure produced by a
script nothing tests is a figure nothing can contradict.
"""

import argparse
import json
import sys

from cdc import pg
from load import apply as applier
from load import workload


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", default=pg.DEFAULT_DSN)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--steps", type=int, default=2000)
    ap.add_argument("--customers", type=int, default=200)
    ap.add_argument("--products", type=int, default=80)
    args = ap.parse_args(argv)

    con = pg.connect(args.dsn)
    cur = con.cursor()

    a = applier.Applier(cur, args.seed)
    existing = a.adopt_existing()

    # The plan needs to know what is already there. Handing it zeroes against a populated
    # database makes it deflect operations that were perfectly possible.
    seed_rows = dict(existing)
    seed_rows["customer"] = max(existing["customer"], args.customers)
    seed_rows["product"] = max(existing["product"], args.products)
    p = workload.plan(args.seed, args.steps, seed_rows=seed_rows)

    start = pg.current_lsn(cur)
    added = a.seed_dimensions(args.customers, args.products)
    applied, skipped = a.run(p.ops)
    end = pg.current_lsn(cur)

    out = {
        "seed": args.seed,
        "steps": args.steps,
        "existing_before": existing,
        "seeded": added,
        "planned": workload.action_totals(p),
        "planned_by_table": workload.counts(p),
        "deflected": p.deflected,
        "applied": applied,
        "skipped": skipped,
        "wal_bytes": pg.lsn_bytes(cur, start, end),
        "final_rows": p.final_rows,
    }
    cur.close()
    con.close()
    print(json.dumps(out, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
