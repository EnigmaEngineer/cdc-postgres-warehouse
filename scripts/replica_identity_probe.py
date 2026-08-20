"""Measure what a table's replica identity costs and what it buys.

Runs the same seeded workload under two settings and reports both. The arms are run in a
fixed order, so a discarded warmup arm goes first. Without it the first arm pays for the
connection, the plan cache and the first write ahead log segment, and the cost lands on an
arm rather than on an observation.

    python -m scripts.replica_identity_probe --steps 2000 --seed 7

Everything printed comes out of cdc/decode.py and cdc/probe.py, both of which have tests.
The database plumbing below does not, and that is named in the README limitations.
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2  # noqa: E402

from cdc import decode, pg, probe  # noqa: E402
from load import apply as applier  # noqa: E402
from load import workload  # noqa: E402

TABLES = ("shop.customer", "shop.product", "shop.order_header", "shop.order_item")
SLOT = "probe_slot"


class SourceRefused(Exception):
    """The source database would not accept the workload under this setting.

    Worth its own type. A setting that loses information downstream and a setting the
    source rejects up front are different answers, and folding them together loses the
    only one that is a hard stop.
    """


def reset(cur, schema_path):
    cur.execute("drop schema if exists shop cascade")
    cur.execute("drop publication if exists cdc_shop")
    with open(schema_path) as fh:
        cur.execute(fh.read())


def run_arm(cur, schema_path, mode, seed, steps, customers, products):
    reset(cur, schema_path)
    for t in TABLES:
        pg.set_replica_identity(cur, t, mode)

    pg.drop_slot_if_exists(cur, SLOT)
    pg.create_slot(cur, SLOT)

    seed_rows = dict.fromkeys(workload.TABLES, 0)
    seed_rows["customer"] = customers
    seed_rows["product"] = products
    plan = workload.plan(seed, steps, seed_rows=seed_rows)

    start = pg.current_lsn(cur)
    a = applier.Applier(cur, seed)
    try:
        a.seed_dimensions(customers, products)
        applied, skipped = a.run(plan.ops)
    except psycopg2.errors.ObjectNotInPrerequisiteState as exc:
        cur.connection.rollback()
        pg.drop_slot_if_exists(cur, SLOT)
        raise SourceRefused(str(exc).strip().splitlines()[0])
    end = pg.current_lsn(cur)

    lines = pg.peek_changes(cur, SLOT)
    summary = decode.summarise(decode.parse_stream(lines))
    pg.drop_slot_if_exists(cur, SLOT)

    return {
        "wal_bytes": pg.lsn_bytes(cur, start, end),
        "summary": summary,
        "applied": applied,
        "skipped": skipped,
        "deflected": plan.deflected,
        "relreplident": [pg.replica_identity(cur, t) for t in TABLES],
    }


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", default=pg.DEFAULT_DSN)
    ap.add_argument("--schema", default=os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "db", "schema.sql"))
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--steps", type=int, default=2000)
    ap.add_argument("--customers", type=int, default=200)
    ap.add_argument("--products", type=int, default=80)
    args = ap.parse_args(argv)

    con = pg.connect(args.dsn)
    cur = con.cursor()

    kw = dict(seed=args.seed, steps=args.steps,
              customers=args.customers, products=args.products)

    run_arm(cur, args.schema, "DEFAULT", **kw)          # warmup, discarded

    arms = {}
    refused = {}
    for name, mode in (("default", "DEFAULT"), ("full", "FULL"), ("nothing", "NOTHING")):
        try:
            arms[name] = run_arm(cur, args.schema, mode, **kw)
        except SourceRefused as exc:
            refused[name] = str(exc)

    # Repeat of the first real arm. If this does not match, the arms are measuring drift
    # rather than the setting, and nothing below is worth reading.
    repeat = run_arm(cur, args.schema, "DEFAULT", **kw)

    out = {
        "arms": probe.compare(arms),
        "refused_by_source": refused,
        "reproducibility": {
            "default_wal_bytes_first": arms["default"]["wal_bytes"],
            "default_wal_bytes_repeat": repeat["wal_bytes"],
            "default_changes_first": arms["default"]["summary"]["changes"],
            "default_changes_repeat": repeat["summary"]["changes"],
        },
        "applied": arms["default"]["applied"],
        "skipped": arms["default"]["skipped"],
        "deflected": arms["default"]["deflected"],
        "by_table_default": arms["default"]["summary"]["by_table"],
        "relreplident": {k: v["relreplident"] for k, v in sorted(arms.items())},
    }
    cur.close()
    con.close()
    print(json.dumps(out, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
