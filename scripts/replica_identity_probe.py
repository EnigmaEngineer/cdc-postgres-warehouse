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
SLOT_PGO = "probe_slot_pgoutput"
PUBLICATION = "cdc_shop"


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
    pg.drop_slot_if_exists(cur, SLOT_PGO)
    pg.create_slot(cur, SLOT)
    # Two slots over one workload. test_decoding is readable and pgoutput is what a real
    # consumer subscribes to. Reading only the readable one would leave the claim that
    # they agree resting on nothing.
    pg.create_slot(cur, SLOT_PGO, "pgoutput")

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
        pg.drop_slot_if_exists(cur, SLOT_PGO)
        raise SourceRefused(str(exc).strip().splitlines()[0])
    end = pg.current_lsn(cur)

    lines = pg.peek_changes(cur, SLOT)
    summary = decode.summarise(decode.parse_stream(lines))
    msgs, pgo_bytes = pg.peek_binary_changes(cur, SLOT_PGO, PUBLICATION)
    pg.drop_slot_if_exists(cur, SLOT)
    pg.drop_slot_if_exists(cur, SLOT_PGO)

    return {
        "wal_bytes": pg.lsn_bytes(cur, start, end),
        "summary": summary,
        "pgoutput_messages": msgs,
        "pgoutput_bytes": pgo_bytes,
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
    ap.add_argument("--passes", type=int, default=3,
                    help="repeats of each arm. The spread across them is the floor under "
                         "any difference the arms show.")
    args = ap.parse_args(argv)

    con = pg.connect(args.dsn)
    cur = con.cursor()

    kw = dict(seed=args.seed, steps=args.steps,
              customers=args.customers, products=args.products)

    run_arm(cur, args.schema, "DEFAULT", **kw)          # warmup, discarded

    passes = {}
    refused = {}
    for name, mode in (("default", "DEFAULT"), ("full", "FULL"), ("nothing", "NOTHING")):
        runs = []
        for _ in range(args.passes):
            try:
                runs.append(run_arm(cur, args.schema, mode, **kw))
            except SourceRefused as exc:
                refused[name] = str(exc)
                break
        if runs:
            passes[name] = runs

    # The first pass of each arm is the one reported. The rest exist to say how much of
    # any gap between two arms is just noise.
    arms = {k: v[0] for k, v in passes.items()}

    out = {
        "arms": probe.compare(arms),
        "refused_by_source": refused,
        "reproducibility": {
            name: {
                "wal_bytes": probe.spread([r["wal_bytes"] for r in runs]),
                "changes": probe.spread([r["summary"]["changes"] for r in runs]),
                "pgoutput_bytes": probe.spread([r["pgoutput_bytes"] for r in runs]),
            }
            for name, runs in sorted(passes.items())
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
