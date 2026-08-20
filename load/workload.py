"""Decide what the load generator is going to do before it touches a database.

Planning and executing are split for one reason. A plan is a value, so a test can assert
on it without a server, and two runs of the same seed can be compared byte for byte. That
matters here because every change count this repo publishes is a property of the plan.

The mix is weighted toward updates because that is what an operational database does. A
warehouse sync that only has to handle inserts is an append and not a sync.
"""

import random
from collections import namedtuple

Op = namedtuple("Op", "action table")
Plan = namedtuple("Plan", "ops deflected final_rows")

TABLES = ("customer", "product", "order_header", "order_item")

# insert, update, delete. Deletes are rare and only order_item takes a hard one. The other
# three deactivate or cancel instead, which is what a real system does, and which is why a
# CDC consumer that only handles DELETE misses most of the removals.
DEFAULT_WEIGHTS = {
    "customer": (0.20, 0.80, 0.00),
    "product": (0.10, 0.90, 0.00),
    "order_header": (0.45, 0.55, 0.00),
    "order_item": (0.50, 0.30, 0.20),
}

TABLE_WEIGHTS = (0.10, 0.10, 0.35, 0.45)


def plan(seed, steps, weights=None, seed_rows=None):
    """Build a feasible op list. Returns the ops, what was deflected, and the row counts.

    Deflection happens when the mix asks for an update or a delete on a table with nothing
    in it. The op is not silently dropped and it is not silently retried until it fits.
    It becomes an insert on the same table and the swap is counted, because a knob whose
    target the code cannot always hit has to report what it really did.
    """
    weights = weights or DEFAULT_WEIGHTS
    rows = dict(seed_rows or {t: 0 for t in TABLES})
    rng = random.Random(seed)
    ops = []
    deflected = {"empty_table": 0}

    for _ in range(steps):
        table = rng.choices(TABLES, weights=TABLE_WEIGHTS, k=1)[0]
        action = rng.choices(("insert", "update", "delete"), weights=weights[table], k=1)[0]

        if action in ("update", "delete") and rows[table] == 0:
            action = "insert"
            deflected["empty_table"] += 1

        if action == "insert":
            rows[table] += 1
        elif action == "delete":
            rows[table] -= 1

        ops.append(Op(action, table))

    return Plan(tuple(ops), deflected, dict(rows))


def counts(plan_):
    """Ops by table and action. This is the denominator for every ratio in the README."""
    out = {}
    for op in plan_.ops:
        key = "{}:{}".format(op.table, op.action)
        out[key] = out.get(key, 0) + 1
    return out


def action_totals(plan_):
    out = {"insert": 0, "update": 0, "delete": 0}
    for op in plan_.ops:
        out[op.action] += 1
    return out
