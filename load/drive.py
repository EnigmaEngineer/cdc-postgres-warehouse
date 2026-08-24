"""One way to drive the workload, shared by every probe that needs a database.

Four scripts used to hold their own copy of this. The copies agreed on the arguments and
that was the problem. `workload.plan` deflects an update against an empty table into an
insert, so the number of rows the planner believes exist changes the first few actions and
therefore every generator draw after them. Two probes here already disagreed about a
published split rate because one of them told the planner the dimension tables were empty
when they were not.

A comment asking the next person to keep two functions in step is the weakest guard
available. This is the function instead.
"""

from load import apply as applier
from load import workload


def seed_row_counts(customers, products):
    """What the planner has to be told already exists before it plans anything.

    The dimension tables are seeded outside the plan, so the planner starts against a
    database that is not empty and has to know it. Every other table starts at zero.
    """
    counts = dict.fromkeys(workload.TABLES, 0)
    counts["customer"] = customers
    counts["product"] = products
    return counts


def run_workload(cur, seed, steps, customers, products):
    """Seed the dimensions, plan, and apply. Returns what really happened, not the target.

    `deflected` is carried out because it is the count that says whether the planner and
    the database agreed about the starting state. A non-zero deflection on a seeded run
    means they did not.
    """
    plan = workload.plan(seed, steps, seed_rows=seed_row_counts(customers, products))
    a = applier.Applier(cur, seed)
    a.adopt_existing()
    seeded = a.seed_dimensions(customers, products)
    applied, skipped = a.run(plan.ops)
    return {"seeded": seeded, "applied": applied, "skipped": skipped,
            "deflected": plan.deflected}
