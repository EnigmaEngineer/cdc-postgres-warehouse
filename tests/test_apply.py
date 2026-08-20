"""Checks over the applier that do not need a server.

Only the paths that decide something are here. Everything that builds SQL is exercised by
scripts/replica_identity_probe.py against a real database instead, and that split is named
in the README.
"""

from load import apply as applier
from load import workload


class FakeCursor:
    """Records statements and never returns a row. Any path that reads one will fail."""

    def __init__(self):
        self.statements = []

    def execute(self, sql, params=None):
        self.statements.append(sql)

    def fetchone(self):
        raise AssertionError("this check must not reach a path that reads a row")


def check_an_operation_the_applier_cannot_perform_is_loud():
    a = applier.Applier(FakeCursor(), seed=1)
    a.ids["customer"] = [1, 2, 3]
    try:
        a.run([workload.Op("delete", "customer")])
    except NotImplementedError as exc:
        assert "delete_customer" in str(exc), exc
        return
    raise AssertionError("a missing operation must raise rather than count as skipped")


def check_an_operation_on_an_empty_table_is_skipped_and_counted():
    a = applier.Applier(FakeCursor(), seed=1)
    applied, skipped = a.run([workload.Op("update", "customer")])
    assert skipped == 1
    assert applied == {"insert": 0, "update": 0, "delete": 0}


def check_the_default_weights_never_ask_for_an_operation_that_does_not_exist():
    # The guard above is only useful if something checks the shipped configuration
    # against it. This is that check.
    p = workload.plan(3, 3000, seed_rows={"customer": 10, "product": 10,
                                          "order_header": 10, "order_item": 10})
    for op in p.ops:
        name = "{}_{}".format(op.action, op.table)
        assert hasattr(applier.Applier, name), "no Applier.{}".format(name)
