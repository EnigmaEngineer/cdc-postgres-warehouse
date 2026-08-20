"""Turn a plan into real rows in Postgres.

Every statement here goes through one function per table so that the SQL a change stream
will later carry lives in one place. Building it inline in a loop is how you end up with
two writers that disagree about what an update touches.
"""

import random

COUNTRIES = ("GB", "US", "DE", "FR", "IE", "NL", "ES", "PL")
STATUSES = ("pending", "paid", "shipped", "cancelled")


class Applier:
    """Holds the live primary keys so an update or a delete can pick a real row.

    The pool is what makes the run reproducible. Postgres assigns the identity values, so
    the ids are read back rather than guessed, and the choice of which id to touch comes
    off a seeded generator that never sees the database.
    """

    def __init__(self, cur, seed):
        self.cur = cur
        self.rng = random.Random(seed * 7919 + 1)
        self.ids = {"customer": [], "product": [], "order_header": [], "order_item": []}

    def _pick(self, table):
        return self.rng.choice(self.ids[table])

    def adopt_existing(self):
        """Load whatever is already in the database into the pool.

        Without this a second run of the generator inserts user0@example.com again and
        dies on the unique index. The names come off a counter, so the counter has to
        start past every value already used rather than at zero.
        """
        self.cur.execute("select customer_id from shop.customer order by customer_id")
        self.ids["customer"] = [r[0] for r in self.cur.fetchall()]
        self.cur.execute("select product_id from shop.product order by product_id")
        self.ids["product"] = [r[0] for r in self.cur.fetchall()]
        self.cur.execute("select order_id from shop.order_header order by order_id")
        self.ids["order_header"] = [r[0] for r in self.cur.fetchall()]
        self.cur.execute("select order_id, line_no from shop.order_item order by 1, 2")
        self.ids["order_item"] = [tuple(r) for r in self.cur.fetchall()]
        return {k: len(v) for k, v in self.ids.items()}

    def seed_dimensions(self, customers, products):
        """Top up to the target rather than insert the target. Idempotent by arithmetic."""
        added = {"customer": 0, "product": 0}
        for _ in range(max(0, customers - len(self.ids["customer"]))):
            self.insert_customer()
            added["customer"] += 1
        for _ in range(max(0, products - len(self.ids["product"]))):
            self.insert_product()
            added["product"] += 1
        return added

    def insert_customer(self):
        # The label comes from the database so a second run cannot collide with the first.
        self.cur.execute(
            "insert into shop.customer (email, full_name, country)"
            " select 'user' || n || '@example.com', 'User ' || n, %s"
            " from nextval('shop.load_label_seq') as n"
            " returning customer_id",
            (self.rng.choice(COUNTRIES),),
        )
        self.ids["customer"].append(self.cur.fetchone()[0])

    def update_customer(self):
        cid = self._pick("customer")
        self.cur.execute(
            "update shop.customer set country = %s, updated_at = now() where customer_id = %s",
            (self.rng.choice(COUNTRIES), cid),
        )

    def insert_product(self):
        self.cur.execute(
            "insert into shop.product (sku, name, price_cents)"
            " select 'SKU-' || lpad(n::text, 6, '0'), 'Product ' || n, %s"
            " from nextval('shop.load_label_seq') as n"
            " returning product_id",
            (self.rng.randrange(199, 9999),),
        )
        self.ids["product"].append(self.cur.fetchone()[0])

    def update_product(self):
        pid = self._pick("product")
        # Price churn is the interesting update. It changes one narrow column, which is
        # exactly the case where sending a whole before image is most obviously wasteful.
        self.cur.execute(
            "update shop.product set price_cents = %s, updated_at = now() where product_id = %s",
            (self.rng.randrange(199, 9999), pid),
        )

    def insert_order_header(self):
        if not self.ids["customer"]:
            self.insert_customer()
        self.cur.execute(
            "insert into shop.order_header (customer_id, status) values (%s, 'pending')"
            " returning order_id",
            (self._pick("customer"),),
        )
        self.ids["order_header"].append(self.cur.fetchone()[0])

    def update_order_header(self):
        oid = self._pick("order_header")
        self.cur.execute(
            "update shop.order_header set status = %s, updated_at = now() where order_id = %s",
            (self.rng.choice(STATUSES), oid),
        )

    def insert_order_item(self):
        if not self.ids["order_header"]:
            self.insert_order_header()
        if not self.ids["product"]:
            self.insert_product()
        oid = self._pick("order_header")
        self.cur.execute(
            "select coalesce(max(line_no), 0) + 1 from shop.order_item where order_id = %s",
            (oid,),
        )
        line = self.cur.fetchone()[0]
        self.cur.execute(
            "insert into shop.order_item (order_id, line_no, product_id, qty, unit_cents)"
            " values (%s, %s, %s, %s, %s)",
            (oid, line, self._pick("product"), self.rng.randrange(1, 5),
             self.rng.randrange(199, 9999)),
        )
        self.ids["order_item"].append((oid, line))

    def update_order_item(self):
        oid, line = self._pick("order_item")
        self.cur.execute(
            "update shop.order_item set qty = %s where order_id = %s and line_no = %s",
            (self.rng.randrange(1, 9), oid, line),
        )

    def delete_order_item(self):
        key = self._pick("order_item")
        self.ids["order_item"].remove(key)
        self.cur.execute(
            "delete from shop.order_item where order_id = %s and line_no = %s", key
        )

    def run(self, ops):
        applied = {"insert": 0, "update": 0, "delete": 0}
        skipped = 0
        for op in ops:
            name = "{}_{}".format(op.action, op.table)
            fn = getattr(self, name, None)
            if fn is None:
                # A weight table that asks for an operation this class cannot perform is a
                # configuration mistake and it has to be loud. Counting it as skipped would
                # let a whole arm of the workload quietly do nothing.
                raise NotImplementedError(
                    "the plan asked for {} and Applier has no {}".format(op, name))
            if not self.ids[op.table] and op.action != "insert":
                # The plan already deflects this case. Reaching it means the plan and the
                # applier disagree about the population, which is a bug and not a warning.
                skipped += 1
                continue
            fn()
            applied[op.action] += 1
        return applied, skipped
