"""Measure what a topic layout costs on the Postgres side.

The layout question looks like a Kafka question. One topic per table or one topic for
everything, how many partitions, what the key is. All of that is downstream of the
broker and none of it touches the database.

The part that does touch the database is how many connectors you run, because one
connector is one replication slot. Splitting tables across connectors to isolate them is
the obvious way to stop a slow table blocking a fast one, and it is the decision this
probe is about.

The arms:

  plugins       which output plugins this server will actually accept
  slot_names    what happens when two connectors keep the connector's default slot name
  split         one connector on four tables against four connectors on one table each,
                all reading the same log, counted by frame kind
  retention     what it really takes to release the log a slot is holding
  key_split     where the rows of one order land, given the key the connector builds

Run it with the server already up. Bring-up and work go in one script, because the server
does not survive past the shell that started it in some sandboxes.

  python3 -m scripts.topic_layout_probe --steps 600 --seed 11
  python3 -m scripts.topic_layout_probe --passes 1 --markdown
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cdc import decode  # noqa: E402
from cdc import pg  # noqa: E402
from cdc import topics  # noqa: E402
from load import apply as applier  # noqa: E402
from load import drive  # noqa: E402
from load import workload  # noqa: E402

SLOT_A = "layout_wide"
PROBE_SLOTS = (SLOT_A, "debezium", "debezium_second")


def per_table_pub(table):
    return "layout_pub_" + table


def per_table_slot(table):
    return "layout_slot_" + table


def drop_probe_objects(cur):
    for slot in PROBE_SLOTS:
        pg.drop_slot_if_exists(cur, slot)
    for plugin in ("pgoutput", "test_decoding", "decoderbufs", "wal2json"):
        pg.drop_slot_if_exists(cur, "plugin_probe_" + plugin)
    for table in workload.TABLES:
        pg.drop_slot_if_exists(cur, per_table_slot(table))
        cur.execute("drop publication if exists " + per_table_pub(table))


def reset_schema(cur, schema_path):
    """Drop and reload the schema, so a run starts from the same place every time.

    scripts/replica_identity_probe.py does this and this one did not, which is how the
    key split arm ended up reporting a row count that grew every time it ran.
    """
    cur.execute("drop schema if exists shop cascade")
    cur.execute("drop publication if exists cdc_shop")
    with open(schema_path) as handle:
        cur.execute(handle.read())


def arm_plugins(cur):
    """Which output plugins the server accepts, tried rather than assumed."""
    results = {}
    for plugin in ("pgoutput", "test_decoding", "decoderbufs", "wal2json"):
        name = "plugin_probe_" + plugin
        pg.drop_slot_if_exists(cur, name)
        error = pg.try_create_slot(cur, name, plugin)
        results[plugin] = {"accepted": error is None, "error": error}
        pg.drop_slot_if_exists(cur, name)
    return results


def arm_slot_names(cur):
    """Two connectors on the connector's default slot name.

    The point is not that a collision is surprising. It is that the default is a single
    fixed string, so the collision is what you get by leaving two configs alone.
    """
    pg.drop_slot_if_exists(cur, "debezium")
    first = pg.try_create_slot(cur, "debezium", "pgoutput")
    second = pg.try_create_slot(cur, "debezium", "pgoutput")
    renamed = pg.try_create_slot(cur, "debezium_second", "pgoutput")
    out = {
        "first_connector": first,
        "second_connector_same_name": second,
        "second_connector_renamed": renamed,
    }
    pg.drop_slot_if_exists(cur, "debezium")
    pg.drop_slot_if_exists(cur, "debezium_second")
    return out




def arm_filtered_and_laggard(cur, steps, seed, customers, products, schema_path=None):
    """Does a narrow publication retain less, and who sets the retention.

    Both questions come off one workload, because two workloads would differ in the log
    they write and the comparison would be measuring that instead.

    The reset happens per pass rather than once at the start. Resetting once would leave
    the first pass running against an empty database and the rest against a full one, so
    the spread across passes would be measuring the reset instead of the noise.
    """
    drop_probe_objects(cur)
    if schema_path:
        reset_schema(cur, schema_path)
    pg.create_slot(cur, SLOT_A, "pgoutput")
    # One publication and one slot per table, which is what splitting a connector per
    # table really means on the source side. All four are measured rather than one being
    # measured and the rest assumed to look like it.
    for table in workload.TABLES:
        cur.execute("create publication {} for table shop.{}".format(
            per_table_pub(table), table))
        pg.create_slot(cur, per_table_slot(table), "pgoutput")

    plan = workload.plan(seed, steps,
                         seed_rows=drive.seed_row_counts(customers, products))

    start = pg.current_lsn(cur)
    hand = applier.Applier(cur, seed)
    hand.adopt_existing()
    hand.seed_dimensions(customers, products)
    applied, skipped = hand.run(plan.ops)
    end = pg.current_lsn(cur)
    written = pg.lsn_bytes(cur, start, end)

    # Every slot sits on the same log. What differs is the publication each decodes
    # through, which is a filter applied when the frames are built.
    def describe(frames, tables):
        kinds = decode.pgoutput_message_types(frames)
        return {
            "tables": tables,
            "messages": len(frames),
            "stream_bytes": sum(len(f) for f in frames),
            "by_kind": kinds,
            "row_change_share": decode.row_change_share(kinds),
        }

    one_connector = describe(pg.peek_binary_frames(cur, SLOT_A, "cdc_shop"), 4)
    per_table = {
        table: describe(
            pg.peek_binary_frames(cur, per_table_slot(table), per_table_pub(table)), 1)
        for table in workload.TABLES
    }

    def snapshot():
        out = {SLOT_A: pg.slot_positions(cur, SLOT_A)}
        for table in workload.TABLES:
            out[per_table_slot(table)] = pg.slot_positions(cur, per_table_slot(table))
        return out

    positions = {"before_any_read": snapshot()}

    # One consumer keeps up and the rest do not. This is the laggard arm.
    pg.consume_binary_changes(cur, SLOT_A, "cdc_shop")
    positions["after_one_reader"] = snapshot()

    # And then the laggards catch up.
    for table in workload.TABLES:
        pg.consume_binary_changes(cur, per_table_slot(table), per_table_pub(table))
    positions["after_all_read"] = snapshot()

    # Reading everything is not what releases the log. The sets of positions above come
    # back with the same restart position, so the obvious reading of a consumed slot is
    # wrong. The restart position is recalculated from a running transactions record,
    # which the server writes at a checkpoint.
    pg.checkpoint(cur)
    positions["after_checkpoint"] = snapshot()

    # And a checkpoint on its own does not move it either. The record has to be decoded,
    # so somebody has to read the slot again after the checkpoint. Two separate things
    # have to happen and neither of them is the one people name.
    pg.consume_binary_changes(cur, SLOT_A, "cdc_shop")
    for table in workload.TABLES:
        pg.consume_binary_changes(cur, per_table_slot(table), per_table_pub(table))
    positions["after_checkpoint_then_read"] = snapshot()

    drop_probe_objects(cur)

    return {
        "workload_steps": steps,
        "applied": applied,
        "skipped": skipped,
        "log_bytes_written": written,
        "one_connector": one_connector,
        "per_table_connectors": per_table,
        "split_cost": topics.split_cost(one_connector["by_kind"],
                                        [v["by_kind"] for v in per_table.values()]),
        "positions": positions,
    }


def arm_key_split(cur, partitions):
    """Where the real order_item rows land, given the key Debezium builds by default.

    The default key is the primary key, and shop.order_item's primary key carries the
    line number. So the lines of one order are hashed independently. Whether that matters
    is a question about the numbers, not about the design, so here are the numbers.
    """
    cur.execute("select order_id, line_no from shop.order_item order by 1, 2")
    rows = [{"order_id": r[0], "line_no": r[1]} for r in cur.fetchall()]
    if not rows:
        return {"rows": 0, "note": "no order_item rows, nothing to measure"}
    composite = topics.split_rate(rows, ("order_id", "line_no"), "order_id", partitions)
    routed = topics.split_rate(rows, ("order_id",), "order_id", partitions)
    return {
        "rows": len(rows),
        "partitions": partitions,
        "default_key_order_id_and_line_no": composite,
        "message_key_columns_order_id_only": routed,
    }


def markdown_table(one_pass):
    """The frames table, produced rather than transcribed.

    Every cell comes out of topics.frame_split over the counts the probe measured. The
    alternative is adding four framing numbers up by hand next to a table, which is how a
    figure ends up with nothing behind it that could contradict it.
    """
    lines = [
        "| | frames | of which framing | of which row changes | row change share |",
        "|---|---|---|---|---|",
    ]

    def row(label, counts):
        s = topics.frame_split(counts)
        return "| {} | {:,} | {:,} | {:,} | {:.4f} |".format(
            label, s["frames"], s["framing"], s["rows"], s["row_change_share"])

    lines.append(row("one connector, 4 tables", one_pass["one_connector"]["by_kind"]))
    for table, v in one_pass["per_table_connectors"].items():
        lines.append(row("`shop.{}` alone".format(table), v["by_kind"]))

    totals = {}
    for v in one_pass["per_table_connectors"].values():
        for kind, n in v["by_kind"].items():
            totals[kind] = totals.get(kind, 0) + n
    s = topics.frame_split(totals)
    lines.append("| **four connectors, totalled** | **{:,}** | **{:,}** | **{:,}** | {:.4f} |".format(
        s["frames"], s["framing"], s["rows"], s["row_change_share"]))
    return "\n".join(lines)


def spread(passes):
    """Range and relative range for the figures that move between passes.

    Counted things are listed here too, on purpose. A count with a nonzero range is a
    count that is not reproducing, and that is worth seeing rather than assuming.
    """
    def pull(path):
        out = []
        for p in passes:
            node = p
            for key in path:
                node = node[key]
            out.append(node)
        return out

    watched = {
        "log_bytes_written": ("log_bytes_written",),
        "one_connector_messages": ("one_connector", "messages"),
        "one_connector_stream_bytes": ("one_connector", "stream_bytes"),
        "split_framing": ("split_cost", "split_framing"),
        "split_rows": ("split_cost", "split_rows"),
        "framing_multiple": ("split_cost", "framing_multiple"),
    }
    out = {}
    for name, path in watched.items():
        values = pull(path)
        low, high = min(values), max(values)
        out[name] = {
            "values": values,
            "range": high - low,
            "relative_range": (high - low) / low if low else None,
        }
    return out


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", default=pg.DEFAULT_DSN)
    parser.add_argument("--steps", type=int, default=600)
    parser.add_argument("--seed", type=int, default=11)
    parser.add_argument("--customers", type=int, default=200)
    parser.add_argument("--products", type=int, default=80)
    parser.add_argument("--passes", type=int, default=3)
    parser.add_argument("--partitions", type=int, default=6)
    parser.add_argument("--prefix", default="shopcdc")
    parser.add_argument("--schema", default=os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "db", "schema.sql"))
    parser.add_argument("--no-reset", dest="reset", action="store_false",
                        help="leave whatever is in the database alone")
    parser.add_argument("--markdown", action="store_true",
                        help="print the frames table instead of the full JSON")
    args = parser.parse_args()

    con = pg.connect(args.dsn)
    try:
        with con.cursor() as cur:
            drop_probe_objects(cur)

            plugins = arm_plugins(cur)
            slot_names = arm_slot_names(cur)
            # A message count is a counted quantity and a log volume is not. The first
            # has to reproduce exactly and the second moves between passes, so a ratio
            # built on the second needs its spread published beside it.
            # The key split arm reads the order_item rows that are really there, so
            # without a reset it reports a row count that depends on how many times the
            # probe has run. The rate held steady at 504 rows and at 693, which is why it
            # took a second look to notice the count was not a fact about anything.
            passes = [arm_filtered_and_laggard(
                cur, args.steps, args.seed, args.customers, args.products,
                args.schema if args.reset else None)
                for _ in range(args.passes)]
            published = pg.publication_tables(cur, "cdc_shop")
            keys = {
                "{}.{}".format(s, t): pg.primary_key_columns(cur, "{}.{}".format(s, t))
                for s, t in published
            }
            key_split = arm_key_split(cur, args.partitions)
    finally:
        with con.cursor() as cur:
            drop_probe_objects(cur)
        con.close()

    report = {
        "plugins": plugins,
        "slot_names": slot_names,
        "passes": passes,
        "spread": spread(passes),
        "key_split": key_split,
        "publication": {"name": "cdc_shop", "tables": [list(t) for t in published],
                        "primary_keys": keys},
    }
    if args.markdown:
        print(markdown_table(passes[-1]))
    else:
        print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
