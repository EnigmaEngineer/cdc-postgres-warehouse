"""Re-run the commands this README publishes and grade every figure it quotes.

    python3 -m scripts.reproduction_report --markdown

Needs a running Postgres. `scripts/bootstrap-local.sh` first, in the same shell if the
server does not survive between them where you are.

Two things make this different from writing the numbers down and eyeballing them.

The published value is read out of `README.md` at run time by
`warehouse/reproduce.py`, addressed by heading and row and column. Nothing here transcribes
a number. A figure whose row the README has lost raises instead of passing.

And the commands are the ones the README tells a reader to run, character for character,
run as subprocesses. So a probe whose interface moved fails here rather than in the first
clone. I have had a report script sit broken for days after an argument was added to the
function it calls, while the table it produced stayed in the README looking fine.
"""

import argparse
import json
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)

from warehouse import reproduce as R  # noqa: E402

COUNTED, VOLUME = R.COUNTED, R.VOLUME

# Every command is the one the README prints, with --markdown swapped for JSON where the
# README shows the table and the probe emits both.
COMMANDS = {
    "replica": ["-m", "scripts.replica_identity_probe", "--steps", "2000", "--seed", "7",
                "--passes", "3"],
    "layout": ["-m", "scripts.topic_layout_probe", "--steps", "600", "--seed", "11",
               "--passes", "1"],
    "split": ["-m", "scripts.topic_layout_probe", "--steps", "4000", "--seed", "11",
              "--passes", "1"],
    "reconcile": ["-m", "scripts.reconcile_probe", "--steps", "4000", "--seed", "11"],
    "chaos": ["-m", "scripts.chaos_probe", "--steps", "4000", "--seed", "11"],
    "merge": ["-m", "scripts.merge_probe", "--steps", "4000", "--seed", "11",
              "--partitions", "6", "--batch", "250"],
}

REPLICA = "The first thing worth knowing"
LAYOUT = "The topic layout, and why it is one connector"
SPLIT = "The key is where the ordering story breaks"
RETAIN = "Reading the whole stream is not what releases it"
RECON = "The reconciliation, and the number it has to publish about itself"
COVER = "Comparing a live source against a lagging target measures the lag"
CHAOS = "Killing the consumer, and the resume point that is not one"
SIZES = "The size of the restart batch decides whether it is harmless or catastrophic"
MERGE = "The merge, and the two things in it that are not what they look like"

# The section this report writes itself into. Its cells are the published figures repeated,
# not new ones, so they do not belong in the denominator.
SELF = "Every published figure, re-measured"

FIGURES = [
    R.Figure("WAL bytes under DEFAULT", REPLICA, "DEFAULT", "WAL bytes", VOLUME,
             "replica_identity_probe"),
    R.Figure("WAL bytes under FULL", REPLICA, "FULL", "WAL bytes", VOLUME,
             "replica_identity_probe"),
    R.Figure("pgoutput bytes under DEFAULT", REPLICA, "DEFAULT", "pgoutput bytes", VOLUME,
             "replica_identity_probe"),
    R.Figure("pgoutput bytes under FULL", REPLICA, "FULL", "pgoutput bytes", VOLUME,
             "replica_identity_probe"),
    R.Figure("mean before fields under FULL", REPLICA, "FULL", "mean before fields",
             COUNTED, "replica_identity_probe"),

    R.Figure("frames, one connector", LAYOUT, "one connector, 4 tables", "frames", COUNTED,
             "topic_layout_probe"),
    R.Figure("framing, one connector", LAYOUT, "one connector, 4 tables",
             "of which framing", COUNTED, "topic_layout_probe"),
    R.Figure("row changes, one connector", LAYOUT, "one connector, 4 tables",
             "of which row changes", COUNTED, "topic_layout_probe"),
    R.Figure("framing, four connectors", LAYOUT, "four connectors, totalled",
             "of which framing", COUNTED, "topic_layout_probe"),
    R.Figure("framing, product alone", LAYOUT, "shop.product alone", "of which framing",
             COUNTED, "topic_layout_probe"),

    R.Figure("retained after the workload", RETAIN, "the workload, nothing read",
             "retained bytes", VOLUME, "topic_layout_probe"),
    R.Figure("retained after reading it all", RETAIN, "reading the entire stream",
             "retained bytes", VOLUME, "topic_layout_probe"),

    R.Figure("missing on lag_25", RECON, "lag_25", "keyed: missing", COUNTED,
             "reconcile_probe"),
    R.Figure("differing on lag_25", RECON, "lag_25", "differing", COUNTED,
             "reconcile_probe"),
    R.Figure("set subtraction on lag_25", RECON, "lag_25", "set subtraction reports",
             COUNTED, "reconcile_probe"),
    R.Figure("keys checked on lag_25", COVER, "lag_25", "keys checked", COUNTED,
             "reconcile_probe"),
    R.Figure("keys checked on lag_75", COVER, "lag_75", "keys checked", COUNTED,
             "reconcile_probe"),
    R.Figure("drift reported on lag_50", COVER, "lag_50", "reported drift, no exclusion",
             COUNTED, "reconcile_probe"),

    R.Figure("records dropped, new batching", CHAOS, "resume_new_batching",
             "records dropped", COUNTED, "chaos_probe"),
    R.Figure("rows missing, new batching", CHAOS, "resume_new_batching", "rows missing",
             COUNTED, "chaos_probe"),
    R.Figure("rows missing, offset before merge", CHAOS, "offset_before_merge",
             "rows missing", COUNTED, "chaos_probe"),
    R.Figure("dropped at restart batch 320", SIZES, "320", "records dropped", COUNTED,
             "chaos_probe"),
    R.Figure("dropped at restart batch 600", SIZES, "600", "records dropped", COUNTED,
             "chaos_probe"),
    R.Figure("merged twice at restart batch 100", SIZES, "100", "merged twice", COUNTED,
             "chaos_probe"),

    R.Figure("rows on log_order", MERGE, "log_order", "rows", COUNTED, "merge_probe"),
    R.Figure("soft deleted on log_order", MERGE, "log_order", "soft deleted", COUNTED,
             "merge_probe"),
    R.Figure("rows on the versioned arm", MERGE, "versioned", "rows", COUNTED,
             "merge_probe"),
    R.Figure("extra on the hard delete shuffle", MERGE, "hard_delete_shuffled", "extra",
             COUNTED, "merge_probe"),
]


def run(args):
    out = {}
    for name, argv in COMMANDS.items():
        sys.stderr.write("running {} ...\n".format(name))
        proc = subprocess.run([sys.executable] + argv, cwd=ROOT, capture_output=True,
                              text=True)
        if proc.returncode != 0:
            raise SystemExit("{} failed:\n{}".format(name, proc.stderr[-2000:]))
        out[name] = json.loads(proc.stdout)
    if args.save:
        with open(args.save, "w") as handle:
            json.dump(out, handle)
    return out


def observe(raw):
    """Pull one number per figure out of the probe output. Keyed by figure name."""
    rip = raw["replica"]["arms"]
    lay = raw["layout"]["passes"][0]
    one, per = lay["one_connector"], lay["per_table_connectors"]
    pos = lay["positions"]
    rec = raw["reconcile"]["arms"]
    ch = raw["chaos"]
    mp = raw["merge"]["arms"]

    def framing(block):
        k = block["by_kind"]
        return k.get("begin", 0) + k.get("commit", 0) + k.get("relation", 0)

    def rows(block):
        k = block["by_kind"]
        return k.get("insert", 0) + k.get("update", 0) + k.get("delete", 0)

    def total(arm, field):
        return sum(c[field] for c in arm["counts"].values())

    def tables(arm, field):
        return sum(t[field] for t in arm["tables"].values())

    def sweep(size, field):
        for row in ch["restart_batch_sweep"]:
            if row["restart_batch"] == size:
                return row[field]
        raise R.Missing("no restart batch row for " + str(size))

    return {
        "WAL bytes under DEFAULT": rip["default"]["wal_bytes"],
        "WAL bytes under FULL": rip["full"]["wal_bytes"],
        "pgoutput bytes under DEFAULT": rip["default"]["pgoutput_bytes"],
        "pgoutput bytes under FULL": rip["full"]["pgoutput_bytes"],
        "mean before fields under FULL": rip["full"]["mean_before_fields"],

        "frames, one connector": one["messages"],
        "framing, one connector": framing(one),
        "row changes, one connector": rows(one),
        "framing, four connectors": sum(framing(b) for b in per.values()),
        "framing, product alone": framing(per["product"]),

        "retained after the workload":
            pos["before_any_read"]["layout_wide"]["retained_bytes"],
        "retained after reading it all":
            pos["after_one_reader"]["layout_wide"]["retained_bytes"],

        "missing on lag_25": rec["lag_25"]["no_exclusion"]["missing"],
        "differing on lag_25": rec["lag_25"]["no_exclusion"]["differing"],
        "set subtraction on lag_25":
            rec["lag_25"]["set_difference_view"]["reported_rows"],
        "keys checked on lag_25": rec["lag_25"]["with_exclusion"]["checked"],
        "keys checked on lag_75": rec["lag_75"]["with_exclusion"]["checked"],
        "drift reported on lag_50": rec["lag_50"]["no_exclusion"]["mismatched"],

        "records dropped, new batching": ch["arms"]["resume_new_batching"]["gap"]["dropped"],
        "rows missing, new batching": ch["arms"]["resume_new_batching"]["grade"]["missing"],
        "rows missing, offset before merge": ch["arms"]["offset_before_merge"]["grade"]["missing"],
        "dropped at restart batch 320": sweep(320, "dropped"),
        "dropped at restart batch 600": sweep(600, "dropped"),
        "merged twice at restart batch 100": sweep(100, "applied_twice"),

        "rows on log_order": total(mp["log_order"], "rows"),
        "soft deleted on log_order": total(mp["log_order"], "soft_deleted"),
        "rows on the versioned arm": total(mp["versioned"], "rows"),
        "extra on the hard delete shuffle": tables(mp["hard_delete_shuffled"], "extra"),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--markdown", action="store_true")
    ap.add_argument("--raw", help="read probe output from a json file instead of running")
    ap.add_argument("--save", help="write the probe output to a json file as well")
    args = ap.parse_args()

    raw = json.load(open(args.raw)) if args.raw else run(args)
    readme = open(os.path.join(ROOT, "README.md")).read()
    results = R.check(readme, FIGURES, observe(raw))
    counts = R.summarise(results)
    cov = R.coverage(readme, FIGURES, ignore_headings=[SELF])

    if args.markdown:
        print(R.markdown(results))
        print()
        print("{} figures re-measured. ".format(len(results)) + ", ".join(
            "{} {}".format(v, k) for k, v in sorted(counts.items())) + ".")
        print("That is {} of the {} numeric cells in this README, {:.1%}. The rest are "
              "not addressed by any figure. {} cells in this report's own table are left "
              "out of the denominator.".format(cov["graded"], cov["cells"], cov["share"],
                                               cov["skipped"]))
    else:
        print(json.dumps({"results": results, "summary": counts, "coverage": cov},
                         indent=2))

    broken = counts.get("BROKEN", 0)
    return 1 if broken else 0


if __name__ == "__main__":
    sys.exit(main())
