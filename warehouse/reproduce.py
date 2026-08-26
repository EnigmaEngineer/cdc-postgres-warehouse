"""Check that a figure this repo publishes still comes back off the code that produced it.

The obvious way to build this is a list of pairs, the number the README says and the number
the probe returns, typed in by hand. That is the version I wrote first somewhere else and
it has a hole in the middle of it. A wrong transcription makes a broken figure look
reproduced, which is the exact failure the thing is for. The published column has to come
out of the README file, not out of my memory of the README file.

So a figure names a heading, a row and a column, and the published value is read from the
markdown table under that heading at run time. If the README moves a number, this reads the
moved one. If the README loses the row, this raises rather than passing.

## Three kinds, not two

A count and a timing need different rules and that is well known. This repo needs a third.

    COUNTED   reproduces exactly or it is broken. No tolerance band, on purpose. The worst
              published row count I have had to correct was wrong by 0.24 percent, and any
              band loose enough to feel comfortable would have waved it through.
    VOLUME    a byte count out of a database. Measured here at a pass to pass range of
              0.0036 to 0.0403 percent on identical work, so it has a floor under it that
              is not zero and is not a timing either. The band is the spread that was
              measured, not a number anybody picked.
    TIMING    wall clock. Moves. Reported as moved rather than as broken.

## Nothing to check is a finding

`check` raises on an empty figure list and `cell` raises on a heading or a row it cannot
find. A checker that reports clean when it was pointed at nothing is a checker that will
eventually be pointed at nothing, and the run that does it will not notice.
"""

COUNTED = "counted"
VOLUME = "volume"
TIMING = "timing"

# The widest pass to pass spread the reproducibility arm has measured on a byte volume,
# 0.0403 percent on the pgoutput stream under FULL. Rounded up to a tenth of a percent so
# the band is a stated round number rather than a figure that moves whenever the arm reruns.
VOLUME_BAND = 0.001


class Missing(Exception):
    """A figure named a place in the README that is not there."""


class Figure(object):
    def __init__(self, name, heading, row, column, kind, source):
        self.name = name
        self.heading = heading
        self.row = row
        self.column = column
        self.kind = kind
        self.source = source


def row_cells(line):
    """The cells of a markdown table row, `[]` for a separator, `None` for anything else.

    One separator rule, in one place. It was written twice, once here and once in the
    cell counter, and the second copy was unreachable in effect because a separator cell
    never parses as a number and never matches a row label. Two spellings of one rule with
    only one of them able to be wrong is worse than either.
    """
    stripped = line.strip()
    if not (stripped.startswith("|") and stripped.endswith("|")):
        return None
    cells = [c.strip() for c in stripped.strip("|").split("|")]
    if set("".join(cells)) <= set("-:"):
        return []
    return cells


def tables(text, heading):
    """Every markdown table under a heading, as lists of rows of stripped cells.

    Stops at the next heading of any depth, so a table in a subsection belongs to the
    subsection and not to its parent. The first version stopped only at a heading of the
    same level or shallower, which let a `##` lookup reach into every `###` beneath it, and
    the docstring claimed the opposite of what the code did. A fixture with the same row
    label in a section and its subsection is what caught it.
    """
    lines = text.splitlines()
    start = None
    for i, line in enumerate(lines):
        if line.startswith("#") and line.lstrip("#").strip() == heading:
            start = i + 1
            break
    if start is None:
        raise Missing("no heading: " + heading)

    out = []
    current = []
    for line in lines[start:]:
        if line.startswith("#"):
            break
        cells = row_cells(line)
        if cells is not None:
            if cells:
                current.append(cells)
            continue
        if current:
            out.append(current)
            current = []
    if current:
        out.append(current)
    return out


def cell(text, heading, row, column):
    """The published value at (row, column) in a table under a heading.

    Row and column are matched on the first column and the header row, with backticks and
    bold markers stripped, because a README writes `DEFAULT` and a probe returns DEFAULT.

    Two matches is an error rather than a first-one-wins. This README carries five rows
    labelled `log_order` across four tables, so an address that matches twice is ambiguous
    and taking the earlier one is a coin flip nobody would see land. The first version did
    take the first one, and the test that was supposed to prove a lost row raises deleted a
    row in a different section and passed.
    """
    want_row = _plain(row)
    want_col = _plain(column)
    hits = []
    for table in tables(text, heading):
        header = [_plain(c) for c in table[0]]
        if want_col not in header:
            continue
        at = header.index(want_col)
        for line in table[1:]:
            if line and _plain(line[0]) == want_row and at < len(line):
                hits.append(line[at])
    if not hits:
        raise Missing("no cell {!r} x {!r} under {!r}".format(row, column, heading))
    if len(hits) > 1:
        raise Missing("ambiguous cell {!r} x {!r} under {!r}, {} matches: {}".format(
            row, column, heading, len(hits), ", ".join(hits)))
    return hits[0]


def _plain(s):
    return s.replace("`", "").replace("*", "").strip()


def number(text):
    """The number in a published cell. Thousands separators and a percent sign come off.

    Raises on a cell with no number in it. A cell reading "refused by the source" is a real
    published value and it is not a figure, so a figure pointing at one is a mistake in the
    figure list rather than something to shrug at.
    """
    cleaned = _plain(text).replace(",", "").replace("%", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        raise Missing("not a number: " + repr(text))


def verdict(published, observed, kind):
    """EXACT, WITHIN_BAND, MOVED or BROKEN."""
    if published == observed:
        return "EXACT"
    if kind == COUNTED:
        return "BROKEN"
    if kind == TIMING:
        return "MOVED"
    if published == 0:
        return "BROKEN"
    if abs(observed - published) / abs(published) <= VOLUME_BAND:
        return "WITHIN_BAND"
    return "BROKEN"


def check(readme_text, figures, observed):
    """Grade every figure. Returns a list of result dicts.

    `observed` maps a figure name to what the code returned today. A figure with no
    observation is an error rather than a skip, because a report that quietly drops the
    figures nobody measured is a report that gets shorter every time it is run.
    """
    if not figures:
        raise Missing("no figures to check")
    missing = [f.name for f in figures if f.name not in observed]
    if missing:
        raise Missing("no observation for: " + ", ".join(sorted(missing)))

    out = []
    for fig in figures:
        raw = cell(readme_text, fig.heading, fig.row, fig.column)
        pub = number(raw)
        got = float(observed[fig.name])
        out.append({
            "name": fig.name,
            "kind": fig.kind,
            "source": fig.source,
            "published": pub,
            "observed": got,
            "verdict": verdict(pub, got, fig.kind),
        })
    return out


def numeric_cells(text):
    """Every numeric cell in every markdown table in the document.

    Each one comes back as a heading and a row label and a column header.

    A figure list written by hand grades what its author remembered. This is the other
    side of it, derived from the file, so the report can say what fraction of the published
    table figures it actually covers instead of implying all of them.

    Prose figures are not counted. There is no address for one, which is a limit on the
    whole approach and it is stated rather than worked around.
    """
    lines = text.splitlines()
    heading = None
    out = []
    block = []
    for line in lines + [""]:
        if line.startswith("#"):
            heading = line.lstrip("#").strip()
        cells = row_cells(line)
        if cells is not None:
            if cells:
                block.append((heading, cells))
            continue
        if block:
            header = [_plain(c) for c in block[0][1]]
            for head, cells in block[1:]:
                for at, raw in enumerate(cells[1:], start=1):
                    if at >= len(header):
                        continue
                    try:
                        number(raw)
                    except Missing:
                        continue
                    out.append((head, _plain(cells[0]), header[at]))
            block = []
    return out


def coverage(text, figures, ignore_headings=()):
    """How many of the document's numeric table cells the figure list addresses.

    `ignore_headings` exists for one thing and it should stay that way. The report writes
    its own results into the document as a table, and those cells are the published figures
    restated rather than new claims. Counting them would make the coverage figure fall
    every time the report ran and rise every time it was trimmed, which is a metric that
    rewards publishing less. The number of cells skipped is returned so the exemption
    cannot be quietly widened.
    """
    ignored = set(ignore_headings)
    every = numeric_cells(text)
    kept = [c for c in every if c[0] not in ignored]
    addressed = {(f.heading, _plain(f.row), _plain(f.column)) for f in figures}
    graded = [c for c in kept if c in addressed]
    return {"cells": len(kept), "graded": len(graded),
            "skipped": len(every) - len(kept),
            "share": round(len(graded) / len(kept), 4) if kept else 0.0}


def summarise(results):
    counts = {}
    for r in results:
        counts[r["verdict"]] = counts.get(r["verdict"], 0) + 1
    return counts


def markdown(results):
    lines = ["| figure | kind | published | re-measured | verdict | produced by |",
             "|---|---|---|---|---|---|"]
    for r in results:
        lines.append("| {} | {} | {} | {} | {} | `{}` |".format(
            r["name"], r["kind"], _fmt(r["published"]), _fmt(r["observed"]),
            r["verdict"], r["source"]))
    return "\n".join(lines)


def _fmt(v):
    if v == int(v):
        return "{:,}".format(int(v))
    return "{:,.4f}".format(v).rstrip("0").rstrip(".")
