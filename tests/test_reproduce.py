"""Checks over the figure grader.

The thing this has to get right is failing. A grader that cannot report BROKEN passes every
README forever and reads like rigour, so most of what is here feeds it documents with a
defect in them and asserts it notices.

The fixture is a small document rather than the real README on purpose. Pointing these at
`README.md` would make them pass or fail on whatever the README happens to say today, which
is a test of the document and not of the code.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from warehouse import reproduce as R  # noqa: E402

DOC = """# Top

## Arms

Some prose.

| arm | rows | bytes |
|---|---|---|
| `log_order` | 1,777 | 616,072 |
| `shuffled` | 1,634 | refused |

### A subsection

| arm | rows | bytes |
|---|---|---|
| `log_order` | 42 | 1,000 |

## Elsewhere

| arm | rows |
|---|---|
| `log_order` | 9 |
"""


def _fig(row="log_order", col="rows", kind=R.COUNTED, heading="Arms"):
    return R.Figure("f", heading, row, col, kind, "nothing")


def check_a_published_cell_is_read_out_of_the_document():
    assert R.cell(DOC, "Arms", "log_order", "rows") == "1,777"
    assert R.cell(DOC, "Arms", "shuffled", "bytes") == "refused"


def check_a_subsection_table_is_not_reachable_through_its_parent_heading():
    # The subsection carries a row with the same label and a different number. If the
    # heading scan ran past the next `###` this would come back ambiguous or wrong.
    assert R.cell(DOC, "A subsection", "log_order", "rows") == "42"
    assert R.cell(DOC, "Arms", "log_order", "rows") == "1,777"


def check_two_matching_cells_under_one_heading_is_an_error():
    doc = DOC.replace("| `shuffled` | 1,634 | refused |",
                      "| `shuffled` | 1,634 | refused |\n| `log_order` | 5 | 5 |")
    try:
        R.cell(doc, "Arms", "log_order", "rows")
    except R.Missing as e:
        assert "ambiguous" in str(e), e
        return
    raise AssertionError("two matches should not resolve to the first one")


def check_a_missing_row_raises_rather_than_passing():
    doc = DOC.replace("| `log_order` | 1,777 | 616,072 |\n", "")
    try:
        R.cell(doc, "Arms", "log_order", "rows")
    except R.Missing:
        return
    raise AssertionError("a lost row should raise")


def check_a_cell_that_is_not_a_number_is_an_error_and_not_a_zero():
    try:
        R.number(R.cell(DOC, "Arms", "shuffled", "bytes"))
    except R.Missing:
        return
    raise AssertionError("refused is not a figure")


def check_thousands_separators_and_percent_signs_come_off():
    assert R.number("1,777") == 1777.0
    assert R.number("48.7%") == 48.7
    assert R.number("**176**") == 176.0


def check_a_counted_figure_has_no_tolerance_band():
    # Deliberate. The worst published row count I have had to correct was 0.24 percent out,
    # and any band wide enough to feel comfortable would have let it through.
    assert R.verdict(1777, 1777, R.COUNTED) == "EXACT"
    assert R.verdict(1777, 1778, R.COUNTED) == "BROKEN"


def check_a_volume_gets_the_band_and_a_count_does_not():
    inside = 616072 * (1 + R.VOLUME_BAND / 2)
    outside = 616072 * (1 + R.VOLUME_BAND * 2)
    assert R.verdict(616072, inside, R.VOLUME) == "WITHIN_BAND"
    assert R.verdict(616072, outside, R.VOLUME) == "BROKEN"
    assert R.verdict(616072, inside, R.COUNTED) == "BROKEN"


def check_a_timing_moves_rather_than_breaking():
    assert R.verdict(10.0, 90.0, R.TIMING) == "MOVED"


def check_a_zero_published_volume_cannot_be_divided_into():
    assert R.verdict(0, 5, R.VOLUME) == "BROKEN"
    assert R.verdict(0, 0, R.VOLUME) == "EXACT"


def check_grading_nothing_is_a_failure_and_not_a_pass():
    try:
        R.check(DOC, [], {})
    except R.Missing:
        return
    raise AssertionError("an empty figure list should raise")


def check_a_figure_with_no_observation_raises_rather_than_being_skipped():
    try:
        R.check(DOC, [_fig()], {})
    except R.Missing as e:
        assert "no observation" in str(e), e
        return
    raise AssertionError("an unmeasured figure should raise")


def check_a_heading_that_is_not_there_raises():
    try:
        R.tables(DOC, "No such heading")
    except R.Missing:
        return
    raise AssertionError("a missing heading should raise")


def check_the_separator_row_is_not_read_as_data():
    for table in R.tables(DOC, "Arms"):
        for line in table:
            assert not set("".join(line)) <= set("-:"), line


def check_an_aligned_separator_row_is_still_a_separator():
    # `|:---:|---:|` joins to exactly the separator character set, so the subset test has
    # to be inclusive. Every plain `|---|` row is a proper subset, so with only those in
    # the fixture a proper subset test skips them too and the rule is never exercised.
    #
    # Asserting that the cell still reads correctly is not enough either. An unskipped
    # separator sits below the header, matches no row label and parses as no number, so
    # every published answer is the same. The row count is what changes.
    doc = "## S\n\n| arm | rows |\n|:---:|---:|\n| `a` | 3 |\n"
    table = R.tables(doc, "S")[0]
    assert len(table) == 2, table
    assert table == [["arm", "rows"], ["`a`", "3"]], table


def check_the_header_row_is_not_returned_as_a_value():
    # Asking for the row named by the first column header must miss. If the scan started at
    # the header instead of below it, this returns the string "rows".
    try:
        R.cell(DOC, "Arms", "arm", "rows")
    except R.Missing:
        pass
    else:
        raise AssertionError("the header is not a data row")
    doc = "## S\n\n| arm | 2026 |\n|---|---|\n| `a` | 3 |\n"
    # The header carries a number. Counting it would make the coverage denominator wrong.
    assert R.numeric_cells(doc) == [("S", "a", "2026")], R.numeric_cells(doc)


def check_a_row_shorter_than_its_header_is_skipped_rather_than_raising():
    doc = "## S\n\n| arm | rows | bytes |\n|---|---|---|\n| `a` | 3 |\n| `b` | 4 | 5 |\n"
    assert R.cell(doc, "S", "b", "bytes") == "5"
    try:
        R.cell(doc, "S", "a", "bytes")
    except R.Missing:
        return
    raise AssertionError("a ragged short row has no cell there")


def check_a_row_longer_than_its_header_does_not_invent_a_column():
    doc = "## S\n\n| arm | rows |\n|---|---|\n| `a` | 3 | 9 |\n"
    assert R.numeric_cells(doc) == [("S", "a", "rows")], R.numeric_cells(doc)


def check_a_cell_is_reported_under_its_own_column_name():
    # An off by one on the column index still counts the right number of cells, so a check
    # that only counts them passes. The name is what says the index is right.
    doc = "## S\n\n| arm | rows | bytes |\n|---|---|---|\n| `a` | 3 | 4 |\n"
    assert R.numeric_cells(doc) == [("S", "a", "rows"), ("S", "a", "bytes")], \
        R.numeric_cells(doc)


def check_the_band_edge_is_inside_the_band():
    # 1 in 1,000 is the band exactly. Picked so the ratio is representable, because a
    # fixture landing a hair under the edge passes whether the comparison is inclusive or
    # not and the boundary is the only part of this worth pinning.
    assert (1001 - 1000) / 1000 == R.VOLUME_BAND
    assert R.verdict(1000, 1001, R.VOLUME) == "WITHIN_BAND"
    assert R.verdict(1000, 1002, R.VOLUME) == "BROKEN"


def check_a_whole_number_prints_without_a_decimal_point():
    # The README tables are pasted out of this, so the rendering is part of what ships.
    # A whole number alone does not pin the branch: 1777.0 formatted the long way is
    # "1,777.0000", and stripping the trailing zeros and the point gives the same "1,777".
    # Only a fractional value tells the two branches apart.
    figs = [R.Figure("rows", "Arms", "log_order", "rows", R.COUNTED, "probe"),
            R.Figure("bytes", "Arms", "log_order", "bytes", R.VOLUME, "probe")]
    text = R.markdown(R.check(DOC, figs, {"rows": 1777, "bytes": 616072.5}))
    assert "| 1,777 | 1,777 |" in text, text
    assert "1777.0" not in text, text
    assert "616,072.5" in text, text


def check_every_numeric_cell_in_the_document_is_found():
    # 1,777 and 616,072 and 1,634 in Arms, 42 and 1,000 in the subsection, 9 in Elsewhere.
    # `refused` is not one. The first column is the row label and is never a figure.
    cells = R.numeric_cells(DOC)
    assert len(cells) == 6, cells
    assert ("Arms", "shuffled", "bytes") not in cells, cells


def check_coverage_is_the_share_of_cells_a_figure_list_addresses():
    got = R.coverage(DOC, [_fig()])
    assert got["cells"] == 6, got
    assert got["graded"] == 1, got
    assert got["skipped"] == 0, got
    assert got["share"] == round(1 / 6, 4), got


def check_an_ignored_heading_leaves_the_denominator_and_is_counted():
    got = R.coverage(DOC, [_fig()], ignore_headings=["Elsewhere"])
    assert got["cells"] == 5 and got["skipped"] == 1, got
    assert got["graded"] == 1, got


def check_coverage_of_nothing_is_zero_rather_than_an_error():
    got = R.coverage(DOC, [])
    assert got["graded"] == 0 and got["share"] == 0.0, got


def check_a_full_grade_reports_the_verdict_per_figure():
    figs = [R.Figure("rows", "Arms", "log_order", "rows", R.COUNTED, "probe"),
            R.Figure("bytes", "Arms", "log_order", "bytes", R.VOLUME, "probe")]
    got = R.check(DOC, figs, {"rows": 1777, "bytes": 616072 * 1.0003})
    by = {r["name"]: r["verdict"] for r in got}
    assert by == {"rows": "EXACT", "bytes": "WITHIN_BAND"}, by
    assert R.summarise(got) == {"EXACT": 1, "WITHIN_BAND": 1}, got
