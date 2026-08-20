"""Read what logical decoding actually emitted, rather than what you assume it emitted.

The output format here is test_decoding, which is the plugin that ships in contrib. It is
human readable, which is the point. The real consumer will read pgoutput binary frames.
Those two plugins carry the same information about what is present and what is absent, so
a question about presence can be answered on the cheap one.

A row looks like one of these.

    table shop.customer: INSERT: customer_id[bigint]:1 email[text]:'a@b.c'
    table shop.customer: UPDATE: customer_id[bigint]:1 email[text]:'new@b.c'
    table shop.customer: UPDATE: old-key: customer_id[bigint]:1 ... new-tuple: ...
    table shop.order_item: DELETE: order_id[bigint]:7 line_no[smallint]:2
    table shop.order_item: DELETE: (no-tuple-data)

The whole point of parsing it is the third and fifth shapes. Whether an update carries an
old image, and whether a delete carries anything at all, is decided by the table's replica
identity and not by the plugin.
"""

import re
from collections import namedtuple

Change = namedtuple("Change", "table action before after")

_HEAD = re.compile(r"^table ([^:]+): (INSERT|UPDATE|DELETE): (.*)$", re.S)

# name[type]:value. Counting these is how field width is measured. A text value containing
# a literal bracket-colon run would inflate the count. Nothing this repo writes does that,
# and tests/test_decode.py pins the behaviour so a future reader knows the limit is known.
_FIELD = re.compile(r"(?:^|\s)([A-Za-z_][A-Za-z0-9_]*)\[[^\]]*\]:")

NO_TUPLE = "(no-tuple-data)"


def _fields(fragment):
    if fragment is None:
        return None
    if NO_TUPLE in fragment:
        return []
    return _FIELD.findall(fragment)


def parse_change(line):
    """One decoded row into a Change, or None for BEGIN and COMMIT markers."""
    m = _HEAD.match(line.strip())
    if not m:
        return None
    table, action, rest = m.group(1), m.group(2), m.group(3)

    if "old-key:" in rest or "old-tuple:" in rest:
        head, _, tail = rest.partition("new-tuple:")
        old = head.split(":", 1)[1] if ":" in head else head
        return Change(table, action, _fields(old), _fields(tail))

    if action == "DELETE":
        # What a delete shows is the row identity, which is a before image and never an
        # after image. Calling it 'after' would make the consumer read it backwards.
        return Change(table, action, _fields(rest), None)

    return Change(table, action, None, _fields(rest))


def parse_stream(lines):
    out = []
    for line in lines:
        c = parse_change(line)
        if c is not None:
            out.append(c)
    return out


def summarise(changes):
    """Counts by table and action, plus how much of each change is actually recoverable.

    before_present is the number that matters. An update or a delete without a before
    image cannot be reconciled against a target row that has drifted, because there is
    nothing to compare.
    """
    by_action = {}
    by_table = {}
    before_present = 0
    before_absent = 0
    after_width = []
    before_width = []

    for c in changes:
        by_action[c.action] = by_action.get(c.action, 0) + 1
        key = (c.table, c.action)
        by_table[key] = by_table.get(key, 0) + 1
        if c.action in ("UPDATE", "DELETE"):
            if c.before:
                before_present += 1
                before_width.append(len(c.before))
            else:
                before_absent += 1
        if c.after:
            after_width.append(len(c.after))

    return {
        "changes": len(changes),
        "by_action": by_action,
        "by_table": {"{}:{}".format(t, a): n for (t, a), n in sorted(by_table.items())},
        "before_present": before_present,
        "before_absent": before_absent,
        "mean_before_fields": _mean(before_width),
        "mean_after_fields": _mean(after_width),
    }


def _mean(xs):
    if not xs:
        return 0.0
    return round(sum(xs) / len(xs), 4)
