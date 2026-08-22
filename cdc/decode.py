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

# Same line, read for values instead of for field names. Two types rather than one flag,
# because a caller that wanted names and got a dict fails on the next line rather than
# three functions later.
Row = namedtuple("Row", "table action before after")

_HEAD = re.compile(r"^table ([^:]+): (INSERT|UPDATE|DELETE): (.*)$", re.S)

# name[type]: is where a field starts. Where it ends is the value's business, so the value
# is read by a scanner rather than by the same regex. A text column can hold a space and it
# can hold a quote written as two quotes, and one pattern cannot stop correctly at both.
_FIELD_START = re.compile(r"(?:^|\s)([A-Za-z_][A-Za-z0-9_]*)\[([^\]]*)\]:")

NO_TUPLE = "(no-tuple-data)"


def _read_value(text, i):
    """Read one value starting at i. Returns the value and the index just past it."""
    if i < len(text) and text[i] == "'":
        i += 1
        buf = []
        while i < len(text):
            if text[i] == "'":
                if text[i + 1:i + 2] == "'":
                    buf.append("'")
                    i += 2
                    continue
                return "".join(buf), i + 1
            buf.append(text[i])
            i += 1
        # A quote that never closes means the line arrived truncated. Returning the part
        # that was read hands the consumer half a value looking like a whole one.
        raise ValueError("unterminated quoted value: " + text[:60])
    j = i
    while j < len(text) and not text[j].isspace():
        j += 1
    raw = text[i:j]
    return (None if raw == "null" else raw), j


def _pairs(fragment):
    """Every field in a fragment, with quotes taken off. Name and type and value each."""
    if fragment is None:
        return None
    if NO_TUPLE in fragment:
        return []
    out = []
    pos = 0
    while True:
        m = _FIELD_START.search(fragment, pos)
        if not m:
            return out
        value, pos = _read_value(fragment, m.end())
        out.append((m.group(1), m.group(2), value))


def _fields(fragment):
    p = _pairs(fragment)
    return None if p is None else [name for name, _, _ in p]


def _split(line):
    """Table, action, and the raw before and after fragments. None if this is not a change.

    Splitting the shape out from the field reading is what lets one line be read two ways.
    The counting path wants names. The consumer wants values. They must not disagree about
    which half of an update is the old row.
    """
    m = _HEAD.match(line.strip())
    if not m:
        return None
    table, action, rest = m.group(1), m.group(2), m.group(3)

    if "old-key:" in rest or "old-tuple:" in rest:
        head, _, tail = rest.partition("new-tuple:")
        old = head.split(":", 1)[1] if ":" in head else head
        return table, action, old, tail

    if action == "DELETE":
        # What a delete shows is the row identity, which is a before image and never an
        # after image. Calling it 'after' would make the consumer read it backwards.
        return table, action, rest, None

    return table, action, None, rest


def parse_change(line):
    """One decoded row into a Change, or None for BEGIN and COMMIT markers."""
    parts = _split(line)
    if parts is None:
        return None
    table, action, before, after = parts
    return Change(table, action, _fields(before), _fields(after))


def parse_row(line):
    """The same line read for its values, which is what a consumer needs.

    before and after come back as column to value dicts. Everything is a string or None,
    because test_decoding prints the output function of the type and this repo is not in
    the business of guessing which Python type a bigint should become.
    """
    parts = _split(line)
    if parts is None:
        return None
    table, action, before, after = parts
    return Row(table, action, _image(before), _image(after))


def _image(fragment):
    p = _pairs(fragment)
    if p is None:
        return None
    return {name: value for name, _, value in p}


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
    updates_with_before = 0
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
                if c.action == "UPDATE":
                    updates_with_before += 1
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
        "updates_with_before_image": updates_with_before,
        "mean_before_fields": _mean(before_width),
        "mean_after_fields": _mean(after_width),
    }


def _mean(xs):
    if not xs:
        return 0.0
    return round(sum(xs) / len(xs), 4)


# The first byte of a pgoutput frame is its message type. This is the version 1 set and
# it is deliberately not exhaustive, because an unknown byte has to come back as unknown
# rather than as a guess.
PGOUTPUT_TYPES = {
    b"B": "begin",
    b"C": "commit",
    b"R": "relation",
    b"I": "insert",
    b"U": "update",
    b"D": "delete",
    b"T": "truncate",
    b"O": "origin",
    b"Y": "type",
}


def pgoutput_message_types(frames):
    """Count the pgoutput frames by kind.

    The reason this exists is that a publication covering one table still produces a
    frame for every transaction on every table, because BEGIN and COMMIT are transaction
    scoped and not table scoped. A message count on its own hides that, and the message
    count is what somebody sizes a consumer from.
    """
    out = {}
    for frame in frames:
        if not frame:
            out["empty"] = out.get("empty", 0) + 1
            continue
        name = PGOUTPUT_TYPES.get(frame[:1], "unknown:" + frame[:1].hex())
        out[name] = out.get(name, 0) + 1
    return out


def row_change_share(counts):
    """Fraction of frames that carry a row change rather than transaction framing.

    Returns None on an empty stream. A share of zero and a share of nothing at all are
    different answers and a caller has to be able to tell them apart.
    """
    total = sum(counts.values())
    if total == 0:
        return None
    changes = sum(counts.get(k, 0) for k in ("insert", "update", "delete", "truncate"))
    return changes / total
