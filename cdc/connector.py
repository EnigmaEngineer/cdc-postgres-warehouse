"""Build a Debezium Postgres connector config, and check it against the real connector.

A connector config is a flat JSON object that somebody pastes into a REST call. It gets
no compiler, no type check and no warning. A misspelled property is accepted and ignored,
and the connector runs with the default you were trying to override.

So two things live here. The config is generated from the database rather than typed out,
which is what stops it drifting from db/schema.sql. And it is validated against the key
list read out of the connector jar, which is db/debezium-configdef.tsv.

The interesting half is not the misspellings. It is the defaults. Three of this
connector's defaults are wrong for this database in a way that fails late or fails
quietly, and REQUIRED_OVERRIDES names them with the reason and the measurement.
"""

import json
import os

CONFIGDEF_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "db", "debezium-configdef.tsv"
)

CONNECTOR_CLASS = "io.debezium.connector.postgresql.PostgresConnector"

# Each entry is a key whose shipped default this database cannot live with, the default
# the connector declares, and what was measured. The declared default is checked against
# db/debezium-configdef.tsv by a test, so a Debezium upgrade that changes one of these
# breaks the build rather than quietly making the note wrong.
REQUIRED_OVERRIDES = {
    "plugin.name": {
        "declared_default": "decoderbufs",
        "reason": (
            "decoderbufs is a C extension that is not built into a stock Postgres. "
            "Creating a slot with it here fails with 'library \"decoderbufs\" may not be "
            "used as an output plugin'. pgoutput ships with the server."
        ),
    },
    "publication.autocreate.mode": {
        "declared_default": "all_tables",
        "reason": (
            "The default creates a publication FOR ALL TABLES when the named one is "
            "missing. db/schema.sql declares an explicit four table publication on "
            "purpose, and FOR ALL TABLES would publish every table in the database and "
            "needs an owner of them all."
        ),
    },
    "slot.name": {
        "declared_default": "debezium",
        "reason": (
            "One fixed default name for every connector. Two connectors against one "
            "database collide on it, and the second gets 'replication slot \"debezium\" "
            "already exists'. That is the failure a per table topic split walks into."
        ),
    },
}


def load_config_keys(path=CONFIGDEF_PATH):
    """Read the key list dumped out of the connector jar.

    Returns exact names and pattern heads separately. A pattern key is one whose name is
    a regular expression, for example the column masking family. Those are matched on the
    literal text before the first bracket, because the declared patterns do not all
    compile into something the documented property form matches.
    """
    exact = {}
    pattern_heads = []
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            line = line.rstrip("\n")
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) != 4:
                raise ValueError("configdef line is not four columns: " + repr(line))
            name, type_name, default, importance = parts
            if "(" in name or "[" in name:
                pattern_heads.append(name.split("(")[0].split("[")[0].rstrip("."))
                continue
            exact[name] = {"type": type_name, "default": default, "importance": importance}
    if not exact:
        raise ValueError("no configuration keys found in " + path)
    return exact, pattern_heads


def unknown_keys(config, keys=None, pattern_heads=None):
    """Keys in a config that the connector does not declare.

    Everything under transforms. and predicates. and the converter prefixes is left
    alone, because those names are invented by whatever transform or converter you named
    and the connector cannot declare them in advance.
    """
    if keys is None or pattern_heads is None:
        keys, pattern_heads = load_config_keys()
    free_prefixes = (
        "transforms.", "predicates.", "key.converter.", "value.converter.",
        "header.converter.", "topic.creation.", "errors.", "producer.override.",
    )
    framework = {
        "connector.class", "tasks.max", "name", "key.converter", "value.converter",
        "header.converter", "transforms", "predicates", "config.action.reload",
        "topic.creation.default.partitions", "topic.creation.default.replication.factor",
    }
    out = []
    for key in config:
        if key in keys or key in framework:
            continue
        if key.startswith(free_prefixes):
            continue
        if any(key.startswith(head) for head in pattern_heads):
            continue
        out.append(key)
    return sorted(out)


def left_on_default(config):
    """Which of the dangerous defaults this config has failed to override."""
    return sorted(k for k in REQUIRED_OVERRIDES if not config.get(k))


def validate(config, keys=None, pattern_heads=None):
    """Every problem found, as a list of strings. Empty means the config is usable.

    A validator that returns a boolean tells you a config is wrong and not which part,
    and the part is the whole value.
    """
    problems = []
    for key in unknown_keys(config, keys, pattern_heads):
        problems.append("unknown key, the connector does not declare it: " + key)
    for key in left_on_default(config):
        problems.append(
            "left on the shipped default, which is {!r}: {}. {}".format(
                REQUIRED_OVERRIDES[key]["declared_default"], key,
                REQUIRED_OVERRIDES[key]["reason"],
            )
        )
    if config.get("connector.class") != CONNECTOR_CLASS:
        problems.append("connector.class must be " + CONNECTOR_CLASS)
    prefix = config.get("topic.prefix")
    if prefix and "." in prefix:
        problems.append("topic.prefix must not contain a dot, it is a topic name segment")
    include = config.get("table.include.list", "")
    if include:
        for entry in include.split(","):
            if entry.count(".") != 1:
                problems.append("table.include.list wants schema.table, got " + repr(entry))
    return problems


def build(prefix, tables, publication, slot, dbname,
          host="postgres", port=5432, user="cdc", password_ref="${file:/run/secrets:pg}"):
    """A connector config for one publication.

    tables is the list of (schema, table) pairs the publication really covers, read from
    the database rather than typed here. If the two disagree the connector streams a set
    of tables nobody chose.

    The password is a reference and not a value. Nothing in this repo ever holds one.
    """
    if not tables:
        raise ValueError("a connector with no tables in it captures nothing")
    include = ",".join("{}.{}".format(schema, table) for schema, table in tables)
    return {
        "connector.class": CONNECTOR_CLASS,
        "tasks.max": "1",
        "database.hostname": host,
        "database.port": str(port),
        "database.user": user,
        "database.password": password_ref,
        "database.dbname": dbname,
        "topic.prefix": prefix,
        "plugin.name": "pgoutput",
        "slot.name": slot,
        "publication.name": publication,
        # The publication is created by db/schema.sql and the connector must not touch it.
        "publication.autocreate.mode": "disabled",
        "table.include.list": include,
        "snapshot.mode": "initial",
        # Deletes have to arrive as deletes. The soft delete decision belongs to the
        # warehouse merge and not to the connector.
        "tombstones.on.delete": "true",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        # A slot with no traffic never advances its restart position, and an idle slot
        # pins the whole write ahead log. A heartbeat moves it on a quiet table.
        "heartbeat.interval.ms": "10000",
        "decimal.handling.mode": "string",
    }


def render(config):
    """The REST body Kafka Connect wants, which wraps the flat config under a name."""
    name = config.get("topic.prefix")
    if not name:
        raise ValueError("cannot name a connector without a topic prefix")
    return json.dumps({"name": name + "-source", "config": config}, indent=2, sort_keys=True)
