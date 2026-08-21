"""Checks over the connector config builder and its validator.

The validator's first job is not catching bad configs. It is not refusing good ones. A
validator that rejects legitimate properties gets switched off inside a week and then it
protects nothing, so the first check here is a list of real Debezium properties that must
all survive it.
"""

from cdc import connector

# Real properties from the Debezium documentation and from the config dump. None of these
# may be reported as unknown. The regex shaped ones are the interesting half, because the
# names the connector declares for them are patterns rather than literals.
MUST_NOT_REFUSE = [
    "database.hostname", "database.port", "database.user", "database.password",
    "database.dbname", "topic.prefix", "plugin.name", "slot.name", "publication.name",
    "publication.autocreate.mode", "table.include.list", "schema.include.list",
    "snapshot.mode", "tombstones.on.delete", "heartbeat.interval.ms",
    "decimal.handling.mode", "binary.handling.mode", "message.key.columns",
    "signal.data.collection", "skipped.operations", "max.batch.size", "max.queue.size",
    "column.mask.with.10.chars", "column.mask.hash.SHA-256.with.salt.pepper",
    "column.truncate.to.20.chars",
    "transforms", "transforms.route.type", "key.converter", "key.converter.schemas.enable",
    "topic.creation.default.partitions", "errors.tolerance", "producer.override.acks",
    "connector.class", "tasks.max", "name",
]


def _base():
    return connector.build(
        prefix="shopcdc",
        tables=[("shop", "customer"), ("shop", "order_item")],
        publication="cdc_shop",
        slot="shop_cdc_slot",
        dbname="shop",
    )


def check_the_validator_refuses_nothing_in_the_known_good_list():
    keys, heads = connector.load_config_keys()
    config = {k: "x" for k in MUST_NOT_REFUSE}
    refused = connector.unknown_keys(config, keys, heads)
    assert refused == [], refused


def check_a_misspelled_property_is_caught():
    config = _base()
    config["publication.autocreate.modee"] = "disabled"
    problems = connector.validate(config)
    assert any("publication.autocreate.modee" in p for p in problems), problems


def check_the_generated_config_validates_clean():
    assert connector.validate(_base()) == []


def check_every_dangerous_default_is_really_the_declared_default():
    # If Debezium changes one of these, this fails and the note gets rewritten. A note
    # about a default that is no longer the default is worse than no note.
    keys, _ = connector.load_config_keys()
    for name, entry in connector.REQUIRED_OVERRIDES.items():
        assert name in keys, name
        assert keys[name]["default"] == entry["declared_default"], (
            name, keys[name]["default"], entry["declared_default"])


def check_leaving_a_dangerous_default_alone_is_reported():
    for name in connector.REQUIRED_OVERRIDES:
        config = _base()
        del config[name]
        problems = connector.validate(config)
        assert any(name in p and "shipped default" in p for p in problems), (name, problems)


def check_the_generated_config_sets_the_publication_to_disabled():
    config = _base()
    assert config["publication.autocreate.mode"] == "disabled"
    assert config["plugin.name"] == "pgoutput"


def check_a_connector_with_no_tables_is_refused():
    try:
        connector.build("shopcdc", [], "cdc_shop", "s", "shop")
    except ValueError:
        return
    raise AssertionError("built a connector that captures nothing")


def check_the_include_list_is_schema_qualified():
    config = _base()
    assert config["table.include.list"] == "shop.customer,shop.order_item"
    config["table.include.list"] = "customer"
    assert any("schema.table" in p for p in connector.validate(config))


def check_a_dotted_prefix_is_refused():
    config = _base()
    config["topic.prefix"] = "shop.cdc"
    assert any("topic.prefix" in p for p in connector.validate(config))


def check_no_password_value_is_ever_written_into_the_config():
    config = _base()
    assert config["database.password"].startswith("${"), config["database.password"]


def check_the_configdef_file_is_read_rather_than_assumed():
    keys, heads = connector.load_config_keys()
    # A loader that silently returns nothing would let the validator pass every config.
    assert len(keys) > 50, len(keys)
    assert heads, heads
    assert "topic.prefix" in keys


def check_the_configdef_loader_refuses_a_file_with_no_keys_in_it(tmp=None):
    import tempfile
    with tempfile.NamedTemporaryFile("w", suffix=".tsv", delete=False) as handle:
        handle.write("# only a comment\n\n")
        path = handle.name
    try:
        connector.load_config_keys(path)
    except ValueError:
        return
    raise AssertionError("the loader reported success on a file carrying no keys")


def check_the_configdef_loader_refuses_a_malformed_line():
    # The message is asserted and not the type. Unpacking a two element list into four
    # names raises ValueError all by itself, so a check that only asserts the type passes
    # whether the guard is there or not. A mutant deleting the guard survived that.
    import tempfile
    with tempfile.NamedTemporaryFile("w", suffix=".tsv", delete=False) as handle:
        handle.write("topic.prefix\tSTRING\n")
        path = handle.name
    try:
        connector.load_config_keys(path)
    except ValueError as exc:
        assert "four columns" in str(exc), str(exc)
        return
    raise AssertionError("the loader accepted a line that is not four columns")


def check_render_produces_the_rest_body_shape():
    import json
    body = json.loads(connector.render(_base()))
    assert body["name"] == "shopcdc-source"
    assert body["config"]["connector.class"] == connector.CONNECTOR_CLASS


def check_render_refuses_a_config_with_no_prefix():
    config = _base()
    del config["topic.prefix"]
    try:
        connector.render(config)
    except ValueError:
        return
    raise AssertionError("rendered a connector with no name")


def check_the_wrong_connector_class_is_caught():
    config = _base()
    config["connector.class"] = "io.debezium.connector.mysql.MySqlConnector"
    assert any("connector.class" in p for p in connector.validate(config))
