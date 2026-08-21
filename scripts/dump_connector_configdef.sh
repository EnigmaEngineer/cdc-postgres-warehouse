#!/usr/bin/env bash
# Read the Debezium Postgres connector's configuration keys out of the jar and write them
# to db/debezium-configdef.tsv.
#
# Why this is a script and not part of the test suite. It needs a JDK and it pulls three
# jars from Maven, and nothing else in this repo needs either. The output is committed so
# that the validator has something to check against on a machine with neither.
#
# It also emits the murmur2 vectors in tests/test_topics.py, which is the other thing
# here that has no business being trusted to a port nobody compared against the original.
#
#   ./scripts/dump_connector_configdef.sh
#   DBZ_VERSION=2.7.3.Final KAFKA_VERSION=3.7.1 ./scripts/dump_connector_configdef.sh

set -euo pipefail

DBZ_VERSION="${DBZ_VERSION:-2.7.3.Final}"
KAFKA_VERSION="${KAFKA_VERSION:-3.7.1}"
SLF4J_VERSION="${SLF4J_VERSION:-1.7.36}"
WORK="${WORK:-/tmp/dbz-configdef}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="$REPO_ROOT/db/debezium-configdef.tsv"
MAVEN=https://repo1.maven.org/maven2

command -v javac >/dev/null || { echo "needs a JDK on PATH, javac not found" >&2; exit 1; }

mkdir -p "$WORK/src" "$WORK/classes"
cd "$WORK"

if [ ! -d debezium-connector-postgres ]; then
  curl -fsSL -o plugin.tar.gz \
    "$MAVEN/io/debezium/debezium-connector-postgres/$DBZ_VERSION/debezium-connector-postgres-$DBZ_VERSION-plugin.tar.gz"
  tar xzf plugin.tar.gz
fi
for jar in "org/apache/kafka/connect-api/$KAFKA_VERSION/connect-api-$KAFKA_VERSION.jar" \
           "org/apache/kafka/kafka-clients/$KAFKA_VERSION/kafka-clients-$KAFKA_VERSION.jar" \
           "org/slf4j/slf4j-api/$SLF4J_VERSION/slf4j-api-$SLF4J_VERSION.jar"; do
  [ -f "$(basename "$jar")" ] || curl -fsSL -O "$MAVEN/$jar"
done

CP="debezium-connector-postgres/*:connect-api-$KAFKA_VERSION.jar:kafka-clients-$KAFKA_VERSION.jar:slf4j-api-$SLF4J_VERSION.jar"

cat > src/Dump.java <<'JAVA'
import org.apache.kafka.common.config.ConfigDef;
import java.util.*;

// Ask the connector what it accepts. Reading the documentation and typing the answer out
// gives you the properties whoever typed it remembered.
public class Dump {
  public static void main(String[] a) throws Exception {
    Class<?> c = Class.forName("io.debezium.connector.postgresql.PostgresConnector");
    Object inst = c.getDeclaredConstructor().newInstance();
    ConfigDef d = (ConfigDef) c.getMethod("config").invoke(inst);
    List<String> names = new ArrayList<>(d.configKeys().keySet());
    Collections.sort(names);
    for (String n : names) {
      ConfigDef.ConfigKey k = d.configKeys().get(n);
      System.out.println(n + "\t" + k.type + "\t"
        + (k.hasDefault() ? String.valueOf(k.defaultValue) : "NO_DEFAULT") + "\t" + k.importance);
    }
  }
}
JAVA

cat > src/Murmur.java <<'JAVA'
import org.apache.kafka.common.utils.Utils;
import java.io.*;
import java.nio.charset.StandardCharsets;

// One key per line on stdin. Prints the key, the unsigned hash, and the partition at the
// count given as the first argument. This is what tests/test_topics.py pins against.
public class Murmur {
  public static void main(String[] a) throws Exception {
    int parts = a.length > 0 ? Integer.parseInt(a[0]) : 6;
    BufferedReader r = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
    String line;
    while ((line = r.readLine()) != null) {
      byte[] b = line.getBytes(StandardCharsets.UTF_8);
      int h = Utils.murmur2(b);
      System.out.println(line + "\t" + Integer.toUnsignedString(h) + "\t" + ((h & 0x7fffffff) % parts));
    }
  }
}
JAVA

javac -cp "$CP" -d classes src/Dump.java src/Murmur.java

{
  cat <<EOF
# Configuration keys the Debezium Postgres connector really declares, read out of the jar
# rather than out of the documentation. Four columns, tab separated. The name and the type
# and the default and the importance. A default of NO_DEFAULT means the key has none.
#
# This file exists so that a connector config in this repo can be checked against the
# connector instead of against somebody's memory of it. A key list written by hand is a
# list of the properties its author happened to remember.
#
# Regenerate with scripts/dump_connector_configdef.sh, which needs a JDK and pulls the
# connector plugin and the Kafka client jars from Maven. Nothing else in this repo needs
# either, which is why the output is committed and the tool is a script.
#
# Source: io.debezium:debezium-connector-postgres:$DBZ_VERSION
EOF
  java -cp "$CP:classes" Dump 2>/dev/null
} > "$OUT"

echo "wrote $OUT, $(grep -vc '^#' "$OUT") keys"
echo
echo "murmur2 vectors, for tests/test_topics.py:"
python3 -c "
import json
for oid in (1,):
    for line in (1, 2, 3):
        print(json.dumps({'order_id': oid, 'line_no': line}, separators=(',', ':')))
for cid in (1, 40):
    print(json.dumps({'customer_id': cid}, separators=(',', ':')))
" | java -cp "$CP:classes" Murmur 6 2>/dev/null
