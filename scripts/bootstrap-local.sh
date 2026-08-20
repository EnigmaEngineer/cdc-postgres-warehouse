#!/usr/bin/env bash
# Bring up Postgres 14 with logical decoding on, without Docker and without root.
#
# Why this exists. docker-compose.yml is the path on a laptop. It needs a Docker daemon,
# and a project whose only bring-up path needs a daemon is a project that has never been
# run anywhere the daemon is missing. This script is the path that has actually run.
#
# It unpacks the Debian packages into a prefix with dpkg -x rather than installing them.
# No package manager lock, no root, nothing written outside the prefix.
#
#   ./scripts/bootstrap-local.sh            bring up and load the schema
#   PG_PORT=55433 ./scripts/bootstrap-local.sh
#
# The server does not survive past the shell that started it in some sandboxes. If you are
# scripting against it, keep the bring-up and the work in one script.

set -euo pipefail

PREFIX="${PG_PREFIX:-/tmp/cdc-pg}"
PGDATA="${PGDATA:-$PREFIX/data}"
PG_PORT="${PG_PORT:-55432}"
PG_SOCKET="${PG_SOCKET:-/tmp}"
PG_VERSION=14
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

PKGS="postgresql-$PG_VERSION postgresql-client-$PG_VERSION libpq5 libllvm14 libicu70 libxslt1.1 libxml2"
BIN="$PREFIX/root/usr/lib/postgresql/$PG_VERSION/bin"
SHARE="$PREFIX/root/usr/share/postgresql/$PG_VERSION"
export LD_LIBRARY_PATH="$PREFIX/root/usr/lib/x86_64-linux-gnu:${LD_LIBRARY_PATH:-}"

if [ ! -x "$BIN/postgres" ]; then
  echo "unpacking postgres $PG_VERSION into $PREFIX"
  mkdir -p "$PREFIX/deb" "$PREFIX/root"
  ( cd "$PREFIX/deb" && apt-get download $PKGS )
  for f in "$PREFIX"/deb/*.deb; do dpkg -x "$f" "$PREFIX/root"; done
fi

if [ ! -f "$PGDATA/PG_VERSION" ]; then
  echo "initdb $PGDATA"
  "$BIN/initdb" -D "$PGDATA" -U postgres --auth=trust --encoding=UTF8 --locale=C -L "$SHARE" >/dev/null
  cat >> "$PGDATA/postgresql.conf" <<CONF
listen_addresses = '127.0.0.1'
port = $PG_PORT
unix_socket_directories = '$PG_SOCKET'
# Logical decoding is the whole reason this database exists here. Without it the write
# ahead log carries enough to recover and not enough to reconstruct a row change.
wal_level = logical
max_replication_slots = 8
max_wal_senders = 8
CONF
fi

# pg_ctl start against a server that is already up prints a warning and then fails, so the
# state has to be read first. A stale postmaster.pid is the other case and it is common
# here, because a sandbox that kills the shell leaves the file behind with a dead pid in it.
if "$BIN/pg_ctl" -D "$PGDATA" status >/dev/null 2>&1; then
  echo "server already running, leaving it alone"
else
  if [ -f "$PGDATA/postmaster.pid" ]; then
    echo "removing stale $PGDATA/postmaster.pid, pg_ctl reports no server running"
    rm -f "$PGDATA/postmaster.pid"
  fi
  "$BIN/pg_ctl" -D "$PGDATA" -l "$PREFIX/postgres.log" -w start
fi

"$BIN/psql" -h "$PG_SOCKET" -p "$PG_PORT" -U postgres -tAc \
  "select 1 from pg_database where datname='shop'" | grep -q 1 \
  || "$BIN/createdb" -h "$PG_SOCKET" -p "$PG_PORT" -U postgres shop

# db/schema.sql is written to run against an empty database, because that is what the
# probe hands it after a drop. Applying it twice fails on the first create table. Checking
# first is what makes a second run of this script a no-op rather than an error.
HAS_SCHEMA=$("$BIN/psql" -h "$PG_SOCKET" -p "$PG_PORT" -U postgres -d shop -tAc \
  "select count(*) from information_schema.tables where table_schema='shop'")
if [ "$HAS_SCHEMA" = "0" ]; then
  "$BIN/psql" -h "$PG_SOCKET" -p "$PG_PORT" -U postgres -d shop -v ON_ERROR_STOP=1 \
    -f "$REPO_ROOT/db/schema.sql" >/dev/null
  echo "schema loaded"
else
  echo "schema already present, $HAS_SCHEMA tables, leaving it alone"
fi

echo "ready: host=$PG_SOCKET port=$PG_PORT dbname=shop user=postgres"
"$BIN/psql" -h "$PG_SOCKET" -p "$PG_PORT" -U postgres -d shop -c "show wal_level" -tA
