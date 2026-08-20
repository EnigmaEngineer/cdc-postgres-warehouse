#!/usr/bin/env bash
# Stop what bootstrap-local.sh started.
#
#   ./scripts/teardown.sh              stop the server, keep the data directory
#   ./scripts/teardown.sh --purge      also remove the prefix
#
# Writing this is how you find out what the bring-up really starts. If a service is in
# docker-compose.yml and not in here, ask whether any code in the repo ever contacts it.

set -uo pipefail

PREFIX="${PG_PREFIX:-/tmp/cdc-pg}"
PGDATA="${PGDATA:-$PREFIX/data}"
BIN="$PREFIX/root/usr/lib/postgresql/14/bin"
export LD_LIBRARY_PATH="$PREFIX/root/usr/lib/x86_64-linux-gnu:${LD_LIBRARY_PATH:-}"

if [ ! -x "$BIN/pg_ctl" ]; then
  echo "nothing unpacked under $PREFIX"
elif "$BIN/pg_ctl" -D "$PGDATA" status >/dev/null 2>&1; then
  "$BIN/pg_ctl" -D "$PGDATA" -m fast -w stop || echo "pg_ctl stop failed, see $PREFIX/postgres.log"
elif [ -f "$PGDATA/postmaster.pid" ]; then
  # The pid file outlives the process when the shell that owned it is killed. Removing it
  # is safe only because pg_ctl status has just said nothing is running.
  echo "no server running, removing stale postmaster.pid"
  rm -f "$PGDATA/postmaster.pid"
else
  echo "no running server found under $PGDATA"
fi

if [ "${1:-}" = "--purge" ]; then
  # Named rather than globbed. This deletes a path the caller may not own, so it says
  # what it is about to remove first.
  echo "removing $PREFIX"
  rm -rf "$PREFIX"
fi
