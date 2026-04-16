#!/bin/zsh

set -euo pipefail

ROOT="/Users/nataliacarvinho/Documents/variaveis_municipios/variaveis_municipios"
POSTGRES_DMG="/tmp/Postgres-2.9.4-16.dmg"
POSTGRES_MOUNT="/Volumes/Postgres-2.9.4-16"
POSTGRES_APP="$POSTGRES_MOUNT/Postgres.app"
PG_BIN="$POSTGRES_APP/Contents/Versions/16/bin"

DATA_DIR="/tmp/variaveis_postgres/data"
RUN_DIR="/tmp/variaveis_postgres/run"
LOG_DIR="/tmp/variaveis_postgres/log"
LOG_FILE="$LOG_DIR/postgres.log"
PORT="5432"

mkdir -p "$RUN_DIR" "$LOG_DIR"

if [ ! -d "$POSTGRES_APP" ]; then
  echo "Postgres.app nao encontrado em $POSTGRES_APP"
  echo "Monte o dmg em $POSTGRES_DMG antes de continuar."
  exit 1
fi

if [ ! -f "$DATA_DIR/PG_VERSION" ]; then
  mkdir -p "$DATA_DIR"
  "$PG_BIN/initdb" -D "$DATA_DIR" -U postgres --auth=trust
fi

if [ -f "$DATA_DIR/postmaster.pid" ]; then
  echo "PostgreSQL ja parece estar inicializado."
fi

"$PG_BIN/pg_ctl" -D "$DATA_DIR" -l "$LOG_FILE" -o "-k $RUN_DIR -p $PORT" start

echo
echo "Servidor PostgreSQL local iniciado."
echo "Host: 127.0.0.1"
echo "Port: $PORT"
echo "Database padrao inicial: postgres"
echo "User: postgres"
