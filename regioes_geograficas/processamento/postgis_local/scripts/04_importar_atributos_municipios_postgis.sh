#!/bin/zsh

set -euo pipefail

ROOT="/Users/nataliacarvinho/Documents/variaveis_municipios/variaveis_municipios"
OGR2OGR="/Applications/QGIS.app/Contents/MacOS/ogr2ogr"
PROJ_LIB="/Applications/QGIS.app/Contents/Resources/qgis/proj"
PG_CONN="PG:host=127.0.0.1 port=5432 dbname=variaveis_geo user=postgres"
PSQL="/Volumes/Postgres-2.9.4-16/Postgres.app/Contents/Versions/16/bin/psql"

"$PSQL" -h 127.0.0.1 -p 5432 -U postgres -d variaveis_geo -c \
  "DROP TABLE IF EXISTS stg_variaveis_municipios_attr;"

env PROJ_LIB="$PROJ_LIB" "$OGR2OGR" \
  -f PostgreSQL "$PG_CONN" \
  "$ROOT/regioes_geograficas/processamento/variaveis_municipios_corrigido.csv" \
  -oo AUTODETECT_TYPE=YES \
  -nln stg_variaveis_municipios_attr \
  -overwrite

echo "Atributos municipais corrigidos importados para stg_variaveis_municipios_attr."
