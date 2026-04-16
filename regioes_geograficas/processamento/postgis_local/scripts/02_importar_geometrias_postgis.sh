#!/bin/zsh

set -euo pipefail

ROOT="/Users/nataliacarvinho/Documents/variaveis_municipios/variaveis_municipios"
OGR2OGR="/Applications/QGIS.app/Contents/MacOS/ogr2ogr"
PROJ_LIB="/Applications/QGIS.app/Contents/Resources/qgis/proj"
PG_CONN="PG:host=127.0.0.1 port=5432 dbname=variaveis_geo user=postgres"

env PROJ_LIB="$PROJ_LIB" "$OGR2OGR" \
  -f PostgreSQL "$PG_CONN" \
  "$ROOT/bronze/BR_Municipios_2025 (1)/BR_Municipios_2025.shp" \
  -nln stg_municipios_geo \
  -overwrite \
  -lco GEOMETRY_NAME=geom \
  -nlt PROMOTE_TO_MULTI \
  -dim 2

env PROJ_LIB="$PROJ_LIB" "$OGR2OGR" \
  -f PostgreSQL "$PG_CONN" \
  "$ROOT/regioes_geograficas/processamento/RG2017_rgi_20180911 (1)/RG2017_rgi.shp" \
  -nln stg_rgi_geo \
  -overwrite \
  -lco GEOMETRY_NAME=geom \
  -nlt PROMOTE_TO_MULTI \
  -dim 2

env PROJ_LIB="$PROJ_LIB" "$OGR2OGR" \
  -f PostgreSQL "$PG_CONN" \
  "$ROOT/regioes_geograficas/processamento/RG2017_rgint_20180911/RG2017_rgint.shp" \
  -nln stg_rgint_geo \
  -overwrite \
  -lco GEOMETRY_NAME=geom \
  -nlt PROMOTE_TO_MULTI \
  -dim 2

echo "Malhas espaciais importadas para o PostGIS."
