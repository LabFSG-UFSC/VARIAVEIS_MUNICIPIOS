# postgis_local

Este e o README principal da pasta `postgis_local`. Aqui ficam a documentacao e os scripts usados para preparar um banco espacial local com `PostgreSQL + PostGIS` para uso no DBeaver.

## Objetivo

Subir um servidor PostgreSQL local e importar 3 camadas espaciais do projeto:

- municipios
- regioes geograficas imediatas (`RGI`)
- regioes geograficas intermediarias (`RGINT`)

## Contexto

Os GeoPackages finais do projeto existem em:

- `regioes_geograficas/processamento/variaveis_municipios.gpkg`
- `regioes_geograficas/processamento/variaveis_regioes_imediatas.gpkg`
- `regioes_geograficas/processamento/variaveis_regioes_intermediarias.gpkg`

Durante a validacao, os 3 arquivos apresentaram inconsistencia no catalogo interno do SQLite/GeoPackage:

- as tabelas continuam referenciadas em `gpkg_contents`
- as camadas principais nao podem mais ser abertas como tabelas normais
- `ogrinfo` retorna erro ao tentar acessar essas layers

Exemplo do problema:

```text
Warning 1: Table/view variaveis_municipios is referenced in gpkg_contents, but does not exist
ERROR 1: Couldn't fetch requested layer variaveis_municipios.
```

Por isso, o fluxo seguido nesta pasta foi:

1. reconstruir os dados tabulares em SQLite
2. localizar fontes espaciais integras para as geometrias
3. subir um servidor PostgreSQL local com PostGIS
4. importar geometrias e atributos corrigidos

## Fontes espaciais

### Municipios

- `bronze/BR_Municipios_2025 (1)/BR_Municipios_2025.shp`

Inspecao realizada:

- `Feature Count: 5573`
- geometria `Polygon`
- referencia SIRGAS 2000
- campos-chave: `CD_MUN`, `NM_MUN`, `CD_RGI`, `CD_RGINT`

### RGI

- `regioes_geograficas/processamento/RG2017_rgi.shp`

Inspecao realizada:

- `Feature Count: 510`
- geometria `3D Polygon`

### RGINT

- `regioes_geograficas/processamento/RG2017_rgint.shp`

Inspecao realizada:

- `Feature Count: 133`
- geometria `3D Polygon`

## Ferramentas utilizadas

### QGIS

Foi usada a instalacao local do QGIS para obter `ogr2ogr`, `ogrinfo` e `gdalinfo`.

- `/Applications/QGIS.app`
- `/Applications/QGIS.app/Contents/MacOS/ogr2ogr`
- `/Applications/QGIS.app/Contents/MacOS/ogrinfo`
- `/Applications/QGIS.app/Contents/MacOS/gdalinfo`

### PostgreSQL

Foi usado o `Postgres.app` oficial.

Artefatos identificados durante a montagem local:

- instalador temporario: `/tmp/Postgres-2.9.4-16.dmg`
- `initdb`: `/Volumes/Postgres-2.9.4-16/Postgres.app/Contents/Versions/16/bin/initdb`
- `pg_ctl`: `/Volumes/Postgres-2.9.4-16/Postgres.app/Contents/Versions/16/bin/pg_ctl`
- `psql`: `/Volumes/Postgres-2.9.4-16/Postgres.app/Contents/Versions/16/bin/psql`
- `createdb`: `/Volumes/Postgres-2.9.4-16/Postgres.app/Contents/Versions/16/bin/createdb`
- extensao PostGIS: `/Volumes/Postgres-2.9.4-16/Postgres.app/Contents/Versions/16/share/postgresql/extension/postgis.control`

## Estrutura local do cluster

O cluster local foi inicializado em:

- `/tmp/variaveis_postgres/data`

Arquivos auxiliares:

- runtime/socket: `/tmp/variaveis_postgres/run`
- log: `/tmp/variaveis_postgres/log/postgres.log`

## Scripts da pasta

Os scripts ficam em [scripts](/Users/nataliacarvinho/Documents/variaveis_municipios/variaveis_municipios/regioes_geograficas/processamento/postgis_local/scripts).

Ordem sugerida:

1. `01_start_postgres_local.sh`
   Inicializa e sobe o PostgreSQL local.
2. `02_importar_geometrias_postgis.sh`
   Importa as geometrias para tabelas de staging.
3. `03_reconstruir_municipios_corrompidos.py`
   Reconstrui os municipios com `cod_mun` quebrado e gera artefatos auxiliares.
4. `04_importar_atributos_municipios_postgis.sh`
   Importa `variaveis_municipios_corrigido.csv` para staging.
5. `05_criar_variaveis_municipios_postgis.sql`
   Cria a tabela final `public.variaveis_municipios`.

## Artefatos gerados no processo

- [reconstrucao_cod_mun_corrompidos.csv](/Users/nataliacarvinho/Documents/variaveis_municipios/variaveis_municipios/regioes_geograficas/processamento/reconstrucao_cod_mun_corrompidos.csv:1)
- [municipios_extras_malha_2025.csv](/Users/nataliacarvinho/Documents/variaveis_municipios/variaveis_municipios/regioes_geograficas/processamento/municipios_extras_malha_2025.csv:1)
- [variaveis_municipios_corrigido.csv](/Users/nataliacarvinho/Documents/variaveis_municipios/variaveis_municipios/regioes_geograficas/processamento/variaveis_municipios_corrigido.csv:1)
- `geodados_dbeaver_corrigido.sqlite`

Resumo:

- `76` municipios com `cod_mun` quebrado foram reconstruidos
- `0` `cod_mun` invalidos restantes na versao corrigida

## Estado final esperado

Configuracao usada no ambiente local:

- `Host`: `127.0.0.1`
- `Port`: `5432`
- `Database`: `variaveis_geo`
- `User`: `postgres`
- `Password`: vazio

Tabela final esperada:

- `public.variaveis_municipios`

Estado documentado da tabela final:

- `5573` geometrias na malha municipal
- `5567` municipios com atributos vinculados
- `6` municipios extras da malha 2025 sem atributos

Municipios extras identificados:

- `2201988` `Brejo do Piaui`
- `2512903` `Rio Tinto`
- `2704807` `Maribondo`
- `4300001` `Area Operacional "Lagoa Mirim"`
- `4300002` `Area Operacional "Lagoa dos Patos"`
- `5101837` `Boa Esperanca do Norte`

## Como abrir no DBeaver

1. Crie uma conexao `PostgreSQL`.
2. Use:
   - `Host`: `127.0.0.1`
   - `Port`: `5432`
   - `Database`: `variaveis_geo`
   - `User`: `postgres`
   - `Password`: vazio
3. Navegue ate `public > tables`.
4. Abra `variaveis_municipios`.

## Observacoes

- o fluxo SQLite continua util para consulta tabular:
  - `regioes_geograficas/processamento/geodados_dbeaver.sqlite`
- para visualizacao espacial no DBeaver, o caminho recomendado nesta pasta e `PostgreSQL + PostGIS`
- a documentacao detalhada e historica da montagem inicial permanece em [README_POSTGRES_POSTGIS.md](/Users/nataliacarvinho/Documents/variaveis_municipios/variaveis_municipios/regioes_geograficas/processamento/postgis_local/README_POSTGRES_POSTGIS.md:1)
