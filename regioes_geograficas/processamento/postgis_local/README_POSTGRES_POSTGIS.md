# PostgreSQL + PostGIS local

Este arquivo documenta o processo seguido para preparar um banco espacial local com `PostgreSQL + PostGIS` para uso no DBeaver.

## Objetivo

Subir um servidor PostgreSQL local na propria maquina e importar 3 camadas espaciais:

- municipios
- regioes geograficas imediatas (`RGI`)
- regioes geograficas intermediarias (`RGINT`)

## Contexto encontrado

Os GeoPackages finais do projeto existem em:

- `regioes_geograficas/processamento/variaveis_municipios.gpkg`
- `regioes_geograficas/processamento/variaveis_regioes_imediatas.gpkg`
- `regioes_geograficas/processamento/variaveis_regioes_intermediarias.gpkg`

Porem, durante a validacao, os 3 arquivos apresentaram inconsistencia no catalogo interno do SQLite/GeoPackage:

- as tabelas ainda aparecem referenciadas em `gpkg_contents`
- mas as camadas principais nao existem mais como tabelas legiveis
- `ogrinfo` retorna erro ao tentar abrir essas layers

Exemplo do problema:

```text
Warning 1: Table/view variaveis_municipios is referenced in gpkg_contents, but does not exist
ERROR 1: Couldn't fetch requested layer variaveis_municipios.
```

Por isso, o caminho seguido foi:

1. reconstruir os dados tabulares em SQLite
2. localizar fontes espaciais integrais alternativas para a geometria
3. iniciar um servidor PostgreSQL local com PostGIS

## Fontes espaciais encontradas

### Municipios

Fonte espacial integra encontrada:

- `bronze/BR_Municipios_2025 (1)/BR_Municipios_2025.shp`

Inspecao feita com `ogrinfo` mostrou:

- `Feature Count: 5573`
- geometria `Polygon`
- SIRGAS 2000
- campos-chave:
  - `CD_MUN`
  - `NM_MUN`
  - `CD_RGI`
  - `CD_RGINT`

### RGI

Fonte espacial integra encontrada:

- `regioes_geograficas/processamento/RG2017_rgi.shp`

Inspecao mostrou:

- `Feature Count: 510`
- geometria `3D Polygon`

### RGINT

Fonte espacial integra encontrada:

- `regioes_geograficas/processamento/RG2017_rgint.shp`

Inspecao mostrou:

- `Feature Count: 133`
- geometria `3D Polygon`

## Ferramentas utilizadas

### QGIS

O projeto nao tinha `ogr2ogr` no `PATH`, mas havia uma instalacao local do QGIS:

- `/Applications/QGIS.app`

Binarios encontrados:

- `/Applications/QGIS.app/Contents/MacOS/ogr2ogr`
- `/Applications/QGIS.app/Contents/MacOS/ogrinfo`
- `/Applications/QGIS.app/Contents/MacOS/gdalinfo`

### PostgreSQL

Nao havia `psql`, `createdb`, `docker` ou `brew` no `PATH`.

Foi usado o `Postgres.app` oficial, baixado temporariamente para:

- `/tmp/Postgres-2.9.4-16.dmg`

Imagem montada em:

- `/Volumes/Postgres-2.9.4-16`

Binarios encontrados:

- `/Volumes/Postgres-2.9.4-16/Postgres.app/Contents/Versions/16/bin/initdb`
- `/Volumes/Postgres-2.9.4-16/Postgres.app/Contents/Versions/16/bin/pg_ctl`
- `/Volumes/Postgres-2.9.4-16/Postgres.app/Contents/Versions/16/bin/psql`
- `/Volumes/Postgres-2.9.4-16/Postgres.app/Contents/Versions/16/bin/createdb`

Confirmacao do PostGIS:

- `/Volumes/Postgres-2.9.4-16/Postgres.app/Contents/Versions/16/share/postgresql/extension/postgis.control`

## Cluster local criado

O cluster do PostgreSQL foi inicializado em:

- `/tmp/variaveis_postgres/data`

Arquivos auxiliares:

- socket/config de runtime: `/tmp/variaveis_postgres/run`
- log: `/tmp/variaveis_postgres/log/postgres.log`

## Comandos executados

### 1. Inicializacao do cluster

A primeira tentativa dentro do sandbox falhou por restricao de memoria compartilhada:

```text
FATAL: could not create shared memory segment: Operation not permitted
```

Entao o `initdb` foi repetido com permissao elevada:

```bash
'/Volumes/Postgres-2.9.4-16/Postgres.app/Contents/Versions/16/bin/initdb' \
  -D /tmp/variaveis_postgres/data \
  -U postgres \
  --auth=trust
```

### 2. Inicializacao do servidor

```bash
'/Volumes/Postgres-2.9.4-16/Postgres.app/Contents/Versions/16/bin/pg_ctl' \
  -D /tmp/variaveis_postgres/data \
  -l /tmp/variaveis_postgres/log/postgres.log \
  -o "-k /tmp/variaveis_postgres/run -p 5432" \
  start
```

## Estado atual

O servidor foi iniciado com sucesso.

Evidencias:

- `postmaster.pid` existe em `/tmp/variaveis_postgres/data`
- o log registra:

```text
listening on IPv6 address "::1", port 5432
listening on IPv4 address "127.0.0.1", port 5432
listening on Unix socket "/tmp/variaveis_postgres/run/.s.PGSQL.5432"
database system is ready to accept connections
```

Tambem foi identificado no `postmaster.pid`:

- PID: `49578`
- porta: `5432`
- socket: `/tmp/variaveis_postgres/run`
- status: `ready`

## O que ainda faltava no momento desta documentacao

O processo foi interrompido antes de concluir:

1. criacao do banco final, por exemplo `variaveis_geo`
2. execucao de `CREATE EXTENSION postgis`
3. importacao das geometrias para PostGIS via `ogr2ogr`
4. carga/juncao dos atributos tabulares reconstruidos
5. validacao final no DBeaver

## Proximo passo recomendado

Retomar a partir daqui:

1. testar conexao com `psql`
2. criar banco `variaveis_geo`
3. habilitar `postgis`
4. importar:
   - `BR_Municipios_2025.shp`
   - `RG2017_rgi.shp`
   - `RG2017_rgint.shp`
5. juntar os atributos analiticos do SQLite recuperado nas camadas espaciais
6. conectar no DBeaver usando `localhost:5432`

## Observacoes importantes

- o fluxo SQLite continua sendo a opcao ja pronta para consulta tabular:
  - `regioes_geograficas/processamento/geodados_dbeaver.sqlite`
- para visualizacao espacial correta no DBeaver, o caminho recomendado continua sendo `PostgreSQL + PostGIS`
- como os `.gpkg` originais estao inconsistentes, a geometria nao deve ser lida deles diretamente sem uma recuperacao espacial adicional
