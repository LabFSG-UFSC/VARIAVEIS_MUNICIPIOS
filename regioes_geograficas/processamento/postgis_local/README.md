# Fluxo local: SQLite e PostgreSQL/PostGIS

Esta pasta concentra a documentacao e os scripts usados para abrir os dados no DBeaver com `PostgreSQL/PostGIS`.

## O que existe aqui

### SQLite para consulta tabular

- [geodados_dbeaver.sqlite](/Users/nataliacarvinho/Documents/variaveis_municipios/variaveis_municipios/regioes_geograficas/processamento/geodados_dbeaver.sqlite)
- [geodados_dbeaver_corrigido.sqlite](/Users/nataliacarvinho/Documents/variaveis_municipios/variaveis_municipios/regioes_geograficas/processamento/geodados_dbeaver_corrigido.sqlite)

Esses bancos servem bem para consulta tabular. A geometria fica como `BLOB`, então nao e a melhor opcao para visualizacao espacial.

### PostgreSQL/PostGIS local

Foi montado um servidor PostgreSQL local temporario com `PostGIS`, usando `Postgres.app` e `QGIS/ogr2ogr`.

Configuracao usada:

- `Host`: `127.0.0.1`
- `Port`: `5432`
- `Database`: `variaveis_geo`
- `User`: `postgres`
- `Password`: vazio

Tabela final pronta:

- `public.variaveis_municipios`

Estado atual dessa tabela:

- `5573` geometrias na malha municipal
- `5567` municipios com atributos vinculados
- `6` municipios extras da malha 2025 sem atributos

Municipios extras identificados:

- `2201988` `Brejo do Piauí`
- `2512903` `Rio Tinto`
- `2704807` `Maribondo`
- `4300001` `Área Operacional "Lagoa Mirim"`
- `4300002` `Área Operacional "Lagoa dos Patos"`
- `5101837` `Boa Esperança do Norte`

## Problema encontrado nos GeoPackages

Os arquivos:

- `variaveis_municipios.gpkg`
- `variaveis_regioes_imediatas.gpkg`
- `variaveis_regioes_intermediarias.gpkg`

ficaram inconsistentes no catalogo interno do SQLite/GeoPackage. As layers continuavam referenciadas em `gpkg_contents`, mas nao podiam mais ser abertas como tabelas normais.

Por causa disso, o fluxo foi:

1. recuperar os dados tabulares em SQLite
2. localizar malhas espaciais integras em shapefile
3. reconstruir os municipios corrompidos por alinhamento com a malha municipal integra
4. importar a malha e os atributos corrigidos para PostGIS

## Fontes espaciais usadas

### Municipios

- `bronze/BR_Municipios_2025 (1)/BR_Municipios_2025.shp`

### Regioes imediatas

- `regioes_geograficas/processamento/RG2017_rgi_20180911 (1)/RG2017_rgi.shp`

### Regioes intermediarias

- `regioes_geograficas/processamento/RG2017_rgint_20180911/RG2017_rgint.shp`

## Artefatos de reconstrucao

- [reconstrucao_cod_mun_corrompidos.csv](/Users/nataliacarvinho/Documents/variaveis_municipios/variaveis_municipios/regioes_geograficas/processamento/reconstrucao_cod_mun_corrompidos.csv:1)
- [municipios_extras_malha_2025.csv](/Users/nataliacarvinho/Documents/variaveis_municipios/variaveis_municipios/regioes_geograficas/processamento/municipios_extras_malha_2025.csv:1)
- [variaveis_municipios_corrigido.csv](/Users/nataliacarvinho/Documents/variaveis_municipios/variaveis_municipios/regioes_geograficas/processamento/variaveis_municipios_corrigido.csv:1)

Resumo da reconstrucao:

- `76` municipios com `cod_mun` quebrado foram reconstruidos
- `0` `cod_mun` invalidos restantes na versao corrigida

## Scripts

Os scripts do fluxo ficam em [scripts](/Users/nataliacarvinho/Documents/variaveis_municipios/variaveis_municipios/regioes_geograficas/processamento/postgis_local/scripts).

### Ordem sugerida

1. `01_start_postgres_local.sh`
   Inicializa e sobe o PostgreSQL local em `/tmp/variaveis_postgres`.

2. `02_importar_geometrias_postgis.sh`
   Importa as tres malhas espaciais para tabelas `stg_*_geo`.

3. `03_reconstruir_municipios_corrompidos.py`
   Gera:
   - `reconstrucao_cod_mun_corrompidos.csv`
   - `municipios_extras_malha_2025.csv`
   - `variaveis_municipios_corrigido.csv`
   - `geodados_dbeaver_corrigido.sqlite`

4. `04_importar_atributos_municipios_postgis.sh`
   Importa `variaveis_municipios_corrigido.csv` para `stg_variaveis_municipios_attr`.

5. `05_criar_variaveis_municipios_postgis.sql`
   Cria a tabela final `public.variaveis_municipios`.

## Como abrir no DBeaver

1. Crie uma conexao `PostgreSQL`
2. Use:
   - `Host`: `127.0.0.1`
   - `Port`: `5432`
   - `Database`: `variaveis_geo`
   - `User`: `postgres`
   - `Password`: vazio
3. Abra:
   - `Databases`
   - `variaveis_geo`
   - `Schemas`
   - `public`
   - `Tables`
4. Consulte `variaveis_municipios`

Para ver dados:

- clique com o botao direito em `variaveis_municipios`
- `View Data`
- `All Rows`

## Observacoes

- o servidor PostgreSQL atual foi criado localmente em `/tmp/variaveis_postgres`
- o instalador usado foi `/tmp/Postgres-2.9.4-16.dmg`
- a documentacao historica do processo interrompido antes da conclusao ficou em [README_POSTGRES_POSTGIS.md](/Users/nataliacarvinho/Documents/variaveis_municipios/variaveis_municipios/regioes_geograficas/processamento/postgis_local/README_POSTGRES_POSTGIS.md:1)
