# Classificacao Municipios

Esta pasta concentra os arquivos geoespaciais separados relacionados a classificacao geografica dos municipios e os scripts auxiliares de classificacao fuzzy por regiao intermediaria.

## Estrutura

- `RG2017_regioesgeograficas2017_20180911/`: arquivo nacional completo com a classificacao geografica de 2017.
- `ufs_brasil_27/`: recortes separados por unidade da federacao.
- `regioes_intermediarias_133/`: recortes separados por regiao geografica intermediaria.
- `scripts/`: scripts de classificacao e apoio analitico deste recorte.
- `processamento/`: saidas geradas pelos scripts desta pasta.
- `referencias/`: amostras e tabelas de apoio para calibracao manual das regras.

## Formatos

Os arquivos estao organizados principalmente como conjuntos de shapefile. Para uso correto, mantenha juntos os arquivos auxiliares de cada camada, como `.shp`, `.dbf`, `.shx`, `.prj` e `.cpg`.

## Observacao

Esta pasta funciona como acervo espacial separado e tambem como area de trabalho para classificacao dos municipios. Ela complementa `regioes_geograficas/`, que concentra scripts de agregacao e produtos finais do projeto em GeoPackage.

## Classificacao Fuzzy Inicial

O script [`scripts/classifica_municipios_fuzzy_rgint.py`](scripts/classifica_municipios_fuzzy_rgint.py) implementa uma classificacao fuzzy dos municipios separadamente dentro de cada regiao intermediaria.

### Objetivo

Gerar uma tipologia inicial dos municipios brasileiros dentro do contexto das 133 regioes intermediarias, aproveitando as variaveis do `merge_v26` para distinguir papeis territoriais e perfis de estrutura.

### Escopo Exato Do Processo

Este processo classifica municipios, nao regioes intermediarias.

Cada municipio recebe:

- seu vinculo espacial com uma `rgint`
- um conjunto de indicadores derivados
- scores fuzzy por eixo
- pertinencias nas classes finais
- uma classe final escolhida pela maior pertinencia

Em seguida, o processo gera:

- um arquivo consolidado com todos os municipios
- um arquivo resumo por `rgint`
- um arquivo separado para cada `rgint`, contendo apenas os municipios daquela regiao intermediaria

### Insumos

- `../prata/processamento/merge_v26.csv`: base municipal consolidada com as variaveis socioeconomicas, urbanas, de conectividade, REGIC e servicos.
- `RG2017_regioesgeograficas2017_20180911/RG2017_regioesgeograficas2017.shp`: camada municipal usada para vincular cada municipio a sua `rgint`, `nome_rgint` e `uf`.

### Scripts Envolvidos

- [`scripts/classifica_municipios_fuzzy_rgint.py`](scripts/classifica_municipios_fuzzy_rgint.py): executa a classificacao fuzzy principal.
- [`scripts/gera_amostra_calibracao_fuzzy.py`](scripts/gera_amostra_calibracao_fuzzy.py): monta uma amostra balanceada para revisao manual e calibracao.

### Parametros Padrao Do Script Principal

No estado atual do script, os caminhos padrao sao:

- `--base-municipal`: `../prata/processamento/merge_v26.csv`
- `--shapefile`: `RG2017_regioesgeograficas2017_20180911/RG2017_regioesgeograficas2017.shp`
- `--output-municipios`: `processamento/classificacao_municipios_fuzzy_rgint.csv`
- `--output-rgint`: `processamento/classificacao_rgint_resumo_fuzzy.csv`
- `--output-dir-rgint`: `processamento/por_rgint/`

Esses caminhos podem ser sobrescritos por linha de comando, mas o README considera o fluxo padrao.

### Colunas Lidas Da Base Municipal

O script principal consome explicitamente as seguintes colunas do `merge_v26.csv`:

- `cod_mun`
- `municipio`
- `pib_total`
- `pop_total`
- `empresas_total`
- `estab_total`
- `vitimas_homicidio_2022`
- `demissoes_2025`
- `registros_seca_2003_2015`
- `via_pav_pct`
- `ilum_pub_pct`
- `calcada_pct`
- `plano_diretor`
- `area_urb_densa_km2`
- `indice_conectividade`
- `cobertura_pop_4g5g`
- `densidade_scm`
- `fibra`
- `regic_var56`
- `regic_var59`
- `regic_var60`
- `regic_var61`
- `regic_var66`

### Colunas Lidas Do Shapefile

O script usa as seguintes colunas espaciais para o vinculo territorial:

- `CD_GEOCODI`: codigo municipal IBGE, convertido para `cod_mun`
- `rgint`: codigo da regiao geografica intermediaria
- `nome_rgint`: nome da regiao geografica intermediaria
- `UF`: codigo da unidade da federacao, convertido para `uf`

### Validacoes De Entrada

Antes de calcular qualquer score fuzzy, o script faz validacoes basicas:

- verifica se `cod_mun` existe na base municipal
- converte `cod_mun` para inteiro anulavel (`Int64`)
- converte as colunas numericas da base para formato numerico
- verifica se o shapefile possui identificador municipal valido
- faz `merge` por `cod_mun` com validacao `one_to_one`
- interrompe a execucao se algum municipio da base ficar sem `rgint`

### Eixos usados na classificacao

- `centralidade_economica`
- `infraestrutura_urbana`
- `conectividade_digital`
- `oferta_servicos`
- `vulnerabilidade`

### Variaveis principais por eixo

- `centralidade_economica`: `pib_pc`, `empresas_1k`, `regic_var56`, `regic_var61`, `regic_var66`
- `infraestrutura_urbana`: `via_pav_pct`, `calcada_pct`, `ilum_pub_pct`, `plano_diretor`, `area_urb_densa_100k`
- `conectividade_digital`: `indice_conectividade`, `cobertura_pop_4g5g`, `densidade_scm`, `fibra`
- `oferta_servicos`: `estab_saude_10k`, `regic_var59`, `regic_var60`, `regic_var61`, `regic_var66`
- `vulnerabilidade`: `homicidios_100k`, `demissoes_1k`, `registros_seca_2003_2015`, deficit de infraestrutura, deficit de conectividade

### Indicadores derivados

Antes de aplicar a logica fuzzy, o script cria indicadores derivados:

- `pib_pc = pib_total / pop_total`
- `empresas_1k = 1000 * empresas_total / pop_total`
- `estab_saude_10k = 10000 * estab_total / pop_total`
- `homicidios_100k = 100000 * vitimas_homicidio_2022 / pop_total`
- `demissoes_1k = 1000 * demissoes_2025 / pop_total`
- `area_urb_densa_100k = 100000 * area_urb_densa_km2 / pop_total`

Esses calculos sao feitos pela funcao `safe_div`, que:

- divide somente quando o denominador e maior que zero
- retorna `NaN` quando a divisao nao e valida
- preserva o indice do DataFrame original

### Higienizacao E Normalizacao Pre-Fuzzy

Tambem sao aplicados ajustes simples:

- truncamento de percentuais e indicadores normalizados para a faixa `0..100`
- normalizacao da seca dentro da propria `rgint`
- tratamento neutro para ausencias na variavel de seca, usando valor intermediario

Detalhando:

- `via_pav_pct`, `ilum_pub_pct`, `calcada_pct`, `indice_conectividade`, `cobertura_pop_4g5g`, `densidade_scm` e `fibra` sao truncadas com `clip(0, 100)`
- `registros_seca_2003_2015` nao entra bruto no modelo; primeiro ele e transformado em `seca_normalizada`
- `seca_normalizada` e calculada por ranking percentual dentro de cada `rgint`
- valores ausentes em `seca_normalizada` recebem `0.5`, que funciona como posicao neutra

### Ordem Exata Do Pipeline No Script Principal

O fluxo executado por [`scripts/classifica_municipios_fuzzy_rgint.py`](scripts/classifica_municipios_fuzzy_rgint.py) e este:

1. `parse_args()`
2. `carrega_base()`
3. `carrega_rgint_por_municipio()`
4. `merge` entre base municipal e shapefile por `cod_mun`
5. `calcula_indicadores()`
6. `calcula_scores_fuzzy()`
7. `classifica_regras()`
8. `gera_resumo_rgint()`
9. gravacao dos CSVs finais

Essa sequencia e importante para rastreabilidade porque cada etapa depende das colunas produzidas pela anterior.

### Logica de comparacao

Cada indicador recebe um score contextual de `0` a `1`:

- `100%` do score vem da posicao relativa do municipio dentro da sua propria `rgint`

Isso significa que:

- cada `rgint` e tratada como um universo proprio de comparacao
- os municipios de uma regiao nao disputam score com municipios de outras regioes
- a escala fuzzy e recalculada separadamente em cada uma das 133 regioes intermediarias

### Formula Exata Do Score Contextual

Para um indicador positivo, o script calcula:

```text
score_bruto = rank_percentual_dentro_da_rgint
```

Para um indicador negativo, o script calcula:

```text
score_bruto = 1 - rank_percentual_dentro_da_rgint
```

Depois disso:

- o score e truncado para a faixa `0..1`
- valores ausentes no ranking recebem tratamento neutro por meio da funcao `percent_rank`
- o ranking e recalculado separadamente para cada `rgint`

### Indicadores Positivos E Negativos

Entram como indicadores positivos:

- `pib_pc`
- `empresas_1k`
- `regic_var56`
- `regic_var59`
- `regic_var60`
- `regic_var61`
- `regic_var66`
- `via_pav_pct`
- `calcada_pct`
- `ilum_pub_pct`
- `plano_diretor`
- `area_urb_densa_100k`
- `indice_conectividade`
- `cobertura_pop_4g5g`
- `densidade_scm`
- `fibra`
- `estab_saude_10k`

Entram como indicadores negativos:

- `homicidios_100k`
- `demissoes_1k`

### Forma De Agregacao Dos Eixos

Todos os eixos sao agregados pela funcao `weighted_mean`, que:

- recebe um subconjunto de colunas
- ignora valores ausentes no calculo da linha
- repondera automaticamente pelos pesos das colunas disponiveis
- retorna `NaN` apenas se nenhum componente daquele eixo estiver disponivel

Isso significa que o eixo e calculado por media ponderada efetiva, e nao por soma simples.

### Agregacao dos eixos

Os scores dos indicadores sao agregados por media ponderada em cada eixo:

- `centralidade_economica`: pesos `0.25`, `0.20`, `0.25`, `0.15`, `0.15`
- `infraestrutura_urbana`: pesos `0.30`, `0.25`, `0.15`, `0.15`, `0.15`
- `conectividade_digital`: pesos `0.35`, `0.20`, `0.25`, `0.20`
- `oferta_servicos`: pesos `0.25`, `0.25`, `0.20`, `0.15`, `0.15`
- `vulnerabilidade`: pesos `0.35`, `0.25`, `0.20`, `0.10`, `0.10`

Expandidos com os nomes dos indicadores:

- `centralidade_economica`
  - `pib_pc`: `0.25`
  - `empresas_1k`: `0.20`
  - `regic_var56`: `0.25`
  - `regic_var61`: `0.15`
  - `regic_var66`: `0.15`

- `infraestrutura_urbana`
  - `via_pav_pct`: `0.30`
  - `calcada_pct`: `0.25`
  - `ilum_pub_pct`: `0.15`
  - `plano_diretor`: `0.15`
  - `area_urb_densa_100k`: `0.15`

- `conectividade_digital`
  - `indice_conectividade`: `0.35`
  - `cobertura_pop_4g5g`: `0.20`
  - `densidade_scm`: `0.25`
  - `fibra`: `0.20`

- `oferta_servicos`
  - `estab_saude_10k`: `0.25`
  - `regic_var59`: `0.25`
  - `regic_var60`: `0.20`
  - `regic_var61`: `0.15`
  - `regic_var66`: `0.15`

- `vulnerabilidade`
  - `score_homicidios_100k`: `0.35`
  - `score_demissoes_1k`: `0.25`
  - `seca_normalizada`: `0.20`
  - `1 - infraestrutura_urbana`: `0.10`
  - `1 - conectividade_digital`: `0.10`

### Observacao Sobre O Eixo De Vulnerabilidade

O eixo `vulnerabilidade` nao e construído apenas com variaveis “negativas” brutas.

Ele combina:

- dois scores invertidos de risco socioeconomico e violencia
- uma componente ambiental resumida (`seca_normalizada`)
- dois deficits estruturais derivados de outros eixos

Por isso, esse eixo funciona como uma vulnerabilidade composta e relativa.

### Funcoes de pertinencia

Cada eixo e convertido em tres pertinencias fuzzy:

- `baixa`
- `media`
- `alta`

As funcoes sao simples e interpretableis:

- `baixa`: predominante ate `0.25`, decaindo ate `0.50`
- `media`: cresce entre `0.20` e `0.50`, e decai entre `0.50` e `0.80`
- `alta`: cresce entre `0.50` e `0.75`, e satura acima disso

Com mais detalhe:

- `pertinencia_baixa(score)`
  - valor `1` quando `score <= 0.25`
  - valor `0` quando `score >= 0.50`
  - interpolacao linear entre `0.25` e `0.50`

- `pertinencia_media(score)`
  - valor `0` ate `0.20`
  - sobe linearmente de `0.20` ate `0.50`
  - cai linearmente de `0.50` ate `0.80`
  - volta a `0` acima de `0.80`

- `pertinencia_alta(score)`
  - valor `0` ate `0.50`
  - sobe linearmente de `0.50` ate `0.75`
  - valor `1` acima de `0.75`

Essas funcoes sao usadas repetidamente sobre os cinco eixos compostos.

### Regras fuzzy para as classes finais

As classes finais sao calculadas a partir de combinacoes dos eixos:

- `muito_alto`: centralidade alta + oferta alta + conectividade alta
- `alto`: centralidade alta + servicos medio/alto + conectividade medio/alta + infraestrutura medio/alta
- `medio`: infraestrutura alta + conectividade medio/alta + centralidade media + vulnerabilidade baixa/media
- `baixo`: centralidade baixa/media + servicos baixa/media + infraestrutura media + conectividade media + vulnerabilidade baixa/media
- `muito_baixo`: vulnerabilidade alta + infraestrutura baixa + conectividade baixa + centralidade baixa

### Formula Operacional Das Regras

No script, as classes nao sao formadas por texto solto, mas por composicoes numericas:

- `muito_alto`
  - `min(central_alta, serv_alta, conect_alta)`

- `alto`
  - media ponderada de:
    - `central_alta`
    - `max(serv_media, serv_alta)`
    - `max(conect_media, conect_alta)`
    - `max(infra_media, infra_alta)`

- `medio`
  - media ponderada de:
    - `infra_alta`
    - `max(conect_media, conect_alta)`
    - `central_media`
    - `max(vuln_baixa, vuln_media)`

- `baixo`
  - media ponderada de:
    - `max(central_baixa, central_media)`
    - `max(serv_baixa, serv_media)`
    - `infra_media`
    - `conect_media`
    - `max(vuln_baixa, vuln_media)`

- `muito_baixo`
  - media ponderada de:
    - `vuln_alta`
    - `infra_baixa`
    - `conect_baixa`
    - `central_baixa`

### Pesos Internos Das Classes Finais

- `alto`
  - `central_alta`: `0.35`
  - `serv_media_alta`: `0.25`
  - `conect_media_alta`: `0.20`
  - `infra_media_alta`: `0.20`

- `medio`
  - `infra_alta`: `0.35`
  - `conect_media_alta`: `0.25`
  - `central_media`: `0.20`
  - `vuln_baixa_media`: `0.20`

- `baixo`
  - `central_baixa_media`: `0.25`
  - `serv_baixa_media`: `0.25`
  - `infra_media`: `0.20`
  - `conect_media`: `0.15`
  - `vuln_baixa_media`: `0.15`

- `muito_baixo`
  - `vuln_alta`: `0.35`
  - `infra_baixa`: `0.25`
  - `conect_baixa`: `0.20`
  - `central_baixa`: `0.20`

### Classes finais geradas

- `muito_alto`
- `alto`
- `medio`
- `baixo`
- `muito_baixo`

Para cada municipio, a classe final e a de maior pertinencia entre essas cinco.

### Regra De Desempate E Confianca

O script produz:

- uma coluna de pertinencia para cada classe final, com prefixo `pert_`
- uma coluna `classificacao_fuzzy`, definida por `idxmax` nessas pertinencias
- uma coluna `confianca_classificacao`, definida pela maior pertinencia entre as cinco classes

Portanto:

- a classe final e sempre a classe com maior valor numerico de pertinencia
- a confianca nao e uma probabilidade estatistica, mas sim a intensidade da maior pertinencia fuzzy
- se duas classes ficarem proximas, a confianca tende a ser mais baixa
- se uma classe dominar claramente, a confianca tende a ser mais alta

### Colunas Produzidas Na Saida Municipal

O arquivo `processamento/classificacao_municipios_fuzzy_rgint.csv` inclui:

- identificacao territorial
  - `cod_mun`
  - `municipio`
  - `uf`
  - `rgint`
  - `nome_rgint`

- resultado final
  - `classificacao_fuzzy`
  - `confianca_classificacao`

- eixos compostos
  - `centralidade_economica`
  - `infraestrutura_urbana`
  - `conectividade_digital`
  - `oferta_servicos`
  - `vulnerabilidade`

- indicadores derivados
  - `pib_pc`
  - `empresas_1k`
  - `estab_saude_10k`
  - `homicidios_100k`
  - `demissoes_1k`
  - `area_urb_densa_100k`

- pertinencias finais
  - `pert_muito_alto`
  - `pert_alto`
  - `pert_medio`
  - `pert_baixo`
  - `pert_muito_baixo`

### Colunas Produzidas Na Saida Por Regiao Intermediaria

O arquivo `processamento/classificacao_rgint_resumo_fuzzy.csv` inclui:

- `rgint`
- `nome_rgint`
- medias regionais dos cinco eixos
- `classificacao_dominante_rgint`
- `participacao_dominante_rgint`

### Arquivos Separados Por RGINT

O script grava tambem uma pasta:

- `processamento/por_rgint/`

Dentro dela, cada `rgint` recebe um CSV proprio, com nome no padrao:

- `codigo_nome_da_rgint.csv`

Exemplos de nome:

- `1101_porto_velho.csv`
- `3501_sao_paulo.csv`
- `5301_distrito_federal.csv`

Esses arquivos contem apenas os municipios da regiao intermediaria correspondente, ja classificados pela logica fuzzy local daquela regiao.

### Como O Resumo Por RGINT E Calculado

O resumo por regiao intermediaria nao reclassifica a `rgint` como se ela fosse um municipio.

Ele faz outra coisa:

1. calcula a distribuicao das classes finais entre os municipios da `rgint`
2. identifica a classe mais frequente dentro dessa regiao
3. calcula a participacao dessa classe dominante no total de municipios da regiao
4. calcula a media dos cinco eixos entre todos os municipios daquela `rgint`

Assim, a `classificacao_dominante_rgint` e apenas um resumo descritivo da distribuicao municipal dentro da regiao.

### Execucao

Da raiz do projeto:

```bash
python3 classificacao_municipios/scripts/classifica_municipios_fuzzy_rgint.py
```

### Saidas padrao

- `processamento/classificacao_municipios_fuzzy_rgint.csv`: classificacao final por municipio, com eixos, indicadores derivados e pertinencias.
- `processamento/classificacao_rgint_resumo_fuzzy.csv`: resumo por regiao intermediaria, com medias dos eixos e classe dominante.
- `processamento/por_rgint/*.csv`: um CSV separado para cada uma das 133 regioes intermediarias.

### Consolidacao Em XLSX

O script [`scripts/gera_xlsx_classificacao_rgint.py`](scripts/gera_xlsx_classificacao_rgint.py) consolida as saidas em um unico arquivo Excel, mantendo os CSVs originais armazenados na pasta `processamento/`.

O arquivo gerado inclui:

- uma aba `municipios` com a saida consolidada municipal
- uma aba `resumo_rgint` com o resumo por regiao intermediaria
- uma aba para cada arquivo de `processamento/por_rgint/`

Execucao:

```bash
python3 classificacao_municipios/scripts/gera_xlsx_classificacao_rgint.py
```

Saida padrao:

- `processamento/classificacao_municipios_fuzzy_rgint.xlsx`

### Exemplo De Execucao Com Caminhos Explicitos

```bash
python3 classificacao_municipios/scripts/classifica_municipios_fuzzy_rgint.py \
  --base-municipal prata/processamento/merge_v26.csv \
  --shapefile classificacao_municipios/RG2017_regioesgeograficas2017_20180911/RG2017_regioesgeograficas2017.shp \
  --output-municipios classificacao_municipios/processamento/classificacao_municipios_fuzzy_rgint.csv \
  --output-rgint classificacao_municipios/processamento/classificacao_rgint_resumo_fuzzy.csv \
  --output-dir-rgint classificacao_municipios/processamento/por_rgint
```

### Logs Esperados Do Script Principal

Ao final da execucao, o script imprime:

- o caminho do arquivo municipal gerado
- o caminho do arquivo resumo por `rgint`
- o caminho da pasta com arquivos separados por `rgint`
- a distribuicao final das classes municipais

Esses logs ajudam a verificar rapidamente se o processamento concluiu sem erro e se a distribuicao ficou plausivel.

## Calibracao Manual Da Classificacao

Depois da primeira rodada do modelo, o passo recomendado e revisar uma amostra pequena de municipios de referencia antes de alterar pesos ou regras.

### Objetivo Da Calibracao

Criar uma base curta e legivel para comparar:

- a classe gerada pelo modelo
- a classe esperada por leitura substantiva do territorio
- os ajustes que fazem sentido nos pesos, limiares ou regras fuzzy

### Script De Amostragem

O script [`scripts/gera_amostra_calibracao_fuzzy.py`](scripts/gera_amostra_calibracao_fuzzy.py) monta uma amostra inicial para revisao manual.

Ele combina dois grupos:

- municipios prioritarios conhecidos nacionalmente ou relevantes para teste de fronteira
- casos extras de alta confianca escolhidos automaticamente, com distribuicao por classe

A amostra final e balanceada por classe alvo, usando os prioritarios primeiro e completando o restante com casos extras de alta confianca.

### Parametros Padrao Do Script De Amostragem

- `--input`: `processamento/classificacao_municipios_fuzzy_rgint.csv`
- `--output`: `referencias/amostra_calibracao_inicial.csv`
- `--alvo-por-classe`: `8`

### Logica De Selecionamento Da Amostra

O script de amostragem segue esta ordem:

1. le a classificacao municipal ja gerada
2. separa um conjunto fixo de municipios prioritarios conhecidos
3. limita os prioritarios a no maximo `alvo-por-classe` dentro de cada classe
4. completa o que faltar em cada classe com casos extras automaticos
5. nos extras, prioriza:
   - maior `confianca_classificacao`
   - maior `centralidade_economica`
   - maior `oferta_servicos`
6. evita repetir municipios e tenta evitar repeticao de `rgint` entre extras usando `drop_duplicates(subset=["rgint"])`
7. grava a amostra final com colunas adicionais para revisao manual

### Campos Da Revisao Manual

No CSV de calibracao, as colunas a serem preenchidas manualmente sao:

- `classe_referencia_esperada`
- `avaliacao_modelo`
- `ajuste_sugerido`
- `observacoes_calibracao`

Uso sugerido:

- `classe_referencia_esperada`: qual classe o municipio deveria ter
- `avaliacao_modelo`: `certo`, `quase`, `errado`, ou outra convencao que voce preferir
- `ajuste_sugerido`: onde o modelo parece estar exagerando ou subestimando
- `observacoes_calibracao`: justificativa substantiva territorial

### Execucao Da Amostra

```bash
python3 classificacao_municipios/scripts/gera_amostra_calibracao_fuzzy.py
```

Saida padrao:

- `referencias/amostra_calibracao_inicial.csv`

### Conteudo Da Amostra

Cada linha traz:

- identificacao do municipio e da regiao intermediaria
- classe atual do modelo
- confianca da classificacao
- scores dos 5 eixos
- campos vazios para revisao manual:
  - `classe_referencia_esperada`
  - `avaliacao_modelo`
  - `ajuste_sugerido`
  - `observacoes_calibracao`

### Fluxo Recomendado De Calibracao

1. gerar a classificacao fuzzy municipal
2. gerar a amostra inicial de calibracao
3. revisar os municipios da amostra e preencher a classe esperada
4. identificar padroes de erro recorrentes
5. ajustar pesos, funcoes de pertinencia ou regras
6. rerodar a classificacao e comparar a melhora

### O Que Deve Ser Rastreado Em Cada Rodada De Ajuste

Para que o processo continue rastreavel, cada rodada futura deve registrar pelo menos:

- data da rodada
- script utilizado
- pesos antigos e novos
- regras antigas e novas
- municipios usados como referencia
- principais erros corrigidos
- distribuicao de classes antes e depois
- observacoes metodologicas sobre efeitos colaterais

Uma forma simples de manter isso e:

- salvar novas amostras em `referencias/`
- registrar mudancas metodologicas neste `README.md`
- manter os scripts versionados na pasta `scripts/`

### Sinais Para Ajuste

Se muitos municipios remotos ou pouco estruturados estiverem saindo como `alto`, vale revisar principalmente:

- peso de `oferta_servicos`
- peso de `regic_var61` e `regic_var66`
- penalizacao por baixa infraestrutura e baixa conectividade
- regra de transicao entre `baixo` e `alto`

### Campos principais da saida municipal

- identificacao: `cod_mun`, `municipio`, `uf`, `rgint`, `nome_rgint`
- resultado final: `classificacao_fuzzy`, `confianca_classificacao`
- eixos: `centralidade_economica`, `infraestrutura_urbana`, `conectividade_digital`, `oferta_servicos`, `vulnerabilidade`
- indicadores derivados: `pib_pc`, `empresas_1k`, `estab_saude_10k`, `homicidios_100k`, `demissoes_1k`, `area_urb_densa_100k`
- pertinencias finais: colunas `pert_*`

### Observacoes metodologicas

- a classificacao e inicial e deve ser calibrada iterativamente
- `registros_seca_2003_2015` funciona como reforco, nao como eixo principal
- `plano_diretor` entra como sinal complementar de estrutura institucional
- o modelo e totalmente intra-regional: cada `rgint` usa apenas seus proprios municipios para ranqueamento
- `classificacao_dominante_rgint` e um resumo descritivo, nao uma classificacao fuzzy da regiao em si
- a confianca da classificacao e uma intensidade fuzzy, nao uma probabilidade
- municipios fora da curva dentro de regioes pequenas podem receber scores altos relativos mesmo sem serem grandes polos nacionais
- por isso, a etapa de calibracao manual e parte obrigatoria do processo, e nao apenas um passo opcional
