# Jornada De Merges Das Variaveis Municipais

Este README centraliza a documentacao da pasta `documentacao` e descreve a ordem de execucao do pipeline.

## Etapa Anterior Ao Projeto Atual

Antes das etapas documentadas aqui, houve um merge inicial feito em outra pasta. Essa etapa anterior agora esta representada por tres arquivos adicionados em `../bronze/`:

- `../bronze/tabela4712.csv`: tabela do IBGE com domicilios particulares permanentes ocupados por municipio, com valores de 2022.
- `../bronze/tabela5938 .csv`: tabela do IBGE com PIB municipal a precos correntes em 2021, incluindo PIB total, impostos liquidos de subsidios e valor adicionado bruto por atividade economica.
- `../bronze/tabela9514 .csv`: tabela do IBGE com populacao residente por municipio em 2022.

Pelo conteudo lido nos arquivos:

- `tabela4712.csv` traz as colunas `Cód.`, `Município` e o valor de `2022` para domicilios ocupados;
- `tabela5938 .csv` traz `Cód.`, `Município` e varias colunas economicas de 2021, como PIB corrente, impostos e componentes do valor adicionado;
- `tabela9514 .csv` traz `Cód.`, `Município`, `Forma de declaração da idade` e o total de populacao residente em 2022.

Esses arquivos fazem parte de um merge preliminar, anterior ao pipeline atual. Por isso, devem ser considerados como antecedentes da base e nao como uma etapa criada pelos scripts desta pasta.

## Estrutura Do Projeto

- `../documentacao/`: scripts Python e documentacao operacional do projeto.
- `../bronze/`: conteudo bruto do processamento.
- `../prata/`: tabelas processadas.
- `../prata/pre_merge/`: CSVs processados antes dos merges principais.
- `../prata/processamento/`: tabelas consolidadas versionadas no nome do arquivo.
- `../prata/pre_merge/indicadores_seguranca_publica_municipal/`: CSVs exportados por UF a partir da planilha de seguranca publica.
- `../prata/pre_merge/regic_2018/`: CSVs da REGIC 2018 preparados para uso posterior em merges.
- `../prata/pre_merge/sinisa_esgoto_base_municipal/`: CSVs processados do SINISA, fora do versionamento.
- `../ouro/`: camada reservada para uso futuro.

## Dependencias

Todos os scripts usam Python 3 e `pandas`.

```bash
pip install pandas
```

## Visao Geral

Hoje a sequencia recomendada e:

1. usar `scripts/merge.py` quando for necessario um merge simples e reutilizavel;
2. executar `scripts/merge_utilizado_tabela9582.py` para gerar a `v1`;
3. executar `scripts/merge_utilizado_7138_receita.py` para gerar a `v2`;
4. executar `scripts/merge_utilizado_fundeb_transferencias.py` para gerar a `v3`;
5. executar `scripts/processa_sinisa_esgoto_base_municipal.py` para exportar os CSVs do SINISA fora do versionamento;
6. executar `scripts/processa_indicadores_seguranca_publica_municipal.py` para exportar a planilha de seguranca publica em um CSV por UF;
7. executar `scripts/agrega_homicidios_municipais_2022.py` para consolidar os homicidios municipais de 2022;
8. executar `scripts/merge_sinisa_atendimento.py` para gerar a `v4`;
9. executar `scripts/merge_homicidios_v4.py` para incorporar os homicidios de 2022 e gerar a `v5`;
10. executar `scripts/remove_colunas_baixa_cobertura_v5.py` para retirar colunas com cobertura insuficiente e gerar a `v6`;
11. executar `scripts/merge_tabela9584_v6.py` para incorporar a tabela 9584 percentual e gerar a `v7`;
12. executar `scripts/merge_tabela9584_absoluta_v7.py` para incorporar a tabela 9584 em valores absolutos e gerar a `v8`;
13. executar `scripts/merge_tabela9584_iluminacao_v8.py` para incorporar a tabela 9584 de iluminacao e gerar a `v9`;
14. executar `scripts/remove_coluna_nao_existe_v9.py` para retirar a coluna `Não existe` e gerar a `v10`;
15. executar `scripts/merge_regic_v10.py` para incorporar as variaveis `VAR56` a `VAR66` da REGIC 2018 e gerar a `v11`;
16. executar `scripts/normaliza_regic_v11.py` para normalizar `VAR56` a `VAR66` entre `0` e `1` e gerar a `v12`.

## Etapa 0: Script Generico

O arquivo `merge.py` e uma versao reutilizavel para merges simples entre dois CSVs.

Ele permite configurar:

- arquivo de origem;
- arquivo base;
- colunas-chave;
- colunas a incorporar;
- tipo de join;
- arquivo de saida.

Exemplo:

```bash
python3 merge.py \
  --file1 "arquivo_origem.csv" \
  --file2 "arquivo_base.csv" \
  --output "saida.csv" \
  --key1 "coluna_chave_origem" \
  --key2 "coluna_chave_base" \
  --how left
```

## Etapa 1: Total De Empresas

Script: `scripts/merge_utilizado_tabela9582.py`

Objetivo:

- ler `../bronze/tabela9582.csv`;
- fazer merge com `../bronze/merge_completo.csv`;
- gerar `../prata/processamento/merge_v1.csv`.

Diferencial desta etapa:

- usa `skiprows=4` porque `tabela9582.csv` tem linhas de metadados antes do cabecalho util.

Configuracao principal:

- chave em ambos os arquivos: `Cód.`
- coluna incorporada: `Total`
- tipo de join: `left`

Execucao:

```bash
python3 scripts/merge_utilizado_tabela9582.py
```

## Etapa 2: Escolarizacao E Receita

Script: `scripts/merge_utilizado_7138_receita.py`

Objetivo:

- ler `../prata/processamento/merge_v1.csv`;
- incorporar `../bronze/tabela7138.csv`;
- incorporar `../bronze/0fcf5cfb-9b3d-45b8-80c8-00d6eb180ff6.csv`;
- gerar `../prata/processamento/merge_v2.csv`.

Como o merge e feito:

- `tabela7138.csv` entra por codigo do municipio via `Cód.`;
- o arquivo `0fcf5...csv` entra por nome do municipio, porque nao possui codigo.

Regras especiais do merge por nome:

- normalizacao do nome do municipio;
- remocao de acentos;
- remocao do sufixo `(UF)` da base;
- restricao a UF `SC` para evitar colisoes entre municipios homonimos.

Colunas adicionadas:

- `taxa_escolarizacao_2024`
- `Valor Receita Prevista`
- `Valor Receita Realizada`

Execucao:

```bash
python3 scripts/merge_utilizado_7138_receita.py
```

## Etapa 3: Transferencias Do Fundeb

Script: `scripts/merge_utilizado_fundeb_transferencias.py`

Objetivo:

- ler `../prata/processamento/merge_v2.csv`;
- processar `../bronze/transferências_para_municípios.csv`;
- gerar `../prata/processamento/merge_v3.csv`.

O que este script resolve:

- o arquivo do Fundeb vem em formato longo;
- cada linha representa um tipo de transferencia;
- o valor vem como moeda em formato brasileiro;
- o arquivo usa `latin1` e separador `;`.

Processamento aplicado:

1. leitura do CSV no formato correto;
2. filtro do ano de interesse;
3. conversao de `Valor Consolidado` para numero;
4. pivot das categorias de `Transferência` em colunas;
5. merge final por codigo do municipio.

Exemplos de colunas geradas:

- `fundeb_coun_vaaf`
- `fundeb_coun_vaar`
- `fundeb_coun_vaat`
- `fundeb_fpe`
- `fundeb_fpm`
- `fundeb_fti`
- `fundeb_icms`
- `fundeb_ipi_exp`
- `fundeb_ipva`
- `fundeb_itcmd`
- `fundeb_itr`

Execucao:

```bash
python3 scripts/merge_utilizado_fundeb_transferencias.py
```

## Ordem Recomendada

Se a ideia for reconstruir a base processada do zero:

1. partir de `../bronze/merge_completo.csv`;
2. rodar `scripts/merge_utilizado_tabela9582.py`;
3. rodar `scripts/merge_utilizado_7138_receita.py`;
4. rodar `scripts/merge_utilizado_fundeb_transferencias.py`.

## Pre-merge Auxiliar: REGIC 2018

Script: `scripts/processa_regic_2018_pre_merge.py`

Arquivos de origem em `../bronze/`:

- `REGIC2018_Arranjos_Populacionais_v2 (1).xlsx`
- `REGIC2018_Cidades_v2 (1).xlsx`

Saidas preparadas em `../prata/pre_merge/regic_2018/`:

- `REGIC2018_Arranjos_Populacionais_v2 (1).csv`
- `REGIC2018_Cidades_v2 (1).csv`

Processamento realizado:

1. conversao manual dos dois arquivos `.xlsx` para `.csv`;
2. leitura do CSV de arranjos populacionais e do CSV de cidades;
3. identificacao dos municipios que aparecem em `Arranjos Populacionais`, mas nao possuem linha propria em `Cidades`;
4. localizacao, no CSV de cidades, da linha do arranjo correspondente por `Código do AP`;
5. replicacao de todas as colunas do arranjo para cada municipio integrante ausente;
6. substituicao dos campos de identificacao do arranjo pelos campos do municipio integrante:
   `COD_CIDADE`, `NOME_CIDADE`, `COD_UF` e `UF`.

Regra aplicada:

- quando um municipio integrante nao existe no arquivo `Cidades`, ele herda os valores das variaveis do arranjo populacional ao qual pertence;
- o relacionamento e feito por `Código do AP` no arquivo de arranjos contra `COD_CIDADE` no arquivo de cidades;
- a linha adicionada mantém os indicadores do arranjo, mas passa a representar o municipio integrante.

Resultado registrado:

- 671 municipios foram adicionados ao CSV de cidades;
- o arquivo `REGIC2018_Cidades_v2 (1).csv` passou de 4899 para 5570 linhas;
- ao final do processamento, todos os municipios presentes no arquivo de arranjos passaram a ter correspondencia no arquivo de cidades.

Execucao:

```bash
python3 scripts/processa_regic_2018_pre_merge.py
```

## Pre-merge Auxiliar: Demissoes Municipais Do Ipea

Script: `scripts/agrega_demissoes_ipea_anuais.py`

Objetivo:

- localizar o arquivo mais recente `ipeadata*.csv` na pasta `../bronze/`;
- ler a serie municipal mensal de demissoes;
- somar os 12 meses para cada municipio;
- gerar uma base anual pronta para merge em `../prata/pre_merge/`.

Como o processamento e feito:

- a primeira linha do arquivo e ignorada por conter apenas o titulo da serie;
- colunas vazias do tipo `Unnamed` sao descartadas;
- as colunas mensais sao identificadas automaticamente pelo padrao `AAAA.MM`;
- o script valida que existe apenas um ano no arquivo e que esse ano possui 12 meses;
- os valores mensais sao convertidos para numericos e somados por municipio;
- se existir uma base consolidada `merge_v*.csv`, a saida e filtrada para manter apenas os codigos municipais presentes na versao mais recente do pipeline;
- a saida e padronizada com as colunas `cod_mun`, `sigla_uf`, `municipio` e `demissoes_AAAA`.

Arquivo gerado:

- `../prata/pre_merge/ipea_demissoes_municipais_AAAA.csv`

Execucao:

```bash
python3 scripts/agrega_demissoes_ipea_anuais.py
```

## Etapa 4: SINISA Esgoto Base Municipal

Script: `scripts/processa_sinisa_esgoto_base_municipal.py`

Objetivo:

- ler os arquivos `.xlsx` da pasta `../bronze/sinisa_esgoto_planilhas_2023_v2/esgoto_base_municipal`;
- exportar cada aba para um CSV separado;
- salvar os resultados em `../prata/pre_merge/sinisa_esgoto_base_municipal/`.

Regra de nomeacao:

- os nomes dos arquivos de saida sao normalizados;
- cada CSV termina com o sufixo `_processado.csv`.

Execucao:

```bash
python3 scripts/processa_sinisa_esgoto_base_municipal.py
```

## Etapa 5: Merge SINISA Atendimento

Script: `scripts/merge_sinisa_atendimento.py`

Objetivo:

- ler `../prata/pre_merge/sinisa_esgoto_base_municipal/sinisa_esgoto_indicadores_base_municipal_2023_v2__atendimento_processado.csv`;
- tratar a linha correta de cabecalho da aba `Atendimento`;
- fazer merge com `../prata/processamento/merge_v3.csv` via codigo do municipio;
- gerar `../prata/processamento/merge_v4.csv`.

Colunas incorporadas:

- `Atendimento da população total com rede coletora de esgoto`
- `Atendimento da população urbana com rede coletora de esgoto`
- `Atendimento dos domicílios totais com rede coletora de esgoto`
- `Atendimento dos domicílios urbanos com rede coletora de esgoto`
- `Atendimento dos domicílios totais com coleta e tratamento de esgoto`
- `Atendimento dos domicílios urbanos com coleta e tratamento de esgoto`

Execucao:

```bash
python3 scripts/merge_sinisa_atendimento.py
```

## Etapa 6: Exportacao Da Planilha De Seguranca Publica

Script: `scripts/processa_indicadores_seguranca_publica_municipal.py`

Objetivo:

- ler `../bronze/indicadoressegurancapublicamunic.xlsx`;
- exportar cada aba da planilha para um CSV separado;
- salvar os resultados em `../prata/pre_merge/indicadores_seguranca_publica_municipal/`.

Organizacao da planilha:

- a planilha possui uma aba por UF;
- todas as abas seguem a mesma estrutura de colunas, incluindo `Cód_IBGE`, `Município`, `Sigla UF`, `Região`, `Mês/Ano` e `Vítimas`.

Regra de nomeacao:

- os nomes dos arquivos de saida sao normalizados;
- cada CSV recebe o nome da planilha seguido da UF da aba.

Exemplos de saida:

- `../prata/pre_merge/indicadores_seguranca_publica_municipal/indicadoressegurancapublicamunic__ac.csv`
- `../prata/pre_merge/indicadores_seguranca_publica_municipal/indicadoressegurancapublicamunic__sc.csv`

Execucao:

```bash
python3 scripts/processa_indicadores_seguranca_publica_municipal.py
```

## Etapa 7: Consolidacao De Homicidios Municipais Em 2022

Script: `scripts/agrega_homicidios_municipais_2022.py`

Objetivo:

- ler todos os CSVs em `../prata/pre_merge/indicadores_seguranca_publica_municipal/`;
- manter apenas os registros do ano de 2022;
- somar o numero de vitimas por municipio ao longo dos meses;
- gerar `../prata/pre_merge/homicidios_municipais_2022.csv`.

Como o processamento e feito:

- a coluna `Mês/Ano` e convertida para data;
- a coluna `Vítimas` e convertida para numero;
- o filtro considera apenas linhas com ano igual a `2022`;
- a agregacao final e feita por `Cód_IBGE`, `Município`, `Sigla UF` e `Região`.

Coluna gerada:

- `vitimas_homicidio_2022`

Execucao:

```bash
python3 scripts/agrega_homicidios_municipais_2022.py
```

## Etapa 8: Merge Da V4 Com Homicidios De 2022

Script: `scripts/merge_homicidios_v4.py`

Objetivo:

- ler `../prata/processamento/merge_v4.csv`;
- ler `../prata/pre_merge/homicidios_municipais_2022.csv`;
- fazer merge via codigo do municipio;
- gerar `../prata/processamento/merge_v5.csv`.

Como o merge e feito:

- a base `v4` usa a coluna `Cód.` como chave;
- a base de homicidios usa a coluna `Cód_IBGE` como chave;
- os codigos sao normalizados antes do merge;
- a base de homicidios e consolidada novamente por codigo antes da juncao para evitar multiplicacao de linhas causada por variacoes de nome;
- municipios sem correspondencia recebem `0` em `vitimas_homicidio_2022`.

Coluna incorporada:

- `vitimas_homicidio_2022`

Execucao:

```bash
python3 scripts/merge_homicidios_v4.py
```

## Etapa 9: Geracao Da V6 Sem Colunas De Baixa Cobertura

Script: `scripts/remove_colunas_baixa_cobertura_v5.py`

Objetivo:

- ler `../prata/processamento/merge_v5.csv`;
- remover as colunas `taxa_escolarizacao_2024`, `Valor Receita Prevista` e `Valor Receita Realizada`;
- gerar `../prata/processamento/merge_v6.csv`.

Justificativa:

- essas colunas foram retiradas porque nao ha dados suficientes para todo o Brasil;
- na base `v5`, `taxa_escolarizacao_2024` possui apenas `27` valores preenchidos em `5.570` municipios;
- `Valor Receita Prevista` possui apenas `295` valores preenchidos em `5.570` municipios;
- `Valor Receita Realizada` possui apenas `295` valores preenchidos em `5.570` municipios.

Execucao:

```bash
python3 scripts/remove_colunas_baixa_cobertura_v5.py
```

## Etapa 10: Merge Da Tabela 9584 Com A V6

Script: `scripts/merge_tabela9584_v6.py`

Objetivo:

- ler `../prata/processamento/merge_v6.csv`;
- ler `../bronze/tabela9584_%.csv`;
- limpar o cabecalho e descartar linhas de legenda da tabela;
- fazer merge via codigo do municipio;
- gerar `../prata/processamento/merge_v7.csv`.

Como o merge e feito:

- a tabela 9584 e lida com `skiprows=5`, usando a ultima linha do cabecalho como nomes reais das colunas;
- linhas sem codigo municipal e a linha de legenda com codigo `0` sao descartadas;
- a base `v6` usa a coluna `Cód.` como chave;
- os codigos sao normalizados antes do merge.

Colunas incorporadas:

- `Via pavimentada - Existe (%)`
- `Existência de iluminação pública - Existe (%)`
- `Existência de calçada / passeio - Existe (%)`

Execucao:

```bash
python3 scripts/merge_tabela9584_v6.py
```

## Etapa 11: Merge Da Tabela 9584 Em Valores Absolutos Com A V7

Script: `scripts/merge_tabela9584_absoluta_v7.py`

Objetivo:

- ler `../prata/processamento/merge_v7.csv`;
- ler `../bronze/tabela9584.csv`;
- limpar o cabecalho e descartar linhas de legenda da tabela;
- fazer merge via codigo do municipio;
- gerar `../prata/processamento/merge_v8.csv`.

Como o merge e feito:

- a tabela 9584 absoluta e lida com `skiprows=5`, usando a ultima linha do cabecalho como nomes reais das colunas;
- linhas sem codigo municipal e a linha de legenda com codigo `0` sao descartadas;
- a base `v7` usa a coluna `Cód.` como chave;
- os codigos sao normalizados antes do merge;
- as colunas novas recebem o sufixo `(N)` para diferenciar os valores absolutos das colunas percentuais ja existentes na `v7`.

Colunas incorporadas:

- `Via pavimentada - Existe (N)`
- `Existência de iluminação pública - Existe (N)`
- `Existência de calçada / passeio - Existe (N)`

Execucao:

```bash
python3 scripts/merge_tabela9584_absoluta_v7.py
```

## Etapa 12: Merge Da Tabela 9584 De Iluminacao Com A V8

Script: `scripts/merge_tabela9584_iluminacao_v8.py`

Objetivo:

- ler `../prata/processamento/merge_v8.csv`;
- ler `../bronze/tabela9584 (1).csv`;
- limpar o cabecalho e descartar linhas de legenda da tabela;
- fazer merge via codigo do municipio;
- descartar a coluna duplicada ja existente na `v8`;
- gerar `../prata/processamento/merge_v9.csv`.

Como o merge e feito:

- a tabela e lida com `skiprows=5`, usando a ultima linha do cabecalho como nomes reais das colunas;
- linhas sem codigo municipal e a linha de legenda com codigo `0` sao descartadas;
- a base `v8` usa a coluna `Cód.` como chave;
- os codigos sao normalizados antes do merge;
- a coluna `Existência de iluminação pública - Existe (%)` e descartada no merge porque ja existe na `v8` com os mesmos valores;
- apenas a coluna nova `Existência de iluminação pública - Não existe (%)` e incorporada;
- na `v9`, as colunas de entorno urbano ficam ordenadas por tema, agrupando percentuais e valores absolutos do mesmo indicador.

Coluna incorporada:

- `Existência de iluminação pública - Não existe (%)`

Execucao:

```bash
python3 scripts/merge_tabela9584_iluminacao_v8.py
```

## Etapa 13: Geracao Da V10 Sem A Coluna Nao Existe

Script: `scripts/remove_coluna_nao_existe_v9.py`

Objetivo:

- ler `../prata/processamento/merge_v9.csv`;
- remover a coluna `Existência de iluminação pública - Não existe (%)`;
- gerar `../prata/processamento/merge_v10.csv`.

Justificativa:

- a base `v10` mantem apenas as colunas de existencia de iluminacao publica usadas na estrutura final desejada;
- a coluna `Não existe` foi retirada por ser informacao redundante em relacao a coluna `Existência de iluminação pública - Existe (%)`, ja que ambas representam o mesmo tema em sentidos complementares.

Execucao:

```bash
python3 scripts/remove_coluna_nao_existe_v9.py
```

## Etapa 14: Merge Da V10 Com A REGIC 2018

Script: `scripts/merge_regic_v10.py`

Objetivo:

- ler `../prata/processamento/merge_v10.csv`;
- ler `../prata/pre_merge/regic_2018/REGIC2018_Cidades_v2 (1).csv`;
- fazer merge via codigo do municipio;
- incorporar apenas as variaveis `VAR56` a `VAR66`;
- gerar `../prata/processamento/merge_v11.csv`.

Como o merge e feito:

- a base `v10` usa a coluna `Cód.` como chave;
- a base REGIC usa a coluna `COD_CIDADE` como chave;
- os codigos sao normalizados antes do merge;
- apenas as colunas `VAR56`, `VAR57`, `VAR58`, `VAR59`, `VAR60`, `VAR61`, `VAR62`, `VAR63`, `VAR64`, `VAR65` e `VAR66` sao selecionadas na REGIC antes da juncao;
- a saida preserva a estrutura da `v10` e adiciona somente essas 11 variaveis ao final.

Colunas incorporadas:

- `VAR56`
- `VAR57`
- `VAR58`
- `VAR59`
- `VAR60`
- `VAR61`
- `VAR62`
- `VAR63`
- `VAR64`
- `VAR65`
- `VAR66`

Resultado registrado:

- a `merge_v11.csv` foi gerada com 5570 linhas;
- houve correspondencia para todos os municipios da `v10`.

Execucao:

```bash
python3 scripts/merge_regic_v10.py
```

## Etapa 15: Normalizacao Das Variaveis REGIC Na V11

Script: `scripts/normaliza_regic_v11.py`

Objetivo:

- ler `../prata/processamento/merge_v11.csv`;
- normalizar as colunas `VAR56` a `VAR66` para a faixa `0` a `1`;
- gerar `../prata/processamento/merge_v12.csv`.

Como a normalizacao e feita:

- cada coluna e tratada individualmente;
- os valores sao convertidos para numericos antes do processamento;
- a transformacao usa normalizacao min-max, com a formula `(valor - minimo) / (maximo - minimo)`;
- se uma coluna tiver amplitude `0`, todos os valores dessa coluna passam a ser `0`.

Colunas normalizadas:

- `VAR56`
- `VAR57`
- `VAR58`
- `VAR59`
- `VAR60`
- `VAR61`
- `VAR62`
- `VAR63`
- `VAR64`
- `VAR65`
- `VAR66`

Resultado registrado:

- a `merge_v12.csv` foi gerada com 5570 linhas;
- todas as colunas `VAR56` a `VAR66` passaram a ter minimo `0` e maximo `1`.

Execucao:

```bash
python3 scripts/normaliza_regic_v11.py
```

## Etapa 16: Merge Da Tabela 5882 (Plano Diretor) Na V12

Script: `scripts/merge_tabela5882_plano_diretor_v13.py`

Objetivo:

- ler `../prata/processamento/merge_v12.csv`;
- ler `../bronze/tabela5882.csv`;
- fazer merge via codigo do municipio;
- incorporar o indicador de existencia de plano diretor;
- gerar `../prata/processamento/merge_v13.csv`.

Como o merge e feito:

- a base `v12` usa a coluna `Cód.` como chave;
- a tabela 5882 tambem usa a coluna `Cód.` como chave;
- os codigos sao normalizados antes do merge;
- a tabela 5882 e lida com `skiprows=4` para ignorar os metadados iniciais do CSV;
- apenas a coluna `Total` e aproveitada da tabela 5882, pois `Existência de Plano Diretor` vem constante como `Com Plano Diretor` nas linhas validas;
- os valores da coluna `Total` sao convertidos para indicador binario, com `1` significando existencia de plano diretor e `-` sendo convertido para `0`;
- a coluna incorporada e renomeada para `Existência de Plano Diretor - Existe`.

Coluna incorporada:

- `Existência de Plano Diretor - Existe`

Resultado registrado:

- a `merge_v13.csv` foi gerada com 5570 linhas;
- houve correspondencia para todos os municipios da `v12`.

Execucao:

```bash
python3 scripts/merge_tabela5882_plano_diretor_v13.py
```

## Etapa 17: Normalizacao Dos Nomes Das Colunas Na V13

Script: `scripts/normaliza_nomes_colunas_v13.py`

Objetivo:

- ler `../prata/processamento/merge_v13.csv`;
- renomear colunas para nomes mais curtos e padronizados;
- manter o sentido das variaveis;
- gerar `../prata/processamento/merge_v14.csv`.

Como a normalizacao e feita:

- os nomes passam para um padrao em `snake_case`;
- acentos, espacos e simbolos longos sao removidos dos nomes finais;
- abreviacoes curtas sao usadas quando o significado segue claro;
- colunas ja curtas e consistentes, como parte das variaveis de PIB e FUNDeb, sao preservadas ou ajustadas minimamente;
- as variaveis `VAR56` a `VAR66` passam a usar o prefixo `regic_` para manter contexto.

Exemplos de renomeacao:

- `Cód.` -> `cod_mun`
- `Município` -> `municipio`
- `Atendimento da população total com rede coletora de esgoto` -> `esgoto_pop_total_rede`
- `Existência de iluminação pública - Existe (%)` -> `ilum_pub_pct`
- `Existência de Plano Diretor - Existe` -> `plano_diretor`
- `VAR56` -> `regic_var56`

Resultado registrado:

- a `merge_v14.csv` foi gerada com 5570 linhas;
- os nomes das colunas ficaram mais curtos sem perder o contexto principal.

Execucao:

```bash
python3 scripts/normaliza_nomes_colunas_v13.py
```

## Etapa 18: Geracao Do Dicionario De Dados Da V14

Script: `scripts/gera_dicionario_dados_v14.py`

Objetivo:

- ler automaticamente a versao mais recente em `../prata/processamento/merge_v*.csv`;
- montar um dicionario de dados em CSV para todas as colunas da base;
- registrar, para cada variavel, o nome usado na `v14`, o nome original, a descricao original e o ano de referencia do dado;
- gerar `dicionario_dados.csv`.

Como o dicionario e montado:

- a ordem das linhas segue a ordem das colunas na `merge_v14.csv`;
- a entrada e descoberta automaticamente a partir do maior arquivo `merge_v*.csv` da pasta `../prata/processamento/`;
- o script valida se todas as colunas da `v14` estao cobertas no dicionario;
- as descricoes da REGIC 2018 sao lidas a partir da aba `Descrição das variáveis` da planilha original;
- quando a coluna da `v14` resulta de transformacao do dado original, isso e registrado em `observacoes`;
- o arquivo final inclui a coluna `tipo`, para classificar a variavel como identificador, categorica, binaria, percentual, contagem, numerica absoluta ou indice normalizado;
- o campo `ano_referencia` e gravado como texto, evitando a leitura com casas decimais como `2021.0`;
- os nomes em `fonte_original` sao padronizados para facilitar uso em relatorios e documentacao.

Arquivo gerado:

- `dicionario_dados.csv`

Execucao:

```bash
python3 scripts/gera_dicionario_dados_v14.py
```

## Etapa 19: Merge Da Tabela 8418 Com A V14

Script: `scripts/merge_tabela8418_v14.py`

Objetivo:

- ler `../prata/processamento/merge_v14.csv`;
- ler `../bronze/tabela8418.csv`;
- fazer merge via codigo do municipio;
- incorporar as medidas de areas urbanizadas da tabela 8418;
- gerar `../prata/processamento/merge_v15.csv`.

Como o merge e feito:

- a tabela 8418 e lida com `skiprows=3` para ignorar as linhas de metadados iniciais;
- a base `v14` usa a coluna `cod_mun` como chave;
- a tabela 8418 usa a coluna `Cód.` como chave;
- os codigos sao normalizados antes do merge;
- linhas de legenda e a linha com codigo `0` sao descartadas;
- as tres colunas incorporadas sao renomeadas para nomes curtos na propria etapa de merge.

Colunas incorporadas:

- `area_urb_densa_km2`
- `loteamento_vazio_km2`
- `vazios_intraurbanos_km2`

Resultado registrado:

- a `merge_v15.csv` foi gerada com 5570 linhas;
- a tabela 8418 usa ano de referencia `2019`.

Execucao:

```bash
python3 scripts/merge_tabela8418_v14.py
```

## Etapa 20: Merge Das Demissoes Anuais Do Ipea Com A V15

Script: `scripts/merge_ipea_demissoes_v15.py`

Objetivo:

- ler `../prata/processamento/merge_v15.csv`;
- ler `../prata/pre_merge/ipea_demissoes_municipais_2025.csv`;
- fazer merge via codigo do municipio;
- incorporar a coluna anual de demissoes do Ipea;
- gerar `../prata/processamento/merge_v16.csv`.

Como o merge e feito:

- a base `v15` usa a coluna `cod_mun` como chave;
- a base anual do Ipea tambem usa a coluna `cod_mun` como chave;
- os codigos sao normalizados antes do merge;
- apenas a coluna `demissoes_2025` e incorporada ao resultado;
- a saida preserva toda a estrutura da `v15` e adiciona a nova variavel ao final.

Coluna incorporada:

- `demissoes_2025`

Resultado registrado:

- a `merge_v16.csv` foi gerada com 5570 linhas;
- houve correspondencia para todos os municipios da `v15`.

Execucao:

```bash
python3 scripts/merge_ipea_demissoes_v15.py
```

## Etapa 21: Merge Do CNES De Ambulatorios Com A V16

Script: `scripts/merge_cnes_ambulatorios_v16.py`

Objetivo:

- ler `../prata/processamento/merge_v16.csv`;
- ler `../bronze/cnes_cnv_atambbr131932200_135_70_71.csv`;
- fazer merge via codigo do municipio;
- incorporar o numero de ambulatorios SUS por municipio;
- gerar `../prata/processamento/merge_v17.csv`.

Como o merge e feito:

- o arquivo do CNES e lido com `encoding=latin1`, separador `;` e `skiprows=3`;
- a base `v16` usa a coluna `cod_mun`, mas o CNES traz o codigo municipal com 6 digitos embutido no inicio do campo `Município`;
- por isso, o merge usa a chave reduzida `cod_mun[:6]` na base `v16` contra os 6 digitos extraidos do campo `Município` no CNES;
- a coluna `SUS` e convertida para numerica e renomeada para `ambulatorios_sus_2026_02`;
- o periodo `Fev/2026` e identificado no cabecalho do arquivo para validar a referencia temporal da variavel.

Coluna incorporada:

- `ambulatorios_sus_2026_02`

Resultado registrado:

- a `merge_v17.csv` foi gerada com 5570 linhas;
- a referencia temporal da coluna incorporada e fevereiro de 2026.

Execucao:

```bash
python3 scripts/merge_cnes_ambulatorios_v16.py
```

## Etapa 22: Merge Do CNES De Estabelecimentos Com A V17

Script: `scripts/merge_cnes_estabelecimentos_v17.py`

Objetivo:

- ler `../prata/processamento/merge_v17.csv`;
- ler `../bronze/cnes_cnv_estabbr134413200_135_70_71.csv`;
- fazer merge via codigo do municipio;
- incorporar as colunas de quantidade de estabelecimentos por tipo do CNES;
- gerar `../prata/processamento/merge_v18.csv`.

Como o merge e feito:

- o arquivo do CNES e lido com `encoding=latin1`, separador `;` e `skiprows=3`;
- a coluna `Município` do CNES traz o codigo municipal com 6 digitos no inicio do texto;
- o merge usa `cod_mun[:6]` na base `v17` contra os 6 digitos extraidos do campo `Município` no CNES;
- todos os valores `-` nas colunas do CNES sao substituidos por `0` antes da conversao para numerico;
- para o municipio de Brasilia, todas as colunas incorporadas do CNES sao zeradas;
- os nomes das colunas incorporadas passam para o padrao `cnes_estab_*_2025_12`, preservando o periodo de referencia do arquivo.

Resultado registrado:

- a `merge_v18.csv` foi gerada com 5570 linhas;
- o periodo de referencia detectado no arquivo foi `Dez/2025`;
- houve correspondencia para todos os municipios da `v17`.

Execucao:

```bash
python3 scripts/merge_cnes_estabelecimentos_v17.py
```

## Etapa 23: Normalizacao Dos Nomes Das Colunas De Estabelecimentos Do CNES Na V18

Script: `scripts/normaliza_nomes_cnes_estabelecimentos_v18.py`

Objetivo:

- ler `../prata/processamento/merge_v18.csv`;
- encurtar os nomes das colunas do bloco de estabelecimentos do CNES;
- manter o sentido das variaveis;
- gerar `../prata/processamento/merge_v19.csv`.

Como a normalizacao e feita:

- apenas as colunas do bloco `cnes_estab_*_2025_12` sao renomeadas;
- os nomes passam para uma forma mais curta, mantendo o prefixo `cnes_` e o periodo `2025_12`;
- abreviacoes como `hosp`, `unid`, `reg`, `caps`, `lacen` e `ubs` sao usadas para reduzir o tamanho dos nomes sem perder legibilidade.

Resultado registrado:

- a `merge_v19.csv` foi gerada com 5570 linhas;
- 39 colunas do CNES tiveram seus nomes encurtados.

Execucao:

```bash
python3 scripts/normaliza_nomes_cnes_estabelecimentos_v18.py
```

## Etapa 24: Remocao Do Prefixo CNES Nos Nomes De Estabelecimentos Na V19

Script: `scripts/normaliza_nomes_cnes_estabelecimentos_v19.py`

Objetivo:

- ler `../prata/processamento/merge_v19.csv`;
- remover o prefixo `cnes_` das colunas de estabelecimentos;
- manter apenas o nome do tipo de estabelecimento e o periodo no nome final;
- gerar `../prata/processamento/merge_v20.csv`.

Como a normalizacao e feita:

- apenas as colunas do bloco de estabelecimentos do CNES sao renomeadas;
- o prefixo `cnes_` e removido;
- os nomes finais ficam no formato `tipo_estabelecimento_2025_12`;
- abreviacoes curtas continuam sendo usadas quando ajudam a manter os nomes compactos e legiveis.

Resultado registrado:

- a `merge_v20.csv` foi gerada com 5570 linhas;
- 39 colunas tiveram o prefixo `cnes_` removido.

Execucao:

```bash
python3 scripts/normaliza_nomes_cnes_estabelecimentos_v19.py
```

## Etapa 25: Remocao Do Periodo Nos Nomes De Estabelecimentos Na V20

Script: `scripts/remove_periodo_nomes_cnes_v20.py`

Objetivo:

- ler `../prata/processamento/merge_v20.csv`;
- remover o sufixo de periodo das colunas de estabelecimentos;
- manter apenas o nome da informacao no nome final da coluna;
- gerar `../prata/processamento/merge_v21.csv`.

Como a normalizacao e feita:

- apenas as colunas do bloco de estabelecimentos do CNES sao renomeadas;
- o sufixo `_2025_12` e removido;
- os nomes finais ficam apenas com o significado da informacao, como `hospital_esp`, `unidade_mista` e `estab_total`.

Resultado registrado:

- a `merge_v21.csv` foi gerada com 5570 linhas;
- 39 colunas tiveram o sufixo de periodo removido.

Execucao:

```bash
python3 scripts/remove_periodo_nomes_cnes_v20.py
```

## Etapa 26: Merge Das Bases Da ANA De Agua E Seca Com A V21

Script: `scripts/merge_ana_agua_seca_v21.py`

Objetivo:

- ler `../prata/processamento/merge_v21.csv`;
- ler `../bronze/Demanda_Total.csv`;
- ler `../bronze/N%C3%BAmero_de_Registros_de_Secas_por_Munic%C3%ADpio_entre_2003_e_2015.csv`;
- fazer merge via codigo do municipio;
- incorporar as variaveis da ANA de demanda de agua e eventos de seca;
- gerar `../prata/processamento/merge_v22.csv`.

Como o merge e feito:

- a `v21` usa `cod_mun` como chave;
- a base `Demanda_Total.csv` entra por `CDMUN`;
- a base de secas entra por `CD_GEOCMU`;
- os codigos municipais sao normalizados antes do merge;
- o arquivo de demanda de agua e validado para garantir um unico ano de referencia;
- apenas as colunas de demanda de agua e de contagem de secas sao incorporadas ao resultado.

Colunas incorporadas:

- `demanda_agua_hum_urb_m3s`
- `demanda_agua_hum_rur_m3s`
- `demanda_agua_ind_m3s`
- `demanda_agua_min_m3s`
- `demanda_agua_term_m3s`
- `demanda_agua_animal_m3s`
- `demanda_agua_irr_m3s`
- `demanda_agua_total_m3s`
- `registros_seca_2003_2015`

Resultado registrado:

- a `merge_v22.csv` foi gerada com 5570 linhas;
- houve correspondencia para todos os municipios na base de demanda de agua;
- houve correspondencia para 2736 municipios na base de registros de seca;
- o ano identificado no arquivo de demanda de agua foi `2020`.

Execucao:

```bash
python3 scripts/merge_ana_agua_seca_v21.py
```

## Regra Permanente Para Novas CSVs E Novas Versoes

Sempre que uma nova CSV for incorporada ao pipeline, a atualizacao nao termina no merge da nova base. A manutencao deve incluir tambem a documentacao da etapa e a atualizacao do dicionario de dados.

Fluxo obrigatorio para qualquer nova etapa:

1. criar ou atualizar o script que processa a nova CSV;
2. gerar a nova versao da base em `../prata/processamento/`;
3. documentar a etapa no `readme.md`;
4. atualizar o dicionario de dados para refletir as colunas da versao final mais recente;
5. regenerar o CSV do dicionario de dados.

Ao documentar uma nova etapa no `readme.md`, registrar sempre:

- nome do script;
- arquivo ou arquivos de entrada;
- chave de merge;
- colunas adicionadas, removidas ou transformadas;
- arquivo de saida gerado;
- ano de referencia do dado incorporado;
- regra de tratamento aplicada, quando houver recodificacao, agregacao, normalizacao ou renomeacao.

Ao atualizar o dicionario de dados, registrar para cada nova coluna:

- `variavel_v14` ou o nome vigente da versao mais recente da base;
- `tipo`;
- `variavel_original`;
- `descricao_original`;
- `ano_referencia`;
- `fonte_original`;
- `observacoes`.

Regras para manutencao do script `scripts/gera_dicionario_dados_v14.py`:

- se a nova etapa apenas renomear colunas existentes, atualizar os nomes em `variavel_v14`, preservando a descricao de origem;
- se a nova etapa adicionar colunas, incluir novas entradas em `METADADOS`;
- se a nova etapa criar um novo tipo de variavel, atualizar tambem o mapeamento `TIPO_POR_VARIAVEL`;
- se a nova etapa usar nova fonte, padronizar o nome em `FONTE_PADRAO`;
- nao e necessario atualizar manualmente `INPUT_FILE` e `OUTPUT_FILE`, porque a versao final mais recente e descoberta automaticamente pelo script.

Validacao obrigatoria apos cada nova incorporacao:

- rodar o script do dicionario de dados;
- verificar se todas as colunas da versao final estao cobertas;
- confirmar se o numero de linhas do dicionario e igual ao numero de colunas da base final;
- revisar se o `ano_referencia` e a `fonte_original` das novas variaveis ficaram preenchidos.

Comando padrao apos qualquer nova etapa:

```bash
python3 scripts/gera_dicionario_dados_v14.py
```

## Saidas Da Jornada

Ao final das etapas atuais, os principais arquivos processados sao:

- `../prata/processamento/merge_v1.csv`
- `../prata/processamento/merge_v2.csv`
- `../prata/processamento/merge_v3.csv`
- `../prata/processamento/merge_v4.csv`
- `../prata/processamento/merge_v5.csv`
- `../prata/processamento/merge_v6.csv`
- `../prata/processamento/merge_v7.csv`
- `../prata/processamento/merge_v8.csv`
- `../prata/processamento/merge_v9.csv`
- `../prata/processamento/merge_v10.csv`
- `../prata/processamento/merge_v11.csv`
- `../prata/processamento/merge_v12.csv`
- `../prata/processamento/merge_v13.csv`
- `../prata/processamento/merge_v14.csv`
- `../prata/processamento/merge_v15.csv`
- `../prata/processamento/merge_v16.csv`
- `../prata/processamento/merge_v17.csv`
- `../prata/processamento/merge_v18.csv`
- `../prata/processamento/merge_v19.csv`
- `../prata/processamento/merge_v20.csv`
- `../prata/processamento/merge_v21.csv`
- `../prata/processamento/merge_v22.csv`
- `dicionario_dados.csv`
- `../prata/pre_merge/homicidios_municipais_2022.csv`
- `../prata/pre_merge/ipea_demissoes_municipais_AAAA.csv`
- `../prata/pre_merge/indicadores_seguranca_publica_municipal/*.csv`
- `../prata/pre_merge/regic_2018/*.csv`
- `../prata/pre_merge/sinisa_esgoto_base_municipal/*.csv`

## Regra Para Proximos Scripts

Este `readme.md` e o documento mestre da pasta `documentacao`.

Quando um novo script de merge for criado, acrescente aqui:

1. nome do script;
2. objetivo da etapa;
3. arquivos de entrada;
4. chave de merge;
5. colunas adicionadas ou transformacoes realizadas;
6. arquivo de saida;
7. comando de execucao.

Quando o novo script alterar a versao final da base, acrescente tambem:

8. atualizacao do dicionario de dados;
9. ano de referencia das novas variaveis;
10. comando para regenerar o CSV do dicionario.

## Arquivos Relacionados

- `scripts/merge.py`
- `scripts/merge_utilizado_tabela9582.py`
- `scripts/merge_utilizado_7138_receita.py`
- `scripts/merge_utilizado_fundeb_transferencias.py`
- `scripts/processa_sinisa_esgoto_base_municipal.py`
- `scripts/processa_indicadores_seguranca_publica_municipal.py`
- `scripts/agrega_demissoes_ipea_anuais.py`
- `scripts/processa_regic_2018_pre_merge.py`
- `scripts/agrega_homicidios_municipais_2022.py`
- `scripts/merge_sinisa_atendimento.py`
- `scripts/merge_homicidios_v4.py`
- `scripts/remove_colunas_baixa_cobertura_v5.py`
- `scripts/merge_tabela9584_v6.py`
- `scripts/merge_tabela9584_absoluta_v7.py`
- `scripts/merge_tabela9584_iluminacao_v8.py`
- `scripts/remove_coluna_nao_existe_v9.py`
- `scripts/merge_regic_v10.py`
- `scripts/normaliza_regic_v11.py`
- `scripts/merge_tabela5882_plano_diretor_v13.py`
- `scripts/normaliza_nomes_colunas_v13.py`
- `scripts/gera_dicionario_dados_v14.py`
- `scripts/merge_tabela8418_v14.py`
- `scripts/merge_ipea_demissoes_v15.py`
- `scripts/merge_cnes_ambulatorios_v16.py`
- `scripts/merge_cnes_estabelecimentos_v17.py`
- `scripts/normaliza_nomes_cnes_estabelecimentos_v18.py`
- `scripts/normaliza_nomes_cnes_estabelecimentos_v19.py`
- `scripts/remove_periodo_nomes_cnes_v20.py`
- `scripts/merge_ana_agua_seca_v21.py`
