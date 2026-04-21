# Classificação de Municípios - Sistema Fuzzy V2

## Visão Geral

Este documento descreve a metodologia do sistema de classificação fuzzy (versão 2) para os 5.570 municípios brasileiros. O sistema classifica cada município dentro do contexto de sua própria região geográfica intermediária (133 rgints), permitindo análises territorialmente sensíveis.

---

## 1. Fluxo de Processamento

```
merge_v26.csv + shapefile → Indicadores Derivados → Scores Intra-Rgint 
    → Eixos Temáticos → Pertinências Fuzzy → Classificação Final
```

---

## 2. Indicadores Derivados

O sistema transforma variáveis brutas em indicadores relativos para reduzir o efeito do tamanho absoluto dos municípios:

### 2.1 Indicadores Per Capita / Por Mil / Por 10 Mil

| Indicador | Fórmula | Unidade |
|-----------|---------|---------|
| pib_pc | pib_total / pop_total | R$/hab |
| empresas_1k | 1000 × empresas_total / pop_total | empresas/1.000 hab |
| estab_saude_10k | 10000 × estab_total / pop_total | estab/10.000 hab |
| homicides_100k | 100000 × vitimas_homicidio_2022 / pop_total | por 100.000 hab |
| demissoes_1k | 1000 × demissoes_2025 / pop_total | por 1.000 hab |
| area_urb_densa_100k | 100000 × area_urb_densa_km2 / pop_total | km²/100.000 hab |

### 2.2 Percentuais Limitados

As variáveis já em formato percentual são limitadas entre 0 e 100:

- via_pav_pct, ilum_pub_pct, calcada_pct
- indice_conectividade, cobertura_pop_4g5g, densidade_scm, fibra

### 2.3 Seca Normalizada

A variável de registros de seca é normalizada por rgint usando percentil:

```
seca_normalizada = percent_rank(registros_seca_2003_2015) dentro de cada rgint
```

---

## 3. Scores Intra-Rgint

Cada município é comparado apenas com os municípios de sua própria região intermediária, não com todos os municípios do Brasil.

### 3.1 Cálculo do Percentil

Para cada variável, o score é calculado como o percentil do município dentro de sua rgint:

```python
score = percent_rank(variavel)  # 0 a 1 dentro de cada rgint
```

### 3.2 Direção do Score

- **Positivo** (maior = melhor): pib_pc, empresas_1k, regic_var56, etc.
- **Negativo** (menor = melhor): homicides_100k, demissoes_1k

Para variáveis negativas, o score é invertido:

```python
score = 1.0 - percent_rank(variavel)
```

---

## 4. Eixos Temáticos

Os scores são agregados em 5 eixos temáticos usando média ponderada:

### 4.1 Centralidade Econômica

| Variável | Peso |
|----------|------|
| score_pib_pc | 0.25 |
| score_empresas_1k | 0.20 |
| score_regic_var56 | 0.25 |
| score_regic_var61 | 0.15 |
| score_regic_var66 | 0.15 |

### 4.2 Infraestrutura Urbana

| Variável | Peso |
|----------|------|
| score_via_pav_pct | 0.30 |
| score_calcada_pct | 0.25 |
| score_ilum_pub_pct | 0.15 |
| score_plano_diretor | 0.15 |
| score_area_urb_densa_100k | 0.15 |

### 4.3 Conectividade Digital

| Variável | Peso |
|----------|------|
| score_indice_conectividade | 0.35 |
| score_cobertura_pop_4g5g | 0.20 |
| score_densidade_scm | 0.25 |
| score_fibra | 0.20 |

### 4.4 Oferta de Serviços

| Variável | Peso |
|----------|------|
| score_estab_saude_10k | 0.25 |
| score_regic_var59 | 0.25 |
| score_regic_var60 | 0.20 |
| score_regic_var61 | 0.15 |
| score_regic_var66 | 0.15 |

### 4.5 Vulnerabilidade

Este eixo é calculado com pesos que consideram tanto indicadores negativos quanto déficits dos outros eixos:

| Componente | Peso |
|------------|------|
| score_homicidios_100k | 0.35 |
| score_demissoes_1k | 0.25 |
| seca_normalizada | 0.20 |
| deficit_infraestrutura (1 - infraestrutura_urbana) | 0.10 |
| deficit_conectividade (1 - conectividade_digital) | 0.10 |

### 4.6 Fórmula da Média Ponderada

```python
def weighted_mean(valores, pesos):
    # valores: dict ou DataFrame com as variáveis
    # pesos: dict com o peso de cada variável
    
    soma_ponderada = Σ(valor × peso)
    soma_pesos = Σ(peso para valores não-NaN)
    
    resultado = soma_ponderada / soma_pesos
    # Se soma_pesos = 0, resultado = NaN
```

---

## 5. Tipos de Região

O sistema classifica as 133 regiões intermediárias em 3 tipos para possível ajuste de pesos:

### 5.1 Critérios de Classificação

| Tipo | Critério |
|------|----------|
| Metropolitana | densidade ≥ P75 E pib_pc_medio ≥ P75 |
| Frontal | qtd_municipios ≥ 50 |
| Interior | Demais regiões |

### 5.2 Distribuição Resultado

| Tipo | Quantidade de Rgints |
|------|---------------------|
| Interior | 76 |
| Frontal | 40 |
| Metropolitana | 17 |

---

## 6. Funções de Pertinência Fuzzy

Cada eixo (com valor entre 0 e 1) é transformado em 5 pertinências (muito baixo, baixo, médio, alto, muito alto).

### 6.1 Funções Trapezoidais

O sistema usa funções de pertinência com thresholds baseados em desvios padrão:

```
muito_baixo: [0, 0, lim_mb, lim_b]
baixo:       [lim_mb, lim_b, lim_b, lim_a]
médio:       [lim_b, lim_a, lim_a, lim_a]  (triângulo centrado)
alto:        [lim_a, lim_ma, lim_ma, 1]
muito_alto:  [lim_ma, lim_ma, 1, 1]
```

Onde:
- lim_mb = μ - 1.5σ (limite muito baixo)
- lim_b = μ - 0.5σ (limite baixo)
- lim_a = μ + 0.5σ (limite alto)
- lim_ma = μ + 1.5σ (limite muito alto)
- μ = média do eixo na rgint
- σ = desvio padrão do eixo na rgint

### 6.2 Função Alternativa (Fixa)

Quando não há variabilidade suficiente (σ ≈ 0), usa-se thresholds fixos:

| Classe | Intervalo |
|--------|-----------|
| muito_baixo | [0, 0.20] |
| baixo | [0.20, 0.50] |
| médio | [0.35, 0.65] |
| alto | [0.50, 0.80] |
| muito_alto | [0.65, 1.0] |

### 6.3 Exemplo de Cálculo

Para um município com `centralidade_economica = 0.75` em uma rgint com μ=0.5 e σ=0.2:

- lim_mb = 0.5 - 1.5×0.2 = 0.2
- lim_b = 0.5 - 0.5×0.2 = 0.4
- lim_a = 0.5 + 0.5×0.2 = 0.6
- lim_ma = 0.5 + 1.5×0.2 = 0.8

Pertinências:
- muito_baixo: 0 (score > lim_b)
- baixo: 0 (score > lim_b)
- médio: 0 (score > lim_a)
- alto: (0.75 - 0.6) / (0.8 - 0.6) = 0.75
- muito_alto: (0.8 - 0.75) / (1.0 - 0.8) = 0.25

---

## 7. Regras de Inferência

As pertinências dos 5 eixos são combinadas para gerar as 5 pertinências finais de classificação.

### 7.1 MUITO_ALTO

Combinações excepcionais de alto desempenho:

```python
# Regra 1: centralidade muito alta + oferta muito alta
rule1 = min(pert_central["muito_alto"], pert_serv["muito_alto"])

# Regra 2: centralidade alta + infraestrutura alta + conectividade alta
rule2 = min(min(pert_central["alto"], pert_infra["alto"]), pert_conect["alto"])

# Regra 3: oferta muito alta + conectividade muito alta
rule3 = min(pert_serv["muito_alto"], pert_conect["muito_alto"])

pert_muito_alto = max(rule1, rule2, rule3)
```

### 7.2 ALTO

Combinações positivas:

```python
# Regra 1: centralidade alta + 2 outros eixos >= médio
rule1 = min(pert_central["alto"], 
            min(max(pert_infra["medio"], pert_infra["alto"]),
                max(pert_conect["medio"], pert_conect["alto"])))

# Regra 2: oferta alta + conectividade alta + vulnerabilidade baixa
rule2 = min(min(pert_serv["alto"], pert_conect["alto"]),
            max(pert_vuln["baixo"], pert_vuln["medio"]))

# Regra 3: infraestrutura alta + serviços médios-altos
rule3 = min(pert_infra["alto"], 
            max(pert_serv["medio"], pert_serv["alto"]))

# Regra 4: média ponderada de componentes
pert_alto = weighted_mean({
    "rule1": rule1,
    "rule2": rule2,
    "rule3": rule3,
    "central_alta": pert_central["alto"],
    "serv_alto": pert_serv["alto"],
    "conect_alta": pert_conect["alto"]
}, {"rule1": 0.25, "rule2": 0.25, "rule3": 0.15, 
    "central_alta": 0.15, "serv_alto": 0.10, "conect_alto": 0.10})
```

### 7.3 MÉDIO

Equilíbrio entre indicadores positivos e negativos:

```python
# Regra 1: 3+ eixos em médio
rule1 = min(min(pert_central["medio"], pert_infra["medio"]), 
            pert_conect["medio"])

# Regra 2: média dos eixos em médio
rule2 = weighted_mean({
    "central_medio": pert_central["medio"],
    "infra_medio": pert_infra["medio"],
    "conect_medio": pert_conect["medio"],
    "serv_medio": pert_serv["medio"],
    "vuln_medio": pert_vuln["medio"]
}, {"central_medio": 0.25, "infra_medio": 0.20, "conect_medio": 0.20,
    "serv_medio": 0.20, "vuln_medio": 0.15})

# Regra 3: infraestrutura alta + vulnerabilidade baixa
rule3 = min(pert_infra["alto"], 
            max(pert_vuln["baixo"], pert_vuln["medio"]))

pert_medio = max(rule1, rule2, rule3)
```

### 7.4 BAIXO

Combinações negativas:

```python
# Regra 1: centralidade baixa + 2 outros eixos <= médio
rule1 = min(pert_central["baixo"],
            min(max(pert_infra["baixo"], pert_infra["medio"]),
                max(pert_conect["baixo"], pert_conect["medio"])))

# Regra 2: vulnerabilidade alta + infraestrutura baixa
rule2 = min(max(pert_vuln["alto"], pert_vuln["medio"]),
            pert_infra["baixo"])

# Regra 3: oferta baixa + conectividade baixa
rule3 = min(pert_serv["baixo"], pert_conect["baixo"])

pert_baixo = weighted_mean({
    "rule1": rule1,
    "rule2": rule2,
    "rule3": rule3,
    "central_baixa": pert_central["baixo"],
    "infra_baixa": pert_infra["baixo"]
}, {"rule1": 0.30, "rule2": 0.25, "rule3": 0.20,
    "central_baixa": 0.15, "infra_baixa": 0.10})
```

### 7.5 MUITO_BAIXO

Combinações bem negativas:

```python
# Regra 1: 3+ eixos em muito baixo
rule1 = min(min(pert_central["muito_baixo"], pert_infra["muito_baixo"]),
            pert_conect["muito_baixo"])

# Regra 2: vulnerabilidade muito alta + combinação negativa
rule2 = min(pert_vuln["muito_alto"],
            max(pert_central["baixo"], pert_infra["baixo"]))

# Regra 3: centralidade muito baixa + infraestrutura muito baixa
rule3 = min(pert_central["muito_baixo"], pert_infra["muito_baixo"])

pert_muito_baixo = max(rule1, rule2, rule3)
```

### 7.6 Normalização

Após calcular as 5 pertinências, normaliza-se para que a soma seja igual a 1:

```python
soma = pert_muito_alto + pert_alto + pert_medio + pert_baixo + pert_muito_baixo
pert_i_normalizada = pert_i / soma
```

---

## 8. Classificação Final

### 8.1 Definição da Classe

A classe final é atribuída pela maior pertinência:

```python
classificacao_fuzzy = argmax(pert_muito_alto, pert_alto, pert_medio, 
                             pert_baixo, pert_muito_baixo)
```

### 8.2 Métricas de Confiança

**Confiança de classificação:**
```python
confianca_classificacao = max(pertinencias)
```

**Confiança de dominância:**
```python
confianca_dominancia = pert_maxima - pert_segunda_mais_alta
```
Valores altos indicam que a classificação é clara (uma pertinência muito maior que as outras).

**Estabilidade:**
```python
estabilidade_pertinencia = variancia(pertinencias)
```
Valores baixos indicam que as pertinências estão próximas (classificação menos definida).

---

## 9. Detecção de Outliers

Para cada eixo, calcula-se se o município é outlier (> 2.5 desvios padrão da média):

```python
outlier_pos = (eixo > media_rgint + 2.5 * desvio_rgint)
outlier_neg = (eixo < media_rgint - 2.5 * desvio_rgint)
```

Um município é considerado outlier geral se tiver 2+ eixos outlier.

---

## 10. Resultados

### 10.1 Distribuição por Classe

| Classe | Quantidade | Percentual |
|--------|------------|------------|
| médio | 3.446 | 61,9% |
| muito_alto | 835 | 15,0% |
| alto | 494 | 8,9% |
| muito_baixo | 405 | 7,3% |
| baixo | 390 | 7,0% |

### 10.2 Métricas de Qualidade

| Métrica | Valor |
|---------|-------|
| Confiança média | 0.567 |
| Confiança de dominância média | 0.264 |
| Estabilidade média (variância) | ~0.05 |
| Municípios outliers | 0 |

---

## 11. Arquivos de Saída

### 11.1 Arquivo Principal

`classificacao_municipios_fuzzy_rgint_v2.csv` contém:

- Dados originais do município
- Indicadores derivados
- Scores por variável
- Scores dos 5 eixos
- Pertinências fuzzy (5 colunas)
- Classificação final
- Métricas de confiança
- Flags de outlier

### 11.2 Resumo por Rgint

`classificacao_rgint_resumo_fuzzy_v2.csv` contém:

- Média dos eixos por rgint
- Classe dominante
- Participação da classe dominante
- Métricas de confiança agregadas
- Contagem de outliers

### 11.3 Arquivos por Região

`por_rgint_v2/*.csv` — 133 arquivos separados, um para cada região intermediária.

### 11.4 GeoPackage Espacial

`classificacao_municipios_fuzzy_rgint_v2.gpkg` contém duas camadas no mesmo arquivo:

- `municipios_fuzzy_v2` com os limites municipais e todos os atributos da classificação fuzzy v2
- `rgint_fuzzy_v2` com os limites das regiões intermediárias e o resumo agregado por rgint

---

## 12. Como Executar

```bash
cd classificacao_municipios/scripts

python3 classifica_municipios_fuzzy_rgint_v2.py \
    --base-municipal ../../prata/processamento/merge_v26.csv \
    --shapefile ../RG2017_regioesgeograficas2017_20180911/RG2017_regioesgeograficas2017.shp \
    --output ../processamento/classificacao_municipios_fuzzy_rgint_v2.csv \
    --output-rgint ../processamento/classificacao_rgint_resumo_fuzzy_v2.csv \
    --output-gpkg ../processamento/classificacao_municipios_fuzzy_rgint_v2.gpkg \
    --shapefile-rgint ../../regioes_geograficas/processamento/RG2017_rgint_20180911/RG2017_rgint.shp \
    --output-dir-rgint ../processamento/por_rgint_v2
```

---

## 13. Dependências

- pandas
- numpy
- geopandas

---

## 14. Fonte de Dados

- **Base municipal:** `prata/processamento/merge_v26.csv`
- **Shapefile municipal:** `classificacao_municipios/RG2017_regioesgeograficas2017_20180911/`
- **Shapefile de rgint:** `regioes_geograficas/processamento/RG2017_rgint_20180911/`

---

*Sistema fuzzy v2 - Classificação de municípios brasileiros por região intermediária*
