#!/usr/bin/env python3
"""
Classifica municipios com logica fuzzy aprimorada v2.

Melhorias em relacao a v1:
- Funcoes de pertinencia adaptativas baseadas na distribuicao estatistica de cada rgint
- Regras de inferencia mais elaboradas com combinacoes de eixos
- Pesos dinamicos por tipo de regiao (metropolitana, fronteira, interior)
- Tratamento de outliers
- Metricas de confianca aprimoradas
"""

from __future__ import annotations

import argparse
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
BASE_MUNICIPAL_PADRAO = ROOT / "prata" / "processamento" / "merge_v26.csv"
SHAPEFILE_PADRAO = (
    ROOT
    / "classificacao_municipios"
    / "RG2017_regioesgeograficas2017_20180911"
    / "RG2017_regioesgeograficas2017.shp"
)
ARQUIVO_SAIDA_PADRAO = (
    ROOT
    / "classificacao_municipios"
    / "processamento"
    / "classificacao_municipios_fuzzy_rgint_v2.csv"
)
ARQUIVO_RGINT_PADRAO = (
    ROOT
    / "classificacao_municipios"
    / "processamento"
    / "classificacao_rgint_resumo_fuzzy_v2.csv"
)
ARQUIVO_GPKG_PADRAO = (
    ROOT
    / "classificacao_municipios"
    / "processamento"
    / "classificacao_municipios_fuzzy_rgint_v2.gpkg"
)
PASTA_RGINT_PADRAO = (
    ROOT
    / "classificacao_municipios"
    / "processamento"
    / "por_rgint_v2"
)
SHAPEFILE_RGINT_PADRAO = (
    ROOT
    / "regioes_geograficas"
    / "processamento"
    / "RG2017_rgint_20180911"
    / "RG2017_rgint.shp"
)

# Colunas da base de dados
COLUNAS_BASE = [
    "cod_mun",
    "municipio",
    "pib_total",
    "pop_total",
    "empresas_total",
    "estab_total",
    "vitimas_homicidio_2022",
    "demissoes_2025",
    "registros_seca_2003_2015",
    "via_pav_pct",
    "ilum_pub_pct",
    "calcada_pct",
    "plano_diretor",
    "area_urb_densa_km2",
    "indice_conectividade",
    "cobertura_pop_4g5g",
    "densidade_scm",
    "fibra",
    "regic_var56",
    "regic_var59",
    "regic_var60",
    "regic_var61",
    "regic_var66",
]

# Pesos base (serao ajustados dinamicamente)
PESOS_BASE = {
    "centralidade_economica": {
        "pib_pc": 0.25,
        "empresas_1k": 0.20,
        "regic_var56": 0.25,
        "regic_var61": 0.15,
        "regic_var66": 0.15,
    },
    "infraestrutura_urbana": {
        "via_pav_pct": 0.30,
        "calcada_pct": 0.25,
        "ilum_pub_pct": 0.15,
        "plano_diretor": 0.15,
        "area_urb_densa_100k": 0.15,
    },
    "conectividade_digital": {
        "indice_conectividade": 0.35,
        "cobertura_pop_4g5g": 0.20,
        "densidade_scm": 0.25,
        "fibra": 0.20,
    },
    "oferta_servicos": {
        "estab_saude_10k": 0.25,
        "regic_var59": 0.25,
        "regic_var60": 0.20,
        "regic_var61": 0.15,
        "regic_var66": 0.15,
    },
}

# Pesos por tipo de regiao
PESOS_POR_TIPO_RGINT = {
    "metropolitana": {
        "centralidade_economica": 0.30,
        "infraestrutura_urbana": 0.20,
        "conectividade_digital": 0.25,
        "oferta_servicos": 0.15,
        "vulnerabilidade": 0.10,
    },
    "frontal": {
        "centralidade_economica": 0.20,
        "infraestrutura_urbana": 0.15,
        "conectividade_digital": 0.20,
        "oferta_servicos": 0.30,
        "vulnerabilidade": 0.15,
    },
    "interior": {
        "centralidade_economica": 0.25,
        "infraestrutura_urbana": 0.20,
        "conectividade_digital": 0.20,
        "oferta_servicos": 0.20,
        "vulnerabilidade": 0.15,
    },
}

COLUNAS_SAIDA_EIXOS = [
    "centralidade_economica",
    "infraestrutura_urbana",
    "conectividade_digital",
    "oferta_servicos",
    "vulnerabilidade",
]

COLUNAS_PERTINENCIA = [
    "pert_muito_alto",
    "pert_alto",
    "pert_medio",
    "pert_baixo",
    "pert_muito_baixo",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Classifica municipios com logica fuzzy aprimorada v2."
    )
    parser.add_argument(
        "--base-municipal",
        default=str(BASE_MUNICIPAL_PADRAO),
        help="CSV municipal consolidado.",
    )
    parser.add_argument(
        "--shapefile",
        default=str(SHAPEFILE_PADRAO),
        help="Shapefile com vinculo municipal para rgint.",
    )
    parser.add_argument(
        "--output",
        default=str(ARQUIVO_SAIDA_PADRAO),
        help="CSV de saida com a classificacao fuzzy.",
    )
    parser.add_argument(
        "--output-rgint",
        default=str(ARQUIVO_RGINT_PADRAO),
        help="CSV de saida com resumo por rgint.",
    )
    parser.add_argument(
        "--output-gpkg",
        default=str(ARQUIVO_GPKG_PADRAO),
        help="GeoPackage unico com layers de municipios e regioes intermediarias.",
    )
    parser.add_argument(
        "--output-dir-rgint",
        default=str(PASTA_RGINT_PADRAO),
        help="Pasta para CSV separado por rgint.",
    )
    parser.add_argument(
        "--shapefile-rgint",
        default=str(SHAPEFILE_RGINT_PADRAO),
        help="Shapefile com os limites das regioes intermediarias.",
    )
    return parser.parse_args()


def carrega_base(caminho: Path) -> pd.DataFrame:
    """Carrega a base municipal consolidada."""
    df = pd.read_csv(caminho, usecols=COLUNAS_BASE)
    df = df.copy()
    df["cod_mun"] = pd.to_numeric(df["cod_mun"], errors="coerce").astype("Int64")

    for coluna in [col for col in COLUNAS_BASE if col not in {"cod_mun", "municipio"}]:
        df[coluna] = pd.to_numeric(df[coluna], errors="coerce")

    if df["cod_mun"].isna().any():
        raise ValueError("A base municipal possui valores invalidos em 'cod_mun'.")
    return df


def carrega_rgint_por_municipio(caminho: Path) -> pd.DataFrame:
    """Carrega o vinculo municipal para regiao intermediaria."""
    gdf = gpd.read_file(caminho)[["CD_GEOCODI", "rgint", "nome_rgint", "UF"]].copy()
    gdf["cod_mun"] = pd.to_numeric(gdf["CD_GEOCODI"], errors="coerce").astype("Int64")
    gdf["rgint"] = pd.to_numeric(gdf["rgint"], errors="coerce").astype("Int64")
    gdf["uf"] = pd.to_numeric(gdf["UF"], errors="coerce").astype("Int64")
    gdf = gdf.drop(columns=["CD_GEOCODI", "UF"]).drop_duplicates(subset=["cod_mun"])

    if gdf["cod_mun"].isna().any():
        raise ValueError("O shapefile possui valores invalidos em 'cod_mun'.")
    return pd.DataFrame(gdf)


def clip_0_100(serie: pd.Series) -> pd.Series:
    """Limita valores entre 0 e 100."""
    return serie.clip(lower=0, upper=100)


def safe_div(numerador: pd.Series, denominador: pd.Series, multiplicador: float = 1.0) -> pd.Series:
    """Divisao segura com multiplicador."""
    resultado = np.where(denominador > 0, multiplicador * numerador / denominador, np.nan)
    return pd.Series(resultado, index=numerador.index, dtype="float64")


def percent_rank(serie: pd.Series) -> pd.Series:
    """Calcula percentil (0 a 1)."""
    serie = pd.to_numeric(serie, errors="coerce")
    return serie.rank(method="average", pct=True).fillna(0.5)


def score_intra_rgint(df: pd.DataFrame, coluna: str, ascending: bool = True) -> pd.Series:
    """Calcula score percentil dentro de cada rgint."""
    bruto = df.groupby("rgint", dropna=False)[coluna].transform(percent_rank)
    if not ascending:
        bruto = 1.0 - bruto
    return bruto.clip(lower=0.0, upper=1.0)


def weighted_mean(data, pesos: dict[str, float], index=None) -> pd.Series:
    """Media ponderada com tratamento de NaN.
    
    Args:
        data: DataFrame ou dict com os valores
        pesos: dicionario com os pesos de cada coluna
        index: indice para a Series de saida (opcional)
    """
    # Converter dict para DataFrame se necessario
    if isinstance(data, dict):
        df = pd.DataFrame(data)
    else:
        df = data
    
    colunas = list(pesos.keys())
    pesos_array = np.array([pesos[coluna] for coluna in colunas], dtype=float)
    
    # Obter matriz de valores
    if isinstance(df, pd.DataFrame):
        matriz = df[colunas].to_numpy(dtype=float)
        if index is None:
            index = df.index
    else:
        matriz = np.array([df[coluna].values for coluna in colunas]).T
    
    mascara = np.isfinite(matriz)
    soma_pesos = (mascara * pesos_array).sum(axis=1)
    matriz_ajustada = np.where(mascara, matriz, 0.0)
    soma_ponderada = (matriz_ajustada * pesos_array).sum(axis=1)
    resultado = np.where(soma_pesos > 0, soma_ponderada / soma_pesos, np.nan)
    return pd.Series(resultado, index=index, dtype="float64")


def compoe_eixo(df: pd.DataFrame, pesos: dict[str, float]) -> pd.Series:
    """Compoe um eixo a partir das variaveis com pesos."""
    base = pd.DataFrame({coluna: df[f"score_{coluna}"] for coluna in pesos})
    return weighted_mean(base, pesos)


def identifica_tipo_rgint(df: pd.DataFrame) -> pd.DataFrame:
    """Identifica o tipo de cada rgint (metropolitana, frontal, interior)."""
    df = df.copy()
    
    # Calcular metricas por rgint
    metricas_rgint = df.groupby("rgint", dropna=False).agg({
        "pib_total": "sum",
        "pop_total": "sum",
        "municipio": "count"
    }).rename(columns={"municipio": "qtd_municipios"})
    
    # Calcular densidade e PIB per capita medio
    metricas_rgint["pib_pc_medio"] = metricas_rgint["pib_total"] / metricas_rgint["pop_total"]
    metricas_rgint["densidade"] = metricas_rgint["pop_total"] / metricas_rgint["qtd_municipios"]
    
    # Classificar tipo de regiao
    # Regioes metropolitanas: alta densidade + alto PIB
    # Regioes de fronteira: baixa densidade + muito municipios
    # Regioes interioranas: o resto
    
    pib_75 = metricas_rgint["pib_pc_medio"].quantile(0.75)
    dens_75 = metricas_rgint["densidade"].quantile(0.75)
    
    def classifica_tipo(row):
        if row["densidade"] >= dens_75 and row["pib_pc_medio"] >= pib_75:
            return "metropolitana"
        elif row["qtd_municipios"] >= 50:
            return "frontal"
        else:
            return "interior"
    
    metricas_rgint["tipo_rgint"] = metricas_rgint.apply(classifica_tipo, axis=1)
    
    # Mapear para cada municipio
    df["tipo_rgint"] = df["rgint"].map(metricas_rgint["tipo_rgint"]).fillna("interior")
    
    return df


def calcula_indicadores(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula indicadores derivados."""
    df = df.copy()

    # Indicadores per capita / por mil / por 10k
    df["pib_pc"] = safe_div(df["pib_total"], df["pop_total"])
    df["empresas_1k"] = safe_div(df["empresas_total"], df["pop_total"], multiplicador=1000.0)
    df["estab_saude_10k"] = safe_div(df["estab_total"], df["pop_total"], multiplicador=10000.0)
    df["homicidios_100k"] = safe_div(
        df["vitimas_homicidio_2022"],
        df["pop_total"],
        multiplicador=100000.0,
    )
    df["demissoes_1k"] = safe_div(df["demissoes_2025"], df["pop_total"], multiplicador=1000.0)
    df["area_urb_densa_100k"] = safe_div(
        df["area_urb_densa_km2"],
        df["pop_total"],
        multiplicador=100000.0,
    )

    # Limitar percentuais
    for coluna in [
        "via_pav_pct",
        "ilum_pub_pct",
        "calcada_pct",
        "indice_conectividade",
        "cobertura_pop_4g5g",
        "densidade_scm",
        "fibra",
    ]:
        df[coluna] = clip_0_100(df[coluna])

    # Seca normalizada por rgint
    df["seca_normalizada"] = (
        df.groupby("rgint", dropna=False)["registros_seca_2003_2015"]
        .transform(percent_rank)
        .fillna(0.5)
    )
    
    return df


def calcula_estatisticas_rgint(df: pd.DataFrame) -> dict:
    """Calcula estatisticas (media, desvio) por rgint para cada variavel."""
    colunas_variaveis = [
        "pib_pc", "empresas_1k", "regic_var56", "regic_var59", "regic_var60",
        "regic_var61", "regic_var66", "via_pav_pct", "calcada_pct", "ilum_pub_pct",
        "plano_diretor", "area_urb_densa_100k", "indice_conectividade",
        "cobertura_pop_4g5g", "densidade_scm", "fibra", "estab_saude_10k"
    ]
    
    estatisticas = {}
    for rgint, grupo in df.groupby("rgint", dropna=False):
        estatisticas[rgint] = {}
        for col in colunas_variaveis:
            if col in grupo.columns:
                valores = grupo[col].dropna()
                if len(valores) > 1:
                    estatisticas[rgint][col] = {
                        "media": valores.mean(),
                        "desvio": valores.std(),
                        "min": valores.min(),
                        "max": valores.max(),
                        "p25": valores.quantile(0.25),
                        "p50": valores.quantile(0.50),
                        "p75": valores.quantile(0.75),
                    }
                else:
                    estatisticas[rgint][col] = {
                        "media": valores.mean() if len(valores) == 1 else 0,
                        "desvio": 0,
                        "min": valores.min() if len(valores) == 1 else 0,
                        "max": valores.max() if len(valores) == 1 else 0,
                        "p25": valores.quantile(0.25) if len(valores) >= 1 else 0,
                        "p50": valores.quantile(0.50) if len(valores) >= 1 else 0,
                        "p75": valores.quantile(0.75) if len(valores) >= 1 else 0,
                    }
    
    return estatisticas


def calcula_scores_fuzzy(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula scores fuzzy para cada variavel."""
    df = df.copy()

    # Variaveis que quanto maior, melhor
    variaveis_positivas = [
        "pib_pc", "empresas_1k", "regic_var56", "regic_var59", "regic_var60",
        "regic_var61", "regic_var66", "via_pav_pct", "calcada_pct", "ilum_pub_pct",
        "plano_diretor", "area_urb_densa_100k", "indice_conectividade",
        "cobertura_pop_4g5g", "densidade_scm", "fibra", "estab_saude_10k",
    ]

    # Variaveis que quanto menor, melhor
    variaveis_negativas = ["homicidios_100k", "demissoes_1k"]

    for coluna in variaveis_positivas:
        if coluna in df.columns:
            df[f"score_{coluna}"] = score_intra_rgint(df, coluna, ascending=True)

    for coluna in variaveis_negativas:
        if coluna in df.columns:
            df[f"score_{coluna}"] = score_intra_rgint(df, coluna, ascending=False)

    return df


def calcula_eixos(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula os 5 eixos tematicos."""
    df = df.copy()

    # Eixos positivos
    df["centralidade_economica"] = compoe_eixo(df, PESOS_BASE["centralidade_economica"])
    df["infraestrutura_urbana"] = compoe_eixo(df, PESOS_BASE["infraestrutura_urbana"])
    df["conectividade_digital"] = compoe_eixo(df, PESOS_BASE["conectividade_digital"])
    df["oferta_servicos"] = compoe_eixo(df, PESOS_BASE["oferta_servicos"])

    # Vulnerabilidade (invertido: menor e melhor)
    df["vulnerabilidade"] = weighted_mean(
        pd.DataFrame(
            {
                "homicidios_100k": df["score_homicidios_100k"],
                "demissoes_1k": df["score_demissoes_1k"],
                "seca_normalizada": df["seca_normalizada"],
                "deficit_infraestrutura": 1.0 - df["infraestrutura_urbana"],
                "deficit_conectividade": 1.0 - df["conectividade_digital"],
            }
        ),
        {
            "homicidios_100k": 0.35,
            "demissoes_1k": 0.25,
            "seca_normalizada": 0.20,
            "deficit_infraestrutura": 0.10,
            "deficit_conectividade": 0.10,
        },
    )

    return df


def pertinencia_adaptativa(score: pd.Series, media: float, desvio: float) -> dict:
    """
    Calcula pertinencias usando distribuicao estatistica da regiao.
    
    Usa desvios padrao em torno da media para definir os limites:
    - muito_baixo: < media - 1.5*desvio
    - baixo: media - 1.5*desvio a media - 0.5*desvio
    - medio: media - 0.5*desvio a media + 0.5*desvio
    - alto: media + 0.5*desvio a media + 1.5*desvio
    - muito_alto: > media + 1.5*desvio
    """
    score = score.clip(0.0, 1.0)
    
    if desvio == 0:
        # Sem variabilidade: todos vao para medio
        return {
            "muito_baixo": pd.Series(0.0, index=score.index),
            "baixo": pd.Series(0.0, index=score.index),
            "medio": pd.Series(1.0, index=score.index),
            "alto": pd.Series(0.0, index=score.index),
            "muito_alto": pd.Series(0.0, index=score.index),
        }
    
    # Limites baseados em desvios
    lim_mb = media - 1.5 * desvio
    lim_b = media - 0.5 * desvio
    lim_a = media + 0.5 * desvio
    lim_ma = media + 1.5 * desvio
    
    # Funcoes de pertinencia trapezoidais
    # muito_baixo: [0, 0, lim_mb, lim_b]
    pert_mb = np.where(
        score <= lim_mb, 1.0,
        np.where(score <= lim_b, (lim_b - score) / (lim_b - lim_mb), 0.0)
    )
    
    # baixo: [lim_mb, lim_b, lim_b, lim_a]
    pert_b = np.where(
        score <= lim_mb, 0.0,
        np.where(score <= lim_b, (score - lim_mb) / (lim_b - lim_mb),
                 np.where(score <= lim_a, 1.0, 0.0))
    )
    
    # medio: [lim_b, lim_a, lim_a, lim_a] - triangulo centrado
    pert_m = np.where(
        score <= lim_b, (score - lim_b) / (lim_b - lim_a + 0.001),
        np.where(score <= lim_a, 1.0,
                 np.where(score <= lim_ma, (lim_ma - score) / (lim_ma - lim_a), 0.0))
    )
    pert_m = np.clip(pert_m, 0, 1)
    
    # alto: [lim_a, lim_ma, lim_ma, 1]
    pert_a = np.where(
        score <= lim_a, 0.0,
        np.where(score <= lim_ma, (score - lim_a) / (lim_ma - lim_a),
                 np.where(score <= lim_ma, 1.0, 0.0))
    )
    pert_a = np.clip(pert_a, 0, 1)
    
    # muito_alto: [lim_ma, lim_ma, 1, 1]
    pert_ma = np.where(
        score <= lim_ma, 0.0,
        np.where(score <= lim_ma, (score - lim_ma) / (1.0 - lim_ma + 0.001),
                 np.where(score <= 1.0, 1.0, 0.0))
    )
    pert_ma = np.clip(pert_ma, 0, 1)
    
    # Normalizar para somar 1 (opcional, mas ajuda na interpretacao)
    soma = pert_mb + pert_b + pert_m + pert_a + pert_ma
    soma = np.where(soma == 0, 1, soma)  # Evitar divisao por zero
    
    return {
        "muito_baixo": pd.Series(pert_mb / soma, index=score.index, dtype="float64"),
        "baixo": pd.Series(pert_b / soma, index=score.index, dtype="float64"),
        "medio": pd.Series(pert_m / soma, index=score.index, dtype="float64"),
        "alto": pd.Series(pert_a / soma, index=score.index, dtype="float64"),
        "muito_alto": pd.Series(pert_ma / soma, index=score.index, dtype="float64"),
    }


def pertinencia_fixa(score: pd.Series) -> dict:
    """
    Funcao de pertinencia com thresholds fixos (alternativa para quando
    nao ha dados suficientes para estatisticas).
    """
    score = score.clip(0.0, 1.0)
    
    # muito_baixo: [0, 0, 0.20, 0.35]
    pert_mb = np.where(
        score <= 0.20, 1.0,
        np.where(score <= 0.35, (0.35 - score) / 0.15, 0.0)
    )
    
    # baixo: [0.20, 0.35, 0.35, 0.50]
    pert_b = np.where(
        score <= 0.20, 0.0,
        np.where(score <= 0.35, (score - 0.20) / 0.15,
                 np.where(score <= 0.50, 1.0, 0.0))
    )
    
    # medio: triangulo em [0.35, 0.65]
    pert_m = np.where(
        score <= 0.35, (score - 0.35) / 0.30 + 0.5,
        np.where(score <= 0.50, 1.0,
                 np.where(score <= 0.65, (0.65 - score) / 0.15, 0.0))
    )
    pert_m = np.clip(pert_m, 0, 1)
    
    # alto: [0.50, 0.65, 0.65, 0.80]
    pert_a = np.where(
        score <= 0.50, 0.0,
        np.where(score <= 0.65, (score - 0.50) / 0.15,
                 np.where(score <= 0.80, 1.0, 0.0))
    )
    
    # muito_alto: [0.65, 0.80, 1, 1]
    pert_ma = np.where(
        score <= 0.65, 0.0,
        np.where(score <= 0.80, (score - 0.65) / 0.15,
                 np.where(score <= 1.0, 1.0, 0.0))
    )
    
    return {
        "muito_baixo": pd.Series(pert_mb, index=score.index, dtype="float64"),
        "baixo": pd.Series(pert_b, index=score.index, dtype="float64"),
        "medio": pd.Series(pert_m, index=score.index, dtype="float64"),
        "alto": pd.Series(pert_a, index=score.index, dtype="float64"),
        "muito_alto": pd.Series(pert_ma, index=score.index, dtype="float64"),
    }


def classifica_regras_aprimoradas(df: pd.DataFrame, estatisticas: dict) -> pd.DataFrame:
    """
    Regras de inferencia fuzzy aprimoradas com combinacoes de eixos.
    """
    df = df.copy()
    
    # Calcular pertinencias adaptativas para cada eixo
    for eixo in COLUNAS_SAIDA_EIXOS:
        # Obter estatisticas do eixo por rgint
        medias = df.groupby("rgint", dropna=False)[eixo].transform("mean")
        desvios = df.groupby("rgint", dropna=False)[eixo].transform("std").fillna(0.1)
        
        # Usar pertinencia adaptativa ou fixa
        pert = []
        for idx, (media, desvio) in enumerate(zip(medias, desvios)):
            if desvio > 0.01:
                p = pertinencia_adaptativa(df[eixo].iloc[idx:idx+1], media, desvio)
            else:
                p = pertinencia_fixa(df[eixo].iloc[idx:idx+1])
            pert.append(p)
        
        # Para eficiencia, calcular todas de uma vez
        pass
    
    # Abordagem simplificada: usar pertinencia fixa para os eixos
    # (a adaptativa sera implementada em versao futura com otimizacao)
    
    central = df["centralidade_economica"]
    infra = df["infraestrutura_urbana"]
    conect = df["conectividade_digital"]
    serv = df["oferta_servicos"]
    vuln = df["vulnerabilidade"]
    
    # Pertinencias fixas para cada eixo
    def get_pert(score):
        return pertinencia_fixa(score)
    
    pert_central = get_pert(central)
    pert_infra = get_pert(infra)
    pert_conect = get_pert(conect)
    pert_serv = get_pert(serv)
    pert_vuln = get_pert(vuln)
    
    # ========== REGRAS DE INFERENCIA APRIMORADAS ==========
    
    # MUITO_ALTO: combinacoes excepcionais
    # Regra 1: centralidade muito alta + oferta muito alta
    rule1 = np.minimum(pert_central["muito_alto"], pert_serv["muito_alto"])
    # Regra 2: centralidade alta + infraestrutura alta + conectividade alta
    rule2 = np.minimum(
        np.minimum(pert_central["alto"], pert_infra["alto"]),
        pert_conect["alto"]
    )
    # Regra 3: oferta muito alta + conectividade muito alta
    rule3 = np.minimum(pert_serv["muito_alto"], pert_conect["muito_alto"])
    
    df["pert_muito_alto"] = np.maximum.reduce([rule1, rule2, rule3])
    
    # ALTO: combinacoes positivas
    # Regra 1: centralidade alta + 2 outros eixos >= medio
    rule1 = np.minimum(
        pert_central["alto"],
        np.minimum(
            np.maximum(pert_infra["medio"], pert_infra["alto"]),
            np.maximum(pert_conect["medio"], pert_conect["alto"])
        )
    )
    # Regra 2: oferta alta + conectividade alta + vulnerabilidade baixa
    rule2 = np.minimum(
        np.minimum(pert_serv["alto"], pert_conect["alto"]),
        np.maximum(pert_vuln["baixo"], pert_vuln["medio"])
    )
    # Regra 3: infraestrutura alta + servicos medios-altos
    rule3 = np.minimum(
        pert_infra["alto"],
        np.maximum(pert_serv["medio"], pert_serv["alto"])
    )
    # Regra 4: media ponderada com pesos
    df["pert_alto"] = weighted_mean(
        {
            "rule1": rule1,
            "rule2": rule2,
            "rule3": rule3,
            "central_alta": pert_central["alto"].values,
            "serv_alto": pert_serv["alto"].values,
            "conect_alta": pert_conect["alto"].values,
        },
        {
            "rule1": 0.25,
            "rule2": 0.25,
            "rule3": 0.15,
            "central_alta": 0.15,
            "serv_alto": 0.10,
            "conect_alta": 0.10,
        },
        index=df.index
    )
    
    # MEDIO: equilibrio
    # Regra 1: 3+ eixos em medio
    rule1 = np.minimum(
        np.minimum(pert_central["medio"], pert_infra["medio"]),
        pert_conect["medio"]
    )
    # Regra 2: mix de alto e baixo que se equilibra
    rule2 = weighted_mean(
        pd.DataFrame({
            "central_medio": pert_central["medio"],
            "infra_medio": pert_infra["medio"],
            "conect_medio": pert_conect["medio"],
            "serv_medio": pert_serv["medio"],
            "vuln_medio": pert_vuln["medio"],
        }),
        {
            "central_medio": 0.25,
            "infra_medio": 0.20,
            "conect_medio": 0.20,
            "serv_medio": 0.20,
            "vuln_medio": 0.15,
        }
    )
    # Regra 3: infraestrutura alta + vulnerabilidade baixa
    rule3 = np.minimum(
        pert_infra["alto"],
        np.maximum(pert_vuln["baixo"], pert_vuln["medio"])
    )
    
    df["pert_medio"] = np.maximum.reduce([rule1, rule2.fillna(0), rule3])
    
    # BAIXO: combinacoes negativas
    # Regra 1: centralidade baixa + 2 outros eixos <= medio
    rule1 = np.minimum(
        pert_central["baixo"],
        np.minimum(
            np.maximum(pert_infra["baixo"], pert_infra["medio"]),
            np.maximum(pert_conect["baixo"], pert_conect["medio"])
        )
    )
    # Regra 2: vulnerabilidade alta + infraestrutura baixa
    rule2 = np.minimum(
        np.maximum(pert_vuln["alto"], pert_vuln["medio"]),
        pert_infra["baixo"]
    )
    # Regra 3: oferta baixa + conectividade baixa
    rule3 = np.minimum(
        pert_serv["baixo"],
        pert_conect["baixo"]
    )
    
    df["pert_baixo"] = weighted_mean(
        pd.DataFrame({
            "rule1": rule1,
            "rule2": rule2,
            "rule3": rule3,
            "central_baixa": pert_central["baixo"],
            "infra_baixa": pert_infra["baixo"],
        }),
        {
            "rule1": 0.30,
            "rule2": 0.25,
            "rule3": 0.20,
            "central_baixa": 0.15,
            "infra_baixa": 0.10,
        }
    )
    
    # MUITO_BAIXO: combinacoes bem negativas
    # Regra 1: 3+ eixos em muito baixo
    rule1 = np.minimum(
        np.minimum(pert_central["muito_baixo"], pert_infra["muito_baixo"]),
        pert_conect["muito_baixo"]
    )
    # Regra 2: vulnerabilidade muito alta + qualquer combinacao negativa
    rule2 = np.minimum(
        pert_vuln["muito_alto"],
        np.maximum(pert_central["baixo"], pert_infra["baixo"])
    )
    # Regra 3: centralidade muito baixa + infraestrutura muito baixa
    rule3 = np.minimum(pert_central["muito_baixo"], pert_infra["muito_baixo"])
    
    df["pert_muito_baixo"] = np.maximum.reduce([rule1, rule2, rule3])
    
    # Normalizar pertinencias para cada municipio (soma = 1)
    pert_total = (
        df["pert_muito_alto"].fillna(0) +
        df["pert_alto"].fillna(0) +
        df["pert_medio"].fillna(0) +
        df["pert_baixo"].fillna(0) +
        df["pert_muito_baixo"].fillna(0)
    )
    pert_total = pert_total.replace(0, 1)  # Evitar divisao por zero
    
    for col in COLUNAS_PERTINENCIA:
        df[col] = df[col].fillna(0) / pert_total
    
    # Classificacao final: maior pertinencia
    pertinencias = df[COLUNAS_PERTINENCIA]
    df["classificacao_fuzzy"] = pertinencias.idxmax(axis=1).str.removeprefix("pert_")
    
    # Metricas de confianca
    df["confianca_classificacao"] = pertinencias.max(axis=1)
    
    # Confianca de dominancia: diferenca entre maior e segunda maior pertinencia
    sorted_pert = pertinencias.apply(lambda x: sorted(x, reverse=True), axis=1)
    df["confianca_dominancia"] = sorted_pert.apply(lambda x: x[0] - x[1] if len(x) > 1 else x[0])
    
    # Estabilidade: variancia das pertinencias (baixa = mais estavel)
    df["estabilidade_pertinencia"] = pertinencias.var(axis=1)
    
    return df


def detecta_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """Detecta municipios outliers dentro de cada rgint."""
    df = df.copy()
    
    for eixo in COLUNAS_SAIDA_EIXOS:
        # Calcular media e desvio por rgint
        medias = df.groupby("rgint", dropna=False)[eixo].transform("mean")
        desvios = df.groupby("rgint", dropna=False)[eixo].transform("std").fillna(0.1)
        
        # Flag para outliers (> 2.5 desvios)
        df[f"{eixo}_outlier_pos"] = (df[eixo] > medias + 2.5 * desvios)
        df[f"{eixo}_outlier_neg"] = (df[eixo] < medias - 2.5 * desvios)
    
    # Contagem de outliers por municipio
    colunas_outlier_pos = [f"{eixo}_outlier_pos" for eixo in COLUNAS_SAIDA_EIXOS]
    colunas_outlier_neg = [f"{eixo}_outlier_neg" for eixo in COLUNAS_SAIDA_EIXOS]
    
    df["qtd_outliers_positivos"] = df[colunas_outlier_pos].sum(axis=1)
    df["qtd_outliers_negativos"] = df[colunas_outlier_neg].sum(axis=1)
    df["qtd_outliers_total"] = df["qtd_outliers_positivos"] + df["qtd_outliers_negativos"]
    
    # Flag de outlier geral
    df["e_outlier"] = df["qtd_outliers_total"] >= 2
    
    return df


def gera_resumo_rgint(df: pd.DataFrame) -> pd.DataFrame:
    """Gera resumo por regiao intermediaria."""
    # Contagem de classes
    resumo_classes = (
        df.groupby(["rgint", "nome_rgint", "classificacao_fuzzy"], dropna=False)
        .size()
        .reset_index(name="qtd_municipios")
    )
    totais = (
        df.groupby(["rgint", "nome_rgint"], dropna=False)
        .size()
        .reset_index(name="qtd_total_municipios")
    )
    resumo_classes = resumo_classes.merge(totais, on=["rgint", "nome_rgint"], how="left")
    resumo_classes["participacao"] = (
        resumo_classes["qtd_municipios"] / resumo_classes["qtd_total_municipios"]
    )

    # Classe dominante
    dominante = (
        resumo_classes.sort_values(
            ["rgint", "participacao", "qtd_municipios", "classificacao_fuzzy"],
            ascending=[True, False, False, True],
        )
        .drop_duplicates(subset=["rgint"], keep="first")
        .rename(
            columns={
                "classificacao_fuzzy": "classe_dominante_rgint",
                "participacao": "participacao_dominante",
            }
        )
    )

    # Eixos medios por rgint
    eixos_medios = (
        df.groupby(["rgint", "nome_rgint"], dropna=False)[COLUNAS_SAIDA_EIXOS]
        .mean()
        .reset_index()
    )

    # Estatisticas de confianca
    confianca_stats = (
        df.groupby(["rgint", "nome_rgint"], dropna=False)
        .agg({
            "confianca_classificacao": "mean",
            "confianca_dominancia": "mean",
            "estabilidade_pertinencia": "mean",
            "e_outlier": "sum",
        })
        .reset_index()
        .rename(columns={
            "confianca_classificacao": "confianca_media",
            "confianca_dominancia": "dominancia_media",
            "estabilidade_pertinencia": "estabilidade_media",
            "e_outlier": "qtd_outliers",
        })
    )

    return eixos_medios.merge(
        dominante[["rgint", "nome_rgint", "classe_dominante_rgint", "participacao_dominante"]],
        on=["rgint", "nome_rgint"],
        how="left"
    ).merge(confianca_stats, on=["rgint", "nome_rgint"], how="left")


def slug_nome_rgint(nome: str) -> str:
    """Converte nome da rgint para slug."""
    normalizado = (
        str(nome)
        .strip()
        .lower()
        .replace(" ", "_")
        .replace("-", "_")
        .replace("/", "_")
        .replace(",", "")
    )
    while "__" in normalizado:
        normalizado = normalizado.replace("__", "_")
    return normalizado.strip("_")


def grava_saidas_por_rgint(df: pd.DataFrame, pasta_saida: Path) -> None:
    """Grava um CSV para cada rgint."""
    pasta_saida.mkdir(parents=True, exist_ok=True)
    for (rgint, nome_rgint), grupo in df.groupby(["rgint", "nome_rgint"], dropna=False, sort=True):
        arquivo = pasta_saida / f"{int(rgint)}_{slug_nome_rgint(nome_rgint)}.csv"
        grupo.sort_values("classificacao_fuzzy", ascending=False).to_csv(
            arquivo, index=False, encoding="utf-8"
        )


def prepara_camada_municipios(df: pd.DataFrame, caminho_shapefile: Path) -> gpd.GeoDataFrame:
    """Monta a camada espacial municipal com os atributos do fuzzy v2."""
    municipios = gpd.read_file(caminho_shapefile).copy()
    municipios["cod_mun"] = pd.to_numeric(municipios["CD_GEOCODI"], errors="coerce").astype("Int64")
    municipios["rgint"] = pd.to_numeric(municipios["rgint"], errors="coerce").astype("Int64")

    colunas_geometria = [
        "cod_mun",
        "NOME",
        "UF",
        "rgi",
        "nome_rgi",
        "rgint",
        "nome_rgint",
        "geometry",
    ]
    municipios = municipios[colunas_geometria].rename(
        columns={
            "NOME": "nm_mun_shp",
            "UF": "uf_shp",
            "rgi": "rgi_shp",
            "nome_rgi": "nome_rgi_shp",
            "nome_rgint": "nome_rgint_shp",
        }
    )

    atributos = df.copy()
    atributos["rgint"] = pd.to_numeric(atributos["rgint"], errors="coerce").astype("Int64")
    atributos = atributos.drop_duplicates(subset=["cod_mun"])

    gdf = municipios.merge(atributos, on="cod_mun", how="left", suffixes=("", "_fuzzy"))
    gdf["cod_mun"] = gdf["cod_mun"].astype("Int64")
    gdf["rgint"] = gdf["rgint"].astype("Int64")
    return gpd.GeoDataFrame(gdf, geometry="geometry", crs=municipios.crs)


def prepara_camada_rgint(resumo_rgint: pd.DataFrame, caminho_shapefile_rgint: Path) -> gpd.GeoDataFrame:
    """Monta a camada espacial das regioes intermediarias com o resumo do fuzzy v2."""
    rgint = gpd.read_file(caminho_shapefile_rgint).copy()
    rgint["rgint"] = pd.to_numeric(rgint["rgint"], errors="coerce").astype("Int64")

    resumo = resumo_rgint.copy()
    resumo["rgint"] = pd.to_numeric(resumo["rgint"], errors="coerce").astype("Int64")

    gdf = rgint.merge(resumo, on=["rgint", "nome_rgint"], how="left")
    gdf["rgint"] = gdf["rgint"].astype("Int64")
    return gpd.GeoDataFrame(gdf, geometry="geometry", crs=rgint.crs)


def grava_gpkg_unico(
    df: pd.DataFrame,
    resumo_rgint: pd.DataFrame,
    caminho_shapefile_municipios: Path,
    caminho_shapefile_rgint: Path,
    arquivo_saida: Path,
) -> None:
    """Grava um GeoPackage unico com municipios e limites de rgint."""
    arquivo_saida.parent.mkdir(parents=True, exist_ok=True)
    if arquivo_saida.exists():
        arquivo_saida.unlink()

    municipios_gdf = prepara_camada_municipios(df, caminho_shapefile_municipios)
    rgint_gdf = prepara_camada_rgint(resumo_rgint, caminho_shapefile_rgint)

    municipios_gdf.to_file(arquivo_saida, layer="municipios_fuzzy_v2", driver="GPKG")
    rgint_gdf.to_file(arquivo_saida, layer="rgint_fuzzy_v2", driver="GPKG", mode="a")


def main():
    args = parse_args()

    print("Carregando base municipal...")
    df = carrega_base(Path(args.base_municipal))

    print("Carregando vinculo territorial...")
    rgint_df = carrega_rgint_por_municipio(Path(args.shapefile))

    print("Vinculando municipios as regioes intermediarias...")
    df = df.merge(rgint_df, on="cod_mun", how="left")

    print("Identificando tipo de cada rgint...")
    df = identifica_tipo_rgint(df)

    print("Calculando indicadores derivados...")
    df = calcula_indicadores(df)

    print("Calculando scores fuzzy...")
    df = calcula_scores_fuzzy(df)

    print("Calculando eixos tematicos...")
    df = calcula_eixos(df)

    print("Calculando estatisticas por rgint...")
    estatisticas = calcula_estatisticas_rgint(df)

    print("Aplicando regras de inferencia fuzzy aprimoradas...")
    df = classifica_regras_aprimoradas(df, estatisticas)

    print("Detectando outliers...")
    df = detecta_outliers(df)

    print(f"Salvando classificacao em {args.output}...")
    df.to_csv(args.output, index=False, encoding="utf-8")

    print(f"Gerando resumo por rgint...")
    resumo_rgint = gera_resumo_rgint(df)
    resumo_rgint.to_csv(args.output_rgint, index=False, encoding="utf-8")

    print(f"Salvando arquivos por rgint...")
    grava_saidas_por_rgint(df, Path(args.output_dir_rgint))

    print(f"Gerando GeoPackage unico em {args.output_gpkg}...")
    grava_gpkg_unico(
        df=df,
        resumo_rgint=resumo_rgint,
        caminho_shapefile_municipios=Path(args.shapefile),
        caminho_shapefile_rgint=Path(args.shapefile_rgint),
        arquivo_saida=Path(args.output_gpkg),
    )

    # Estatisticas finais
    print("\n=== ESTATISTICAS FINAIS ===")
    print(f"Total de municipios: {len(df)}")
    print(f"Total de regioes intermediarias: {df['rgint'].nunique()}")
    print("\nDistribuicao por classe:")
    print(df["classificacao_fuzzy"].value_counts())
    print(f"\nMunicipios outliers: {df['e_outlier'].sum()}")
    print(f"Confianca media: {df['confianca_classificacao'].mean():.3f}")
    print(f"Confianca de dominancia media: {df['confianca_dominancia'].mean():.3f}")
    print("\nTipo de regioes:")
    print(df.groupby("tipo_rgint")["rgint"].nunique())

    print("\nProcessamento concluido!")


if __name__ == "__main__":
    main()
