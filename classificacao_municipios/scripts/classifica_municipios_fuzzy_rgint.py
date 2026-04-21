#!/usr/bin/env python3

"""
Classifica municipios com uma logica fuzzy orientada ao contexto das
133 regioes geograficas intermediarias.
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
ARQUIVO_MUNICIPIOS_PADRAO = (
    ROOT
    / "classificacao_municipios"
    / "processamento"
    / "classificacao_municipios_fuzzy_rgint.csv"
)
ARQUIVO_RGINT_PADRAO = (
    ROOT
    / "classificacao_municipios"
    / "processamento"
    / "classificacao_rgint_resumo_fuzzy.csv"
)
PASTA_RGINT_PADRAO = (
    ROOT
    / "classificacao_municipios"
    / "processamento"
    / "por_rgint"
)

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

PESOS_EIXOS = {
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
        description=(
            "Classifica municipios em logica fuzzy usando variaveis do "
            "merge_v26 e o contexto das 133 regioes intermediarias."
        )
    )
    parser.add_argument(
        "--base-municipal",
        default=str(BASE_MUNICIPAL_PADRAO),
        help="CSV municipal consolidado. Padrao: prata/processamento/merge_v26.csv",
    )
    parser.add_argument(
        "--shapefile",
        default=str(SHAPEFILE_PADRAO),
        help="Shapefile com o vinculo municipal para rgint.",
    )
    parser.add_argument(
        "--output-municipios",
        default=str(ARQUIVO_MUNICIPIOS_PADRAO),
        help="CSV de saida com a classificacao fuzzy por municipio.",
    )
    parser.add_argument(
        "--output-rgint",
        default=str(ARQUIVO_RGINT_PADRAO),
        help="CSV de saida com o resumo por regiao intermediaria.",
    )
    parser.add_argument(
        "--output-dir-rgint",
        default=str(PASTA_RGINT_PADRAO),
        help="Pasta onde sera gravado um CSV separado para cada rgint.",
    )
    return parser.parse_args()


def carrega_base(caminho: Path) -> pd.DataFrame:
    df = pd.read_csv(caminho, usecols=COLUNAS_BASE)
    df = df.copy()
    df["cod_mun"] = pd.to_numeric(df["cod_mun"], errors="coerce").astype("Int64")

    for coluna in [col for col in COLUNAS_BASE if col not in {"cod_mun", "municipio"}]:
        df[coluna] = pd.to_numeric(df[coluna], errors="coerce")

    if df["cod_mun"].isna().any():
        raise ValueError("A base municipal possui valores invalidos em 'cod_mun'.")
    return df


def carrega_rgint_por_municipio(caminho: Path) -> pd.DataFrame:
    gdf = gpd.read_file(caminho)[["CD_GEOCODI", "rgint", "nome_rgint", "UF"]].copy()
    gdf["cod_mun"] = pd.to_numeric(gdf["CD_GEOCODI"], errors="coerce").astype("Int64")
    gdf["rgint"] = pd.to_numeric(gdf["rgint"], errors="coerce").astype("Int64")
    gdf["uf"] = pd.to_numeric(gdf["UF"], errors="coerce").astype("Int64")
    gdf = gdf.drop(columns=["CD_GEOCODI", "UF"]).drop_duplicates(subset=["cod_mun"])

    if gdf["cod_mun"].isna().any():
        raise ValueError("O shapefile possui valores invalidos em 'cod_mun'.")
    return pd.DataFrame(gdf)


def clip_0_100(serie: pd.Series) -> pd.Series:
    return serie.clip(lower=0, upper=100)


def safe_div(numerador: pd.Series, denominador: pd.Series, multiplicador: float = 1.0) -> pd.Series:
    resultado = np.where(denominador > 0, multiplicador * numerador / denominador, np.nan)
    return pd.Series(resultado, index=numerador.index, dtype="float64")


def percent_rank(serie: pd.Series) -> pd.Series:
    serie = pd.to_numeric(serie, errors="coerce")
    return serie.rank(method="average", pct=True).fillna(0.5)


def score_intra_rgint(df: pd.DataFrame, coluna: str, ascending: bool = True) -> pd.Series:
    bruto = df.groupby("rgint", dropna=False)[coluna].transform(percent_rank)
    if not ascending:
        bruto = 1.0 - bruto
    return bruto.clip(lower=0.0, upper=1.0)


def weighted_mean(df: pd.DataFrame, pesos: dict[str, float]) -> pd.Series:
    colunas = list(pesos.keys())
    pesos_array = np.array([pesos[coluna] for coluna in colunas], dtype=float)
    matriz = df[colunas].to_numpy(dtype=float)
    mascara = np.isfinite(matriz)
    soma_pesos = (mascara * pesos_array).sum(axis=1)
    matriz_ajustada = np.where(mascara, matriz, 0.0)
    soma_ponderada = (matriz_ajustada * pesos_array).sum(axis=1)
    resultado = np.where(soma_pesos > 0, soma_ponderada / soma_pesos, np.nan)
    return pd.Series(resultado, index=df.index, dtype="float64")


def compoe_eixo(df: pd.DataFrame, pesos: dict[str, float]) -> pd.Series:
    base = pd.DataFrame({coluna: df[f"score_{coluna}"] for coluna in pesos})
    return weighted_mean(base, pesos)


def calcula_indicadores(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

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

    df["seca_normalizada"] = (
        df.groupby("rgint", dropna=False)["registros_seca_2003_2015"]
        .transform(percent_rank)
        .fillna(0.5)
    )
    return df


def calcula_scores_fuzzy(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for coluna in [
        "pib_pc",
        "empresas_1k",
        "regic_var56",
        "regic_var59",
        "regic_var60",
        "regic_var61",
        "regic_var66",
        "via_pav_pct",
        "calcada_pct",
        "ilum_pub_pct",
        "plano_diretor",
        "area_urb_densa_100k",
        "indice_conectividade",
        "cobertura_pop_4g5g",
        "densidade_scm",
        "fibra",
        "estab_saude_10k",
    ]:
        df[f"score_{coluna}"] = score_intra_rgint(df, coluna, ascending=True)

    for coluna in ["homicidios_100k", "demissoes_1k"]:
        df[f"score_{coluna}"] = score_intra_rgint(df, coluna, ascending=False)

    df["centralidade_economica"] = compoe_eixo(df, PESOS_EIXOS["centralidade_economica"])
    df["infraestrutura_urbana"] = compoe_eixo(df, PESOS_EIXOS["infraestrutura_urbana"])
    df["conectividade_digital"] = compoe_eixo(df, PESOS_EIXOS["conectividade_digital"])
    df["oferta_servicos"] = compoe_eixo(df, PESOS_EIXOS["oferta_servicos"])

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


def pertinencia_baixa(score: pd.Series) -> pd.Series:
    score = score.clip(0.0, 1.0)
    return np.where(score <= 0.25, 1.0, np.where(score >= 0.50, 0.0, (0.50 - score) / 0.25))


def pertinencia_media(score: pd.Series) -> pd.Series:
    score = score.clip(0.0, 1.0)
    subida = np.where(
        score <= 0.20,
        0.0,
        np.where(score < 0.50, (score - 0.20) / 0.30, 1.0),
    )
    descida = np.where(
        score <= 0.50,
        1.0,
        np.where(score < 0.80, (0.80 - score) / 0.30, 0.0),
    )
    return np.minimum(subida, descida)


def pertinencia_alta(score: pd.Series) -> pd.Series:
    score = score.clip(0.0, 1.0)
    return np.where(score <= 0.50, 0.0, np.where(score >= 0.75, 1.0, (score - 0.50) / 0.25))


def classifica_regras(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    central_alta = pertinencia_alta(df["centralidade_economica"])
    central_media = pertinencia_media(df["centralidade_economica"])
    central_baixa = pertinencia_baixa(df["centralidade_economica"])

    infra_alta = pertinencia_alta(df["infraestrutura_urbana"])
    infra_media = pertinencia_media(df["infraestrutura_urbana"])
    infra_baixa = pertinencia_baixa(df["infraestrutura_urbana"])

    conect_alta = pertinencia_alta(df["conectividade_digital"])
    conect_media = pertinencia_media(df["conectividade_digital"])
    conect_baixa = pertinencia_baixa(df["conectividade_digital"])

    serv_alta = pertinencia_alta(df["oferta_servicos"])
    serv_media = pertinencia_media(df["oferta_servicos"])
    serv_baixa = pertinencia_baixa(df["oferta_servicos"])

    vuln_alta = pertinencia_alta(df["vulnerabilidade"])
    vuln_media = pertinencia_media(df["vulnerabilidade"])
    vuln_baixa = pertinencia_baixa(df["vulnerabilidade"])

    df["pert_muito_alto"] = np.minimum.reduce(
        [central_alta, serv_alta, conect_alta]
    )
    df["pert_alto"] = weighted_mean(
        pd.DataFrame(
            {
                "central_alta": central_alta,
                "serv_media_alta": np.maximum(serv_media, serv_alta),
                "conect_media_alta": np.maximum(conect_media, conect_alta),
                "infra_media_alta": np.maximum(infra_media, infra_alta),
            }
        ),
        {
            "central_alta": 0.35,
            "serv_media_alta": 0.25,
            "conect_media_alta": 0.20,
            "infra_media_alta": 0.20,
        },
    )
    df["pert_medio"] = weighted_mean(
        pd.DataFrame(
            {
                "infra_alta": infra_alta,
                "conect_media_alta": np.maximum(conect_media, conect_alta),
                "central_media": central_media,
                "vuln_baixa_media": np.maximum(vuln_baixa, vuln_media),
            }
        ),
        {
            "infra_alta": 0.35,
            "conect_media_alta": 0.25,
            "central_media": 0.20,
            "vuln_baixa_media": 0.20,
        },
    )
    df["pert_baixo"] = weighted_mean(
        pd.DataFrame(
            {
                "central_baixa_media": np.maximum(central_baixa, central_media),
                "serv_baixa_media": np.maximum(serv_baixa, serv_media),
                "infra_media": infra_media,
                "conect_media": conect_media,
                "vuln_baixa_media": np.maximum(vuln_baixa, vuln_media),
            }
        ),
        {
            "central_baixa_media": 0.25,
            "serv_baixa_media": 0.25,
            "infra_media": 0.20,
            "conect_media": 0.15,
            "vuln_baixa_media": 0.15,
        },
    )
    df["pert_muito_baixo"] = weighted_mean(
        pd.DataFrame(
            {
                "vuln_alta": vuln_alta,
                "infra_baixa": infra_baixa,
                "conect_baixa": conect_baixa,
                "central_baixa": central_baixa,
            }
        ),
        {
            "vuln_alta": 0.35,
            "infra_baixa": 0.25,
            "conect_baixa": 0.20,
            "central_baixa": 0.20,
        },
    )

    pertinencias = df[COLUNAS_PERTINENCIA]
    df["classificacao_fuzzy"] = pertinencias.idxmax(axis=1).str.removeprefix("pert_")
    df["confianca_classificacao"] = pertinencias.max(axis=1)
    return df


def gera_resumo_rgint(df: pd.DataFrame) -> pd.DataFrame:
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

    dominante = (
        resumo_classes.sort_values(
            ["rgint", "participacao", "qtd_municipios", "classificacao_fuzzy"],
            ascending=[True, False, False, True],
        )
        .drop_duplicates(subset=["rgint"], keep="first")
        .rename(
            columns={
                "classificacao_fuzzy": "classificacao_dominante_rgint",
                "participacao": "participacao_dominante_rgint",
            }
        )
    )

    eixos_medios = (
        df.groupby(["rgint", "nome_rgint"], dropna=False)[COLUNAS_SAIDA_EIXOS]
        .mean()
        .reset_index()
    )

    return eixos_medios.merge(
        dominante[
            [
                "rgint",
                "nome_rgint",
                "classificacao_dominante_rgint",
                "participacao_dominante_rgint",
            ]
        ],
        on=["rgint", "nome_rgint"],
        how="left",
    )


def slug_nome_rgint(nome: str) -> str:
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
    pasta_saida.mkdir(parents=True, exist_ok=True)
    for (rgint, nome_rgint), grupo in df.groupby(["rgint", "nome_rgint"], dropna=False, sort=True):
        arquivo = pasta_saida / f"{int(rgint)}_{slug_nome_rgint(nome_rgint)}.csv"
        grupo.sort_values(
            ["classificacao_fuzzy", "confianca_classificacao", "municipio"],
            ascending=[True, False, True],
        ).to_csv(arquivo, index=False)


def main() -> None:
    args = parse_args()

    base = carrega_base(Path(args.base_municipal).resolve())
    rgint = carrega_rgint_por_municipio(Path(args.shapefile).resolve())

    df = base.merge(rgint, on="cod_mun", how="left", validate="one_to_one")
    if df["rgint"].isna().any():
        faltantes = int(df["rgint"].isna().sum())
        raise ValueError(f"{faltantes} municipios ficaram sem vinculo com regiao intermediaria.")

    df = calcula_indicadores(df)
    df = calcula_scores_fuzzy(df)
    df = classifica_regras(df)

    df["rgint"] = df["rgint"].astype("Int64")
    df["uf"] = df["uf"].astype("Int64")

    saida_municipios = Path(args.output_municipios).resolve()
    saida_rgint = Path(args.output_rgint).resolve()
    pasta_saida_rgint = Path(args.output_dir_rgint).resolve()
    saida_municipios.parent.mkdir(parents=True, exist_ok=True)
    saida_rgint.parent.mkdir(parents=True, exist_ok=True)
    pasta_saida_rgint.mkdir(parents=True, exist_ok=True)

    colunas_saida = [
        "cod_mun",
        "municipio",
        "uf",
        "rgint",
        "nome_rgint",
        "classificacao_fuzzy",
        "confianca_classificacao",
        *COLUNAS_SAIDA_EIXOS,
        "pib_pc",
        "empresas_1k",
        "estab_saude_10k",
        "homicidios_100k",
        "demissoes_1k",
        "area_urb_densa_100k",
        *COLUNAS_PERTINENCIA,
    ]
    df[colunas_saida].to_csv(saida_municipios, index=False)
    grava_saidas_por_rgint(df[colunas_saida], pasta_saida_rgint)

    resumo_rgint = gera_resumo_rgint(df)
    resumo_rgint.to_csv(saida_rgint, index=False)

    print(f"Arquivo gerado: {saida_municipios}")
    print(f"Arquivo gerado: {saida_rgint}")
    print(f"Pasta gerada com arquivos separados por rgint: {pasta_saida_rgint}")
    print("Distribuicao final das classes:")
    print(df["classificacao_fuzzy"].value_counts(dropna=False).to_string())


if __name__ == "__main__":
    main()
